"""
BRAX SMART DESK v2 — Real-time multi-asset analysis engine
Data spine: Binance WebSocket (BTC, PAXG) + TwelveData poll (XAU/USD)
Run: python brax_desk.py  (Render: gunicorn not needed, Flask thread keeps port open)
"""
import asyncio, os, json, time, math, random, logging
from collections import deque
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional
import numpy as np
import pandas as pd
import aiohttp
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from gtts import gTTS
import pytz
from flask import Flask
from threading import Thread

# ---------------------------------------------------------------- CONFIG
logging.basicConfig(level=logging.INFO,
    format='%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger("BRAX")

TOKEN     = os.getenv("TELEGRAM_TOKEN")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
TD_KEY    = os.getenv("TWELVEDATA_API_KEY")
SELF_URL  = os.getenv("RENDER_EXTERNAL_URL", "")   # keeps free tier awake
DESK_INTERVAL   = 15 * 60     # full desk update cadence
TICK_INTERVAL   = 30          # live engine tick
GOLD_POLL       = 120         # TwelveData poll seconds (fits 800/day)
EAT = pytz.timezone("Africa/Nairobi")

for v in (TOKEN, CHAT_ID, TD_KEY):
    if not v: raise ValueError("Missing TELEGRAM_TOKEN / TELEGRAM_CHAT_ID / TWELVEDATA_API_KEY")

# ---------------------------------------------------------------- SESSIONS
def session_now() -> tuple[str, bool]:
    h = datetime.now(EAT).hour + datetime.now(EAT).minute / 60
    if 13 <= h < 17:   return "NY KILLZONE", True
    if 17 <= h < 21:   return "NY AFTERNOON", False
    if 8 <= h < 13:    return "LONDON", h < 10        # London open = killzone-ish
    if 2 <= h < 8:     return "ASIAN", False
    return "OFF-HOURS", False

def now_eat(): return datetime.now(EAT)

# ---------------------------------------------------------------- INDICATORS
def rsi(close: pd.Series, n=14) -> float:
    d = close.diff()
    up = d.clip(lower=0).ewm(alpha=1/n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1/n, adjust=False).mean()
    rs = up / dn.replace(0, np.nan)
    out = (100 - 100 / (1 + rs)).iloc[-1]
    return float(out) if not np.isnan(out) else 50.0

def atr(df: pd.DataFrame, n=14) -> float:
    tr = pd.concat([df.h - df.l, (df.h - df.c.shift()).abs(),
                    (df.l - df.c.shift()).abs()], axis=1).max(axis=1)
    return float(tr.ewm(alpha=1/n, adjust=False).mean().iloc[-1])

def ema_last(s: pd.Series, n) -> float:
    return float(s.ewm(span=n, adjust=False).mean().iloc[-1])

def adx(df: pd.DataFrame, n=14) -> float:
    up, dn = df.h.diff(), -df.l.diff()
    plus  = pd.Series(np.where((up > dn) & (up > 0), up, 0.0), index=df.index)
    minus = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0), index=df.index)
    tr = pd.concat([df.h - df.l, (df.h - df.c.shift()).abs(),
                    (df.l - df.c.shift()).abs()], axis=1).max(axis=1)
    atr_ = tr.ewm(alpha=1/n, adjust=False).mean().replace(0, np.nan)
    pdi = 100 * plus.ewm(alpha=1/n, adjust=False).mean() / atr_
    mdi = 100 * minus.ewm(alpha=1/n, adjust=False).mean() / atr_
    dx = ((pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan) * 100)
    v = dx.ewm(alpha=1/n, adjust=False).mean().iloc[-1]
    return float(v) if not np.isnan(v) else 0.0

def swings(df, k=3):
    sh, sl, h, l = [], [], df.h.values, df.l.values
    for i in range(k, len(df) - k):
        if h[i] >= h[i-k:i+k+1].max(): sh.append((i, float(h[i])))
        if l[i] <= l[i-k:i+k+1].min(): sl.append((i, float(l[i])))
    return sh, sl

def detect_fvg(df, a: float):
    for i in range(len(df) - 1, max(2, len(df) - 6), -1):
        if df.l.iloc[i] > df.h.iloc[i-2] and df.l.iloc[i] - df.h.iloc[i-2] > 0.1 * a:
            return True, "BULLISH", float((df.l.iloc[i] + df.h.iloc[i-2]) / 2)
        if df.h.iloc[i] < df.l.iloc[i-2] and df.l.iloc[i-2] - df.h.iloc[i] > 0.1 * a:
            return True, "BEARISH", float((df.h.iloc[i] + df.l.iloc[i-2]) / 2)
    return False, "NONE", 0.0

def detect_bos(df, k=3):
    sh, sl = swings(df, k)
    if len(df) < 10 or not sh or not sl:
        return "NONE", None, None
    last_sh = [p for i, p in sh if i < len(df) - 1]
    last_sl = [p for i, p in sl if i < len(df) - 1]
    c = float(df.c.iloc[-1])
    sh_list = [p for _, p in sh][-3:]; sl_list = [p for _, p in sl][-3:]
    if last_sh and c > max(sh_list): return "UP", max(sh_list), min(sl_list) if sl_list else None
    if last_sl and c < min(sl_list): return "DOWN", max(sh_list) if sh_list else None, min(sl_list)
    return "RANGE", max(sh_list) if sh_list else None, min(sl_list) if sl_list else None

def psych_level(price, name):
    step = 1000 if "BTC" in name else (5 if "GOLD" in name else 1)
    return round(price / step) * step

# ---------------------------------------------------------------- CANDLE STORE
class CandleStore:
    def __init__(self, name, ws_sym=None, td_sym=None):
        self.name, self.ws_sym, self.td_sym = name, ws_sym, td_sym
        self._c = {}                       # minute_epoch -> [o,h,l,c,v]
        self._df, self._df_ts = None, 0
        self.price, self.day_open = 0.0, None
        self.cvd_ticks = deque(maxlen=20000)   # (ts, signed_vol)
        self.last20 = deque(maxlen=20)         # True=bull print
        self.last_update = 0

    def ingest_kline(self, k: dict):
        t = int(k["t"]) // 60000
        o, h, l, c, v = (float(k["o"]), float(k["h"]), float(k["l"]),
                         float(k["c"]), float(k["v"]))
        self._c[t] = [o, h, l, c, v]
        self.price = c
        self.last_update = time.time()
        self._trim(); self._df = None
        utc_today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).timestamp() // 60
        todays = [cd[0] for tt, cd in self._c.items() if tt >= utc_today]
        if todays: self.day_open = todays[0]

    def ingest_trade(self, t: dict):
        p, q = float(t["p"]), float(t["q"])
        bull = not t["m"]                      # maker=True => sell aggressor
        self.cvd_ticks.append((t["T"] / 1000, q if bull else -q))
        self.last20.append(bull)
        self.price = p; self.last_update = time.time()
        self._df = None  # live candle changes; cheap enough to rebuild

    def ingest_td(self, values: list):         # TwelveData 1min, newest-first
        for r in values:
            try:
                ts = int(datetime.strptime(r["datetime"], "%Y-%m-%d %H:%M:%S")
                         .replace(tzinfo=pytz.utc).timestamp() // 60)
                self._c[ts] = [float(r["open"]), float(r["high"]),
                               float(r["low"]), float(r["close"]),
                               float(r.get("volume") or 0)]
            except Exception:
                continue
        self.price = float(values[0]["close"])
        self.last_update = time.time(); self._df = None

    def _trim(self):
        cutoff = time.time() - 3 * 86400
        for k in [k for k in self._c if k < cutoff]: del self._c[k]

    def df(self, rule="1min", bars=None) -> pd.DataFrame:
        if not self._c: return pd.DataFrame()
        if self._df is None or time.time() - self._df_ts > 3:
            items = sorted(self._c.items())
            idx = pd.to_datetime([i[0] * 60 for i in items], unit="s", utc=True)
            self._df = pd.DataFrame([i[1] for i in items], index=idx,
                                    columns=["o", "h", "l", "c", "v"])
            self._df_ts = time.time()
        df = self._df
        if rule != "1min":
            df = df.resample(rule).agg({"o": "first", "h": "max", "l": "min",
                                        "c": "last", "v": "sum"}).dropna()
        return df.tail(bars) if bars else df

    def cvd(self, window=1800) -> float:
        cut = time.time() - window
        return sum(v for ts, v in self.cvd_ticks if ts >= cut)

# ---------------------------------------------------------------- FEEDS
BINANCE_HOSTS = ["wss://data-stream.binance.vision/stream?streams=",
                 "wss://stream.binance.com:9443/stream?streams="]

async def binance_worker(stores: list[CandleStore]):
    streams = []
    for s in stores:
        if s.ws_sym:
            streams += [f"{s.ws_sym}@kline_1m", f"{s.ws_sym}@aggTrade"]
    if not streams: return
    backoff = 5
    async with aiohttp.ClientSession() as ses:
        while True:
            host = random.choice(BINANCE_HOSTS)
            url = host + "/".join(streams)
            try:
                async with ses.ws_connect(url, heartbeat=25) as ws:
                    log.info(f"WS connected: {host}")
                    backoff = 5
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT: continue
                        d = json.loads(msg.data).get("data", {})
                        sym = d.get("s", "").lower()
                        st = next((x for x in stores if x.ws_sym == sym), None)
                        if not st: continue
                        if d.get("e") == "kline":
                            st.ingest_kline(d["k"])
                        elif d.get("e") == "aggTrade":
                            st.ingest_trade(d)
            except Exception as e:
                log.warning(f"WS down ({e}) — retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120)

async def gold_worker(gold: CandleStore, ses: aiohttp.ClientSession):
    while True:
        try:
            async with ses.get(f"https://api.twelvedata.com/time_series"
                               f"?symbol=XAU/USD&interval=1min&outputsize=60&apikey={TD_KEY}") as r:
                d = await r.json()
            if d.get("values"):
                gold.ingest_td(d["values"])
                log.info(f"GOLD synced ${gold.price:.2f}")
            else:
                async with ses.get("https://data-asg.goldprice.org/dbXRates/USD") as r2:
                    gp = (await r2.json())["items"][0]["xauPrice"]
                if gold.price <= 0 or abs(gp - gold.price) > 0.5:
                    gold.price = float(gp); gold.last_update = time.time()
                log.info(f"GOLD via goldprice.org ${gp:.2f}")
        except Exception as e:
            log.error(f"Gold feed: {e}")
        await asyncio.sleep(GOLD_POLL)

# ---------------------------------------------------------------- ANALYSIS ENGINE
def analyze(store: CandleStore, proxy: Optional[CandleStore] = None,
            corr: float = 0.0) -> dict:
    A = {"store": store, "name": store.name, "price": store.price, "full": False}
    df15 = store.df("15min", 96)
    if len(df15) < 20: return A
    A["full"] = True
    a = atr(df15)
    a50 = float(pd.Series([atr(df15.iloc[max(0, i-50):i]) for i in
                           range(max(14, len(df15)-10), len(df15))]).mean()) if len(df15) > 60 else a
    A["atr"], A["atr_state"] = a, ("EXPANDING" if a > a50 * 1.05 else
                                   "CONTRACTING" if a < a50 * 0.9 else "STABLE")
    A["rsi"] = rsi(df15.c)
    adx_v = adx(df15)
    A["adx"] = adx_v
    A["regime"] = ("TRENDING" if adx_v >= 25 else
                   "RANGING" if adx_v >= 20 else "CHOPPY")
    A["tradeable"] = adx_v >= 22

    # multi-horizon
    trends, bull_n = {}, 0
    for tf, rule in (("H1", "1h"), ("M15", "15min"), ("M5", "5min"), ("M1", "1min")):
        d = store.df(rule, 120)
        if len(d) < 25: trends[tf] = ("FLAT", 0); continue
        e9, e21 = ema_last(d.c, 9), ema_last(d.c, 21)
        c = float(d.c.iloc[-1])
        t = "BULL" if e9 > e21 and c > e21 else "BEAR" if e9 < e21 and c < e21 else "FLAT"
        trends[tf] = (t, round(abs(e9 - e21) / a, 2) if a else 0)
        if t == "BULL": bull_n += 1
        elif t == "BEAR": bull_n -= 1
    A["trends"] = trends
    A["align"] = "BULL" if bull_n >= 3 else "BEAR" if bull_n <= -3 else "MIXED"
    A["strength"] = min(3, abs(bull_n))

    # structure
    bos, sh, sl = detect_bos(df15)
    fvg, fvg_t, fvg_lv = detect_fvg(df15, a)
    A["bos"], A["sh"], A["sl"] = bos, sh, sl
    A["fvg"], A["fvg_t"], A["fvg_lv"] = fvg, fvg_t, fvg_lv

    # premium / discount (24h range)
    hi, lo = float(df15.h.max()), float(df15.l.min())
    eq = (hi + lo) / 2
    A.update(prem=hi - 0.25 * (hi - lo), disc=lo + 0.25 * (hi - lo), eq=eq,
             zpos=("PREMIUM" if A["price"] > eq + 0.1 * (hi - lo) else
                   "DISCOUNT" if A["price"] < eq - 0.1 * (hi - lo) else "EQUILIBRIUM"))

    # liquidity map: swing levels above/below price + psych
    sh_p = sorted({p for _, p in swings(df15, 3)[0] if p > A["price"]})
    sl_p = sorted({p for _, p in swings(df15, 3)[1] if p < A["price"]}, reverse=True)
    psy = psych_level(A["price"], A["name"])
    if psy > A["price"]: sh_p.append(float(psy))
    elif psy < A["price"]: sl_p.append(float(psy))
    A["bsl"], A["bsl2"] = (sh_p[0], sh_p[1]) if len(sh_p) > 1 else (sh_p[0] if sh_p else hi, None)
    A["ssl"], A["ssl2"] = (sl_p[0], sl_p[1]) if len(sl_p) > 1 else (sl_p[0] if sl_p else lo, None)
    d5 = store.df("5min", 6)
    A["sweep_bsl"] = A["sweep_ssl"] = False
    if len(d5) >= 2:
        lc = d5.iloc[-2]
        if A["bsl"] and lc.h > A["bsl"] and lc.c < A["bsl"]: A["sweep_bsl"] = True
        if A["ssl"] and lc.l < A["ssl"] and lc.c > A["ssl"]: A["sweep_ssl"] = True

    # order flow (PAXG proxy for gold)
    flow_store = store if store.cvd_ticks else proxy
    if flow_store and flow_store.cvd_ticks:
        cvd_now = flow_store.cvd(1800)
        cvd_ago = sum(v for ts, v in flow_store.cvd_ticks
                      if time.time() - 3600 <= ts <= time.time() - 1800)
        px = flow_store.price
        px_ago = flow_store.df("5min", 8).c.iloc[0] if len(flow_store.df("5min", 8)) else px
        A["bull_div"] = px > px_ago and cvd_now < cvd_ago     # price up, flow down
        A["bear_div"] = px < px_ago and cvd_now > cvd_ago     # price down, flow up
        A["cvd"], A["cvd_ago"] = cvd_now, cvd_ago
        bulls = sum(1 for b in flow_store.last20 if b)
        A["bull_n"], A["bear_n"] = bulls, 20 - bulls
        A["flow_msg"] = ("Bullish — buyers lifting offers" if cvd_now > 0 and not A["bear_div"]
                         else "Bearish — sellers hitting bids" if cvd_now < 0 and not A["bull_div"]
                         else "Divergence — flow vs price disagree, caution")
    else:
        A["cvd"] = 0.0; A["bull_div"] = A["bear_div"] = False
        A["bull_n"] = A["bear_n"] = 0
        A["flow_msg"] = "Flow proxy unavailable"

    A["corr"] = corr
    A["sess"], A["killzone"] = session_now()
    A["day_open"] = store.day_open or A["price"]
    return A

# ---------------------------------------------------------------- CONFLUENCE ("AI SWARM" — 4 weighted voters)
def swarm(A: dict):
    trend_v = "BUY" if A["align"] == "BULL" else "SELL" if A["align"] == "BEAR" else "WAIT"
    struct_v = ("BUY" if (A["bos"] == "UP" or (A["fvg"] and A["fvg_t"] == "BULLISH"))
                else "SELL" if (A["bos"] == "DOWN" or (A["fvg"] and A["fvg_t"] == "BEARISH"))
                else "WAIT")
    flow_v = ("WAIT" if A.get("bear_div") and True else
              "BUY" if A.get("cvd", 0) > 0 and not A.get("bear_div")
              else "SELL" if A.get("cvd", 0) < 0 and not A.get("bull_div") else "WAIT")
    zone_v = ("BUY" if A["sweep_ssl"] or A["zpos"] == "DISCOUNT"
              else "SELL" if A["sweep_bsl"] or A["zpos"] == "PREMIUM" else "WAIT")
    votes = {"TREND": trend_v, "STRUCT": struct_v, "FLOW": flow_v, "ZONE": zone_v}
    buys, sells = sum(1 for v in votes.values() if v == "BUY"), sum(1 for v in votes.values() if v == "SELL")
    if buys >= 3 and sells == 0:   d, conf = "BUY", buys / 4 * 100
    elif sells >= 3 and buys == 0: d, conf = "SELL", sells / 4 * 100
    elif buys >= 3 and sells == 1: d, conf = "BUY", 68
    elif sells >= 3 and buys == 1: d, conf = "SELL", 68
    else:                          d, conf = "WAIT", max(buys, sells) / 4 * 100
    if d != "WAIT" and not A["tradeable"]: d, conf = "WAIT", conf * 0.8
    return d, round(conf), votes

# ---------------------------------------------------------------- SIGNAL + NARRATIVE
def build_levels(A, d):
    a, p = A["atr"], A["price"]
    if d == "BUY":
        entry = p - 0.1 * a
        sl = min(A["ssl"] or entry - 1.3 * a, entry - 0.8 * a) - 0.25 * a
        t1 = A["bsl"] or entry + 1.5 * a
        t2 = A["bsl2"] or entry + 3 * a
    elif d == "SELL":
        entry = p + 0.1 * a
        sl = max(A["bsl"] or entry + 1.3 * a, entry + 0.8 * a) + 0.25 * a
        t1 = A["ssl"] or entry - 1.5 * a
        t2 = A["ssl2"] or entry - 3 * a
    else:
        entry = sl = t1 = t2 = p
    return entry, sl, t1, t2

def fp(x, name):  # format price
    return f"${x:,.0f}" if "BTC" in name else f"${x:,.2f}"

def narrative(A: dict, d, conf, votes, audit_str: str) -> str:
    n, p = A["name"], A["price"]
    e, sl, t1, t2 = build_levels(A, d)
    sgn = "BULLISH" if d == "BUY" else "BEARISH" if d == "SELL" else "NEUTRAL"
    L = [f"<b>WHAT {n} WILL DO NEXT — LIVE ENGINE</b>",
         f"<code>{now_eat().strftime('%H:%M EAT')} | {A['sess']}{' ⚡KILLZONE' if A['killzone'] else ''}</code>", ""]
    L.append(f"<b>SHORT TERM (next 1–3h):</b>")
    if d == "BUY":
        L.append(f"- <b>{sgn}</b> — holding {fp(A['disc'],n)} discount zone, structure {A['bos']}")
        L.append(f"- Target up: <b>{fp(t1,n)}</b> (BSL). If breaks = <b>{fp(t2,n)}</b> next pool")
        L.append(f"- Invalidation: <b>{fp(sl,n)}</b>. If loses = {fp(A['ssl2'] or sl-2*A['atr'],n)} liquidity below")
    elif d == "SELL":
        L.append(f"- <b>{sgn}</b> — rejected {fp(A['prem'],n)} premium zone, structure {A['bos']}")
        L.append(f"- Target down: <b>{fp(t1,n)}</b> (SSL). If breaks = <b>{fp(t2,n)}</b> next pool")
        L.append(f"- Invalidation: <b>{fp(sl,n)}</b>. If reclaims = {fp(A['bsl2'] or sl+2*A['atr'],n)} above")
    else:
        L.append(f"- <b>MIXED</b> — chop expected around {fp(p,n)}")
        L.append(f"- Range: {fp(A['eq']-0.5*A['atr'],n)} – {fp(A['eq']+0.5*A['atr'],n)}. No trade until voters align")
    L.append("")
    L.append("<b>WHY:</b>")
    L.append(f"- Structure: BOS <b>{A['bos']}</b>{f', {A['fvg_t']} FVG at {fp(A['fvg_lv'],n)}' if A['fvg'] else ''}")
    L.append(f"- Sweep: {'SSL swept → bullish' if A['sweep_ssl'] else 'BSL swept → bearish' if A['sweep_bsl'] else 'none in last 30m'}")
    L.append(f"- Flow: CVD <b>{A['cvd']:+,.0f}</b>, prints {A['bull_n']}bull/{A['bear_n']}bear — {A['flow_msg']}")
    if A.get("bull_div"): L.append("- ⚠️ <b>Bullish CVD divergence</b> — price low, buyers absorbing")
    if A.get("bear_div"): L.append("- ⚠️ <b>Bearish CVD divergence</b> — price high, sellers absorbing")
    L.append(f"- Volatility: ATR {A['atr']:.2f} {A['atr_state']} | Regime: <b>{A['regime']}</b> ({'tradeable' if A['tradeable'] else 'stand aside'})")
    L.append(f"- Zones: {A['zpos']} | Prem {fp(A['prem'],n)} / Eq {fp(A['eq'],n)} / Disc {fp(A['disc'],n)}")
    if A.get("corr") and abs(A["corr"]) > 0.4:
        L.append(f"- Correlation: Gold↔BTC r={A['corr']:.2f} (24h) — "
                 f"{'moving together' if A['corr'] > 0 else 'diverging'}")
    L.append("")
    L.append("<b>MEDIUM (rest of day):</b>")
    if d == "BUY":
        L.append(f"- Holds above {fp(A['eq'],n)} = premium visit {fp(A['prem'],n)}")
        L.append(f"- Loses {fp(A['disc'],n)} = full range flush to {fp(A['ssl2'] or A['disc']-2*A['atr'],n)}")
    elif d == "SELL":
        L.append(f"- Rejected at prem = discount visit {fp(A['disc'],n)}")
        L.append(f"- Reclaims {fp(A['prem'],n)} = continuation to {fp(A['bsl2'] or A['prem']+2*A['atr'],n)}")
    else:
        L.append(f"- Above {fp(A['eq'],n)} favors bulls → {fp(A['prem'],n)}; below favors bears → {fp(A['disc'],n)}")
    L.append("")
    L.append(f"<b>DESK:</b> {d} — voters {votes['TREND']}/{votes['STRUCT']}/{votes['FLOW']}/{votes['ZONE']} — conf <b>{conf}%</b>")
    if d != "WAIT":
        L.append(f"Entry {fp(e,n)} | SL {fp(sl,n)} | T1 {fp(t1,n)} | T2 {fp(t2,n)}")
    L.append(f"<code>{audit_str} • not financial advice • live {TICK_INTERVAL}s tick</code>")
    return "\n".join(L)

# ---------------------------------------------------------------- PREDICTION LEDGER (self-audit)
class Ledger:
    def __init__(self): self.open, self.results = [], deque(maxlen=200)
    def add(self, name, direction, target, invalid, hours=6):
        self.open.append({"name": name, "dir": direction, "target": target,
                          "invalid": invalid, "exp": time.time() + hours * 3600,
                          "made": now_eat().strftime("%H:%M")})
    def check(self, price_by_name):
        done = []
        for pr in self.open:
            p = price_by_name.get(pr["name"], 0)
            if not p: continue
            if pr["dir"] == "BUY":
                if p >= pr["target"]: self.results.append(True); done.append((pr, "HIT ✓"))
                elif p <= pr["invalid"]: self.results.append(False); done.append((pr, "INVALIDATED ✗"))
            else:
                if p <= pr["target"]: self.results.append(True); done.append((pr, "HIT ✓"))
                elif p >= pr["invalid"]: self.results.append(False); done.append((pr, "INVALIDATED ✗"))
        self.open = [p for p in self.open if p not in [d[0] for d in done]]
        return done
    def audit_str(self):
        recent = list(self.results)[-20:]
        if not recent: return "SELF-AUDIT: building track record (0 scored yet)"
        return f"SELF-AUDIT: {sum(recent)}/{len(recent)} predictions correct ({sum(recent)/len(recent)*100:.0f}%)"

# ---------------------------------------------------------------- ACTIVE SIGNAL TRACKER
class Tracker:
    def __init__(self): self.active = {}   # name -> dict
    def arm(self, A, d, conf, entry, sl, t1, t2):
        self.active[A["name"]] = {"d": d, "conf": conf, "entry": entry, "sl": sl,
                                  "t1": t1, "t2": t2, "t1_hit": False,
                                  "opened": now_eat().strftime("%H:%M")}
    def check(self, A) -> list[str]:
        s = self.active.get(A["name"]); out = []
        if not s or not A["full"]: return out
        p = A["price"]
        if s["d"] == "BUY":
            if not s["t1_hit"] and p >= s["t1"]:
                s["t1_hit"] = True
                out.append(f"🎯 {A['name']} TP1 HIT at {fp(p, A['name'])} — stop moved to entry. T2 {fp(s['t2'], A['name'])} is live.")
            elif p >= s["t2"]:
                out.append(f"🏁 {A['name']} TP2 SMASHED at {fp(p, A['name'])}. Full target. Desk closes tracking.")
                self.active.pop(A["name"], None)
            elif p <= s["sl"]:
                out.append(f"🛑 {A['name']} STOPPED at {fp(p, A['name'])}. Structure invalidated — regrouping.")
                self.active.pop(A["name"], None)
        else:
            if not s["t1_hit"] and p <= s["t1"]:
                s["t1_hit"] = True
                out.append(f"🎯 {A['name']} TP1 HIT at {fp(p, A['name'])} — stop moved to entry. T2 {fp(s['t2'], A['name'])} is live.")
            elif p <= s["t2"]:
                out.append(f"🏁 {A['name']} TP2 SMASHED at {fp(p, A['name'])}. Full target.")
                self.active.pop(A["name"], None)
            elif p >= s["sl"]:
                out.append(f"🛑 {A['name']} STOPPED at {fp(p, A['name'])}. Structure invalidated.")
                self.active.pop(A["name"], None)
        return out

# ---------------------------------------------------------------- CHART
def make_chart(A, d, entry, sl, t1, t2, path):
    df = A["store"].df("15min", 80)
    fig, ax = plt.subplots(figsize=(11, 6), dpi=100)
    fig.patch.set_facecolor("#131722"); ax.set_facecolor("#131722")
    up, dn = "#26a69a", "#ef5350"
    for i, (_, r) in enumerate(df.iterrows()):
        c = up if r.c >= r.o else dn
        ax.vlines(i, r.l, r.h, color=c, lw=1)
        ax.bar(i, r.c - r.o, bottom=r.o, width=0.6, color=c,
               edgecolor=c, zorder=3)
    n = A["name"]; lines = [(entry, "#ffd54f", "-", "ENTRY"), (sl, "#ef5350", "--", "SL"),
                            (t1, "#26a69a", "--", "T1"), (t2, "#26a69a", "--", "T2")]
    for lv, col, ls, lab in lines:
        if lv and lv != A["price"]: ax.axhline(lv, color=col, ls=ls, lw=1.4, label=lab)
    for lv, col in ((A["bsl"], "#8d6e63"), (A["ssl"], "#8d6e63")):
        if lv: ax.axhline(lv, color=col, ls=":", lw=1.2)
    ax.set_title(f"{n} 15m — {d} conf {d and ''}", color="white", fontsize=13)
    ax.set_title(f"{n} 15m — {d} | {A['regime']} | {A['sess']}", color="white", fontsize=12)
    ax.tick_params(colors="white"); [s.set_color("#444") for s in ax.spines.values()]
    leg = ax.legend(loc="upper left", fontsize=8, facecolor="#1e222d",
                    edgecolor="#444", labelcolor="white")
    plt.tight_layout(); plt.savefig(path, facecolor="#131722"); plt.close(fig)

# ---------------------------------------------------------------- TELEGRAM
class TG:
    def __init__(self): self.base = f"https://api.telegram.org/bot{TOKEN}"
    async def text(self, ses, msg):
        try:
            async with ses.post(f"{self.base}/sendMessage",
                                json={"chat_id": CHAT_ID, "text": msg,
                                      "parse_mode": "HTML", "disable_web_page_preview": True}):
                pass
        except Exception as e: log.error(f"TG text: {e}")
    async def photo(self, ses, path, caption=""):
        try:
            with open(path, "rb") as f:
                fd = aiohttp.FormData()
                fd.add_field("chat_id", CHAT_ID); fd.add_field("caption", caption[:1000])
                fd.add_field("photo", f, filename="chart.png")
                async with ses.post(f"{self.base}/sendPhoto", data=fd): pass
        except Exception as e: log.error(f"TG photo: {e}")
    async def voice(self, ses, path):
        try:
            with open(path, "rb") as f:
                fd = aiohttp.FormData()
                fd.add_field("chat_id", CHAT_ID); fd.add_field("voice", f, filename="b.mp3")
                async with ses.post(f"{self.base}/sendVoice", data=fd, timeout=aiohttp.ClientTimeout(total=60)): pass
        except Exception as e: log.error(f"TG voice: {e}")

async def voice_note(ses, tg, text):
    path = "/tmp/v.mp3"
    try:
        await asyncio.to_thread(lambda: gTTS(text=text, lang="en").save(path))
        await tg.voice(ses, path)
    except Exception as e:
        log.error(f"voice: {e}")
    finally:
        if os.path.exists(path): os.remove(path)

# ---------------------------------------------------------------- CORE
