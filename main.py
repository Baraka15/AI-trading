"""
BRAX SMART DESK v2 — Real-time multi-asset analysis engine
===========================================================
Data spine:  Binance WebSocket (BTC, PAXG)  +  TwelveData poll (XAU/USD)  +  goldprice.org backup
Modules:     Multi-horizon trends • Session macro • Premium/Discount • Liquidity map (BSL/SSL + sweeps)
             CVD order flow • BOS/FVG structure • Volatility engine • Regime detection • Confluence swarm
             T1/T2/SL tracker • Self-auditing prediction ledger • Correlation engine
             Autonomous day outlook • Voice briefings • TradingView-style charts
Deploy:      Render → Start Command: python main.py
Env vars:    TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, TWELVEDATA_API_KEY (RENDER_EXTERNAL_URL is auto-set)
"""
import asyncio, os, json, time, random, logging
from collections import deque
from datetime import datetime
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
SELF_URL  = os.getenv("RENDER_EXTERNAL_URL", "")   # keeps Render free tier awake

DESK_INTERVAL = 15 * 60   # full desk update cadence (seconds)
TICK_INTERVAL = 30        # live engine tick (seconds)
GOLD_POLL     = 120       # TwelveData poll (seconds — fits free tier)
EAT = pytz.timezone("Africa/Nairobi")

for v in (TOKEN, CHAT_ID, TD_KEY):
    if not v: raise ValueError("Missing TELEGRAM_TOKEN / TELEGRAM_CHAT_ID / TWELVEDATA_API_KEY")

def now_eat(): return datetime.now(EAT)

def session_now() -> tuple[str, bool]:
    h = now_eat().hour + now_eat().minute / 60
    if 13 <= h < 17:   return "NY KILLZONE", True
    if 17 <= h < 21:   return "NY AFTERNOON", False
    if 8 <= h < 13:    return "LONDON", h < 10
    if 2 <= h < 8:     return "ASIAN", False
    return "OFF-HOURS", False

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
    c = float(df.c.iloc[-1])
    sh_list = [p for _, p in sh][-3:]; sl_list = [p for _, p in sl][-3:]
    if c > max(sh_list): return "UP", max(sh_list), min(sl_list) if sl_list else None
    if c < min(sl_list): return "DOWN", max(sh_list) if sh_list else None, min(sl_list)
    return "RANGE", max(sh_list) if sh_list else None, min(sl_list) if sl_list else None

def psych_level(price, name):
    step = 1000 if "BTC" in name else (5 if "GOLD" in name else 1)
    return round(price / step) * step

def fp(x, name):  # format price
    return f"${x:,.0f}" if "BTC" in name else f"${x:,.2f}"

# ---------------------------------------------------------------- CANDLE STORE
class CandleStore:
    def __init__(self, name, ws_sym=None):
        self.name, self.ws_sym = name, ws_sym
        self._c = {}                        # minute_epoch -> [o,h,l,c,v]
        self._df, self._df_ts = None, 0.0
        self.price, self.day_open = 0.0, None
        self.cvd_ticks = deque(maxlen=20000)  # (ts, signed_vol)
        self.last20 = deque(maxlen=20)        # True = bull print
        self.last_update = 0.0

    def ingest_kline(self, k: dict):
        t = int(k["t"]) // 60000
        o, h, l, c, v = (float(k["o"]), float(k["h"]), float(k["l"]),
                         float(k["c"]), float(k["v"]))
        self._c[t] = [o, h, l, c, v]
        self.price = c
        self.last_update = time.time()
        self._trim(); self._df = None
        utc_today = datetime.now(pytz.utc).replace(
            hour=0, minute=0, second=0, microsecond=0).timestamp() // 60
        todays = [cd[0] for tt, cd in self._c.items() if tt >= utc_today]
        if todays: self.day_open = todays[0]

    def ingest_trade(self, t: dict):
        p, q = float(t["p"]), float(t["q"])
        bull = not t["m"]                    # maker=True => sell aggressor
        self.cvd_ticks.append((t["T"] / 1000, q if bull else -q))
        self.last20.append(bull)
        self.price = p; self.last_update = time.time()
        self._df = None

    def ingest_td(self, values: list):       # TwelveData 1min, newest-first
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
def analyze(store: CandleStore, proxy: CandleStore | None = None,
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

    # multi-horizon trends
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

    # liquidity map: swing levels above/below price + psych level
    sh_p = sorted({p for _, p in swings(df15, 3)[0] if p > A["price"]})
    sl_p = sorted({p for _, p in swings(df15, 3)[1] if p < A["price"]}, reverse=True)
    psy = psych_level(A["price"], A["name"])
    if psy > A["price"]: sh_p.append(float(psy))
    elif psy < A["price"]: sl_p.append(float(psy))
    A["bsl"], A["bsl2"] = (sh_p[0], sh_p[1]) if len(sh_p) > 1 else (sh_p[0] if sh_p else hi, None)
    A["ssl"], A["ssl2"] = (sl_p[0], sl_p[1]) if len(sl_p) > 1 else (sl_p[0] if sl_p else lo, None)

    # sweep detection (last closed 5m candle)
    d5 = store.df("5min", 6)
    A["sweep_bsl"] = A["sweep_ssl"] = False
    if len(d5) >= 2:
        lc = d5.iloc[-2]
        if A["bsl"] and lc.h > A["bsl"] and lc.c < A["bsl"]: A["sweep_bsl"] = True
        if A["ssl"] and lc.l < A["ssl"] and lc.c > A["ssl"]: A["sweep_ssl"] = True

    # order flow (PAXG as proxy for GOLD)
    flow_store = store if store.cvd_ticks else proxy
    if flow_store and flow_store.cvd_ticks:
        cvd_now = flow_store.cvd(1800)
        cvd_ago = sum(v for ts, v in flow_store.cvd_ticks
                      if time.time() - 3600 <= ts <= time.time() - 1800)
        px = flow_store.price
        d5f = flow_store.df("5min", 8)
        px_ago = float(d5f.c.iloc[0]) if len(d5f) else px
        A["bull_div"] = px > px_ago and cvd_now < cvd_ago     # price up, flow down
        A["bear_div"] = px < px_ago and cvd_now > cvd_ago     # price down, flow up
        A["cvd"] = cvd_now
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

# ---------------------------------------------------------------- CONFLUENCE SWARM (4 voters)
def swarm(A: dict):
    trend_v = "BUY" if A["align"] == "BULL" else "SELL" if A["align"] == "BEAR" else "WAIT"
    struct_v = ("BUY" if (A["bos"] == "UP" or (A["fvg"] and A["fvg_t"] == "BULLISH"))
                else "SELL" if (A["bos"] == "DOWN" or (A["fvg"] and A["fvg_t"] == "BEARISH"))
                else "WAIT")
    if A.get("bull_div"):   flow_v = "BUY"    # price low, buyers absorbing
    elif A.get("bear_div"): flow_v = "SELL"   # price high, sellers absorbing
    elif A.get("cvd", 0) > 0: flow_v = "BUY"
    elif A.get("cvd", 0) < 0: flow_v = "SELL"
    else: flow_v = "WAIT"
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

# ---------------------------------------------------------------- LEVELS + NARRATIVE
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

def narrative(A: dict, d, conf, votes, audit_str: str) -> str:
    n, p = A["name"], A["price"]
    e, sl, t1, t2 = build_levels(A, d)
    sgn = "BULLISH" if d == "BUY" else "BEARISH" if d == "SELL" else "NEUTRAL"
    L = [f"<b>WHAT {n} WILL DO NEXT — LIVE ENGINE</b>",
         f"<code>{now_eat().strftime('%H:%M EAT')} | {A['sess']}{' ⚡KILLZONE' if A['killzone'] else ''}</code>", ""]
    L.append("<b>SHORT TERM (next 1–3h):</b>")
    if d == "BUY":
        L.append(f"- <b>{sgn}</b> — holding {fp(A['disc'], n)} discount zone, structure {A['bos']}")
        L.append(f"- Target up: <b>{fp(t1, n)}</b> (BSL). If breaks = <b>{fp(t2, n)}</b> next pool")
        L.append(f"- Invalidation: <b>{fp(sl, n)}</b>. If loses = {fp(A['ssl2'] or sl - 2*A['atr'], n)} liquidity below")
    elif d == "SELL":
        L.append(f"- <b>{sgn}</b> — rejected {fp(A['prem'], n)} premium zone, structure {A['bos']}")
        L.append(f"- Target down: <b>{fp(t1, n)}</b> (SSL). If breaks = <b>{fp(t2, n)}</b> next pool")
        L.append(f"- Invalidation: <b>{fp(sl, n)}</b>. If reclaims = {fp(A['bsl2'] or sl + 2*A['atr'], n)} above")
    else:
        L.append(f"- <b>MIXED</b> — chop expected around {fp(p, n)}")
        L.append(f"- Range: {fp(A['eq'] - 0.5*A['atr'], n)} – {fp(A['eq'] + 0.5*A['atr'], n)}. No trade until voters align")
    L.append("")
    L.append("<b>WHY:</b>")
    L.append(f"- Structure: BOS <b>{A['bos']}</b>"
             + (f", {A['fvg_t']} FVG at {fp(A['fvg_lv'], n)}" if A["fvg"] else ""))
    L.append(f"- Sweep: {'SSL swept → bullish' if A['sweep_ssl'] else 'BSL swept → bearish' if A['sweep_bsl'] else 'none in last 30m'}")
    L.append(f"- Flow: CVD <b>{A['cvd']:+,.0f}</b>, prints {A['bull_n']}bull/{A['bear_n']}bear — {A['flow_msg']}")
    if A.get("bull_div"): L.append("- ⚠️ <b>Bullish CVD divergence</b> — price low, buyers absorbing")
    if A.get("bear_div"): L.append("- ⚠️ <b>Bearish CVD divergence</b> — price high, sellers absorbing")
    L.append(f"- Volatility: ATR {A['atr']:.2f} {A['atr_state']} | Regime: <b>{A['regime']}</b> "
             f"({'tradeable' if A['tradeable'] else 'stand aside'})")
    L.append(f"- Zones: {A['zpos']} | Prem {fp(A['prem'], n)} / Eq {fp(A['eq'], n)} / Disc {fp(A['disc'], n)}")
    if A.get("corr") and abs(A["corr"]) > 0.4:
        L.append(f"- Correlation: Gold↔BTC r={A['corr']:.2f} (24h) — "
                 f"{'moving together' if A['corr'] > 0 else 'diverging'}")
    L.append("")
    L.append("<b>MEDIUM (rest of day):</b>")
    if d == "BUY":
        L.append(f"- Holds above {fp(A['eq'], n)} = premium visit {fp(A['prem'], n)}")
        L.append(f"- Loses {fp(A['disc'], n)} = full range flush to {fp(A['ssl2'] or A['disc'] - 2*A['atr'], n)}")
    elif d == "SELL":
        L.append(f"- Rejected at prem = discount visit {fp(A['disc'], n)}")
        L.append(f"- Reclaims {fp(A['prem'], n)} = continuation to {fp(A['bsl2'] or A['prem'] + 2*A['atr'], n)}")
    else:
        L.append(f"- Above {fp(A['eq'], n)} favors bulls → {fp(A['prem'], n)}; below favors bears → {fp(A['disc'], n)}")
    L.append("")
    L.append(f"<b>DESK:</b> {d} — voters {votes['TREND']}/{votes['STRUCT']}/{votes['FLOW']}/{votes['ZONE']} — conf <b>{conf}%</b>")
    if d != "WAIT":
        L.append(f"Entry {fp(e, n)} | SL {fp(sl, n)} | T1 {fp(t1, n)} | T2 {fp(t2, n)}")
    L.append(f"<code>{audit_str} • analysis, not financial advice • live {TICK_INTERVAL}s tick</code>")
    return "\n".join(L)

# ---------------------------------------------------------------- PREDICTION LEDGER (self-audit)
class Ledger:
    def __init__(self):
        self.open, self.results = [], deque(maxlen=200)

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
        return (f"SELF-AUDIT: {sum(recent)}/{len(recent)} predictions correct "
                f"({sum(recent)/len(recent)*100:.0f}%)")

# ---------------------------------------------------------------- ACTIVE SIGNAL TRACKER
class Tracker:
    def __init__(self): self.active = {}   # name -> state dict

    def arm(self, A, d, conf, entry, sl, t1, t2):
        self.active[A["name"]] = {"d": d, "conf": conf, "entry": entry, "sl": sl,
                                  "t1": t1, "t2": t2, "t1_hit": False,
                                  "opened": now_eat().strftime("%H:%M")}

    def check(self, A) -> list[str]:
        s = self.active.get(A["name"]); out = []
        if not s or not A["full"]: return out
        p, n = A["price"], A["name"]
        if s["d"] == "BUY":
            hit, run, stop = p >= s["t1"], p >= s["t2"], p <= s["sl"]
        else:
            hit, run, stop = p <= s["t1"], p <= s["t2"], p >= s["sl"]
        if not s["t1_hit"] and hit:
            s["t1_hit"] = True
            out.append(f"🎯 {n} TP1 HIT at {fp(p, n)} — stop moved to entry. "
                       f"T2 {fp(s['t2'], n)} is live.")
        elif run:
            out.append(f"🏁 {n} TP2 SMASHED at {fp(p, n)}. Full target. Desk closes tracking.")
            self.active.pop(n, None)
        elif stop:
            out.append(f"🛑 {n} STOPPED at {fp(p, n)}. Structure invalidated — regrouping.")
            self.active.pop(n, None)
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
        ax.bar(i, r.c - r.o, bottom=r.o, width=0.6, color=c, edgecolor=c, zorder=3)
    n = A["name"]
    for lv, col, ls, lab in [(entry, "#ffd54f", "-", "ENTRY"), (sl, "#ef5350", "--", "SL"),
                             (t1, "#26a69a", "--", "T1"), (t2, "#26a69a", "--", "T2")]:
        if lv and lv != A["price"]:
            ax.axhline(lv, color=col, ls=ls, lw=1.4, label=lab)
    for lv in (A["bsl"], A["ssl"]):
        if lv: ax.axhline(lv, color="#8d6e63", ls=":", lw=1.2)
    ax.set_title(f"{n} 15m — {d} | {A['regime']} | {A['sess']}", color="white", fontsize=12)
    ax.tick_params(colors="white")
    for s in ax.spines.values(): s.set_color("#444")
    ax.legend(loc="upper left", fontsize=8, facecolor="#1e222d",
              edgecolor="#444", labelcolor="white")
    plt.tight_layout(); plt.savefig(path, facecolor="#131722"); plt.close(fig)

# ---------------------------------------------------------------- TELEGRAM
class TG:
    def __init__(self): self.base = f"https://api.telegram.org/bot{TOKEN}"

    async def text(self, ses, msg):
        try:
            async with ses.post(f"{self.base}/sendMessage",
                                json={"chat_id": CHAT_ID, "text": msg,
                                      "parse_mode": "HTML",
                                      "disable_web_page_preview": True}):
                pass
        except Exception as e: log.error(f"TG text: {e}")

    async def photo(self, ses, path, caption=""):
        try:
            with open(path, "rb") as f:
                fd = aiohttp.FormData()
                fd.add_field("chat_id", CHAT_ID)
                fd.add_field("caption", caption[:1000])
                fd.add_field("photo", f, filename="chart.png")
                async with ses.post(f"{self.base}/sendPhoto", data=fd): pass
        except Exception as e: log.error(f"TG photo: {e}")

    async def voice(self, ses, path):
        try:
            with open(path, "rb") as f:
                fd = aiohttp.FormData()
                fd.add_field("chat_id", CHAT_ID)
                fd.add_field("voice", f, filename="b.mp3")
                async with ses.post(f"{self.base}/sendVoice", data=fd,
                                    timeout=aiohttp.ClientTimeout(total=60)): pass
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

# ---------------------------------------------------------------- CORRELATION ENGINE
async def corr_worker(btc: CandleStore, gold: CandleStore, out: dict):
    while True:
        try:
            b = btc.df("5min", 288).c    # 24h of 5m closes
            g = gold.df("5min", 288).c
            if len(b) > 50 and len(g) > 50:
                m = min(len(b), len(g))
                c = float(np.corrcoef(b.iloc[-m:].values, g.iloc[-m:].values)[0, 1])
                if not np.isnan(c):
                    out["val"] = c
                    log.info(f"Corr Gold↔BTC (24h): {c:.2f}")
        except Exception as e:
            log.error(f"corr: {e}")
        await asyncio.sleep(300)

# ---------------------------------------------------------------- DAY OUTLOOK (autonomous)
def day_outlook(A: dict, ledger: Ledger) -> str:
    n = A["name"]; a = A["atr"]
    d, conf, votes = swarm(A)
    e, sl, t1, t2 = build_levels(A, d)
    L = [f"<b>🌍 AUTONOMOUS DAY OUTLOOK — {n}</b>",
         f"<code>{now_eat().strftime('%H:%M EAT')} | {A['sess']} | Regime {A['regime']} | "
         f"Vol {A['atr_state']} | Align {A['align']} ({A['strength']}/3)</code>", ""]
    if d == "WAIT":
        L.append("<b>PRIMARY SCENARIO (~55%):</b> Range rotation")
        L.append(f"- Rotation between {fp(A['disc'], n)} (discount) and {fp(A['prem'], n)} (premium), eq {fp(A['eq'], n)}")
        L.append(f"- Fade extremes: SELL toward {fp(A['prem'], n)}, BUY toward {fp(A['disc'], n)}")
        L.append("- Breakout trades ONLY after a 15m close outside range + retest holds")
        ledger.add(n, "BUY", A["prem"], A["disc"] - 0.5 * a, hours=10)
        ledger.add(n, "SELL", A["disc"], A["prem"] + 0.5 * a, hours=10)
    else:
        opp = "SELL" if d == "BUY" else "BUY"
        rev = (A["ssl2"] or e - 3 * a) if d == "BUY" else (A["bsl2"] or e + 3 * a)
        L.append(f"<b>PRIMARY SCENARIO ({conf}%):</b> {d}")
        if d == "BUY":
            L.append(f"- Path: hold above {fp(A['eq'], n)} → run BSL {fp(t1, n)} → extend {fp(t2, n)}")
            L.append(f"- Invalidation: lose {fp(sl, n)}")
        else:
            L.append(f"- Path: hold below {fp(A['eq'], n)} → tap SSL {fp(t1, n)} → extend {fp(t2, n)}")
            L.append(f"- Invalidation: reclaim {fp(sl, n)}")
        L.append(f"<b>ALTERNATE SCENARIO (~{max(10, 100 - conf - 20)}%):</b> {opp} sweep")
        L.append(f"- If {fp(sl, n)} breaks → liquidity flush to {fp(rev, n)} before any real reversal")
        ledger.add(n, d, t1, sl, hours=8)
        ledger.add(n, opp, rev, e, hours=8)
    L.append("")
    L.append("<b>SESSION PLAN (EAT):</b>")
    L.append("- 08:00–10:00 London open: first manipulation move, watch for sweep of Asian high/low")
    L.append("- 13:00–17:00 NY KILLZONE: the real directional move of the day usually lives here")
    L.append("- After 17:00: reduce expectations, NY afternoon = profit-taking flows")
    if A.get("corr") and abs(A["corr"]) > 0.4:
        L.append(f"\n<b>MACRO LINK:</b> Gold↔BTC r={A['corr']:.2f} — "
                 f"{'they move together; a flush in one likely drags the other' if A['corr'] > 0 else 'they are diverging; relative strength tells the story'}")
    L.append(f"\n<code>{ledger.audit_str()} • scenarios graded automatically at expiry</code>")
    return "\n".join(L)

def voice_brief(A: dict, d, conf) -> str:
    n = A["name"]
    if d == "WAIT":
        return (f"{n} update. Price {A['price']:,.1f}. Regime {A['regime']}. "
                f"Voters not aligned, standing aside. {A['flow_msg']}.")
    e, sl, t1, t2 = build_levels(A, d)
    return (f"Sniper setup on {n}. {d} with {conf} percent conviction. "
            f"Entry {e:,.1f}, stop {sl:,.1f}, first target {t1:,.1f}, second target {t2:,.1f}. "
            f"{A['flow_msg']}.")

# ---------------------------------------------------------------- LIVE ENGINE
async def live_loop(stores, paxg, corr, ledger, tracker, tg, ses):
    last_desk = {n: 0.0 for n in stores}
    last_event, last_event_t = {}, {}
    outlook_done = set()

    await tg.text(ses, "<b>🛰 BRAX SMART DESK v2 — LIVE ENGINE ONLINE</b>\n"
                       "WebSocket tick feed • 30s live tick • event alerts • "
                       "TP/SL tracker • self-auditing predictions\n"
                       "<code>Warming up data… first desk drop shortly.</code>")

    while True:
        try:
            prices = {n: st.price for n, st in stores.items()}
            for pr, status in ledger.check(prices):
                await tg.text(ses, f"<b>📊 LEDGER:</b> {pr['name']} {pr['dir']} "
                                   f"(made {pr['made']}) → <b>{status}</b>\n"
                                   f"<code>{ledger.audit_str()}</code>")

            # autonomous outlook at London open (08:00) and NY killzone (13:00) EAT
            t = now_eat()
            key = (t.date(), t.hour)
            if t.hour in (8, 13) and key not in outlook_done and all(s.price > 0 for s in stores.values()):
                outlook_done.add(key)
                for n, st in stores.items():
                    proxy = paxg if n == "GOLD" else None
                    A = analyze(st, proxy, corr.get("val", 0.0))
                    if A["full"]:
                        await tg.text(ses, day_outlook(A, ledger))
                        d, conf, _ = swarm(A)
                        v = (f"Autonomous day outlook for {n}. Primary scenario {d} with "
                             f"{conf} percent conviction." if d != "WAIT" else
                             f"Autonomous day outlook for {n}. Primary scenario is range rotation.")
                        await voice_note(ses, tg, v)

            for n, st in stores.items():
                if st.price <= 0:
                    continue
                proxy = paxg if n == "GOLD" else None
                A = analyze(st, proxy, corr.get("val", 0.0))
                if not A["full"]:
                    continue

                # --- TP/SL tracker (fires instantly)
                for msg in tracker.check(A):
                    await tg.text(ses, msg)
                    await voice_note(ses, tg, msg.replace("🎯", "")
                                          .replace("🏁", "").replace("🛑", ""))

                # --- instant event alerts (BOS change / liquidity sweep)
                ev = f"{A['bos']}|{A['sweep_bsl']}|{A['sweep_ssl']}"
                if n in last_event and ev != last_event[n] and time.time() - last_event_t.get(n, 0) > 300:
                    parts = []
                    if A["sweep_ssl"]:
                        parts.append(f"🔻 <b>SSL SWEEP</b> below {fp(A['ssl'], n)} — bullish reversal watch")
                    if A["sweep_bsl"]:
                        parts.append(f"🔺 <b>BSL SWEEP</b> above {fp(A['bsl'], n)} — bearish reversal watch")
                    if A["bos"] in ("UP", "DOWN") and last_event.get(n, "").split("|")[0] != A["bos"]:
                        parts.append(f"⚡ <b>BOS {A['bos']}</b> — structure shifted "
                                     f"{'bullish' if A['bos'] == 'UP' else 'bearish'}")
                    if parts:
                        await tg.text(ses, f"<b>{n} EVENT</b> @ {fp(A['price'], n)}\n"
                                     + "\n".join(parts)
                                     + f"\n<code>{now_eat().strftime('%H:%M:%S EAT')} • detected within seconds of print</code>")
                    last_event_t[n] = time.time()
                last_event[n] = ev

                # --- full desk drop every 15 min per asset
                if time.time() - last_desk[n] >= DESK_INTERVAL:
                    last_desk[n] = time.time()
                    d, conf, votes = swarm(A)
                    await tg.text(ses, narrative(A, d, conf, votes, ledger.audit_str()))
                    if d != "WAIT" and n not in tracker.active and conf >= 68:
                        e, sl, t1, t2 = build_levels(A, d)
                        tracker.arm(A, d, conf, e, sl, t1, t2)
                        path = f"/tmp/{n.replace('/', '')}.png"
                        await asyncio.to_thread(make_chart, A, d, e, sl, t1, t2, path)
                        await tg.photo(ses, path, f"{n} — {d} {conf}% | {A['sess']} | {A['regime']}")
                        if os.path.exists(path): os.remove(path)
                        await voice_note(ses, tg, voice_brief(A, d, conf))
                    log.info(f"Desk → {n} | {d} {conf}% | full")
        except Exception as e:
            log.error(f"live_loop: {e}")
        await asyncio.sleep(TICK_INTERVAL)

# ---------------------------------------------------------------- KEEPALIVE (Render free tier)
async def keepalive(ses):
    if not SELF_URL:
        return
    while True:
        try:
            await ses.get(SELF_URL)
        except Exception:
            pass
        await asyncio.sleep(600)

# ---------------------------------------------------------------- MAIN
async def main():
    btc  = CandleStore("BITCOIN", ws_sym="btcusdt")
    paxg = CandleStore("PAXG",    ws_sym="paxgusdt")
    gold = CandleStore("GOLD")            # TwelveData poll + goldprice.org backup
    corr = {"val": 0.0}
    ledger, tracker = Ledger(), Tracker()
    tg = TG()

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25)) as ses:
        tasks = [asyncio.create_task(binance_worker([btc, paxg])),
                 asyncio.create_task(gold_worker(gold, ses)),
                 asyncio.create_task(corr_worker(btc, paxg, corr)),
                 asyncio.create_task(keepalive(ses))]
        try:
            await live_loop({"BITCOIN": btc, "GOLD": gold}, paxg, corr,
                            ledger, tracker, tg, ses)
        finally:
            for t in tasks:
                t.cancel()

app = Flask(__name__)

@app.route("/")
def health():
    return "BRAX SMART DESK v2 — LIVE ENGINE RUNNING", 200

def run_flask():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)), use_reloader=False)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    asyncio.run(main())
