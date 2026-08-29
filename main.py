"""
BRAX SMART DESK v4 — Real-time multi-asset analysis engine
===========================================================
Data spine (all REAL, all live):
  • Binance WS (kline_1m + aggTrade) for BTC & PAXG — 24/7, with REST bootstrap
    (2×1000 bars ≈ 33h history) and REST fallback if WS is geo-blocked
  • Binance Futures REST: funding rate, open interest, long/short account ratio
  • Binance spot REST: live order-book depth → bid/ask imbalance
  • alternative.me: Fear & Greed index
  • ForexFactory weekly calendar: high-impact news window gate
  • TwelveData 1min XAU/USD + goldprice.org backup, with real spot-hours
    awareness (XAU closed Fri 22:00 UTC → Sun 22:00 UTC → signals suppressed,
    PAXG runs as the 24/7 tokenized-gold desk)

Analysis modules:
  Multi-horizon EMA trends • Session macro & killzones • Premium/Discount
  Liquidity map (BSL/SSL + EQH/EQL + sweeps) • BOS/FVG/Order Blocks
  Session VWAP • CVD order flow + divergence • ATR volatility engine
  BB Squeeze detection • Fibonacci grid • Regime (ADX) detection
  Confluence swarm w/ news & liquidity gates • T1/T2/SL tracker
  Risk-based position sizing • Self-auditing prediction ledger (with expiry)
  BTC↔Gold/PAXG correlation • Autonomous day outlook • Asian-range judas
  context • Voice briefings • TradingView-style charts • Market School
  Telegram command console • Feed watchdog

Deploy:  Render → Start Command: python main.py
Env:     TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, TWELVEDATA_API_KEY
         ACCOUNT_BALANCE (optional, default 10000), RISK_PCT (optional, default 1.0)
"""
import asyncio, os, json, time, random, logging
from collections import deque
from datetime import datetime, timedelta
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

TOKEN   = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TD_KEY  = os.getenv("TWELVEDATA_API_KEY")
SELF_URL = os.getenv("RENDER_EXTERNAL_URL", "")

BINANCE_REST  = "https://data-api.binance.vision/api/v3"
BINANCE_FAPI  = "https://fapi.binance.com"
BINANCE_HOSTS = ["wss://data-stream.binance.vision/stream?streams=",
                 "wss://stream.binance.com:9443/stream?streams="]

DESK_INTERVAL        = 15 * 60
TICK_INTERVAL        = 30
GOLD_POLL            = 120
GOLD_POLL_CLOSED     = 900
GOLD_STATUS_INTERVAL = 2 * 3600
CTX_INTERVAL         = 120
NEWS_INTERVAL        = 1800
STALE_CRYPTO_SEC     = 90
STALE_FEED_SEC       = 300
WATCHDOG_INTERVAL    = 30 * 60
EAT = pytz.timezone("Africa/Nairobi")

ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "10000"))
RISK_PCT        = float(os.getenv("RISK_PCT", "1.0"))

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

# ---------------------------------------------------------------- XAU MARKET HOURS
def gold_market_open() -> bool:
    now = datetime.now(pytz.utc)
    wd, m = now.weekday(), now.hour * 60 + now.minute
    if wd == 5:                                       return False   # Saturday
    if wd == 6 and m < 22 * 60 + 1:                   return False   # Sunday pre-open
    if wd == 4 and m >= 22 * 60:                      return False   # Friday post-close
    if wd in (0, 1, 2, 3) and 21 * 60 <= m < 22 * 60: return False   # daily break
    return True

def gold_next_open_eat() -> datetime:
    now = datetime.now(pytz.utc)
    days = (6 - now.weekday()) % 7
    if days == 0 and now.hour * 60 + now.minute >= 22 * 60:
        days = 7
    t = (now + timedelta(days=days)).replace(hour=22, minute=0, second=0, microsecond=0)
    return t.astimezone(EAT)

# ---------------------------------------------------------------- MARKET SCHOOL
LESSONS = [
    "Stops cluster above old highs and below old lows. Big players push price into those pools to fill size — that is why breakouts often fail seconds after a sweep. Trade the reaction to the sweep, not the sweep itself.",
    "Most real directional moves are born in the London open (08:00-10:00 EAT) and the NY killzone (13:00-17:00 EAT). Mid-session breakouts are usually traps designed to harvest late entries.",
    "Never buy in premium and never sell in discount of the dealing range. Institutions accumulate at wholesale (discount) and distribute at retail (premium). Your entries should follow the same logic.",
    "When price prints new highs but CVD falls, buyers are exhausted and the move is being distributed into strength. Price leads, flow confirms — when they disagree, the flow usually wins.",
    "XAU/USD spot shuts Friday 22:00 UTC until Sunday 22:00 UTC. Weekend 'gold moves' come from stale quotes, not a live market. PAXG, the tokenized gold on Binance, trades 24/7 and tracks spot within a few dollars.",
    "Around CPI, NFP and FOMC, spreads widen 5-10x and both sides get wicked before the real move. Professionals wait 15 minutes after the print, then trade the confirmed direction, not the spike.",
    "ATR tells you what regime you are in. CONTRACTING volatility breeds fake breakouts and mean reversion. EXPANDING volatility favors continuation. Size stops by ATR, never by fixed distances.",
    "When Gold and BTC correlation spikes above 0.6 during dollar-liquidity events, a flush in one drags the other. Do not hedge one with the other blindly — you would be doubling risk, not hedging.",
    "The last opposing candle before an impulsive BOS marks institutional entry — the order block. Its first retest after the break is one of the highest-probability entries in price action.",
    "The first 15 minutes of London frequently sweeps the Asian range high or low — the judas swing — before the true daily direction reveals itself. The sweep is the bait; the reaction is the trade.",
    "Funding rate is the crowd's rent. Deeply positive funding means longs are paying and are crowded — squeezes fuel downward. Deeply negative funding means the opposite. Fade the crowd when funding hits extremes.",
    "Equal highs and equal lows are engineered liquidity. Algorithms see those obvious double-tops and double-bottoms as fuel. When you spot EQH or EQL, expect a raid before the real move.",
]
def market_lesson() -> str:
    return LESSONS[int(time.time() // 3600) % len(LESSONS)]

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

def bb_squeeze(df: pd.DataFrame, n=20) -> tuple[float, str]:
    """Bollinger bandwidth percentile rank over last 100 bars → squeeze state."""
    mid = df.c.rolling(n).mean()
    sd = df.c.rolling(n).std()
    bw = ((mid + 2*sd) - (mid - 2*sd)) / mid
    bw = bw.dropna()
    if len(bw) < 30: return 0.0, "N/A"
    last = float(bw.iloc[-1])
    pct = float((bw.iloc[-100:] < last).mean() * 100)
    state = ("SQUEEZE" if pct < 20 else "EXPANSION" if pct > 80 else "NORMAL")
    return pct, state

def session_vwap(df1: pd.DataFrame) -> float:
    """UTC-day-anchored VWAP. Falls back to typical-price mean when volume is absent."""
    if df1.empty: return 0.0
    today = df1[df1.index >= df1.index[-1].floor("D")]
    if today.empty: today = df1.tail(240)
    tp = (today.h + today.l + today.c) / 3
    v = today.v
    if float(v.sum()) <= 0:
        return float(tp.mean())
    return float((tp * v).sum() / v.sum())

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

def detect_order_block(df, a: float):
    """Last opposing candle before a >=1.2*ATR impulse — institutional entry zone."""
    if len(df) < 16: return None
    o, c, h, l = df.o.values, df.c.values, df.h.values, df.l.values
    for i in range(len(df) - 4, max(2, len(df) - 16), -1):
        move = c[i + 3] - c[i]
        if move > 1.2 * a and o[i] > c[i]:
            return {"type": "BULLISH", "hi": float(h[i]), "lo": float(l[i])}
        if -move > 1.2 * a and c[i] > o[i]:
            return {"type": "BEARISH", "hi": float(h[i]), "lo": float(l[i])}
    return None

def detect_equal_levels(df, a: float, k=3):
    """EQH/EQL — two swings within 0.15*ATR = engineered liquidity."""
    sh, sl = swings(df, k)
    eqh = eql = None
    hs = [p for _, p in sh]; ls = [p for _, p in sl]
    for i in range(len(hs) - 2, -1, -1):
        if abs(hs[i + 1] - hs[i]) < 0.15 * a:
            eqh = max(hs[i], hs[i + 1]); break
    for i in range(len(ls) - 2, -1, -1):
        if abs(ls[i + 1] - ls[i]) < 0.15 * a:
            eql = min(ls[i], ls[i + 1]); break
    return eqh, eql

def fib_grid(hi: float, lo: float):
    d = hi - lo
    return {lvl: hi - d * r for lvl, r in
            (("0.236", .236), ("0.382", .382), ("0.5", .5), ("0.618", .618), ("0.786", .786))}

def nearest_fibs(fibs: dict, price: float):
    above = min(((abs(v - price), k, v) for k, v in fibs.items() if v > price), default=None)
    below = min(((abs(v - price), k, v) for k, v in fibs.items() if v <= price), default=None)
    return (below[1:], above[1:]) if below and above else (None, None)

def asian_range(store: "CandleStore"):
    """Today's Asian session (02:00-08:00 EAT) high/low — judas swing context."""
    df = store.df("1min")
    if df.empty: return None, None
    dfe = df.tz_convert(EAT)
    today = now_eat().date()
    m = dfe[(dfe.index.date == today) & (dfe.index.hour >= 2) & (dfe.index.hour < 8)]
    if len(m) < 30: return None, None
    return float(m.h.max()), float(m.l.min())

def psych_level(price, name):
    step = 1000 if "BTC" in name else (5 if ("GOLD" in name or "PAXG" in name) else 1)
    return round(price / step) * step

def fp(x, name):
    return f"${x:,.0f}" if "BTC" in name else f"${x:,.2f}"

# ---------------------------------------------------------------- CANDLE STORE
class CandleStore:
    def __init__(self, name, ws_sym=None):
        self.name, self.ws_sym = name, ws_sym
        self._c = {}                          # minute_epoch -> [o,h,l,c,v]
        self._df, self._df_ts = None, 0.0
        self.price, self.day_open = 0.0, None
        self.cvd_ticks = deque(maxlen=20000)  # (ts, signed_vol)
        self.last20 = deque(maxlen=20)        # True = bull print
        self.last_update = 0.0
        self.source = "—"

    def _update_day_open(self):
        utc_today = datetime.now(pytz.utc).replace(
            hour=0, minute=0, second=0, microsecond=0).timestamp() // 60
        todays = [cd[0] for tt, cd in self._c.items() if tt >= utc_today]
        if todays: self.day_open = todays[0]

    def ingest_kline(self, k: dict):
        self.ingest_kline_tuple(int(k["t"]) // 60000, float(k["o"]), float(k["h"]),
                                float(k["l"]), float(k["c"]), float(k["v"]))

    def ingest_kline_tuple(self, t_min, o, h, l, c, v):
        self._c[int(t_min)] = [o, h, l, c, v]
        self.price = c
        self.last_update = time.time()
        self._trim(); self._df = None
        self._update_day_open()

    def ingest_trade(self, t: dict):
        p, q = float(t["p"]), float(t["q"])
        bull = not t["m"]                     # maker=True → sell aggressor
        self.cvd_ticks.append((t["T"] / 1000, q if bull else -q))
        self.last20.append(bull)
        self.price = p; self.last_update = time.time()
        self._df = None

    def ingest_td(self, values: list):
        for r in values:
            try:
                ts = int(datetime.strptime(r["datetime"], "%Y-%m-%d %H:%M:%S")
                         .replace(tzinfo=pytz.utc).timestamp() // 60)
                self._c[ts] = [float(r["open"]), float(r["high"]),
                               float(r["low"]), float(r["close"]),
                               float(r.get("volume") or 0)]
            except Exception:
                continue
        if values: self.price = float(values[0]["close"])
        self.last_update = time.time()
        self._trim(); self._df = None
        self._update_day_open()

    def _trim(self):
        cutoff = (time.time() - 3 * 86400) // 60      # keys are MINUTE epochs — CRITICAL FIX
        for k in [k for k in self._c if k < cutoff]: del self._c[k]

    def data_age(self) -> float:
        if not self._c: return 1e9
        return time.time() - max(self._c) * 60

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
                    for s in stores:
                        if s.ws_sym: s.source = "BINANCE WS"
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

async def crypto_rest_fallback(stores: list[CandleStore], ses: aiohttp.ClientSession):
    """If the WS is silent (geo-block, network), keep prices real via REST."""
    while True:
        await asyncio.sleep(30)
        for st in stores:
            if not st.ws_sym: continue
            if time.time() - st.last_update < STALE_CRYPTO_SEC: continue
            sym = st.ws_sym.upper()
            try:
                log.warning(f"{st.name}: feed silent {int(time.time()-st.last_update)}s → REST fallback")
                async with ses.get(f"{BINANCE_REST}/klines?symbol={sym}&interval=1m&limit=3") as r:
                    kl = await r.json()
                if isinstance(kl, list) and kl:
                    for k in kl:
                        st.ingest_kline_tuple(int(k[0]) // 60000, float(k[1]), float(k[2]),
                                              float(k[3]), float(k[4]), float(k[5]))
                    st.source = "BINANCE REST (WS blocked)"
                    log.info(f"{st.name}: REST fallback OK @ {st.price}")
            except Exception as e:
                log.error(f"REST fallback {st.name}: {e}")

async def bootstrap_crypto(stores: list[CandleStore], ses: aiohttp.ClientSession):
    """Seed ~2000×1m bars (≈33h) + last 1000 aggTrades so every horizon is live at boot."""
    for st in stores:
        if not st.ws_sym: continue
        sym = st.ws_sym.upper()
        try:
            async with ses.get(f"{BINANCE_REST}/klines?symbol={sym}&interval=1m&limit=1000") as r:
                kl = await r.json()
            kl_all = kl if isinstance(kl, list) and kl else []
            if kl_all:
                async with ses.get(f"{BINANCE_REST}/klines?symbol={sym}&interval=1m&limit=1000"
                                   f"&endTime={int(kl[0][0]) - 1}") as r2:
                    kl2 = await r2.json()
                if isinstance(kl2, list) and kl2:
                    kl_all = kl2 + kl_all          # chronological: older page first
                for k in kl_all:
                    st._c[int(k[0]) // 60000] = [float(k[1]), float(k[2]),
                                                 float(k[3]), float(k[4]), float(k[5])]
                st.price = float(kl_all[-1][4]); st.last_update = time.time(); st._df = None
                st._update_day_open()
                log.info(f"{st.name}: bootstrapped {len(kl_all)} 1m bars (≈{len(kl_all)/60:.0f}h)")
        except Exception as e:
            log.error(f"bootstrap klines {st.name}: {e}")
        try:
            async with ses.get(f"{BINANCE_REST}/aggTrades?symbol={sym}&limit=1000") as r:
                tr = await r.json()
            if isinstance(tr, list):
                for t in tr:
                    st.ingest_trade({"p": t["p"], "q": t["q"], "m": t["m"], "T": t["T"]})
                log.info(f"{st.name}: CVD seeded with {len(tr)} trades")
        except Exception as e:
            log.error(f"bootstrap aggTrades {st.name}: {e}")

async def bootstrap_gold(gold: CandleStore, ses: aiohttp.ClientSession):
    try:
        async with ses.get(f"https://api.twelvedata.com/time_series"
                           f"?symbol=XAU/USD&interval=1min&outputsize=500&apikey={TD_KEY}") as r:
            d = await r.json()
        if d.get("values"):
            gold.ingest_td(d["values"])
            gold.source = "TWELVEDATA"
            log.info(f"GOLD bootstrapped {len(d['values'])} bars @ ${gold.price:.2f}")
    except Exception as e:
        log.error(f"bootstrap gold: {e}")

async def gold_worker(gold: CandleStore, ses: aiohttp.ClientSession):
    while True:
        try:
            async with ses.get(f"https://api.twelvedata.com/time_series"
                               f"?symbol=XAU/USD&interval=1min&outputsize=60&apikey={TD_KEY}") as r:
                d = await r.json()
            if d.get("values"):
                gold.ingest_td(d["values"])
                gold.source = "TWELVEDATA"
                log.info(f"GOLD synced ${gold.price:.2f}")
            else:
                async with ses.get("https://data-asg.goldprice.org/dbXRates/USD") as r2:
                    gp = (await r2.json())["items"][0]["xauPrice"]
                if gold.price <= 0 or abs(gp - gold.price) > 0.5:
                    gold.price = float(gp); gold.last_update = time.time()
                gold.source = "GOLDPRICE.ORG"
                log.info(f"GOLD via goldprice.org ${gp:.2f}")
        except Exception as e:
            log.error(f"Gold feed: {e}")
        await asyncio.sleep(GOLD_POLL if gold_market_open() else GOLD_POLL_CLOSED)

# -------- futures context: funding, OI, long/short ratio, order book, FNG
async def context_worker(ctx: dict, ses: aiohttp.ClientSession):
    syms = {"BTCUSDT": "BITCOIN", "PAXGUSDT": "PAXG"}
    while True:
        try:
            for fsym, name in syms.items():
                c = ctx.setdefault(name, {})
                try:
                    async with ses.get(f"{BINANCE_FAPI}/fapi/v1/premiumIndex?symbol={fsym}") as r:
                        d = await r.json()
                    c["funding"] = float(d.get("lastFundingRate", 0)) * 100
                    c["mark"] = float(d.get("markPrice", 0))
                except Exception: pass
                try:
                    async with ses.get(f"{BINANCE_FAPI}/futures/data/openInterestHist"
                                       f"?symbol={fsym}&period=5m&limit=2") as r:
                        d = await r.json()
                    if isinstance(d, list) and len(d) >= 2:
                        oi0, oi1 = float(d[0]["sumOpenInterest"]), float(d[-1]["sumOpenInterest"])
                        c["oi_chg"] = (oi1 - oi0) / oi0 * 100 if oi0 else 0.0
                except Exception: pass
                try:
                    async with ses.get(f"{BINANCE_FAPI}/futures/data/globalLongShortAccountRatio"
                                       f"?symbol={fsym}&period=5m&limit=1") as r:
                        d = await r.json()
                    if isinstance(d, list) and d:
                        c["ls_ratio"] = float(d[-1]["longShortRatio"])
                except Exception: pass
                try:
                    async with ses.get(f"{BINANCE_REST}/depth?symbol={fsym}&limit=100") as r:
                        d = await r.json()
                    bid = sum(float(q) * float(p) for p, q in d.get("bids", []))
                    ask = sum(float(q) * float(p) for p, q in d.get("asks", []))
                    if bid + ask > 0:
                        c["book"] = (bid - ask) / (bid + ask) * 100   # + = bid-heavy
                except Exception: pass
            try:
                async with ses.get("https://api.alternative.me/fng/?limit=1") as r:
                    d = await r.json()
                ctx.setdefault("BITCOIN", {})["fng"] = int(d["data"][0]["value"])
                ctx["BITCOIN"]["fng_label"] = d["data"][0]["value_classification"]
            except Exception: pass
        except Exception as e:
            log.error(f"context: {e}")
        await asyncio.sleep(CTX_INTERVAL)

# -------- real economic calendar (ForexFactory weekly JSON, high-impact USD)
async def news_worker(ctx: dict, ses: aiohttp.ClientSession):
    while True:
        try:
            async with ses.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json") as r:
                evs = await r.json()
            now = datetime.now(pytz.utc)
            upcoming = []
            for e in evs:
                if str(e.get("impact", "")).lower() != "high": continue
                if e.get("country") not in ("USD", "ALL"): continue
                try:
                    dt = datetime.fromisoformat(e["date"]).astimezone(pytz.utc)
                except Exception: continue
                if -timedelta(hours=1) <= dt - now <= timedelta(hours=6):
                    upcoming.append({"title": e.get("title", "?"),
                                     "at": dt.astimezone(EAT)})
            ctx["news"] = upcoming
            if upcoming:
                log.info("News window: " + "; ".join(
                    f"{x['title']} @ {x['at'].strftime('%H:%M EAT')}" for x in upcoming))
        except Exception as e:
            log.error(f"news: {e}")
        await asyncio.sleep(NEWS_INTERVAL)

def news_risk(ctx: dict) -> list[str]:
    out = []
    now = datetime.now(EAT)
    for x in ctx.get("news", []):
        mins = (x["at"] - now).total_seconds() / 60
        if -15 <= mins <= 60:
            out.append(f"⚡ HIGH-IMPACT: {x['title']} at {x['at'].strftime('%H:%M EAT')} "
                       f"({'in ' + str(int(mins)) + ' min' if mins > 0 else 'JUST RELEASED — wait 15 min'})")
    return out

# ---------------------------------------------------------------- ANALYSIS ENGINE
def analyze(store: CandleStore, proxy: CandleStore | None = None,
            corr: float = 0.0, market_open: bool = True) -> dict:
    A = {"store": store, "name": store.name, "price": store.price, "full": False}
    A["source"], A["age"] = store.source, store.data_age()
    A["live"] = market_open and A["age"] < (180 if store.ws_sym else STALE_FEED_SEC)
    A["market_open"] = market_open
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
    A["tradeable"] = adx_v >= 22 and A["live"]

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
    A["ob"] = detect_order_block(df15, a)
    A["eqh"], A["eql"] = detect_equal_levels(df15, a)

    # premium / discount (24h range)
    hi, lo = float(df15.h.max()), float(df15.l.min())
    eq = (hi + lo) / 2
    A.update(hi24=hi, lo24=lo, prem=hi - 0.25 * (hi - lo), disc=lo + 0.25 * (hi - lo), eq=eq,
             zpos=("PREMIUM" if A["price"] > eq + 0.1 * (hi - lo) else
                   "DISCOUNT" if A["price"] < eq - 0.1 * (hi - lo) else "EQUILIBRIUM"))
    A["fibs"] = fib_grid(hi, lo)
    A["chg24"] = ((A["price"] - store.day_open) / store.day_open * 100) if store.day_open else 0.0

    # VWAP + squeeze + Asian range
    A["vwap"] = session_vwap(store.df("1min"))
    A["bb_pct"], A["bb_state"] = bb_squeeze(df15)
    A["asian_hi"], A["asian_lo"] = asian_range(store)

    # liquidity map
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

    # order flow (PAXG proxies GOLD)
    flow_store = store if store.cvd_ticks else proxy
    if flow_store and flow_store.cvd_ticks:
        cvd_now = flow_store.cvd(1800)
        cvd_ago = sum(v for ts, v in flow_store.cvd_ticks
                      if time.time() - 3600 <= ts <= time.time() - 1800)
        px = flow_store.price
        d5f = flow_store.df("5min", 8)
        px_ago = float(d5f.c.iloc[0]) if len(d5f) else px
        A["bull_div"] = px > px_ago and cvd_now < cvd_ago
        A["bear_div"] = px < px_ago and cvd_now > cvd_ago
        A["cvd"] = cvd_now
        bulls = sum(1 for b in flow_store.last20 if b)
        A["bull_n"], A["bear_n"] = bulls, 20 - bulls
        A["flow_msg"] = ("Bullish — buyers lifting offers" if cvd_now > 0 and not A["bear_div"]
                         else "Bearish — sellers hitting bids" if cvd_now < 0 and not A["bull_div"]
                         else "Divergence — flow vs price disagree, caution")
    else:
        A["cvd"] = 0.0; A["bull_div"] = A["bear_div"] = False
        A["bull_n"], A["bear_n"] = 0, 0
        A["flow_msg"] = "Flow proxy unavailable"

    A["corr"] = corr
    A["sess"], A["killzone"] = session_now()
    A["day_open"] = store.day_open or A["price"]
    return A

# ---------------------------------------------------------------- CONFLUENCE SWARM
def swarm(A: dict, ctx: dict | None = None):
    trend_v = "BUY" if A["align"] == "BULL" else "SELL" if A["align"] == "BEAR" else "WAIT"
    struct_v = ("BUY" if (A["bos"] == "UP" or (A["fvg"] and A["fvg_t"] == "BULLISH"))
                else "SELL" if (A["bos"] == "DOWN" or (A["fvg"] and A["fvg_t"] == "BEARISH"))
                else "WAIT")
    if A.get("bull_div"):   flow_v = "BUY"
    elif A.get("bear_div"): flow_v = "SELL"
    elif A.get("cvd", 0) > 0: flow_v = "BUY"
    elif A.get("cvd", 0) < 0: flow_v = "SELL"
    else: flow_v = "WAIT"
    zone_v = ("BUY" if A["sweep_ssl"] or A["zpos"] == "DISCOUNT"
              else "SELL" if A["sweep_bsl"] or A["zpos"] == "PREMIUM" else "WAIT")
    votes = {"TREND": trend_v, "STRUCT": struct_v, "FLOW": flow_v, "ZONE": zone_v}

    # HARD GATE — never fire on stale/closed data
    if not A.get("live", True):
        return "WAIT", 0, votes

    buys = sum(1 for v in votes.values() if v == "BUY")
    sells = sum(1 for v in votes.values() if v == "SELL")
    if buys >= 3 and sells == 0:   d, conf = "BUY", buys / 4 * 100
    elif sells >= 3 and buys == 0: d, conf = "SELL", sells / 4 * 100
    elif buys >= 3 and sells == 1: d, conf = "BUY", 68
    elif sells >= 3 and buys == 1: d, conf = "SELL", 68
    else:                          d, conf = "WAIT", max(buys, sells) / 4 * 100

    if d != "WAIT" and not A["tradeable"]:
        d, conf = "WAIT", conf * 0.8

    # news gate — high-impact event imminent → crush conviction
    A["news_flags"] = news_risk(ctx or {})
    if d != "WAIT" and A["news_flags"]:
        d, conf = "WAIT", conf * 0.6

    # squeeze context — chases during expansion + chop get discounted
    if d != "WAIT" and A.get("bb_state") == "EXPANSION" and A.get("regime") == "CHOPPY":
        conf = int(conf * 0.85)

    return d, round(conf), votes

# ---------------------------------------------------------------- LEVELS + RISK
def build_levels(A, d):
    a, p = A["atr"], A["price"]
    if d == "BUY":
        entry = p - 0.1 * a
        sl = min(A["ssl"] or entry - 1.3 * a, entry - 0.8 * a) - 0.25 * a
        t1 = A["bsl"] or entry + 1.5 * a
        t2 = A["bsl2"] or max(entry + 3 * a, t1 + 1.2 * a)   # never equal to T1
    elif d == "SELL":
        entry = p + 0.1 * a
        sl = max(A["bsl"] or entry + 1.3 * a, entry + 0.8 * a) + 0.25 * a
        t1 = A["ssl"] or entry - 1.5 * a
        t2 = A["ssl2"] or min(entry - 3 * a, t1 - 1.2 * a)   # never equal to T1
    else:
        entry = sl = t1 = t2 = p
    return entry, sl, t1, t2

def risk_line(A, entry, sl) -> str:
    dist = abs(entry - sl)
    if dist <= 0: return ""
    risk_usd = ACCOUNT_BALANCE * RISK_PCT / 100
    units = risk_usd / dist
    notional = units * entry
    return (f"Risk {RISK_PCT:g}% of ${ACCOUNT_BALANCE:,.0f} (${risk_usd:,.0f}) → "
            f"size ≈ {units:,.4f} units (≈ ${notional:,.0f} notional)")

def narrative(A: dict, d, conf, votes, audit_str: str, ctx: dict) -> str:
    n, p = A["name"], A["price"]
    e, sl, t1, t2 = build_levels(A, d)
    sgn = "BULLISH" if d == "BUY" else "BEARISH" if d == "SELL" else "NEUTRAL"
    c = ctx.get(n, {})
    fb, fa = nearest_fibs(A["fibs"], p)
    L = [f"<b>WHAT {n} WILL DO NEXT — LIVE ENGINE</b>",
         f"<code>{now_eat().strftime('%H:%M EAT')} | {A['sess']}{' ⚡KILLZONE' if A['killzone'] else ''} | "
         f"FEED: {A['source']} ({int(A['age'])}s old)</code>",
         f"<code>{fp(p, n)}  24h {A['chg24']:+.2f}%  RSI {A['rsi']:.0f}  "
         f"ADX {A['adx']:.0f}  {A['bb_state']}</code>", ""]
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
    if A.get("ob"):
        ob = A["ob"]
        L.append(f"- Order block: <b>{ob['type']}</b> {fp(ob['lo'], n)}–{fp(ob['hi'], n)} "
                 f"(institutional entry zone — watch for retest)")
    L.append(f"- Sweep: {'SSL swept → bullish' if A['sweep_ssl'] else 'BSL swept → bearish' if A['sweep_bsl'] else 'none in last 30m'}")
    if A.get("eqh") or A.get("eql"):
        eq_parts = []
        if A.get("eqh"): eq_parts.append(f"EQH {fp(A['eqh'], n)} (buy-side fuel)")
        if A.get("eql"): eq_parts.append(f"EQL {fp(A['eql'], n)} (sell-side fuel)")
        L.append("- Engineered liquidity: " + " • ".join(eq_parts))
    L.append(f"- Flow: CVD <b>{A['cvd']:+,.0f}</b>, prints {A['bull_n']}bull/{A['bear_n']}bear — {A['flow_msg']}")
    if A.get("bull_div"): L.append("- ⚠️ <b>Bullish CVD divergence</b> — price low, buyers absorbing")
    if A.get("bear_div"): L.append("- ⚠️ <b>Bearish CVD divergence</b> — price high, sellers absorbing")
    L.append(f"- Volatility: ATR {A['atr']:.2f} {A['atr_state']} | BB {A['bb_state']} ({A['bb_pct']:.0f}th pct) | "
             f"Regime: <b>{A['regime']}</b> ({'tradeable' if A['tradeable'] else 'stand aside'})")
    vwap_pos = "above VWAP (bullish bias)" if p > A["vwap"] else "below VWAP (bearish bias)"
    L.append(f"- VWAP: {fp(A['vwap'], n)} — price {vwap_pos}")
    L.append(f"- Zones: {A['zpos']} | Prem {fp(A['prem'], n)} / Eq {fp(A['eq'], n)} / Disc {fp(A['disc'], n)}")
    if fb:
        line = f"- Fib: {fb[0]} @ {fp(fb[1], n)} (support)"
        if fa: line += f" | {fa[0]} @ {fp(fa[1], n)} (resistance)"
        L.append(line)
    if A.get("asian_hi"):
        L.append(f"- Asian range: {fp(A['asian_lo'], n)} – {fp(A['asian_hi'], n)} "
                 f"({'price INSIDE — judas sweep risk at edges' if A['asian_lo'] < p < A['asian_hi'] else 'already resolved'})")
    # live derivatives / microstructure context
    cx = []
    if "funding" in c: cx.append(f"Funding {c['funding']:+.3f}%")
    if "oi_chg" in c: cx.append(f"OI {c['oi_chg']:+.2f}%/5m")
    if "ls_ratio" in c: cx.append(f"L/S {c['ls_ratio']:.2f}")
    if "book" in c: cx.append(f"Book {'bid-heavy' if c['book'] > 0 else 'ask-heavy'} {c['book']:+.1f}%")
    if cx: L.append(f"- Derivatives: {' | '.join(cx)}")
    if n == "BITCOIN" and "fng" in c:
        L.append(f"- Fear & Greed: <b>{c['fng']}</b> ({c.get('fng_label', '?')})")
    if A.get("corr") and abs(A["corr"]) > 0.4:
        L.append(f"- Correlation: r={A['corr']:.2f} (24h) — "
                 f"{'moving together' if A['corr'] > 0 else 'diverging'}")
    for fl in A.get("news_flags", []):
        L.append(f"- {fl}")
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
        rl = risk_line(A, e, sl)
        if rl: L.append(f"<i>{rl}</i>")
    L.append(f"\n🎓 <b>MARKET SECRET:</b> {market_lesson()}")
    L.append(f"<code>{audit_str} • analysis, not financial advice • live {TICK_INTERVAL}s tick</code>")
    return "\n".join(L)

# ---------------------------------------------------------------- PREDICTION LEDGER
class Ledger:
    def __init__(self):
        self.open, self.results = [], deque(maxlen=200)

    def add(self, name, direction, target, invalid, hours=6):
        self.open.append({"name": name, "dir": direction, "target": target,
                          "invalid": invalid, "exp": time.time() + hours * 3600,
                          "made": now_eat().strftime("%H:%M")})

    def check(self, price_by_name):
        done, expired = [], []
        for pr in self.open:
            p = price_by_name.get(pr["name"], 0)
            if not p: continue
            resolved = False
            if pr["dir"] == "BUY":
                if p >= pr["target"]:
                    self.results.append(True); done.append((pr, "HIT ✓")); resolved = True
                elif p <= pr["invalid"]:
                    self.results.append(False); done.append((pr, "INVALIDATED ✗")); resolved = True
            else:
                if p <= pr["target"]:
                    self.results.append(True); done.append((pr, "HIT ✓")); resolved = True
                elif p >= pr["invalid"]:
                    self.results.append(False); done.append((pr, "INVALIDATED ✗")); resolved = True
            if not resolved and time.time() > pr["exp"]:
                expired.append((pr, "EXPIRED ⏳ (no resolution — not scored)"))
        done_names = {id(d[0]) for d in done} | {id(x[0]) for x in expired}
        self.open = [pr for pr in self.open if id(pr) not in done_names]
        return done + expired

    def audit_str(self):
        recent = list(self.results)[-20:]
        if not recent: return "SELF-AUDIT: building track record (0 scored yet)"
        return (f"SELF-AUDIT: {sum(recent)}/{len(recent)} predictions correct "
                f"({sum(recent)/len(recent)*100:.0f}%)")

# ---------------------------------------------------------------- TRACKER
class Tracker:
    def __init__(self): self.active = {}

    def arm(self, A, d, conf, entry, sl, t1, t2):
        self.active[A["name"]] = {"d": d, "conf": conf, "entry": entry, "sl": sl,
                                  "t1": t1, "t2": t2, "t1_hit": False,
                                  "opened": now_eat().strftime("%H:%M")}

    def check(self, A) -> list[str]:
        s = self.active.get(A["name"]); out = []
        if not s or not A["full"] or not A.get("live", True): return out
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
    fig, (ax, axv) = plt.subplots(2, 1, figsize=(11, 7), dpi=100, sharex=True,
                                  gridspec_kw={"height_ratios": [4, 1]})
    fig.patch.set_facecolor("#131722")
    for a_ in (ax, axv): a_.set_facecolor("#131722")
    up, dn = "#26a69a", "#ef5350"
    for i, (_, r) in enumerate(df.iterrows()):
        c = up if r.c >= r.o else dn
        ax.vlines(i, r.l, r.h, color=c, lw=1)
        ax.bar(i, r.c - r.o, bottom=r.o, width=0.6, color=c, edgecolor=c, zorder=3)
        axv.bar(i, r.v if pd.notna(r.v) else 0, width=0.6, color=c, alpha=0.6)
    n = A["name"]
    if A.get("ob"):
        ob = A["ob"]
        col = "#26a69a" if ob["type"] == "BULLISH" else "#ef5350"
        ax.axhspan(ob["lo"], ob["hi"], color=col, alpha=0.12)
        ax.text(1, ob["hi"], f"OB {ob['type']}", color=col, fontsize=8, va="bottom")
    if A.get("vwap"):
        ax.axhline(A["vwap"], color="#42a5f5", ls="-.", lw=1.2, label="VWAP")
    for lv, col, ls, lab in [(entry, "#ffd54f", "-", "ENTRY"), (sl, "#ef5350", "--", "SL"),
                             (t1, "#26a69a", "--", "T1"), (t2, "#26a69a", "--", "T2")]:
        if lv and lv != A["price"]:
            ax.axhline(lv, color=col, ls=ls, lw=1.4, label=lab)
    for lv in (A["bsl"], A["ssl"]):
        if lv: ax.axhline(lv, color="#8d6e63", ls=":", lw=1.2)
    ax.set_title(f"{n} 15m — {d} | {A['regime']} | {A['sess']} | {A['source']}",
                 color="white", fontsize=12)
    for a_ in (ax, axv):
        a_.tick_params(colors="white")
        for s in a_.spines.values(): s.set_color("#444")
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

# ---------------------------------------------------------------- CORRELATION
async def corr_worker(btc: CandleStore, gold: CandleStore, paxg: CandleStore, out: dict):
    """BTC↔spot gold when market is open; BTC↔PAXG on weekends (both live)."""
    while True:
        try:
            ref = gold if (gold_market_open() and gold.data_age() < STALE_FEED_SEC) else paxg
            b = btc.df("5min", 288).c
            g = ref.df("5min", 288).c
            if len(b) > 50 and len(g) > 50:
                m = min(len(b), len(g))
                c = float(np.corrcoef(b.iloc[-m:].values, g.iloc[-m:].values)[0, 1])
                if not np.isnan(c):
                    out["val"] = c
                    out["ref"] = ref.name
                    log.info(f"Corr BTC↔{ref.name} (24h): {c:.2f}")
        except Exception as e:
            log.error(f"corr: {e}")
        await asyncio.sleep(300)

# ---------------------------------------------------------------- DAY OUTLOOK
def day_outlook(A: dict, ledger: Ledger, ctx: dict) -> str:
    n = A["name"]; a = A["atr"]
    d, conf, votes = swarm(A, ctx)
    e, sl, t1, t2 = build_levels(A, d)
    L = [f"<b>🌍 AUTONOMOUS DAY OUTLOOK — {n}</b>",
         f"<code>{now_eat().strftime('%H:%M EAT')} | {A['sess']} | Regime {A['regime']} | "
         f"Vol {A['atr_state']} | BB {A['bb_state']} | Align {A['align']} ({A['strength']}/3) | "
         f"{A['source']}</code>", ""]
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
    if A.get("asian_hi"):
        L.append(f"\n<b>JUDAS CONTEXT:</b> Asian range {fp(A['asian_lo'], n)} – {fp(A['asian_hi'], n)}. "
                 f"London's first move loves to sweep one edge before the true direction.")
    fl = A.get("news_flags") or news_risk(ctx)
    if fl:
        L.append("\n<b>⚠️ NEWS RISK:</b>")
        L += [f"- {x}" for x in fl]
    L.append("")
    L.append("<b>SESSION PLAN (EAT):</b>")
    L.append("- 08:00–10:00 London open: first manipulation move, watch for sweep of Asian high/low")
    L.append("- 13:00–17:00 NY KILLZONE: the real directional move of the day usually lives here")
    L.append("- After 17:00: reduce expectations, NY afternoon = profit-taking flows")
    if A.get("corr") and abs(A["corr"]) > 0.4:
        L.append(f"\n<b>MACRO LINK:</b> r={A['corr']:.2f} — "
                 f"{'they move together; a flush in one likely drags the other' if A['corr'] > 0 else 'they are diverging; relative strength tells the story'}")
    L.append(f"\n🎓 <b>MARKET SECRET:</b> {market_lesson()}")
    L.append(f"<code>{ledger.audit_str()} • scenarios graded automatically at expiry</code>")
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

# ---------------------------------------------------------------- TELEGRAM COMMAND CONSOLE
HELP = ("<b>BRAX v4 COMMANDS</b>\n"
        "/status — all desks: price, feed, regime, desk bias\n"
        "/desk BITCOIN|GOLD|PAXG — force a full desk drop now\n"
        "/positions — active tracked signals\n"
        "/health — feed ages + news window\n"
        "/lesson — random market secret\n"
        "/school — all 12 lessons\n"
        "/help — this menu")

async def command_worker(stores, paxg, corr, ctx, ledger, tracker, tg, ses, state):
    offset = 0
    while True:
        try:
            async with ses.get(f"{tg.base}/getUpdates?timeout=25&offset={offset}") as r:
                d = await r.json()
            for u in d.get("result", []):
                offset = u["update_id"] + 1
                msg = u.get("message") or {}
                if str(msg.get("chat", {}).get("id", "")) != str(CHAT_ID): continue
                text = (msg.get("text") or "").strip()
                if not text.startswith("/"): continue
                cmd, *args = text.split()
                cmd = cmd.split("@")[0].lower()
                if cmd in ("/help", "/start"):
                    await tg.text(ses, HELP)
                elif cmd == "/lesson":
                    await tg.text(ses, f"🎓 <b>MARKET SECRET:</b> {random.choice(LESSONS)}")
                elif cmd == "/school":
                    await tg.text(ses, "<b>🎓 BRAX MARKET SCHOOL — 12 SECRETS</b>\n" +
                                  "\n\n".join(f"<b>{i+1}.</b> {x}" for i, x in enumerate(LESSONS)))
                elif cmd == "/positions":
                    if not tracker.active:
                        await tg.text(ses, "No active tracked signals.")
                    else:
                        lines = [f"<b>{n}</b> {s['d']} {s['conf']}% (opened {s['opened']})\n"
                                 f"Entry {fp(s['entry'], n)} | SL {fp(s['sl'], n)} | "
                                 f"T1 {fp(s['t1'], n)}{' ✓' if s['t1_hit'] else ''} | T2 {fp(s['t2'], n)}"
                                 for n, s in tracker.active.items()]
                        await tg.text(ses, "<b>📡 ACTIVE SIGNALS</b>\n" + "\n\n".join(lines))
                elif cmd == "/health":
                    lines = [f"{st.name}: {st.source} | price {fp(st.price, st.name)} | "
                             f"age {int(st.data_age())}s" for st in stores.values()]
                    nw = ctx.get("news", [])
                    news = ("\nNews: " + "; ".join(f"{x['title']} {x['at'].strftime('%H:%M EAT')}"
                                                   for x in nw)) if nw else "\nNews: clear (6h)"
                    await tg.text(ses, "<b>🩺 FEED HEALTH</b>\n" + "\n".join(lines) + news)
                elif cmd in ("/status", "/desk"):
                    if cmd == "/desk" and args:
                        want = args[0].upper()
                        if want in state["last_desk"]:
                            state["last_desk"][want] = 0.0
                            await tg.text(ses, f"Queueing full desk drop for {want}…")
                            continue
                    lines = []
                    for n, st in stores.items():
                        if st.price <= 0: continue
                        if n == "GOLD" and not gold_market_open():
                            lines.append(f"<b>{n}</b>: SPOT CLOSED — reopens "
                                         f"{gold_next_open_eat().strftime('%a %H:%M EAT')} | "
                                         f"PAXG live @ {fp(paxg.price, 'PAXG')}")
                            continue
                        A = analyze(st, paxg if n == "GOLD" else None, corr.get("val", 0.0))
                        if A["full"]:
                            dd, cc, _ = swarm(A, ctx)
                            lines.append(f"<b>{n}</b> {fp(st.price, n)} ({A['chg24']:+.2f}%) | "
                                         f"{A['regime']} | {A['bb_state']} | DESK: <b>{dd} {cc}%</b> | "
                                         f"{st.source} {int(st.data_age())}s")
                        else:
                            lines.append(f"<b>{n}</b>: warming ({int(st.data_age())}s)")
                    await tg.text(ses, "<b>📊 DESK STATUS</b>\n" + "\n".join(lines))
        except Exception as e:
            log.error(f"cmd: {e}")
            await asyncio.sleep(5)

# ---------------------------------------------------------------- LIVE ENGINE
async def live_loop(stores, paxg, corr, ledger, tracker, tg, ses, ctx, state):
    last_desk = state["last_desk"]
    last_event, last_event_t = {}, {}
    outlook_done = set()
    last_gold_status = 0.0
    last_watchdog = 0.0

    await tg.text(ses, "<b>🛰 BRAX SMART DESK v4 — LIVE ENGINE ONLINE</b>\n"
                       "BTC + PAXG: WS + REST fallback, 33h bootstrapped history (24/7)\n"
                       "GOLD: XAU/USD spot w/ market-hours gate (PAXG = weekend gold desk)\n"
                       "New: funding/OI/L-S ratio • order-book imbalance • VWAP • order blocks\n"
                       "EQH/EQL • Fib grid • BB squeeze • news gate • risk sizing • commands\n"
                       "<code>/help for the command console</code>")

    while True:
        try:
            gold_open = gold_market_open()
            prices = {n: st.price for n, st in stores.items()}
            for pr, status in ledger.check(prices):
                await tg.text(ses, f"<b>📊 LEDGER:</b> {pr['name']} {pr['dir']} "
                                   f"(made {pr['made']}) → <b>{status}</b>\n"
                                   f"<code>{ledger.audit_str()}</code>")

            # autonomous outlook at London open (08:00) and NY killzone (13:00) EAT
            t = now_eat()
            key = (t.date(), t.hour)
            if t.hour in (8, 13) and key not in outlook_done:
                outlook_done.add(key)
                for n in ("BITCOIN", "GOLD", "PAXG"):
                    if n == "GOLD" and not gold_open: continue
                    st = stores[n]
                    if st.price <= 0: continue
                    A = analyze(st, paxg if n == "GOLD" else None, corr.get("val", 0.0))
                    if A["full"]:
                        await tg.text(ses, day_outlook(A, ledger, ctx))
                        d, conf, _ = swarm(A, ctx)
                        v = (f"Autonomous day outlook for {n}. Primary scenario {d} with "
                             f"{conf} percent conviction." if d != "WAIT" else
                             f"Autonomous day outlook for {n}. Primary scenario is range rotation.")
                        await voice_note(ses, tg, v)

            for n, st in stores.items():
                if st.price <= 0: continue

                # GOLD spot closed: honest status, zero fake signals
                if n == "GOLD" and not gold_open:
                    if time.time() - last_gold_status >= GOLD_STATUS_INTERVAL:
                        last_gold_status = time.time()
                        px = gold.price or paxg.price
                        await tg.text(ses,
                            f"<b>🟡 GOLD — XAU/USD SPOT MARKET CLOSED</b>\n"
                            f"Last close: <b>{fp(px, 'GOLD')}</b> ({gold.source})\n"
                            f"Spot reopens: <b>{gold_next_open_eat().strftime('%a %H:%M EAT')}</b>\n"
                            f"Weekend 'gold moves' are stale Friday quotes — signals suppressed.\n"
                            f"<b>Live alternative:</b> PAXG desk @ {fp(paxg.price, 'PAXG')} "
                            f"(24/7 tokenized gold)\n<code>{ledger.audit_str()}</code>")
                    continue

                A = analyze(st, paxg if n == "GOLD" else None, corr.get("val", 0.0))
                if not A["full"]:
                    if time.time() - last_desk[n] >= DESK_INTERVAL:
                        last_desk[n] = time.time()
                        await tg.text(ses, f"<b>{n}</b>: warming up — {len(st.df('15min'))}/20 15m bars "
                                           f"({A['source']}, price {fp(st.price, n)})")
                    continue

                # TP/SL tracker (fires instantly)
                for msg in tracker.check(A):
                    await tg.text(ses, msg)
                    await voice_note(ses, tg, msg.replace("🎯", "")
                                          .replace("🏁", "").replace("🛑", ""))

                # instant event alerts (BOS change / liquidity sweep)
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
                                     + f"\n<code>{now_eat().strftime('%H:%M:%S EAT')} • feed {A['source']}</code>")
                    last_event_t[n] = time.time()
                last_event[n] = ev

                # full desk drop every 15 min per asset
                if time.time() - last_desk[n] >= DESK_INTERVAL:
                    last_desk[n] = time.time()
                    d, conf, votes = swarm(A, ctx)
                    await tg.text(ses, narrative(A, d, conf, votes, ledger.audit_str(), ctx))
                    if d != "WAIT" and n not in tracker.active and conf >= 68:
                        e, sl, t1, t2 = build_levels(A, d)
                        tracker.arm(A, d, conf, e, sl, t1, t2)
                        path = f"/tmp/{n.replace('/', '').replace(' ', '')}.png"
                        await asyncio.to_thread(make_chart, A, d, e, sl, t1, t2, path)
                        await tg.photo(ses, path, f"{n} — {d} {conf}% | {A['sess']} | {A['regime']}")
                        if os.path.exists(path): os.remove(path)
                        await voice_note(ses, tg, voice_brief(A, d, conf))
                    log.info(f"Desk → {n} | {d} {conf}% | feed {A['source']}")

            # watchdog: warn if any live feed degraded
            if time.time() - last_watchdog >= WATCHDOG_INTERVAL:
                last_watchdog = time.time()
                bad = [n for n, st in stores.items()
                       if (n != "GOLD" or gold_open) and st.data_age() > (180 if st.ws_sym else STALE_FEED_SEC)]
                if bad:
                    await tg.text(ses, f"⚠️ <b>WATCHDOG:</b> stale feed(s): {', '.join(bad)} — "
                                       f"REST fallback active or upstream down. Signals gated for safety.")
        except Exception as e:
            log.error(f"live_loop: {e}")
        await asyncio.sleep(TICK_INTERVAL)

# ---------------------------------------------------------------- KEEPALIVE
async def keepalive(ses):
    if not SELF_URL: return
    while True:
        try: await ses.get(SELF_URL)
        except Exception: pass
        await asyncio.sleep(600)

# ---------------------------------------------------------------- MAIN
async def main():
    btc  = CandleStore("BITCOIN", ws_sym="btcusdt")
    paxg = CandleStore("PAXG",    ws_sym="paxgusdt")
    gold = CandleStore("GOLD")
    corr, ctx = {"val": 0.0, "ref": "—"}, {}
    ledger, tracker = Ledger(), Tracker()
    tg = TG()
    state = {"last_desk": {n: 0.0 for n in ("BITCOIN", "GOLD", "PAXG")}}

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25)) as ses:
        await bootstrap_crypto([btc, paxg], ses)   # ~33h real history → full analysis at boot
        await bootstrap_gold(gold, ses)

        stores = {"BITCOIN": btc, "GOLD": gold, "PAXG": paxg}
        tasks = [asyncio.create_task(binance_worker([btc, paxg])),
                 asyncio.create_task(crypto_rest_fallback([btc, paxg], ses)),
                 asyncio.create_task(gold_worker(gold, ses)),
                 asyncio.create_task(context_worker(ctx, ses)),
                 asyncio.create_task(news_worker(ctx, ses)),
                 asyncio.create_task(corr_worker(btc, gold, paxg, corr)),
                 asyncio.create_task(command_worker(stores, paxg, corr, ctx,
                                                    ledger, tracker, tg, ses, state)),
                 asyncio.create_task(keepalive(ses))]
        try:
            await live_loop(stores, paxg, corr, ledger, tracker, tg, ses, ctx, state)
        finally:
            for t in tasks:
                t.cancel()

# ---------------------------------------------------------------- FLASK + ENTRY
app = Flask(__name__)

@app.route("/")
def health():
    return "BRAX SMART DESK v4 — LIVE ENGINE RUNNING", 200

def run_flask():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)), use_reloader=False)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    asyncio.run(main())
