"""
BRAX TERMINAL v5 — Real-time market observation terminal
=========================================================
PHILOSOPHY: Reports what IS, never what will be.
  No signals, no targets, no predictions — pure real-time market data.

Live feeds (all real):
  • Binance WS: 1m klines + aggTrades (BTC, PAXG) — 24/7, REST bootstrap + fallback
  • Binance REST: order-book depth, funding, open interest, long/short ratio
  • TwelveData: XAU/USD 1-min (spot-hours aware) + goldprice.org backup
  • alternative.me: Fear & Greed   • ForexFactory: high-impact calendar

Outputs (facts only):
  • Compact market snapshot every 10 min (price, flow, derivatives, volatility)
  • Factual event notices: new session high/low, funding flip, volatility shift
  • On-demand: /t /book /flow /derivs /vol /corr /news /health /chart /help

Deploy:  Render → Start Command: python main.py
Env:     TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, TWELVEDATA_API_KEY
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

SNAPSHOT_INTERVAL   = 10 * 60     # compact snapshot cadence
TICK_INTERVAL       = 15
GOLD_POLL           = 120
GOLD_POLL_CLOSED    = 900
CTX_INTERVAL        = 120
NEWS_INTERVAL       = 1800
STALE_CRYPTO_SEC    = 90
STALE_FEED_SEC      = 300
EAT = pytz.timezone("Africa/Nairobi")

for v in (TOKEN, CHAT_ID, TD_KEY):
    if not v: raise ValueError("Missing TELEGRAM_TOKEN / TELEGRAM_CHAT_ID / TWELVEDATA_API_KEY")

def now_eat(): return datetime.now(EAT)

def session_now() -> str:
    h = now_eat().hour + now_eat().minute / 60
    if 13 <= h < 17: return "NY KILLZONE"
    if 17 <= h < 21: return "NY AFTERNOON"
    if 8 <= h < 13:  return "LONDON"
    if 2 <= h < 8:   return "ASIAN"
    return "OFF-HOURS"

# ---------------------------------------------------------------- XAU SPOT HOURS (facts)
def gold_market_open() -> bool:
    now = datetime.now(pytz.utc)
    wd, m = now.weekday(), now.hour * 60 + now.minute
    if wd == 5:                                       return False
    if wd == 6 and m < 22 * 60 + 1:                   return False
    if wd == 4 and m >= 22 * 60:                      return False
    if wd in (0, 1, 2, 3) and 21 * 60 <= m < 22 * 60: return False
    return True

def gold_next_open_eat() -> datetime:
    now = datetime.now(pytz.utc)
    days = (6 - now.weekday()) % 7
    if days == 0 and now.hour * 60 + now.minute >= 22 * 60:
        days = 7
    t = (now + timedelta(days=days)).replace(hour=22, minute=0, second=0, microsecond=0)
    return t.astimezone(EAT)

# ---------------------------------------------------------------- INDICATORS (descriptive)
def atr(df: pd.DataFrame, n=14) -> float:
    tr = pd.concat([df.h - df.l, (df.h - df.c.shift()).abs(),
                    (df.l - df.c.shift()).abs()], axis=1).max(axis=1)
    return float(tr.ewm(alpha=1/n, adjust=False).mean().iloc[-1])

def realized_vol_pct(df: pd.DataFrame, bars=96) -> float:
    """Annualized realized volatility from 15m log returns (factual)."""
    c = df.c.tail(bars + 1)
    if len(c) < 30: return 0.0
    r = np.log(c / c.shift()).dropna()
    return float(r.std() * np.sqrt(len(r) / (bars + 1) * 96 * 365) * 100)

def session_vwap(df1: pd.DataFrame) -> float:
    if df1.empty: return 0.0
    today = df1[df1.index >= df1.index[-1].floor("D")]
    if today.empty: today = df1.tail(240)
    tp = (today.h + today.l + today.c) / 3
    v = today.v
    if float(v.sum()) <= 0: return float(tp.mean())
    return float((tp * v).sum() / v.sum())

def swings(df, k=3):
    sh, sl, h, l = [], [], df.h.values, df.l.values
    for i in range(k, len(df) - k):
        if h[i] >= h[i-k:i+k+1].max(): sh.append((i, float(h[i])))
        if l[i] <= l[i-k:i+k+1].min(): sl.append((i, float(l[i])))
    return sh, sl

def fp(x, name):
    return f"${x:,.0f}" if "BTC" in name else f"${x:,.2f}"

def fmt_signed(x, dp=0, suffix=""):
    return f"{x:+,.{dp}f}{suffix}"

# ---------------------------------------------------------------- CANDLE STORE
class CandleStore:
    def __init__(self, name, ws_sym=None):
        self.name, self.ws_sym = name, ws_sym
        self._c = {}                          # minute_epoch -> [o,h,l,c,v]
        self._df, self._df_ts = None, 0.0
        self.price, self.day_open = 0.0, None
        self.cvd_ticks = deque(maxlen=20000)  # (ts, signed_vol)
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
        cutoff = (time.time() - 3 * 86400) // 60   # keys are MINUTE epochs
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

    def cvd_window(self, lo_s, hi_s) -> float:
        return sum(v for ts, v in self.cvd_ticks if lo_s <= ts <= hi_s)

    def trade_count(self, window=60) -> tuple[int, int]:
        cut = time.time() - window
        bulls = sum(1 for ts, v in self.cvd_ticks if ts >= cut and v > 0)
        bears = sum(1 for ts, v in self.cvd_ticks if ts >= cut and v < 0)
        return bulls, bears

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
    while True:
        await asyncio.sleep(30)
        for st in stores:
            if not st.ws_sym: continue
            if time.time() - st.last_update < STALE_CRYPTO_SEC: continue
            sym = st.ws_sym.upper()
            try:
                log.warning(f"{st.name}: feed silent → REST fallback")
                async with ses.get(f"{BINANCE_REST}/klines?symbol={sym}&interval=1m&limit=3") as r:
                    kl = await r.json()
                if isinstance(kl, list) and kl:
                    for k in kl:
                        st.ingest_kline_tuple(int(k[0]) // 60000, float(k[1]), float(k[2]),
                                              float(k[3]), float(k[4]), float(k[5]))
                    st.source = "BINANCE REST"
            except Exception as e:
                log.error(f"REST fallback {st.name}: {e}")

async def bootstrap_crypto(stores: list[CandleStore], ses: aiohttp.ClientSession):
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
                    kl_all = kl2 + kl_all
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
                log.info(f"{st.name}: flow seeded with {len(tr)} trades")
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
                except Exception: pass
                try:
                    async with ses.get(f"{BINANCE_FAPI}/fapi/v1/openInterest?symbol={fsym}") as r:
                        d = await r.json()
                    c["oi"] = float(d.get("openInterest", 0))
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
                    spread = (float(d["asks"][0][0]) - float(d["bids"][0][0])) \
                             / float(d["asks"][0][0]) * 10000 if d.get("bids") and d.get("asks") else 0
                    if bid + ask > 0:
                        c["book"] = (bid - ask) / (bid + ask) * 100
                        c["spread_bp"] = spread
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
                if timedelta(0) <= dt - now <= timedelta(hours=24):
                    upcoming.append({"title": e.get("title", "?"),
                                     "at": dt.astimezone(EAT),
                                     "in_h": (dt - now).total_seconds() / 3600})
            upcoming.sort(key=lambda x: x["in_h"])
            ctx["news"] = upcoming[:5]
        except Exception as e:
            log.error(f"news: {e}")
        await asyncio.sleep(NEWS_INTERVAL)

async def corr_worker(btc: CandleStore, gold: CandleStore, paxg: CandleStore, out: dict):
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
        except Exception as e:
            log.error(f"corr: {e}")
        await asyncio.sleep(300)

# ---------------------------------------------------------------- MEASUREMENT (facts only)
def measure(st: CandleStore) -> dict:
    M = {"name": st.name, "price": st.price, "source": st.source,
         "age": st.data_age(), "full": False}
    if st.price <= 0: return M
    df15 = st.df("15min", 96)
    if len(df15) < 20: return M
    M["full"] = True
    M["chg_day"] = ((st.price - st.day_open) / st.day_open * 100) if st.day_open else 0.0
    M["hi24"], M["lo24"] = float(df15.h.max()), float(df15.l.min())
    M["atr"] = atr(df15)
    M["atr_prev"] = atr(df15.iloc[:-12]) if len(df15) > 32 else M["atr"]
    M["rvol"] = realized_vol_pct(df15)
    M["vwap"] = session_vwap(st.df("1min"))
    M["vwap_dev"] = ((st.price - M["vwap"]) / M["vwap"] * 100) if M["vwap"] else 0.0
    M["cvd_30m"] = st.cvd(1800)
    M["cvd_30m_prev"] = st.cvd_window(time.time() - 3600, time.time() - 1800)
    M["vol_1h"] = float(df15.v.tail(4).sum())
    M["vol_prev_1h"] = float(df15.v.tail(8).head(4).sum())
    M["range_pos"] = ((st.price - M["lo24"]) / (M["hi24"] - M["lo24"]) * 100) \
                     if M["hi24"] > M["lo24"] else 50.0
    sh, sl = swings(df15, 3)
    M["swing_hi"] = [p for _, p in sh][-3:]
    M["swing_lo"] = [p for _, p in sl][-3:]
    return M

def fact_line(M: dict) -> str:
    """One compact factual line per asset."""
    n = M["name"]
    if not M["full"]:
        return f"<b>{n}</b> {fp(M['price'], n)} — warming ({M['source']}, {int(M['age'])}s)"
    p = M["price"]
    cvd_d = M["cvd_30m"] - M["cvd_30m_prev"]
    vol_d = ((M["vol_1h"] - M["vol_prev_1h"]) / M["vol_prev_1h"] * 100) \
            if M["vol_prev_1h"] > 0 else 0.0
    atr_d = (M["atr"] / M["atr_prev"] - 1) * 100 if M["atr_prev"] else 0.0
    return (f"<b>{n}</b> {fp(p, n)} ({fmt_signed(M['chg_day'], 2, '%')} d)\n"
            f"24h {fp(M['lo24'], n)}–{fp(M['hi24'], n)} | range pos {M['range_pos']:.0f}%\n"
            f"VWAP {fp(M['vwap'], n)} ({fmt_signed(M['vwap_dev'], 2, '%')}) | "
            f"ATR15 {M['atr']:.2f} ({fmt_signed(atr_d, 0, '%')}) | RVol {M['rvol']:.0f}% ann.\n"
            f"CVD 30m {fmt_signed(M['cvd_30m'])} (Δ vs prev 30m: {fmt_signed(cvd_d)}) | "
            f"volume 1h {fmt_signed(vol_d, 0, '%')}")

# ---------------------------------------------------------------- SNAPSHOT (compact, factual)
def snapshot(stores: dict, ctx: dict, corr: dict, paxg: CandleStore) -> str:
    L = [f"<b>📡 MARKET SNAPSHOT</b> <code>{now_eat().strftime('%H:%M EAT')} | {session_now()}</code>", ""]
    for n, st in stores.items():
        if n == "GOLD" and not gold_market_open():
            if st.price > 0:
                L.append(f"<b>GOLD</b> {fp(st.price, 'GOLD')} — spot closed "
                         f"(reopens {gold_next_open_eat().strftime('%a %H:%M EAT')})")
                if paxg.price > 0:
                    L.append(f"PAXG (24/7 proxy): {fp(paxg.price, 'PAXG')} | "
                             f"basis vs XAU close {fmt_signed(paxg.price - st.price, 2)}")
            continue
        M = measure(st)
        L.append(fact_line(M))
        c = ctx.get(n, {})
        cx = []
        if "funding" in c: cx.append(f"funding {c['funding']:+.4f}%")
        if "oi" in c and c["oi"]: cx.append(f"OI {c['oi']:,.0f}")
        if "ls_ratio" in c: cx.append(f"L/S {c['ls_ratio']:.2f}")
        if "book" in c: cx.append(f"book {fmt_signed(c['book'], 1, '%')}")
        if "spread_bp" in c: cx.append(f"spread {c['spread_bp']:.1f}bp")
        if cx: L.append("derivs: " + " | ".join(cx))
        if n == "BITCOIN" and "fng" in c:
            L.append(f"Fear&Greed: {c['fng']} ({c.get('fng_label', '')})")
        L.append("")
    if corr.get("ref"):
        L.append(f"Corr BTC↔{corr['ref']} (24h): {corr.get('val', 0):.2f}")
    nw = ctx.get("news", [])
    if nw:
        L.append("Next high-impact: " + "; ".join(
            f"{x['title']} in {x['in_h']:.0f}h" for x in nw[:3]))
    return "\n".join(L)

# ---------------------------------------------------------------- FACTUAL EVENT WATCH
class EventWatch:
    """Notices factual state changes only — no interpretation."""
    def __init__(self):
        self.hi_lo = {}        # name -> (session_hi, session_lo)
        self.funding = {}      # name -> last funding sign

    def check(self, M: dict, ctx: dict) -> list[str]:
        out = []
        n = M["name"]
        if not M["full"]: return out
        hi, lo = self.hi_lo.get(n, (None, None))
        if hi is None or M["hi24"] > hi:
            out.append(f"📈 {n}: new 24h high {fp(M['hi24'], n)}")
        if lo is None and M["lo24"] is not None and (lo is None or M["lo24"] < lo):
            out.append(f"📉 {n}: new 24h low {fp(M['lo24'], n)}")
        self.hi_lo[n] = (max(hi or M["hi24"], M["hi24"]), min(lo or M["lo24"], M["lo24"]))
        f = ctx.get(n, {}).get("funding")
        if f is not None:
            prev = self.funding.get(n)
            if prev is not None and (prev >= 0) != (f >= 0):
                out.append(f"🔁 {n}: funding flipped {'positive' if f >= 0 else 'negative'} "
                           f"({f:+.4f}%)")
            self.funding[n] = f
        return out

# ---------------------------------------------------------------- CHART (factual)
def make_chart(st: CandleStore, path):
    df = st.df("15min", 80)
    fig, (ax, axv) = plt.subplots(2, 1, figsize=(11, 6), dpi=100, sharex=True,
                                  gridspec_kw={"height_ratios": [4, 1]})
    fig.patch.set_facecolor("#131722")
    for a_ in (ax, axv): a_.set_facecolor("#131722")
    up, dn = "#26a69a", "#ef5350"
    for i, (_, r) in enumerate(df.iterrows()):
        c = up if r.c >= r.o else dn
        ax.vlines(i, r.l, r.h, color=c, lw=1)
        ax.bar(i, r.c - r.o, bottom=r.o, width=0.6, color=c, edgecolor=c, zorder=3)
        axv.bar(i, r.v if pd.notna(r.v) else 0, width=0.6, color=c, alpha=0.6)
    for s in (ax, axv):
        s.tick_params(colors="white")
        for sp in s.spines.values(): sp.set_color("#444")
    ax.set_title(f"{st.name} 15m | {now_eat().strftime('%H:%M EAT')} | {st.source}",
                 color="white", fontsize=12)
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

# ---------------------------------------------------------------- COMMANDS
HELP = ("<b>BRAX TERMINAL v5 — COMMANDS</b>\n"
        "/t — instant tick: all assets, facts only\n"
        "/book BITCOIN|PAXG — order-book imbalance + spread\n"
        "/flow BITCOIN|PAXG — CVD detail, aggressor counts\n"
        "/derivs — funding, OI, long/short ratio\n"
        "/vol — volatility panel (ATR, realized vol, range pos)\n"
        "/corr — BTC↔gold correlation (24h)\n"
        "/news — upcoming high-impact calendar\n"
        "/chart BTC|PAXG|GOLD — 15m candles + volume\n"
        "/health — feed status\n"
        "/help — this menu")

def want_store(args, stores, paxg):
    if not args: return None
    w = args[0].upper()
    if w in ("BTC", "BITCOIN"): return stores["BITCOIN"]
    if w == "PAXG": return paxg
    if w == "GOLD": return stores["GOLD"]
    return None

async def command_worker(stores, paxg, corr, ctx, tg, ses):
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

                elif cmd == "/t":
                    await tg.text(ses, snapshot(stores, ctx, corr, paxg))

                elif cmd == "/book":
                    st = want_store(args, stores, paxg)
                    if not st or not st.ws_sym:
                        await tg.text(ses, "Usage: /book BITCOIN or /book PAXG")
                        continue
                    c = ctx.get(st.name, {})
                    lines = [f"<b>{st.name} ORDER BOOK</b> @ {fp(st.price, st.name)}"]
                    if "book" in c:
                        lines.append(f"Top-100 notional imbalance: {fmt_signed(c['book'], 1, '%')} "
                                     f"({'bid-side heavier' if c['book'] > 0 else 'ask-side heavier' if c['book'] < 0 else 'balanced'})")
                    if "spread_bp" in c:
                        lines.append(f"Best spread: {c['spread_bp']:.2f} bp")
                    lines.append(f"<i>Snapshot of resting orders — resting orders are not trades.</i>")
                    await tg.text(ses, "\n".join(lines))

                elif cmd == "/flow":
                    st = want_store(args, stores, paxg)
                    if not st or not st.ws_sym:
                        await tg.text(ses, "Usage: /flow BITCOIN or /flow PAXG")
                        continue
                    if not st.cvd_ticks:
                        await tg.text(ses, f"{st.name}: flow feed still warming up.")
                        continue
                    bulls, bears = st.trade_count(60)
                    now = time.time()
                    windows = [("5m", 300), ("30m", 1800), ("1h", 3600), ("3h", 10800)]
                    lines = [f"<b>{st.name} ORDER FLOW</b> @ {fp(st.price, st.name)}",
                             f"Last 60s aggressors: {bulls} buy / {bears} sell", ""]
                    for lab, w in windows:
                        cw = st.cvd(w)
                        pw = st.cvd_window(now - 2 * w, now - w)
                        lines.append(f"CVD {lab}: {fmt_signed(cw)} | prev window: {fmt_signed(pw)} "
                                     f"| Δ {fmt_signed(cw - pw)}")
                    lines.append("")
                    lines.append("<i>Cumulative volume delta from real aggressor trades. "
                                 "Rising = market buys dominate; falling = market sells dominate.</i>")
                    await tg.text(ses, "\n".join(lines))

                elif cmd == "/derivs":
                    lines = ["<b>DERIVATIVES PANEL</b>"]
                    for n in ("BITCOIN", "PAXG"):
                        c = ctx.get(n, {})
                        parts = []
                        if "funding" in c: parts.append(f"funding {c['funding']:+.4f}%/8h")
                        if "oi" in c and c["oi"]: parts.append(f"OI {c['oi']:,.0f}")
                        if "ls_ratio" in c: parts.append(f"accounts L/S {c['ls_ratio']:.2f}")
                        lines.append(f"<b>{n}</b>: " + (" | ".join(parts) if parts else "loading…"))
                    lines.append("")
                    lines.append("<i>Funding = periodic payment between longs and shorts. "
                                 "Positive: longs pay shorts. Negative: shorts pay longs.</i>")
                    await tg.text(ses, "\n".join(lines))

                elif cmd == "/vol":
                    lines = [f"<b>VOLATILITY PANEL</b> <code>{now_eat().strftime('%H:%M EAT')}</code>", ""]
                    for n in ("BITCOIN", "PAXG"):
                        M = measure(stores[n])
                        if M["full"]:
                            lines.append(f"<b>{n}</b> ATR15 {M['atr']:.2f} | "
                                         f"RVol {M['rvol']:.0f}% ann. | "
                                         f"24h range {M['range_pos']:.0f}% traversed")
                    await tg.text(ses, "\n".join(lines))

                elif cmd == "/corr":
                    if corr.get("ref"):
                        await tg.text(ses, f"<b>CORRELATION</b>\nBTC ↔ {corr['ref']} (24h, 5m closes): "
                                           f"r = {corr.get('val', 0):.2f}")
                    else:
                        await tg.text(ses, "Correlation warming up.")

                elif cmd == "/news":
                    nw = ctx.get("news", [])
                    if not nw:
                        await tg.text(ses, "No high-impact USD events scheduled in the next 24h.")
                    else:
                        lines = ["<b>📅 HIGH-IMPACT UPCOMING (EAT)</b>"]
                        for x in nw:
                            lines.append(f"- {x['at'].strftime('%a %H:%M')} — {x['title']} "
                                         f"(in {x['in_h']:.1f}h)")
                        await tg.text(ses, "\n".join(lines))

                elif cmd == "/chart":
                    st = want_store(args, stores, paxg)
                    if not st:
                        await tg.text(ses, "Usage: /chart BTC | /chart PAXG | /chart GOLD")
                        continue
                    path = f"/tmp/{st.name}.png"
                    await asyncio.get_event_loop().run_in_executor(None, make_chart, st, path)
                    await tg.photo(ses, path, f"{st.name} 15m — {fp(st.price, st.name)}")
                    if os.path.exists(path): os.remove(path)

                elif cmd == "/health":
                    lines = ["<b>🩺 FEED HEALTH</b>"]
                    for st in stores.values():
                        live = st.data_age() < (180 if st.ws_sym else STALE_FEED_SEC)
                        lines.append(f"{st.name}: {st.source} | {fp(st.price, st.name)} | "
                                     f"data {int(st.data_age())}s old | {'LIVE' if live else 'STALE'}")
                    await tg.text(ses, "\n".join(lines))
        except Exception as e:
            log.error(f"cmd: {e}")
            await asyncio.sleep(5)

# ---------------------------------------------------------------- MAIN LOOP
async def live_loop(stores, paxg, corr, ctx, tg, ses):
    watch = EventWatch()
    last_snapshot = 0.0
    last_event_t = {}

    await tg.text(ses, "<b>🛰 BRAX TERMINAL v5 — ONLINE</b>\n"
                       "Real-time market observation. Facts only — no forecasts.\n"
                       "BTC + PAXG live 24/7 (WS + REST) • GOLD spot-hours aware\n"
                       "Snapshot every 10 min • /help for commands")

    while True:
        try:
            now = time.time()
            gold_open = gold_market_open()

            # factual event notices
            for n, st in stores.items():
                if st.price <= 0: continue
                if n == "GOLD" and not gold_open: continue
                M = measure(st)
                for ev in watch.check(M, ctx):
                    if now - last_event_t.get(n, 0) > 300:
                        await tg.text(ses, ev)
                        last_event_t[n] = now

            # compact snapshot
            if now - last_snapshot >= SNAPSHOT_INTERVAL:
                last_snapshot = now
                await tg.text(ses, snapshot(stores, ctx, corr, paxg))
        except Exception as e:
            log.error(f"live_loop: {e}")
        await asyncio.sleep(TICK_INTERVAL)

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
    tg = TG()

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25)) as ses:
        await bootstrap_crypto([btc, paxg], ses)
        await bootstrap_gold(gold, ses)

        stores = {"BITCOIN": btc, "GOLD": gold, "PAXG": paxg}
        tasks = [asyncio.create_task(binance_worker([btc, paxg])),
                 asyncio.create_task(crypto_rest_fallback([btc, paxg], ses)),
                 asyncio.create_task(gold_worker(gold, ses)),
                 asyncio.create_task(context_worker(ctx, ses)),
                 asyncio.create_task(news_worker(ctx, ses)),
                 asyncio.create_task(corr_worker(btc, gold, paxg, corr)),
                 asyncio.create_task(command_worker(stores, paxg, corr, ctx, tg, ses)),
                 asyncio.create_task(keepalive(ses))]
        try:
            await live_loop(stores, paxg, corr, ctx, tg, ses)
        finally:
            for t in tasks:
                t.cancel()

app = Flask(__name__)

@app.route("/")
def health():
    return "BRAX TERMINAL v5 — RUNNING", 200

def run_flask():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)), use_reloader=False)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    asyncio.run(main())
