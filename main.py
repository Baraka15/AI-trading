"""
BRAX FX v2 FINAL — Institutional Flow Desk
===========================================
Real-time market intelligence, institutional style. NO signals, NO targets,
NO predictions — factual state reads + conviction-gated real-time alerts.

AUTO POSTS
  • Session Market Open  — Asia 02:00 / London 08:00 / NY 13:00 / NY PM 17:00 EAT
  • FLOW UPDATE          — hourly inside each session
  • REAL-TIME ALERTS     — flow flip (≥30% one-sidedness) · absorption ·
                           liquidity sweep · VWAP cross (hysteresis-gated)
                           GOLD/PAXG deduplicated · never from proxy data
  • NY Close 21:00 · Weekend Review Sat 10:00 · Reopen notice Sun 21:30 EAT

COMMANDS  /now /flow /book /derivs /chart /health /help

FEEDS
  BTC, PAXG : Binance WS kline_1m + aggTrade (24/7) · REST bootstrap ~33h · fallback
  XAU/USD   : TwelveData 1min, spot-hours aware · goldprice.org backup
  Derivs    : Binance Futures funding + OI · spot order book imbalance

DEPLOY   Render · Start Command: python main.py
ENV      TELEGRAM_TOKEN · TELEGRAM_CHAT_ID · TWELVEDATA_API_KEY
"""
import asyncio, os, json, time, random, logging, io
from collections import deque
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import aiohttp
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pytz
from flask import Flask, jsonify
from threading import Thread

# ---------------------------------------------------------------- CONFIG
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("BRAXFX")

TOKEN    = os.getenv("TELEGRAM_TOKEN")
CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID")
TD_KEY   = os.getenv("TWELVEDATA_API_KEY")
SELF_URL = os.getenv("RENDER_EXTERNAL_URL", "")
PORT     = int(os.getenv("PORT", "10000"))

BINANCE_REST  = "https://data-api.binance.vision/api/v3"
BINANCE_FAPI  = "https://fapi.binance.com"
BINANCE_HOSTS = ["wss://data-stream.binance.vision/stream?streams=",
                 "wss://stream.binance.com:9443/stream?streams="]

TICK_INTERVAL      = 10     # alert scan every 10s → real-time
GOLD_POLL          = 120
GOLD_POLL_CLOSED   = 900
CTX_INTERVAL       = 120
STALE_CRYPTO_SEC   = 90
STALE_FEED_SEC     = 300
ALERT_COOLDOWN     = 300    # per-asset per-type throttle
MIN_ONESIDED_ALERT = 0.30   # flow flip needs ≥30% one-sidedness (Medium/High)
VWAP_DEV_MIN       = 0.05   # % beyond VWAP before a cross counts (hysteresis)

EAT   = pytz.timezone("Africa/Nairobi")
BRAND = "BRAX FX // INSTITUTIONAL FLOW DESK"
FOOT  = "BRAX FX · Institutional Flow Desk\nEducational market intelligence. Not financial advice."

for _v in (TOKEN, CHAT_ID, TD_KEY):
    if not _v:
        raise ValueError("Missing TELEGRAM_TOKEN / TELEGRAM_CHAT_ID / TWELVEDATA_API_KEY")

def now_eat():
    return datetime.now(EAT)

SESSIONS   = [("ASIA", 2, 8), ("LONDON", 8, 13), ("NEW YORK", 13, 17), ("NY PM", 17, 21)]
FLOW_HOURS = {3, 4, 5, 6, 7, 9, 10, 11, 12, 14, 15, 16, 18, 19, 20}

def session_name():
    h = now_eat().hour
    for name, a, b in SESSIONS:
        if a <= h < b:
            return name
    return None

# ---------------------------------------------------------------- XAU SPOT HOURS
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
    if days == 0 and now.hour * 60 + now.minute >= 22 * 60 + 1:
        days = 7
    return (now + timedelta(days=days)).replace(
        hour=22, minute=1, second=0, microsecond=0).astimezone(EAT)

# ---------------------------------------------------------------- HELPERS
def fp(x, name):
    return f"${x:,.0f}" if "BTC" in name else f"${x:,.2f}"

def fmt_vol(v):
    for u, d in (("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if abs(v) >= d:
            return f"{v/d:,.2f}{u}"
    return f"{v:,.2f}"

DIR_EMOJI = {"BULL": "🟢", "BEAR": "🔴", "NEUTRAL": "⚪"}
DIR_WORD  = {"BULL": "Bullish", "BEAR": "Bearish", "NEUTRAL": "Neutral"}

def bar(pct: int) -> str:
    filled = max(1, round(pct / 10)) if pct > 0 else 0
    return "█" * filled + "░" * (10 - filled)

# ---------------------------------------------------------------- CANDLE + FLOW STORE
class CandleStore:
    def __init__(self, name, ws_sym=None):
        self.name, self.ws_sym = name, ws_sym
        self._c = {}                          # minute-epoch -> [o,h,l,c,v]
        self._df, self._df_ts = None, 0.0
        self.price, self.day_open = 0.0, None
        self.cvd_ticks = deque(maxlen=60000)  # (ts_sec, signed base volume)
        self.last_update = 0.0
        self.source = "—"

    def _update_day_open(self):
        utc_mid = datetime.now(pytz.utc).replace(
            hour=0, minute=0, second=0, microsecond=0).timestamp() // 60
        todays = [cd[0] for tt, cd in self._c.items() if tt >= utc_mid]
        if todays:
            self.day_open = todays[0]

    def ingest_kline(self, k):
        self.ingest_kline_tuple(int(k["t"]) // 60000, float(k["o"]), float(k["h"]),
                                float(k["l"]), float(k["c"]), float(k["v"]))

    def ingest_kline_tuple(self, t_min, o, h, l, c, v):
        self._c[int(t_min)] = [o, h, l, c, v]
        self.price = c
        self.last_update = time.time()
        self._trim(); self._df = None; self._update_day_open()

    def ingest_trade(self, t):
        p, q = float(t["p"]), float(t["q"])
        signed = -q if t["m"] else q          # m=True → buyer was maker → sell aggressor
        self.cvd_ticks.append((int(t["T"]) / 1000, signed))
        self.price = p
        self.last_update = time.time()
        self._df = None

    def ingest_td(self, values):
        for r in values:
            try:
                dt = datetime.strptime(r["datetime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.utc)
                ts = int(dt.timestamp() // 60)
                self._c[ts] = [float(r["open"]), float(r["high"]),
                               float(r["low"]), float(r["close"]), float(r.get("volume") or 0)]
            except Exception:
                continue
        if values:
            self.price = float(values[0]["close"])
        self.last_update = time.time()
        self._trim(); self._df = None; self._update_day_open()

    def _trim(self):
        cutoff = (time.time() - 5 * 86400) // 60      # MINUTE-epoch cutoff (critical fix)
        for k in [k for k in self._c if k < cutoff]:
            del self._c[k]

    def data_age(self) -> float:
        if not self._c:
            return 1e9
        return time.time() - max(self._c) * 60

    def df(self, rule="1min", bars=None) -> pd.DataFrame:
        if not self._c:
            return pd.DataFrame()
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

    # ---- flow primitives (real aggressor trades)
    def cvd(self, window=3600) -> float:
        cut = time.time() - window
        return sum(v for ts, v in self.cvd_ticks if ts >= cut)

    def total_vol(self, window=3600) -> float:
        cut = time.time() - window
        return sum(abs(v) for ts, v in self.cvd_ticks if ts >= cut)

    # ---- VWAP (UTC day, from 1m bars)
    def vwap(self):
        df = self.df("1min")
        if df.empty:
            return None
        today = df[df.index >= df.index[-1].floor("D")]
        if today.empty:
            return None
        tp = (today.h + today.l + today.c) / 3
        v = today.v
        vs = float(v.sum())
        if vs > 0:
            return float((tp * v).sum() / vs)
        return float(tp.mean())

    def vwap_dev_pct(self) -> float:
        vw = self.vwap()
        if not vw or not self.price:
            return 0.0
        return (self.price - vw) / vw * 100

# ---------------------------------------------------------------- INDICATORS
def ema_stack(df: pd.DataFrame) -> str:
    if len(df) < 55:
        return "NEUTRAL"
    c = df.c
    e9  = float(c.ewm(span=9,  adjust=False).mean().iloc[-1])
    e21 = float(c.ewm(span=21, adjust=False).mean().iloc[-1])
    e50 = float(c.ewm(span=50, adjust=False).mean().iloc[-1])
    if e9 > e21 > e50:
        return "BULL"
    if e9 < e21 < e50:
        return "BEAR"
    return "NEUTRAL"

def structure_read(st: CandleStore, h4: dict) -> tuple:
    s15 = ema_stack(st.df("15min", 120))
    s1h = ema_stack(st.df("1h", 120))
    if s1h == s15:
        intra = s1h
    elif s1h == "NEUTRAL":
        intra = s15
    elif s15 == "NEUTRAL":
        intra = s1h
    else:
        intra = "NEUTRAL"
    wk = "NEUTRAL"
    s = h4.get(st.name)
    if s is not None and len(s) >= 55:
        e9  = float(s.ewm(span=9,  adjust=False).mean().iloc[-1])
        e21 = float(s.ewm(span=21, adjust=False).mean().iloc[-1])
        e50 = float(s.ewm(span=50, adjust=False).mean().iloc[-1])
        wk = "BULL" if e9 > e21 > e50 else "BEAR" if e9 < e21 < e50 else "NEUTRAL"
    return intra, wk

def flow_metrics(fs: CandleStore):
    """Institutional flow read from real aggressor trades."""
    if fs is None or not fs.cvd_ticks:
        return None
    c15, c1h, c3h = fs.cvd(900), fs.cvd(3600), fs.cvd(10800)
    tot15, tot1h = fs.total_vol(900), fs.total_vol(3600)
    if tot15 <= 0 or tot1h <= 0:
        return None
    ones15 = abs(c15) / tot15
    ones1h = abs(c1h) / tot1h
    direction = "BULL" if c1h > 0 else "BEAR" if c1h < 0 else "NEUTRAL"
    conv = "High" if ones1h > 0.60 else "Medium" if ones1h > 0.30 else "Low"
    if abs(c15) < 0.25 * tot15:
        regime = "BALANCED"
    elif (c15 > 0) == (c1h > 0):
        regime = "ACCUMULATION" if c1h > 0 else "DISTRIBUTION"
    else:
        regime = "ABSORPTION"
    return {"c15": c15, "c1h": c1h, "c3h": c3h,
            "ones15": ones15, "ones1h": ones1h,
            "dir": direction, "conv": conv, "regime": regime}

def agreement(intra: str, flow: str) -> tuple:
    if intra == flow and intra != "NEUTRAL":
        pct = 100
    elif "NEUTRAL" in (intra, flow):
        pct = 67
    else:
        pct = 33
    grade = "A" if pct >= 80 else "B" if pct >= 65 else "C"
    return pct, grade

def alignment_note(intra, fl, regime):
    if regime == "ABSORPTION":
        return "⚠️ Absorption — short-window flow fights the larger flow; one side is being absorbed at these prices."
    if intra == fl and intra != "NEUTRAL":
        return f"Structure and flow aligned {DIR_WORD[intra].lower()} — participation confirms direction."
    if intra != "NEUTRAL" and fl != "NEUTRAL":
        return (f"Structure {DIR_WORD[intra].lower()} but flow {DIR_WORD[fl].lower()} — "
                "participation is not confirming direction.")
    if intra == "NEUTRAL":
        return f"Structure neutral — flow is {DIR_WORD[fl].lower()} without trend confirmation."
    return f"Flow balanced — trend {DIR_WORD[intra].lower()} but participation is two-sided."

# ---------------------------------------------------------------- TELEGRAM
HTTP = None  # aiohttp session, set in main()

async def tg(text: str):
    try:
        async with HTTP.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML",
                  "disable_web_page_preview": True}) as r:
            if r.status != 200:
                log.error(f"TG send {r.status}: {await r.text()}")
    except Exception as e:
        log.error(f"TG send: {e}")

async def tg_photo(png: bytes, caption: str):
    try:
        form = aiohttp.FormData()
        form.add_field("chat_id", CHAT_ID)
        form.add_field("caption", caption)
        form.add_field("photo", png, filename="chart.png")
        async with HTTP.post(f"https://api.telegram.org/bot{TOKEN}/sendPhoto", data=form) as r:
            if r.status != 200:
                log.error(f"TG photo {r.status}: {await r.text()}")
    except Exception as e:
        log.error(f"TG photo: {e}")

# ---------------------------------------------------------------- FEEDS
async def binance_worker(stores: list):
    streams = []
    for s in stores:
        if s.ws_sym:
            streams += [f"{s.ws_sym}@kline_1m", f"{s.ws_sym}@aggTrade"]
    if not streams:
        return
    backoff = 5
    while True:
        host = random.choice(BINANCE_HOSTS)
        try:
            async with HTTP.ws_connect(host + "/".join(streams), heartbeat=25) as ws:
                log.info(f"WS connected: {host}")
                backoff = 5
                for s in stores:
                    if s.ws_sym:
                        s.source = "BINANCE WS"
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    d = json.loads(msg.data).get("data", {})
                    st = next((x for x in stores
                               if x.ws_sym == d.get("s", "").lower()), None)
                    if not st:
                        continue
                    if d.get("e") == "kline":
                        st.ingest_kline(d["k"])
                    elif d.get("e") == "aggTrade":
                        st.ingest_trade(d)
        except Exception as e:
            log.warning(f"WS down ({e}) — retry in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 120)

async def crypto_rest_fallback(stores: list):
    while True:
        await asyncio.sleep(30)
        for st in stores:
            if not st.ws_sym or time.time() - st.last_update < STALE_CRYPTO_SEC:
                continue
            try:
                async with HTTP.get(f"{BINANCE_REST}/klines?symbol={st.ws_sym.upper()}"
                                    "&interval=1m&limit=3") as r:
                    kl = await r.json()
                if isinstance(kl, list) and kl:
                    for k in kl:
                        st.ingest_kline_tuple(int(k[0]) // 60000, float(k[1]),
                                              float(k[2]), float(k[3]),
                                              float(k[4]), float(k[5]))
                    st.source = "BINANCE REST"
            except Exception as e:
                log.error(f"REST fallback {st.name}: {e}")

async def bootstrap_crypto(stores: list):
    for st in stores:
        if not st.ws_sym:
            continue
        sym = st.ws_sym.upper()
        try:
            async with HTTP.get(f"{BINANCE_REST}/klines?symbol={sym}&interval=1m&limit=1000") as r:
                kl = await r.json()
            kl_all = kl if isinstance(kl, list) and kl else []
            if kl_all:
                async with HTTP.get(f"{BINANCE_REST}/klines?symbol={sym}&interval=1m"
                                    f"&limit=1000&endTime={int(kl[0][0]) - 1}") as r2:
                    kl2 = await r2.json()
                if isinstance(kl2, list) and kl2:
                    kl_all = kl2 + kl_all
                for k in kl_all:
                    st._c[int(k[0]) // 60000] = [float(k[1]), float(k[2]),
                                                 float(k[3]), float(k[4]), float(k[5])]
                st.price = float(kl_all[-1][4])
                st.last_update = time.time()
                st._df = None
                st._update_day_open()
                log.info(f"{st.name}: bootstrapped {len(kl_all)} 1m bars")
        except Exception as e:
            log.error(f"bootstrap {st.name}: {e}")
        try:
            async with HTTP.get(f"{BINANCE_REST}/aggTrades?symbol={sym}&limit=1000") as r:
                tr = await r.json()
            if isinstance(tr, list):
                for t in tr:
                    st.ingest_trade({"p": t["p"], "q": t["q"], "m": t["m"], "T": t["T"]})
                log.info(f"{st.name}: flow seeded ({len(tr)} trades)")
        except Exception as e:
            log.error(f"bootstrap trades {st.name}: {e}")

async def bootstrap_gold(gold: CandleStore):
    try:
        async with HTTP.get("https://api.twelvedata.com/time_series"
                            f"?symbol=XAU/USD&interval=1min&outputsize=500&apikey={TD_KEY}") as r:
            d = await r.json()
        if d.get("values"):
            gold.ingest_td(d["values"])
            gold.source = "TWELVEDATA"
            log.info(f"GOLD bootstrapped {len(d['values'])} bars")
    except Exception as e:
        log.error(f"bootstrap gold: {e}")

async def gold_worker(gold: CandleStore):
    while True:
        try:
            async with HTTP.get("https://api.twelvedata.com/time_series"
                                f"?symbol=XAU/USD&interval=1min&outputsize=60&apikey={TD_KEY}") as r:
                d = await r.json()
            if d.get("values"):
                gold.ingest_td(d["values"])
                gold.source = "TWELVEDATA"
            else:
                async with HTTP.get("https://data-asg.goldprice.org/dbXRates/USD") as r2:
                    gp = (await r2.json())["items"][0]["xauPrice"]
                if gold.price <= 0 or abs(gp - gold.price) > 0.5:
                    gold.price = float(gp)
                    gold.last_update = time.time()
                gold.source = "GOLDPRICE.ORG"
        except Exception as e:
            log.error(f"gold feed: {e}")
        await asyncio.sleep(GOLD_POLL if gold_market_open() else GOLD_POLL_CLOSED)

async def h4_worker(h4: dict):
    while True:
        try:
            for sym, name in (("BTCUSDT", "BITCOIN"), ("PAXGUSDT", "PAXG")):
                async with HTTP.get(f"{BINANCE_REST}/klines?symbol={sym}&interval=4h&limit=120") as r:
                    kl = await r.json()
                if isinstance(kl, list) and kl:
                    h4[name] = pd.Series([float(k[4]) for k in kl])
            if gold_market_open():
                async with HTTP.get("https://api.twelvedata.com/time_series"
                                    f"?symbol=XAU/USD&interval=4h&outputsize=120&apikey={TD_KEY}") as r:
                    d = await r.json()
                if d.get("values"):
                    h4["GOLD"] = pd.Series([float(x["close"]) for x in d["values"]])
        except Exception as e:
            log.error(f"h4: {e}")
        await asyncio.sleep(1800)

async def context_worker(ctx: dict):
    syms = {"BTCUSDT": "BITCOIN", "PAXGUSDT": "PAXG"}
    while True:
        try:
            for fsym, name in syms.items():
                c = ctx.setdefault(name, {})
                try:
                    async with HTTP.get(f"{BINANCE_FAPI}/fapi/v1/premiumIndex?symbol={fsym}") as r:
                        c["funding"] = float((await r.json()).get("lastFundingRate", 0)) * 100
                except Exception:
                    pass
                try:
                    async with HTTP.get(f"{BINANCE_FAPI}/fapi/v1/openInterest?symbol={fsym}") as r:
                        c["oi"] = float((await r.json()).get("openInterest", 0))
                except Exception:
                    pass
                try:
                    async with HTTP.get(f"{BINANCE_REST}/depth?symbol={fsym}&limit=100") as r:
                        d = await r.json()
                    bid = sum(float(q) * float(p) for p, q in d.get("bids", []))
                    ask = sum(float(q) * float(p) for p, q in d.get("asks", []))
                    if bid + ask > 0:
                        c["book"] = (bid - ask) / (bid + ask) * 100
                except Exception:
                    pass
        except Exception as e:
            log.error(f"context: {e}")
        await asyncio.sleep(CTX_INTERVAL)

# ---------------------------------------------------------------- ALERT ENGINE (final)
class AlertEngine:
    """Fires factual flow-state alerts within seconds.
    Rules:
      • Alerts ONLY from live trade flow — never proxy data (honesty gate)
      • Flow flips gated by one-sidedness ≥ 30% (noise kill)
      • GOLD/PAXG linked — one metal, one alert (dedup window 15 min)
      • VWAP cross uses a hysteresis band (no 0.00% tick-cross spam)
    """

    LINK_GROUPS = {"GOLD", "PAXG"}

    def __init__(self):
        self.state = {}
        self.link_last = {}

    def _cool(self, name, key, seconds) -> bool:
        S = self.state.setdefault(name, {})
        last = S.get(key, 0)
        if time.time() - last < seconds:
            return False
        S[key] = time.time()
        return True

    def _link_cool(self, name, seconds) -> bool:
        if name not in self.LINK_GROUPS:
            return True
        last = self.link_last.get("GOLDPAIR", 0)
        if time.time() - last < seconds:
            return False
        self.link_last["GOLDPAIR"] = time.time()
        return True

    def scan(self, st: CandleStore, proxy: CandleStore) -> list:
        out = []
        if st.cvd_ticks is None or not st.cvd_ticks:
            return out
        if proxy is not st and st.name == "GOLD":
            return out                 # GOLD never alerts from proxy tape
        fm = flow_metrics(st)
        if not fm:
            return out
        n = st.name
        S = self.state.setdefault(n, {})

        # 1) FLOW FLIP — 1h CVD sign change, conviction-gated
        if ("flow_dir" in S and S["flow_dir"] != fm["dir"]
                and fm["dir"] != "NEUTRAL"
                and fm["ones1h"] >= MIN_ONESIDED_ALERT):
            if self._cool(n, "flip", ALERT_COOLDOWN) and self._link_cool(n, 900):
                out.append(
                    f"🔁 <b>{n} · FLOW FLIP</b>\n"
                    f"1h CVD turned <b>{DIR_WORD[fm['dir']].lower()}</b> "
                    f"(one-sidedness {fm['ones1h']*100:.0f}%, {fm['conv']} conviction) "
                    f"@ {fp(st.price, n)}\n\n<i>{BRAND}</i>")
        S["flow_dir"] = fm["dir"]

        # 2) ABSORPTION — new session extreme while 15m CVD opposes
        df15 = st.df("15min", 96)
        if len(df15) >= 20:
            hi, lo = float(df15.h.max()), float(df15.l.min())
            if st.price >= hi and fm["c15"] < 0 and self._cool(n, "absorb_hi", 900) and self._link_cool(n, 900):
                out.append(
                    f"🧲 <b>{n} · ABSORPTION</b>\n"
                    f"Price at session high {fp(hi, n)} while 15m CVD is "
                    f"<b>selling</b> ({fm['c15']:+,.0f}). Buyers being absorbed into strength.\n\n<i>{BRAND}</i>")
            elif st.price <= lo and fm["c15"] > 0 and self._cool(n, "absorb_lo", 900) and self._link_cool(n, 900):
                out.append(
                    f"🧲 <b>{n} · ABSORPTION</b>\n"
                    f"Price at session low {fp(lo, n)} while 15m CVD is "
                    f"<b>buying</b> ({fm['c15']:+,.0f}). Sellers being absorbed into weakness.\n\n<i>{BRAND}</i>")

        # 3) LIQUIDITY SWEEP — 5m wick beyond swing extreme, close back inside
        d5 = st.df("5min", 30)
        if len(d5) >= 12:
            lc = d5.iloc[-2]
            prior = d5.iloc[:-2]
            sw_hi, sw_lo = float(prior.h.max()), float(prior.l.min())
            if lc.h > sw_hi and lc.c < sw_hi and self._cool(n, "sweep_hi", 900) and self._link_cool(n, 900):
                out.append(
                    f"⚔️ <b>{n} · LIQUIDITY SWEEP</b>\n"
                    f"5m wick took {fp(sw_hi, n)} and closed back below — "
                    f"buy-side stops run above the swing high.\n\n<i>{BRAND}</i>")
            if lc.l < sw_lo and lc.c > sw_lo and self._cool(n, "sweep_lo", 900) and self._link_cool(n, 900):
                out.append(
                    f"⚔️ <b>{n} · LIQUIDITY SWEEP</b>\n"
                    f"5m wick took {fp(sw_lo, n)} and closed back above — "
                    f"sell-side stops run below the swing low.\n\n<i>{BRAND}</i>")

        # 4) VWAP CROSS — hysteresis band
        vw = st.vwap()
        if vw and st.price:
            dev = (st.price - vw) / vw * 100
            if st.price > vw * (1 + VWAP_DEV_MIN / 100):
                side = "above"
            elif st.price < vw * (1 - VWAP_DEV_MIN / 100):
                side = "below"
            else:
                side = S.get("vwap_side", "above")
            if S.get("vwap_side") and S["vwap_side"] != side and self._cool(n, "vwap", 600) and self._link_cool(n, 600):
                confirming = (fm["c1h"] > 0) == (side == "above") and fm["dir"] != "NEUTRAL"
                note = "flow confirming" if confirming else "flow NOT confirming"
                out.append(
                    f"📍 <b>{n} · VWAP CROSS</b>\n"
                    f"Price crossed <b>{side}</b> session VWAP ({fp(vw, n)}, dev {abs(dev):.2f}%) — {note}.\n\n<i>{BRAND}</i>")
            S["vwap_side"] = side

        return out

# ---------------------------------------------------------------- FORMATTING
def header(title: str) -> str:
    return f"📡 <b>{title}</b>\nBRAX FX // MARKET INTELLIGENCE\n"

def footer() -> str:
    return f"\n\n<i>{FOOT}</i>"

def asset_block(st: CandleStore, proxy: CandleStore, h4: dict, ctx: dict,
                feed_note: str = "") -> str:
    n = st.name
    p = st.price
    intra, wk = structure_read(st, h4)
    fs = st if st.cvd_ticks else proxy
    fs_label = "live trades" if fs is st else "PAXG proxy"
    fm = flow_metrics(fs)
    fl = fm["dir"] if fm else "NEUTRAL"
    conv = fm["conv"] if fm else "—"
    pct, grade = agreement(intra, fl)
    lines = [f"<b>{n}</b> — {fp(p, n)}" if p else f"<b>{n}</b> — awaiting data"]
    df24 = st.df("1h", 24)
    if not df24.empty and p:
        hi, lo = float(df24.h.max()), float(df24.l.min())
        if hi > lo:
            lines.append(f"24h {fp(lo, n)} – {fp(hi, n)} · {((p - lo) / (hi - lo) * 100):.0f}% of range")
        else:
            lines.append(f"24h {fp(lo, n)}")
    vw = st.vwap()
    if vw:
        lines.append(f"VWAP {fp(vw, n)} ({st.vwap_dev_pct():+.2f}%)")
    if fm:
        lines.append(f"Flow {DIR_EMOJI[fm['dir']]} {DIR_WORD[fm['dir']]} ({fm['conv']}) · "
                     f"{fm['regime']} · {fs_label}")
amend = None
    lines.append(f"Structure {DIR_EMOJI[intra]} {DIR_WORD[intra]}")
    lines.append(f"Weekly {DIR_EMOJI[wk]} {DIR_WORD[wk]}")
    lines.append(f"Agreement {pct}% {bar(pct)} · Grade {grade}")
    if fm:
        lines.append(f"CVD 15m {fm['c15']:+,.0f} · 1h {fm['c1h']:+,.0f} · 3h {fm['c3h']:+,.0f}")
    c = ctx.get(n, {})
    der = []
    if "funding" in c:
        der.append(f"Funding {c['funding']:+.4f}%")
    if "oi" in c and c["oi"]:
        der.append(f"OI {fmt_vol(c['oi'])}")
    if "book" in c:
        der.append(f"Book {'bid' if c['book'] > 0 else 'ask'}-heavy {abs(c['book']):.1f}%")
    if der:
        lines.append(" · ".join(der))
    if feed_note:
        lines.append(feed_note)
    age = st.data_age()
    age_tag = "live" if age < STALE_FEED_SEC else f"stale {int(age/60)}m"
    lines.append(f"Feed: {st.source} · {age_tag}")
    return "\n".join(lines)

def build_session_open(stores, proxies, h4, ctx) -> str:
    s = session_name()
    t = now_eat().strftime("%H:%M")
    msg = header(f"{s} SESSION · MARKET OPEN — {t} EAT")
    for st in stores:
        proxy = proxies.get(st.name, st)
        note = ""
        if st.name == "GOLD":
            note = ("XAU spot open · TwelveData feed" if gold_market_open()
                    else ("XAU spot CLOSED until "
                          f"{gold_next_open_eat().strftime('%a %H:%M EAT')} — showing Friday close; "
                          "PAXG is the live 24/7 reference"))
        msg += "\n" + "—" * 22 + "\n" + asset_block(st, proxy, h4, ctx, note) + "\n"
    msg += "\n" + "—" * 22 + "\nFlow, structure and agreement are factual state reads — not forecasts."
    return msg + footer()

def build_flow_update(stores, proxies, h4) -> str:
    s = session_name()
    t = now_eat().strftime("%H:%M")
    msg = header(f"{s} SESSION · FLOW UPDATE — {t} EAT")
    for st in stores:
        proxy = proxies.get(st.name, st)
        fs = st if st.cvd_ticks else proxy
        fm = flow_metrics(fs)
        intra, wk = structure_read(st, h4)
        n = st.name
        lines = [f"<b>{n}</b> — {fp(st.price, n)}" if st.price else f"<b>{n}</b>"]
        if fm:
            fs_label = "live" if fs is st else "PAXG proxy"
            lines.append(f"Flow {DIR_EMOJI[fm['dir']]} {DIR_WORD[fm['dir']]} ({fm['conv']}) "
                         f"· {fm['regime']} · {fs_label}")
            lines.append(f"CVD 15m {fm['c15']:+,.0f} · 1h {fm['c1h']:+,.0f} · "
                         f"one-sidedness {fm['ones1h']*100:.0f}%")
        else:
            lines.append("Flow warming up — collecting aggressor trades")
        lines.append(f"Structure {DIR_EMOJI[intra]} {DIR_WORD[intra]} · Weekly "
                     f"{DIR_EMOJI[wk]} {DIR_WORD[wk]}")
        pct, grade = agreement(intra, fm["dir"] if fm else "NEUTRAL")
        lines.append(f"Agreement {pct}% {bar(pct)} · Grade {grade}")
        lines.append(alignment_note(intra, fm["dir"] if fm else "NEUTRAL",
                                    fm["regime"] if fm else "BALANCED"))
        msg += "\n" + "—" * 22 + "\n" + "\n".join(lines) + "\n"
    return msg + footer()

def build_weekend_review(stores, proxies, h4, ctx) -> str:
    msg = header(f"WEEKEND STRUCTURE REVIEW — {now_eat().strftime('%a %H:%M')} EAT")
    for st in stores:
        proxy = proxies.get(st.name, st)
        note = ""
        if st.name == "GOLD":
            note = ("XAU spot CLOSED for the weekend — Friday close shown; "
                    "PAXG is the live 24/7 gold reference (thinner liquidity).")
        msg += "\n" + "—" * 22 + "\n" + asset_block(st, proxy, h4, ctx, note) + "\n"
    msg += ("\n" + "—" * 22 +
            "\nWeekend reads are factual states of live 24/7 markets (BTC, PAXG). "
            "XAU/USD spot reopens Sunday 22:00 UTC.")
    return msg + footer()

# ---------------------------------------------------------------- CHART
def make_chart(st: CandleStore):
    try:
        df = st.df("1h", 72)
        if len(df) < 20:
            return None
        fig, ax = plt.subplots(figsize=(9, 4.2), dpi=110)
        ax.plot(df.index, df.c, lw=1.4, color="#d4af37", label=f"{st.name} 1h close")
        vw = st.vwap()
        if vw:
            ax.axhline(vw, color="#4da6ff", lw=1, ls="--", label="Session VWAP")
        e21 = df.c.ewm(span=21, adjust=False).mean()
        ax.plot(df.index, e21, lw=1, color="#888888", label="EMA21 (1h)")
        ax.set_title(f"BRAX FX · {st.name} · last 72h · {now_eat().strftime('%d %b %H:%M EAT')}",
                     fontsize=10)
        ax.legend(fontsize=8)
        ax.grid(alpha=0.25)
        fig.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        plt.close(fig)
        return buf.getvalue()
    except Exception as e:
        log.error(f"chart: {e}")
        return None

# ---------------------------------------------------------------- LOOPS
async def alert_loop(stores, proxies, h4, engine: AlertEngine):
    await asyncio.sleep(60)
    while True:
        for st in stores:
            if st.data_age() > STALE_FEED_SEC:
                continue
            proxy = proxies.get(st.name, st)
            try:
                for alert in engine.scan(st, proxy):
                    await tg(alert)
            except Exception as e:
                log.error(f"alert scan {st.name}: {e}")
        await asyncio.sleep(TICK_INTERVAL)

def _posted_key(prefix) -> str:
    return f"{prefix}-{now_eat().strftime('%Y-%m-%d-%H')}"

async def post_loop(stores, proxies, h4, ctx):
    posted = set()
    while True:
        try:
            t = now_eat()
            wd, h, m = t.weekday(), t.hour, t.minute
            if m <= 1:
                if wd < 5 and h in (2, 8, 13, 17):
                    k = _posted_key("open")
                    if k not in posted:
                        posted.add(k)
                        await tg(build_session_open(stores, proxies, h4, ctx))
                elif wd < 5 and h in FLOW_HOURS and session_name():
                    k = _posted_key("flow")
                    if k not in posted:
                        posted.add(k)
                        await tg(build_flow_update(stores, proxies, h4))
                elif wd < 5 and h == 21:
                    k = _posted_key("close")
                    if k not in posted:
                        posted.add(k)
                        await tg(header("NEW YORK CLOSE — 21:00 EAT")
                                 + "Session cycle complete. Asia reopens 02:00 EAT."
                                 + footer())
                elif wd == 5 and h == 10:
                    k = _posted_key("wknd")
                    if k not in posted:
                        posted.add(k)
                        await tg(build_weekend_review(stores, proxies, h4, ctx))
                elif wd == 6 and h == 21:
                    k = _posted_key("reopen")
                    if k not in posted:
                        posted.add(k)
                        await tg(header("WEEKEND REOPEN NOTICE")
                                 + "XAU/USD spot reopens 22:00 UTC (01:00 Mon EAT). "
                                   "BTC and PAXG have been live throughout."
                                 + footer())
        except Exception as e:
            log.error(f"post_loop: {e}")
        await asyncio.sleep(30)

# ---------------------------------------------------------------- COMMANDS
HELP = ("🛠 <b>BRAX FX · Commands</b>\n"
        "/now — full market state snapshot\n"
        "/flow — institutional flow read (CVD, regime, conviction)\n"
        "/book — spot order book imbalance\n"
        "/derivs — funding · open interest\n"
        "/chart — 72h chart\n"
        "/health — feed status\n"
        "/help — this menu")

async def cmd_now(stores, proxies, h4, ctx):
    msg = header(f"MARKET STATE — {now_eat().strftime('%a %d %b · %H:%M')} EAT")
    for st in stores:
        proxy = proxies.get(st.name, st)
        note = ""
        if st.name == "GOLD":
            note = ("XAU spot open" if gold_market_open()
                    else ("XAU spot CLOSED — Friday close shown "
                          f"(reopens {gold_next_open_eat().strftime('%a %H:%M EAT')})"))
        msg += "\n" + "—" * 22 + "\n" + asset_block(st, proxy, h4, ctx, note) + "\n"
    await tg(msg + footer())

async def cmd_flow(stores, proxies):
    msg = header("FLOW DESK — institutional read")
    for st in stores:
        proxy = proxies.get(st.name, st)
        fs = st if st.cvd_ticks else proxy
        fm = flow_metrics(fs)
        n = st.name
        fs_label = "live trades" if fs is st else "PAXG proxy"
        if fm:
            msg += (f"\n<b>{n}</b> — {fp(st.price, n)}\n"
                    f"Flow {DIR_EMOJI[fm['dir']]} {DIR_WORD[fm['dir']]} ({fm['conv']}) · "
                    f"{fm['regime']} · {fs_label}\n"
                    f"CVD 15m {fm['c15']:+,.0f} · 1h {fm['c1h']:+,.0f} · 3h {fm['c3h']:+,.0f}\n"
                    f"One-sidedness 15m {fm['ones15']*100:.0f}% · 1h {fm['ones1h']*100:.0f}%\n\n")
        else:
            msg += f"\n<b>{n}</b> — flow warming up ({fs_label})\n\n"
    await tg(msg + footer())

async def cmd_book(ctx):
    msg = header("ORDER BOOK · spot imbalance (top 100 levels)")
    for n in ("BITCOIN", "PAXG"):
        c = ctx.get(n, {})
        if "book" in c:
            side = "BIDS heavier" if c["book"] > 0 else "ASKS heavier"
            msg += f"<b>{n}</b> · {side} by {abs(c['book']):.1f}% (notional)\n"
        else:
            msg += f"<b>{n}</b> · loading…\n"
    await tg(msg + "\nFactual depth snapshot — book can flip in seconds." + footer())

async def cmd_derivs(ctx):
    msg = header("DERIVATIVES · Binance Futures")
    for n in ("BITCOIN", "PAXG"):
        c = ctx.get(n, {})
        f, oi = c.get("funding"), c.get("oi")
        ftxt = f"{f:+.4f}%" if f is not None else "—"
        oitxt = fmt_vol(oi) if oi else "—"
        msg += f"<b>{n}</b> · Funding {ftxt} · OI {oitxt}\n"
    await tg(msg + "Positive funding = longs pay shorts. Factual derivatives state." + footer())

async def cmd_chart(stores):
    for st in stores:
        png = make_chart(st)
        if png:
            vw = st.vwap()
            cap = (f"BRAX FX · {st.name} — {fp(st.price, st.name)} · "
                   f"VWAP {fp(vw, st.name)} ({st.vwap_dev_pct():+.2f}%)" if vw
                   else f"BRAX FX · {st.name}")
            await tg_photo(png, cap + "\n" + FOOT)
            await asyncio.sleep(1)

async def cmd_health(stores):
    msg = header("SYSTEM HEALTH")
    gm = ("OPEN" if gold_market_open()
          else f"CLOSED (reopens {gold_next_open_eat().strftime('%a %H:%M EAT')})")
    for st in stores:
        age = st.data_age()
        tag = "🟢 live" if age < STALE_FEED_SEC else f"🟡 stale {int(age/60)}m"
        msg += (f"<b>{st.name}</b> · {tag} · src {st.source} · "
                f"bars {len(st._c)} · flow ticks {len(st.cvd_ticks)}\n")
    msg += (f"XAU spot: {gm}\n"
            f"Engine tick: {TICK_INTERVAL}s · Alert throttle: {ALERT_COOLDOWN}s · "
            f"Flip gate: {MIN_ONESIDED_ALERT*100:.0f}% one-sidedness")
    await tg(msg + footer())

async def command_worker(stores, proxies, h4, ctx):
    offset = 0
    try:
        async with HTTP.post(f"https://api.telegram.org/bot{TOKEN}/deleteWebhook",
                             json={"drop_pending_updates": False}):
            pass
    except Exception:
        pass
    while True:
        try:
            async with HTTP.get(f"https://api.telegram.org/bot{TOKEN}/getUpdates"
                                f"?timeout=25&offset={offset}") as r:
                data = await r.json()
            for u in data.get("result", []):
                offset = u["update_id"] + 1
                text = (u.get("message") or {}).get("text", "").strip().lower()
                if not text.startswith("/"):
                    continue
                cmd = text.split()[0].split("@")[0]
                log.info(f"cmd: {cmd}")
                if cmd == "/now":
                    await cmd_now(stores, proxies, h4, ctx)
                elif cmd == "/flow":
                    await cmd_flow(stores, proxies)
                elif cmd == "/book":
                    await cmd_book(ctx)
                elif cmd == "/derivs":
                    await cmd_derivs(ctx)
                elif cmd == "/chart":
                    await cmd_chart(stores)
                elif cmd == "/health":
                    await cmd_health(stores)
                elif cmd in ("/start", "/help"):
                    await tg(f"<b>BRAX FX</b> · Institutional Flow Desk\n\n{HELP}" + footer())
        except Exception as e:
            log.error(f"commands: {e}")
            await asyncio.sleep(3)

# ---------------------------------------------------------------- KEEPALIVE
flask = Flask(__name__)
STATE = {"status": "starting", "started": datetime.now(pytz.utc).isoformat()}

@flask.route("/")
def health():
    return jsonify({"service": "BRAX FX · Institutional Flow Desk",
                    "status": STATE["status"],
                    "started": STATE["started"],
                    "time_utc": datetime.now(pytz.utc).isoformat()})

def run_flask():
    flask.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

# ---------------------------------------------------------------- MAIN
async def main():
    btc  = CandleStore("BITCOIN", "btcusdt")
    paxg = CandleStore("PAXG",    "paxgusdt")
    gold = CandleStore("GOLD")
    stores  = [btc, gold, paxg]
    proxies = {"BITCOIN": btc, "PAXG": paxg, "GOLD": paxg}
    h4, ctx = {}, {}
    engine = AlertEngine()
    STATE["status"] = "bootstrapping"

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as ses:
        globals()["HTTP"] = ses
        await bootstrap_crypto([btc, paxg])
        await bootstrap_gold(gold)
        STATE["status"] = "running"
        await tg(header("DESK ONLINE — Institutional Flow Desk")
                 + "Real-time flow engine active.\n"
                   "Alerts: flow flip (≥30% one-sidedness) · absorption · "
                   "liquidity sweep · VWAP cross.\n"
                   "Hourly flow updates in-session. BTC & PAXG live 24/7 · "
                   "XAU spot-hours aware.\n"
                   "Commands: /now /flow /book /derivs /chart /health /help"
                 + footer())
        tasks = [
            asyncio.create_task(binance_worker([btc, paxg])),
            asyncio.create_task(crypto_rest_fallback([btc, paxg])),
            asyncio.create_task(gold_worker(gold)),
            asyncio.create_task(h4_worker(h4)),
            asyncio.create_task(context_worker(ctx)),
            asyncio.create_task(alert_loop(stores, proxies, h4, engine)),
            asyncio.create_task(post_loop(stores, proxies, h4, ctx)),
            asyncio.create_task(command_worker(stores, proxies, h4, ctx)),
        ]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("BRAX FX shutting down.")
