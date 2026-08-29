"""
BRAX FX — Real-Time Market Intelligence v1
===========================================
Style: compact branded intelligence posts (structure · flow · agreement)
Posts:  Session Market Open (Asia 02:00 / London 08:00 / NY 13:00 EAT)
        Flow Update every 2h during sessions
        Weekend Structure Review (Sat 10:00) + reopen notice (Sun 21:30)
        Session Close (21:00 EAT)
        On-demand: /now /flow /book /chart /health

Reads (factual states, no price targets):
  Structure  = EMA 9/21/50 stack on H1 + M15 live candles
  Weekly     = same read on real H4 candles
  Flow       = CVD direction + conviction from real aggressor trades
  Agreement  = structure-vs-flow alignment %, Grade A/B/C

Feeds: Binance WS+REST (BTC, PAXG — 24/7) · TwelveData XAU/USD (spot-hours
aware, goldprice.org backup) · Binance Futures (funding/OI) · order book

Deploy: Render → Start Command: python main.py
Env:    TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, TWELVEDATA_API_KEY
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
log = logging.getLogger("BRAXFX")

TOKEN   = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TD_KEY  = os.getenv("TWELVEDATA_API_KEY")
SELF_URL = os.getenv("RENDER_EXTERNAL_URL", "")

BINANCE_REST  = "https://data-api.binance.vision/api/v3"
BINANCE_FAPI  = "https://fapi.binance.com"
BINANCE_HOSTS = ["wss://data-stream.binance.vision/stream?streams=",
                 "wss://stream.binance.com:9443/stream?streams="]

TICK_INTERVAL     = 20
GOLD_POLL         = 120
GOLD_POLL_CLOSED  = 900
CTX_INTERVAL      = 120
H4_INTERVAL       = 1800
STALE_CRYPTO_SEC  = 90
STALE_FEED_SEC    = 300
EAT = pytz.timezone("Africa/Nairobi")
BRAND = "BRAX FX // MARKET INTELLIGENCE"
FOOT  = "BRAX FX · Real-Time Market Intelligence\nEducational market intelligence. Not financial advice."

for v in (TOKEN, CHAT_ID, TD_KEY):
    if not v: raise ValueError("Missing TELEGRAM_TOKEN / TELEGRAM_CHAT_ID / TWELVEDATA_API_KEY")

def now_eat(): return datetime.now(EAT)

SESSIONS = [("ASIA", 2, 8), ("LONDON", 8, 13), ("NEW YORK", 13, 17), ("NY AFTERNOON", 17, 21)]
FLOW_HOURS = {4, 6, 10, 12, 15, 19}

def session_name() -> str | None:
    h = now_eat().hour
    for name, a, b in SESSIONS:
        if a <= h < b: return name
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
    if days == 0 and now.hour * 60 + now.minute >= 22 * 60:
        days = 7
    t = (now + timedelta(days=days)).replace(hour=22, minute=0, second=0, microsecond=0)
    return t.astimezone(EAT)

# ---------------------------------------------------------------- LESSONS (rotating)
LESSONS = [
    "Structure tells you the direction of least resistance; order flow tells you whether participants are actually paying for it right now. Direction without participation is a trap.",
    "When structure and flow disagree, the market is usually in a redistribution phase — one side is being absorbed before the real move.",
    "Agreement is not about being bullish or bearish — it is about whether price and participation are telling the same story.",
    "Conviction grades the size of participation behind a flow read, not the certainty of the outcome.",
    "CVD measures aggressor volume: market buys minus market sells. It is the cleanest record of who is paying up to get filled.",
    "A high-conviction flow read against a fresh structure break is the classic fingerprint of absorption at the extremes.",
    "Weekend crypto flows are thinner — the same CVD reading carries more information on PAXG/BTC on Saturday than in a weekday session.",
    "Funding is the derivatives crowd's rent. Whoever pays rent is usually the crowded side.",
]
def lesson() -> str:
    return LESSONS[int(time.time() // 3600) % len(LESSONS)]

# ---------------------------------------------------------------- INDICATORS
def ema_stack(df: pd.DataFrame) -> str:
    if len(df) < 55: return "NEUTRAL"
    c = df.c
    e9, e21, e50 = (float(c.ewm(span=n, adjust=False).mean().iloc[-1]) for n in (9, 21, 50))
    if e9 > e21 > e50: return "BULL"
    if e9 < e21 < e50: return "BEAR"
    return "NEUTRAL"

def combine(*reads: str) -> str:
    reads = list(reads)
    if all(r == "BULL" for r in reads): return "BULL"
    if all(r == "BEAR" for r in reads): return "BEAR"
    bulls, bears = reads.count("BULL"), reads.count("BEAR")
    if bulls and not bears: return "BULL"
    if bears and not bulls: return "BEAR"
    return "NEUTRAL"

def fp(x, name):
    return f"${x:,.0f}" if "BTC" in name else f"${x:,.2f}"

# ---------------------------------------------------------------- CANDLE STORE
class CandleStore:
    def __init__(self, name, ws_sym=None):
        self.name, self.ws_sym = name, ws_sym
        self._c = {}
        self._df, self._df_ts = None, 0.0
        self.price, self.day_open = 0.0, None
        self.cvd_ticks = deque(maxlen=20000)
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
        self.price = c; self.last_update = time.time()
        self._trim(); self._df = None; self._update_day_open()

    def ingest_trade(self, t: dict):
        p, q = float(t["p"]), float(t["q"])
        bull = not t["m"]
        self.cvd_ticks.append((t["T"] / 1000, q if bull else -q))
        self.price = p; self.last_update = time.time(); self._df = None

    def ingest_td(self, values: list):
        for r in values:
            try:
                ts = int(datetime.strptime(r["datetime"], "%Y-%m-%d %H:%M:%S")
                         .replace(tzinfo=pytz.utc).timestamp() // 60)
                self._c[ts] = [float(r["open"]), float(r["high"]), float(r["low"]),
                               float(r["close"]), float(r.get("volume") or 0)]
            except Exception:
                continue
        if values: self.price = float(values[0]["close"])
        self.last_update = time.time()
        self._trim(); self._df = None; self._update_day_open()

    def _trim(self):
        cutoff = (time.time() - 5 * 86400) // 60     # keys are MINUTE epochs
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

    def cvd(self, window=3600) -> float:
        cut = time.time() - window
        return sum(v for ts, v in self.cvd_ticks if ts >= cut)

    def cvd_window(self, lo_s, hi_s) -> float:
        return sum(v for ts, v in self.cvd_ticks if lo_s <= ts <= hi_s)

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
            try:
                async with ses.ws_connect(host + "/".join(streams), heartbeat=25) as ws:
                    log.info(f"WS connected: {host}")
                    backoff = 5
                    for s in stores:
                        if s.ws_sym: s.source = "BINANCE WS"
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT: continue
                        d = json.loads(msg.data).get("data", {})
                        st = next((x for x in stores if x.ws_sym == d.get("s", "").lower()), None)
                        if not st: continue
                        if d.get("e") == "kline": st.ingest_kline(d["k"])
                        elif d.get("e") == "aggTrade": st.ingest_trade(d)
            except Exception as e:
                log.warning(f"WS down ({e}) — retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120)

async def crypto_rest_fallback(stores: list[CandleStore], ses: aiohttp.ClientSession):
    while True:
        await asyncio.sleep(30)
        for st in stores:
            if not st.ws_sym or time.time() - st.last_update < STALE_CRYPTO_SEC: continue
            try:
                async with ses.get(f"{BINANCE_REST}/klines?symbol={st.ws_sym.upper()}"
                                   f"&interval=1m&limit=3") as r:
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
                if isinstance(kl2, list) and kl2: kl_all = kl2 + kl_all
                for k in kl_all:
                    st._c[int(k[0]) // 60000] = [float(k[1]), float(k[2]),
                                                 float(k[3]), float(k[4]), float(k[5])]
                st.price = float(kl_all[-1][4]); st.last_update = time.time(); st._df = None
                st._update_day_open()
                log.info(f"{st.name}: bootstrapped {len(kl_all)} 1m bars")
        except Exception as e:
            log.error(f"bootstrap {st.name}: {e}")
        try:
            async with ses.get(f"{BINANCE_REST}/aggTrades?symbol={sym}&limit=1000") as r:
                tr = await r.json()
            if isinstance(tr, list):
                for t in tr:
                    st.ingest_trade({"p": t["p"], "q": t["q"], "m": t["m"], "T": t["T"]})
                log.info(f"{st.name}: flow seeded ({len(tr)} trades)")
        except Exception as e:
            log.error(f"bootstrap trades {st.name}: {e}")

async def bootstrap_gold(gold: CandleStore, ses: aiohttp.ClientSession):
    try:
        async with ses.get(f"https://api.twelvedata.com/time_series"
                           f"?symbol=XAU/USD&interval=1min&outputsize=500&apikey={TD_KEY}") as r:
            d = await r.json()
        if d.get("values"):
            gold.ingest_td(d["values"]); gold.source = "TWELVEDATA"
            log.info(f"GOLD bootstrapped {len(d['values'])} bars")
    except Exception as e:
        log.error(f"bootstrap gold: {e}")

async def gold_worker(gold: CandleStore, ses: aiohttp.ClientSession):
    while True:
        try:
            async with ses.get(f"https://api.twelvedata.com/time_series"
                               f"?symbol=XAU/USD&interval=1min&outputsize=60&apikey={TD_KEY}") as r:
                d = await r.json()
            if d.get("values"):
                gold.ingest_td(d["values"]); gold.source = "TWELVEDATA"
            else:
                async with ses.get("https://data-asg.goldprice.org/dbXRates/USD") as r2:
                    gp = (await r2.json())["items"][0]["xauPrice"]
                if gold.price <= 0 or abs(gp - gold.price) > 0.5:
                    gold.price = float(gp); gold.last_update = time.time()
                gold.source = "GOLDPRICE.ORG"
        except Exception as e:
            log.error(f"gold feed: {e}")
        await asyncio.sleep(GOLD_POLL if gold_market_open() else GOLD_POLL_CLOSED)

async def h4_worker(h4: dict, ses: aiohttp.ClientSession):
    while True:
        try:
            for sym, name in (("BTCUSDT", "BITCOIN"), ("PAXGUSDT", "PAXG")):
                async with ses.get(f"{BINANCE_REST}/klines?symbol={sym}&interval=4h&limit=120") as r:
                    kl = await r.json()
                if isinstance(kl, list) and kl:
                    h4[name] = pd.Series([float(k[4]) for k in kl])
            if gold_market_open():
                async with ses.get(f"https://api.twelvedata.com/time_series"
                                   f"?symbol=XAU/USD&interval=4h&outputsize=120&apikey={TD_KEY}") as r:
                    d = await r.json()
                if d.get("values"):
                    h4["GOLD"] = pd.Series([float(x["close"]) for x in d["values"]])
        except Exception as e:
            log.error(f"h4: {e}")
        await asyncio.sleep(H4_INTERVAL)

async def context_worker(ctx: dict, ses: aiohttp.ClientSession):
    syms = {"BTCUSDT": "BITCOIN", "PAXGUSDT": "PAXG"}
    while True:
        try:
            for fsym, name in syms.items():
                c = ctx.setdefault(name, {})
                try:
                    async with ses.get(f"{BINANCE_FAPI}/fapi/v1/premiumIndex?symbol={fsym}") as r:
                        c["funding"] = float((await r.json()).get("lastFundingRate", 0)) * 100
                except Exception: pass
                try:
                    async with ses.get(f"{BINANCE_FAPI}/fapi/v1/openInterest?symbol={fsym}") as r:
                        c["oi"] = float((await r.json()).get("openInterest", 0))
                except Exception: pass
                try:
                    async with ses.get(f"{BINANCE_REST}/depth?symbol={fsym}&limit=100") as r:
                        d = await r.json()
                    bid = sum(float(q) * float(p) for p, q in d.get("bids", []))
                    ask = sum(float(q) * float(p) for p, q in d.get("asks", []))
                    if bid + ask > 0: c["book"] = (bid - ask) / (bid + ask) * 100
                except Exception: pass
        except Exception as e:
            log.error(f"context: {e}")
        await asyncio.sleep(CTX_INTERVAL)

# ---------------------------------------------------------------- READS
DIR_EMOJI = {"BULL": "🟢", "BEAR": "🔴", "NEUTRAL": "⚪"}
DIR_WORD  = {"BULL": "Bullish", "BEAR": "Bearish", "NEUTRAL": "Neutral"}

def structure_read(st: CandleStore, h4: dict) -> tuple[str, str]:
    intra = combine(ema_stack(st.df("1h", 120)), ema_stack(st.df("15min", 120)))
    wk = "NEUTRAL"
    s = h4.get(st.name)
    if s is not None and len(s) >= 55:
        e9 = float(s.ewm(span=9, adjust=False).mean().iloc[-1])
        e21 = float(s.ewm(span=21, adjust=False).mean().iloc[-1])
        e50 = float(s.ewm(span=50, adjust=False).mean().iloc[-1])
        wk = "BULL" if e9 > e21 > e50 else "BEAR" if e9 < e21 < e50 else "NEUTRAL"
    return intra, wk

def flow_read(st: CandleStore, proxy: CandleStore | None) -> tuple[str, str]:
    fs = st if st.cvd_ticks else proxy
    if not fs or not fs.cvd_ticks:
        return "NEUTRAL", "—"
    c1h = fs.cvd(3600)
    vol = sum(abs(v) for _, v in fs.cvd_ticks) / max(1, len(fs.cvd_ticks))
    norm = abs(c1h) / (vol * 60) if vol > 0 else 0
    if c1h > 0: d = "BULL"
    elif c1h < 0: d = "BEAR"
    else: return "NEUTRAL", "—"
    conv = "High" if norm > 0.6 else "Medium" if norm > 0.25 else "Low"
    return d, conv

def agreement(intra: str, flow: str) -> tuple[int, str]:
    if intra == flow and intra != "NEUTRAL": pct = 100
    elif "NEUTRAL" in (intra, flow):         pct = 67
    elif intra != flow:                      pct = 33
    else:                                    pct = 67
    grade = "A" if pct >= 80 else "B" if pct >= 65 else "C"
    return pct, grade

def bar(pct: int) -> str:
    filled = round(pct / 10)
    return "█" * filled + "░" * (10 - filled)

def alignment_note(intra: str, flow: str) -> str:
    if intra == flow and intra != "NEUTRAL":
        return f"Trend and flow both {DIR_WORD[intra].lower()} — participation is confirming direction."
    if intra != "NEUTRAL" and flow != "NEUTRAL":
        return (f"Trend is {DIR_WORD[intra].lower()} but flow is {DIR_WORD[flow].lower()}ing — "
                f"participation is not confirming direction.")
    if intra == "NEUTRAL":
        return f"Structure neutral — flow is {DIR_WORD[flow].lower()}ing without trend confirmation."
    return f"Flow neutral — trend is {DIR_WORD[intra].lower()} without participation behind it."

def _reads(st, h4, proxy):
    intra, _ = structure_read(st, h4)
    fl, _ = flow_read(st, proxy)
    return intra, fl

def state_line(st: CandleStore, h4: dict, proxy: CandleStore | None,
               ctx: dict) -> str:
    intra, wk = structure_read(st, h4)
    fl, conv = flow_read(st, proxy)
    pct, grade = agreement(intra, fl)
    c = ctx.get(st.name, {})
    chg = ((st.price - st.day_open) / st.day_open * 100) if st.day_open else 0.0
    L = [f"{st.name} — Structure {DIR_EMOJI[intra]} {DIR_WORD[intra]} · "
         f"Flow {DIR_EMOJI[fl]} {DIR_WORD[fl]} ({conv}) · Weekly {DIR_WORD[wk]}",
         f"{bar(pct)} Agreement {pct}% · Grade {grade} · {fp(st.price, st.name)} ({chg:+.2f}% d)"]
    cx = []
    if "funding" in c: cx.append(f"funding {c['funding']:+.4f}%")
    if "oi" in c and c["oi"]: cx.append(f"OI {c['oi']:,.0f}")
    if "book" in c: cx.append(f"book {c['book']:+.1f}%")
    if cx: L.append("Derivatives: " + " | ".join(cx))
    return "\n".join(L)

def post_footer() -> str:
    return f"\n\n{FOOT}"

# ---------------------------------------------------------------- POSTS
def market_open_post(stores, h4, paxg, ctx, sess: str) -> str:
    L = [f"🌎 BRAX FX MARKET OPEN — {sess} SESSION", ""]
    for n, st in stores.items():
        if st.price <= 0: continue
        if n == "GOLD" and not gold_market_open(): continue
        L.append(state_line(st, h4, paxg if n == "GOLD" else None, ctx))
        L.append(f"  {alignment_note(*_reads(st, h4, paxg if n == 'GOLD' else None))}")
        L.append("")
    if not gold_market_open():
        L.append(f"XAU/USD spot closed — reopens {gold_next_open_eat().strftime('%a %H:%M EAT')}. "
                 f"PAXG tracked live as the 24/7 gold reference.")
    L.append(f"{sess} session initialized.")
    return "\n".join(L) + post_footer()

def flow_update_post(stores, h4, paxg, ctx, sess: str) -> str:
    L = [f"📡 {sess} SESSION · FLOW UPDATE", ""]
    for n, st in stores.items():
        if st.price <= 0: continue
        if n == "GOLD" and not gold_market_open(): continue
        intra, _ = structure_read(st, h4)
        fl, conv = flow_read(st, paxg if n == "GOLD" else None)
        pct, _ = agreement(intra, fl)
        L.append(f"• {n} — Flow {DIR_EMOJI[fl]} {DIR_WORD[fl]} ({conv}), "
                 f"structure {DIR_EMOJI[intra]} {DIR_WORD[intra]} · Agreement {pct}%")
        L.append(f"  {alignment_note(intra, fl)}")
        L.append("")
    return "\n".join(L).rstrip() + post_footer()

def right_now_post(stores, h4, paxg, ctx) -> str:
    L = [f"RIGHT NOW · LIVE READS  <code>{now_eat().strftime('%H:%M EAT')}</code>", ""]
    for n, st in stores.items():
        if st.price <= 0: continue
        if n == "GOLD" and not gold_market_open(): continue
        L.append(f"<b>{n}</b>")
        L.append(state_line(st, h4, paxg if n == "GOLD" else None, ctx))
        L.append(f"  {alignment_note(*_reads(st, h4, paxg if n == 'GOLD' else None))}")
        L.append("")
    return "\n".join(L).rstrip() + post_footer()

def weekend_review_post(stores, h4, paxg, ctx) -> str:
    L = ["📚 WEEKEND STRUCTURE REVIEW",
         "Spot gold desks are closed over the weekend — structure study only.", ""]
    for n, st in stores.items():
        if st.price <= 0: continue
        intra, wk = structure_read(st, h4)
        fl, conv = flow_read(st, paxg if n == "GOLD" else None)
        if n == "GOLD":
            L.append(f"• {n} — weekly structure {DIR_WORD[wk]}; last flow read "
                     f"{DIR_WORD[fl]} ({conv}) [Friday close {fp(st.price, n)}]")
        else:
            L.append(f"• {n} — weekly structure {DIR_WORD[wk]}; flow {DIR_WORD[fl]} ({conv}) "
                     f"· live {fp(st.price, n)}")
    L.append("")
    L.append(f"🎓 Lesson: {lesson()}")
    L.append(f"Spot gold reopens {gold_next_open_eat().strftime('%a %H:%M EAT')}. "
             f"BTC & PAXG tracked live through the weekend.")
    return "\n".join(L) + post_footer()

def session_close_post(stores, h4, paxg, ctx, sess: str) -> str:
    L = [f"🌙 {sess} SESSION · CLOSE", ""]
    for n, st in stores.items():
        if st.price <= 0: continue
        if n == "GOLD" and not gold_market_open(): continue
        chg = ((st.price - st.day_open) / st.day_open * 100) if st.day_open else 0.0
        intra, _ = structure_read(st, h4)
        fl, conv = flow_read(st, paxg if n == "GOLD" else None)
        L.append(f"• {n} — {fp(st.price, n)} ({chg:+.2f}% session day) · "
                 f"closing state: structure {DIR_WORD[intra]}, flow {DIR_WORD[fl]} ({conv})")
    L.append("")
    if now_eat().weekday() == 4:
        L.append("XAU/USD spot closes for the weekend at 01:00 EAT (Fri 22:00 UTC). "
                 "BTC & PAXG remain live.")
    return "\n".join(L) + post_footer()

# ---------------------------------------------------------------- SCHEDULER
async def post_loop(stores, h4, paxg, ctx, tg, ses):
    done_opens, done_flows, done_special = set(), set(), set()

    await tg.text(ses,
        f"<b>{BRAND}</b>\n"
        "Real-time market state engine — structure · flow · agreement.\n"
        "Session opens, 2-hourly flow updates, weekend structure review.\n"
        "BTC & PAXG live 24/7 · XAU/USD spot-hours aware\n"
        "<code>/now for an instant read</code>")

    while True:
        try:
            t = now_eat()
            d, h = t.date(), t.hour
            sess = session_name()

            if h in (2, 8, 13, 17) and (d, h) not in done_opens:
                done_opens.add((d, h))
                name = next(s for s, a, b in SESSIONS if a <= h < b)
                await tg.text(ses, market_open_post(stores, h4, paxg, ctx, name))

            if h in FLOW_HOURS and (d, h) not in done_flows and sess:
                done_flows.add((d, h))
                await tg.text(ses, flow_update_post(stores, h4, paxg, ctx, sess))

            if t.weekday() == 5 and h == 10 and (d, "wk") not in done_special:
                done_special.add((d, "wk"))
                await tg.text(ses, weekend_review_post(stores, h4, paxg, ctx))
            if t.weekday() == 6 and h == 21 and t.minute >= 30 and (d, "re") not in done_special:
                done_special.add((d, "re"))
                await tg.text(ses,
                    "🌎 XAU/USD SPOT REOPENS — Sun 22:00 UTC (01:00 Mon EAT)\n"
                    "Gold desk resumes live tracking on the next update."
                    + post_footer())
            if h == 21 and (d, "cl") not in done_special:
                done_special.add((d, "cl"))
                await tg.text(ses, session_close_post(stores, h4, paxg, ctx, "NY"))
        except Exception as e:
            log.error(f"post_loop: {e}")
        await asyncio.sleep(TICK_INTERVAL)

# ---------------------------------------------------------------- CHART
def make_chart(st: CandleStore, path):
    df = st.df("15min", 80)
    fig, (ax, axv) = plt.subplots(2, 1, figsize=(11, 6), dpi=100, sharex=True,
                                  gridspec_kw={"height_ratios": [4, 1]})
    fig.patch.set_facecolor("#131722")
    for a in (ax, axv): a.set_facecolor("#131722")
    up, dn = "#26a69a", "#ef5350"
    for i, (_, r) in enumerate(df.iterrows()):
        c = up if r.c >= r.o else dn
        ax.vlines(i, r.l, r.h, color=c, lw=1)
        ax.bar(i, r.c - r.o, bottom=r.o, width=0.6, color=c, edgecolor=c, zorder=3)
        axv.bar(i, r.v if pd.notna(r.v) else 0, width=0.6, color=c, alpha=0.6)
    for a in (ax, axv):
        a.tick_params(colors="white")
        for sp in a.spines.values(): sp.set_color("#444")
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
HELP = (f"<b>{BRAND} — COMMANDS</b>\n"
        "/now — instant live reads (all assets)\n"
        "/flow BTC|PAXG — CVD detail from aggressor trades\n"
        "/book BTC|PAXG — order-book imbalance + funding\n"
        "/chart BTC|PAXG|GOLD — 15m candles + volume\n"
        "/health — feed status\n"
        "/help — this menu")

def pick(args, stores, paxg):
    if not args: return None
    w = args[0].upper()
    return {"BTC": stores["BITCOIN"], "BITCOIN": stores["BITCOIN"],
            "PAXG": paxg, "GOLD": stores["GOLD"]}.get(w)

async def command_worker(stores, paxg, h4, ctx, tg, ses):
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

                elif cmd == "/now":
                    await tg.text(ses, right_now_post(stores, h4, paxg, ctx))

                elif cmd == "/flow":
                    st = pick(args, stores, paxg)
                    if not st or not st.ws_sym:
                        await tg.text(ses, "Usage: /flow BTC or /flow PAXG"); continue
                    if not st.cvd_ticks:
                        await tg.text(ses, f"{st.name}: flow feed warming up."); continue
                    now = time.time()
                    L = [f"<b>{st.name} ORDER FLOW</b> @ {fp(st.price, st.name)}", ""]
                    for lab, w in (("5m", 300), ("30m", 1800), ("1h", 3600), ("3h", 10800)):
                        cw = st.cvd(w)
                        pw = st.cvd_window(now - 2 * w, now - w)
                        L.append(f"CVD {lab}: {cw:+,.0f} | prev: {pw:+,.0f} | Δ {cw - pw:+,.0f}")
                    L.append("")
                    L.append("CVD = market buys − market sells (real aggressor trades).")
                    await tg.text(ses, "\n".join(L))

                elif cmd == "/book":
                    st = pick(args, stores, paxg)
                    if not st or not st.ws_sym:
                        await tg.text(ses, "Usage: /book BTC or /book PAXG"); continue
                    c = ctx.get(st.name, {})
                    L = [f"<b>{st.name} BOOK</b> @ {fp(st.price, st.name)}"]
                    if "book" in c:
                        L.append(f"Top-100 notional imbalance: {c['book']:+.1f}% "
                                 f"({'bid-side heavier' if c['book'] > 0 else 'ask-side heavier' if c['book'] < 0 else 'balanced'})")
                    if "funding" in c: L.append(f"Perp funding: {c['funding']:+.4f}%/8h")
                    if "oi" in c and c["oi"]: L.append(f"Open interest: {c['oi']:,.0f}")
                    L.append("<i>Resting orders are not trades.</i>")
                    await tg.text(ses, "\n".join(L))

                elif cmd == "/chart":
                    st = pick(args, stores, paxg)
                    if not st:
                        await tg.text(ses, "Usage: /chart BTC | /chart PAXG | /chart GOLD"); continue
                    path = f"/tmp/{st.name}.png"
                    await asyncio.get_event_loop().run_in_executor(None, make_chart, st, path)
                    await tg.photo(ses, path, f"{st.name} 15m — {fp(st.price, st.name)}")
                    if os.path.exists(path): os.remove(path)

                elif cmd == "/health":
                    L = ["<b>🩺 FEED HEALTH</b>"]
                    for st in stores.values():
                        live = st.data_age() < (180 if st.ws_sym else STALE_FEED_SEC)
                        L.append(f"{st.name}: {st.source} | {fp(st.price, st.name)} | "
                                 f"{int(st.data_age())}s | {'LIVE' if live else 'STALE'}")
                    if not gold_market_open():
                        L.append(f"XAU spot closed — reopens {gold_next_open_eat().strftime('%a %H:%M EAT')}")
                    await tg.text(ses, "\n".join(L))
        except Exception as e:
            log.error(f"cmd: {e}")
            await asyncio.sleep(5)

# ---------------------------------------------------------------- KEEPALIVE / MAIN
async def keepalive(ses):
    if not SELF_URL: return
    while True:
        try: await ses.get(SELF_URL)
        except Exception: pass
        await asyncio.sleep(600)

async def main():
    btc  = CandleStore("BITCOIN", ws_sym="btcusdt")
    paxg = CandleStore("PAXG",    ws_sym="paxgusdt")
    gold = CandleStore("GOLD")
    h4: dict = {}
    ctx: dict = {}
    tg = TG()

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25)) as ses:
        await bootstrap_crypto([btc, paxg], ses)
        await bootstrap_gold(gold, ses)

        stores = {"BITCOIN": btc, "GOLD": gold, "PAXG": paxg}
        tasks = [asyncio.create_task(binance_worker([btc, paxg])),
                 asyncio.create_task(crypto_rest_fallback([btc, paxg], ses)),
                 asyncio.create_task(gold_worker(gold, ses)),
                 asyncio.create_task(h4_worker(h4, ses)),
                 asyncio.create_task(context_worker(ctx, ses)),
                 asyncio.create_task(command_worker(stores, paxg, h4, ctx, tg, ses)),
                 asyncio.create_task(keepalive(ses))]
        try:
            await post_loop(stores, h4, paxg, ctx, tg, ses)
        finally:
            for t in tasks:
                t.cancel()

app = Flask(__name__)

@app.route("/")
def health():
    return "BRAX FX — MARKET INTELLIGENCE RUNNING", 200

def run_flask():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)), use_reloader=False)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    asyncio.run(main())
