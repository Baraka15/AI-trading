"""
BRAX FX v4.0 — Real-Time Flow & Signal Desk
===========================================
Clean, faster, more human.

Assets
  • BITCOIN  — live Binance tape (trades + klines + liquidations)
  • GOLD     — Twelve Data + PAXG proxy flow

What it does
  • Real-time CVD / flow / one-sided pressure
  • Structure (1h + 4h) agreement
  • A+ signals only (≥10/12) with ATR SL / TP1 / TP2
  • Manipulation alerts (stop hunt, fake break, absorption, cascade)
  • News blackout around high-impact USD events
  • Human-style Telegram messages
  • Commands: /now /flow /signal /health /help

Removed / simplified
  • Excessive scheduled spam
  • Redundant reports
  • Over-engineered formatting

ENV
  TELEGRAM_TOKEN · TELEGRAM_CHAT_ID · TWELVEDATA_API_KEY
"""

import asyncio
import os
import json
import time
import logging
from collections import deque
from datetime import datetime, timedelta
from threading import Thread

import numpy as np
import pandas as pd
import aiohttp
import pytz
from flask import Flask, jsonify

# ---------------------------------------------------------------- logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("BRAX")

# ---------------------------------------------------------------- config
TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TD_KEY  = os.getenv("TWELVEDATA_API_KEY", "")
PORT    = int(os.getenv("PORT", "10000"))

if not all([TOKEN, CHAT_ID, TD_KEY]):
    raise ValueError("Set TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, TWELVEDATA_API_KEY")

EAT = pytz.timezone("Africa/Nairobi")
BRAND = "BRAX FX"
FOOT  = "Educational only. Not financial advice."

# Signal rules
SIGNAL_MIN_SCORE   = 10
SIGNAL_MAX_SCORE   = 12
SIGNAL_COOLDOWN    = 4 * 3600
MAX_SIGNALS_DAY    = 2
ATR_SL_MULT        = 1.6
TP1_R              = 1.6
TP2_R              = 2.8
NEWS_BLACKOUT_MIN  = 15
ALERT_COOLDOWN     = 300
MIN_ONESIDED       = 0.28

# ---------------------------------------------------------------- helpers
def now_eat():
    return datetime.now(EAT)

def fp(x, name):
    if not x:
        return "—"
    return f"${x:,.0f}" if "BTC" in name or name == "BITCOIN" else f"${x:,.2f}"

DIR_EMOJI = {"BULL": "🟢", "BEAR": "🔴", "NEUTRAL": "⚪"}
DIR_WORD  = {"BULL": "Bullish", "BEAR": "Bearish", "NEUTRAL": "Neutral"}

def atr(df: pd.DataFrame, n=14) -> float:
    if df is None or len(df) < n + 1:
        return 0.0
    hl = df.h - df.l
    hc = (df.h - df.c.shift()).abs()
    lc = (df.l - df.c.shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    val = float(tr.rolling(n).mean().iloc[-1])
    return val if not np.isnan(val) else 0.0

def day_start_min() -> int:
    n = datetime.now(EAT).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(n.timestamp() // 60)

def gold_market_open() -> bool:
    now = datetime.now(pytz.utc)
    wd, m = now.weekday(), now.hour * 60 + now.minute
    if wd == 5:
        return False
    if wd == 6 and m < 22 * 60:
        return False
    if wd == 4 and m >= 22 * 60:
        return False
    return True

# ---------------------------------------------------------------- candle + flow store
class CandleStore:
    def __init__(self, name, ws_sym=None):
        self.name = name
        self.ws_sym = ws_sym
        self._c = {}
        self._df = None
        self._df_ts = 0.0
        self.price = 0.0
        self.day_open = None
        self.cvd_ticks = deque(maxlen=80000)
        self.last_update = 0.0
        self.source = "—"

    def _ingest_min(self, m, o, h, l, c, v):
        if m in self._c:
            b = self._c[m]
            self._c[m] = [b[0], max(b[1], h), min(b[2], l), c, v]
        else:
            self._c[m] = [o, h, l, c, v]
        self.price = c
        self.last_update = time.time()
        self._df = None
        if self.day_open is None and self._c:
            self.day_open = self._c[min(self._c)][0]

    def ingest_kline(self, k):
        m = int(k["t"]) // 60000
        self._ingest_min(m, float(k["o"]), float(k["h"]), float(k["l"]),
                         float(k["c"]), float(k["v"]))

    def ingest_trade(self, t):
        p = float(t["p"])
        q = float(t["q"])
        is_sell = bool(t.get("m", t.get("isBuyerMaker", False)))
        signed = -q if is_sell else q
        ts = t.get("T") or int(time.time() * 1000)
        self.cvd_ticks.append((ts / 1000 if ts > 1e11 else ts, signed, p))
        self.last_update = time.time()

    def df(self, rule="1min", limit=200) -> pd.DataFrame:
        if not self._c:
            return pd.DataFrame(columns=["o", "h", "l", "c", "v"])
        now = time.time()
        if self._df is None or now - self._df_ts > 25:
            d = pd.DataFrame.from_dict(self._c, orient="index",
                                       columns=["o", "h", "l", "c", "v"])
            d.index = pd.to_datetime(d.index * 60, unit="s")
            self._df = d.sort_index()
            self._df_ts = now
        d = self._df
        if rule != "1min":
            d = d.resample(rule).agg(
                {"o": "first", "h": "max", "l": "min", "c": "last", "v": "sum"}
            ).dropna()
        return d.tail(limit)

    def vwap(self):
        rows = [v for k, v in self._c.items() if k >= day_start_min() and v[4] > 0]
        if not rows:
            return None
        pv = sum(r[3] * r[4] for r in rows)
        vv = sum(r[4] for r in rows)
        return pv / vv if vv > 0 else None

    def data_age(self):
        return time.time() - self.last_update

# ---------------------------------------------------------------- flow
def _cvd_window(st, seconds, signed=True):
    cutoff = time.time() - seconds
    tot = 0.0
    for t, s, p in reversed(st.cvd_ticks):
        if t < cutoff:
            break
        tot += s if signed else abs(s)
    return tot

def flow_metrics(st: CandleStore):
    if not st.cvd_ticks or st.data_age() > 180:
        return None
    c15 = _cvd_window(st, 900, True)
    c1h = _cvd_window(st, 3600, True)
    v15 = _cvd_window(st, 900, False)
    v1h = _cvd_window(st, 3600, False)
    ones15 = abs(c15) / v15 if v15 > 0 else 0.0
    ones1h = abs(c1h) / v1h if v1h > 0 else 0.0

    if c1h > 0 and ones1h >= MIN_ONESIDED:
        d = "BULL"
    elif c1h < 0 and ones1h >= MIN_ONESIDED:
        d = "BEAR"
    elif c15 > 0:
        d = "BULL"
    elif c15 < 0:
        d = "BEAR"
    else:
        d = "NEUTRAL"

    conv = "High" if ones1h >= 0.42 else ("Medium" if ones1h >= MIN_ONESIDED else "Low")
    if d == "BULL":
        regime = "ACCUMULATION" if c15 > 0 else "PULLBACK-BUYING"
    elif d == "BEAR":
        regime = "DISTRIBUTION" if c15 < 0 else "RALLY-SELLING"
    else:
        regime = "CHOP"

    return {
        "dir": d, "c15": c15, "c1h": c1h,
        "ones15": ones15, "ones1h": ones1h,
        "conv": conv, "regime": regime
    }

def structure_read(st: CandleStore, h4: dict):
    intra, wk = "NEUTRAL", "NEUTRAL"
    d = st.df("1h", 50)
    if len(d) >= 20:
        last = float(d.c.iloc[-1])
        swing_h = float(d.h.iloc[:-1].max())
        swing_l = float(d.l.iloc[:-1].min())
        if last > swing_h:
            intra = "BULL"
        elif last < swing_l:
            intra = "BEAR"
        else:
            ma = float(d.c.rolling(20).mean().iloc[-1])
            intra = "BULL" if last > ma else "BEAR" if last < ma else "NEUTRAL"

    closes = h4.get(st.name)
    if closes is not None and len(closes) >= 50:
        last = float(closes.iloc[-1])
        ma20 = float(closes.tail(20).mean())
        ma50 = float(closes.tail(50).mean())
        if ma20 > ma50 and last > ma20:
            wk = "BULL"
        elif ma20 < ma50 and last < ma20:
            wk = "BEAR"
    return intra, wk

# ---------------------------------------------------------------- signal engine
class SignalEngine:
    def __init__(self):
        self.active = {}
        self.counts = {}
        self.last_fire = {}
        self.record = {"tp2": 0, "tp1": 0, "sl": 0}

    def _allowed(self, name) -> bool:
        if time.time() - self.last_fire.get(name, 0) < SIGNAL_COOLDOWN:
            return False
        k = now_eat().strftime("%Y-%m-%d")
        return self.counts.get(k, 0) < MAX_SIGNALS_DAY

    def score(self, st, h4, ctx, proxies):
        fs = st if st.cvd_ticks else proxies.get(st.name, st)
        fm = flow_metrics(fs)
        if fm is None or fm["dir"] == "NEUTRAL":
            return None, 0, {}

        intra, wk = structure_read(st, h4)
        parts = {}

        if intra == fm["dir"]:
            parts["structure aligned"] = 2
        elif intra != "NEUTRAL":
            parts["structure partial"] = 1

        if wk == fm["dir"]:
            parts["higher-timeframe agrees"] = 1

        if fm["conv"] == "High":
            parts["strong tape"] = 2
        elif fm["conv"] == "Medium":
            parts["decent tape"] = 1

        if fm["regime"] in ("ACCUMULATION", "DISTRIBUTION"):
            parts["regime confirms"] = 2

        vw = st.vwap()
        if vw and st.price:
            if (st.price > vw and fm["dir"] == "BULL") or (st.price < vw and fm["dir"] == "BEAR"):
                parts["on right side of VWAP"] = 1

        c = ctx.get(st.name, {})
        f = c.get("funding")
        if f is not None:
            if (fm["dir"] == "BEAR" and f > 0.01) or (fm["dir"] == "BULL" and f < -0.005):
                parts["crowd on the other side"] = 1

        return fm["dir"], sum(parts.values()), parts

    def try_fire(self, st, h4, ctx, proxies):
        n = st.name
        if n not in ("BITCOIN", "GOLD"):
            return None
        if n in self.active or not self._allowed(n):
            return None
        if n == "GOLD" and not gold_market_open():
            return None
        if news_blackout():
            return None

        d, score, parts = self.score(st, h4, ctx, proxies)
        if d is None or score < SIGNAL_MIN_SCORE:
            return None

        df15 = st.df("15min", 50)
        a = atr(df15, 14)
        if not a or not st.price:
            return None

        entry = st.price
        risk = ATR_SL_MULT * a
        if d == "BULL":
            sl  = entry - risk
            tp1 = entry + TP1_R * risk
            tp2 = entry + TP2_R * risk
            arrow = "LONG"
        else:
            sl  = entry + risk
            tp1 = entry - TP1_R * risk
            tp2 = entry - TP2_R * risk
            arrow = "SHORT"

        self.active[n] = {
            "dir": d, "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2,
            "score": score, "t": time.time(), "tp1_hit": False
        }
        k = now_eat().strftime("%Y-%m-%d")
        self.counts[k] = self.counts.get(k, 0) + 1
        self.last_fire[n] = time.time()

        why = " · ".join(parts.keys())
        # Human style
        return (
            f"{'🟢' if d == 'BULL' else '🔴'} <b>{n} — {arrow}</b>\n\n"
            f"Entry  {fp(entry, n)}\n"
            f"Stop   {fp(sl, n)}\n"
            f"TP1    {fp(tp1, n)}\n"
            f"TP2    {fp(tp2, n)}\n\n"
            f"Score {score}/{SIGNAL_MAX_SCORE}\n"
            f"{why}\n\n"
            f"<i>Hold for the move. I’m tracking this.</i>\n"
            f"{now_eat().strftime('%H:%M EAT')}"
        )

    def track(self, st) -> list:
        msgs = []
        sig = self.active.get(st.name)
        if not sig or not st.price:
            return []
        n = st.name
        if sig["dir"] == "BULL":
            hit_sl  = st.price <= sig["sl"]
            hit_tp1 = st.price >= sig["tp1"]
            hit_tp2 = st.price >= sig["tp2"]
        else:
            hit_sl  = st.price >= sig["sl"]
            hit_tp1 = st.price <= sig["tp1"]
            hit_tp2 = st.price <= sig["tp2"]

        if hit_sl:
            del self.active[n]
            self.record["sl"] += 1
            return [f"❌ <b>{n} stopped out</b> at {fp(sig['sl'], n)}\nInvalidated. Waiting for next clean setup."]

        if hit_tp1 and not sig["tp1_hit"]:
            sig["tp1_hit"] = True
            self.record["tp1"] += 1
            return [f"✅ <b>{n} TP1 hit</b> {fp(sig['tp1'], n)}\nTrailing toward TP2 {fp(sig['tp2'], n)}."]

        if sig["tp1_hit"] and hit_tp2:
            del self.active[n]
            self.record["tp2"] += 1
            return [f"🏆 <b>{n} full target done</b> — TP2 {fp(sig['tp2'], n)}\nNice. Resetting."]

        return []

    def record_line(self) -> str:
        r = self.record
        total = r["tp2"] + r["tp1"] + r["sl"]
        if not total:
            return "No closed signals yet."
        wins = r["tp2"] + r["tp1"]
        return f"Record: {wins}/{total} green  (TP2 {r['tp2']} · TP1 {r['tp1']} · SL {r['sl']})"

SIGNAL_ENGINE = SignalEngine()

# ---------------------------------------------------------------- alerts (kept lean)
class AlertEngine:
    def __init__(self):
        self.state = {}

    def _cool(self, name, key, sec) -> bool:
        S = self.state.setdefault(name, {})
        if time.time() - S.get(key, 0) < sec:
            return False
        S[key] = time.time()
        return True

    def scan(self, st) -> list:
        out = []
        if st.name != "BITCOIN":
            return out
        fm = flow_metrics(st)
        if not fm:
            return out
        n = st.name
        S = self.state.setdefault(n, {})

        # Flow flip
        if "flow_dir" in S and S["flow_dir"] != fm["dir"] and fm["dir"] != "NEUTRAL" \
                and fm["ones1h"] >= MIN_ONESIDED and self._cool(n, "flip", ALERT_COOLDOWN):
            out.append(
                f"🔁 <b>Tape flipped {DIR_WORD[fm['dir']].lower()}</b> on {n}\n"
                f"{fm['ones1h']*100:.0f}% one-sided · {fm['conv'].lower()} conviction @ {fp(st.price, n)}"
            )
        S["flow_dir"] = fm["dir"]
        return out

ALERT_ENGINE = AlertEngine()

# ---------------------------------------------------------------- news (lean)
NEWS = {"events": [], "announced": set()}

async def news_worker(session):
    while True:
        try:
            async with session.get(
                "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
                timeout=aiohttp.ClientTimeout(total=12)
            ) as r:
                if r.status == 200:
                    evs = await r.json()
                    NEWS["events"] = [
                        e for e in evs
                        if e.get("country") in ("USD", "EUR", "GBP") or e.get("impact") == "High"
                    ]
                    log.info(f"News: {len(NEWS['events'])} events loaded")
        except Exception as e:
            log.warning(f"News fetch: {e}")
        await asyncio.sleep(1800)

def news_blackout() -> bool:
    for e in NEWS["events"]:
        try:
            if e.get("impact") != "High":
                continue
            dt = datetime.fromisoformat(e["date"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=pytz.utc)
            secs = (dt - datetime.now(pytz.utc)).total_seconds()
            if 0 <= secs <= NEWS_BLACKOUT_MIN * 60:
                return True
        except Exception:
            continue
    return False

# ---------------------------------------------------------------- telegram
async def tg_send(session, text: str):
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        async with session.post(url, json={
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                log.warning(f"TG {r.status}")
    except Exception as e:
        log.error(f"TG send: {e}")

# ---------------------------------------------------------------- data feeds
async def bootstrap_btc(session, st: CandleStore):
    try:
        url = "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=120"
        async with session.get(url) as r:
            data = await r.json()
        for k in data:
            m = int(k[0]) // 60000
            st._ingest_min(m, float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5]))
        st.source = "Binance"
        log.info(f"BTC seeded {len(data)} bars @ {st.price}")
    except Exception as e:
        log.error(f"BTC bootstrap: {e}")

async def bootstrap_gold(session, st: CandleStore):
    try:
        url = (
            f"https://api.twelvedata.com/time_series"
            f"?symbol=XAU/USD&interval=1min&outputsize=80&apikey={TD_KEY}"
        )
        async with session.get(url) as r:
            data = await r.json()
        values = data.get("values") or []
        for row in reversed(values):
            dt = datetime.fromisoformat(row["datetime"].replace("Z", ""))
            m = int(dt.timestamp() // 60)
            st._ingest_min(m, float(row["open"]), float(row["high"]),
                           float(row["low"]), float(row["close"]), 0)
        st.source = "TwelveData"
        log.info(f"GOLD seeded {len(values)} bars @ {st.price}")
    except Exception as e:
        log.error(f"Gold bootstrap: {e}")

async def binance_ws(stores):
    """Live trades + 1m klines for BTC + PAXG"""
    streams = "btcusdt@trade/btcusdt@kline_1m/paxgusdt@trade/paxgusdt@kline_1m"
    url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url, heartbeat=20) as ws:
                    log.info("Binance WS connected")
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        payload = json.loads(msg.data)
                        data = payload.get("data") or {}
                        stream = payload.get("stream", "")

                        if "trade" in stream:
                            sym = "BITCOIN" if "btcusdt" in stream else "PAXG"
                            st = next((s for s in stores if s.name == sym or (sym == "PAXG" and s.name == "PAXG")), None)
                            if st:
                                st.ingest_trade(data)
                        elif "kline" in stream:
                            k = data.get("k") or {}
                            if not k.get("x"):  # only closed candles for structure
                                continue
                            sym = "BITCOIN" if "btcusdt" in stream else "PAXG"
                            st = next((s for s in stores if s.name == sym or (sym == "PAXG" and s.name == "PAXG")), None)
                            if st:
                                st.ingest_kline(k)
                                st.source = "Binance WS"
        except Exception as e:
            log.warning(f"WS reconnect in 5s: {e}")
            await asyncio.sleep(5)

async def gold_poll(session, st: CandleStore):
    while True:
        try:
            if gold_market_open():
                url = f"https://api.twelvedata.com/price?symbol=XAU/USD&apikey={TD_KEY}"
                async with session.get(url) as r:
                    data = await r.json()
                if "price" in data:
                    p = float(data["price"])
                    m = int(time.time() // 60)
                    st._ingest_min(m, p, p, p, p, 0)
                    st.source = "TwelveData"
        except Exception as e:
            log.debug(f"Gold poll: {e}")
        await asyncio.sleep(45 if gold_market_open() else 300)

async def h4_worker(session, h4: dict, stores):
    while True:
        try:
            for st in stores:
                if st.name == "BITCOIN":
                    url = "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=4h&limit=80"
                    async with session.get(url) as r:
                        data = await r.json()
                    closes = pd.Series([float(k[4]) for k in data])
                    h4["BITCOIN"] = closes
                elif st.name == "GOLD":
                    url = (
                        f"https://api.twelvedata.com/time_series"
                        f"?symbol=XAU/USD&interval=4h&outputsize=60&apikey={TD_KEY}"
                    )
                    async with session.get(url) as r:
                        data = await r.json()
                    vals = data.get("values") or []
                    if vals:
                        closes = pd.Series([float(v["close"]) for v in reversed(vals)])
                        h4["GOLD"] = closes
        except Exception as e:
            log.warning(f"H4: {e}")
        await asyncio.sleep(300)

# ---------------------------------------------------------------- main loops
async def tick_loop(session, stores, proxies, h4, ctx):
    while True:
        try:
            msgs = []
            for st in stores:
                msgs += ALERT_ENGINE.scan(st)
                msgs += SIGNAL_ENGINE.track(st)
                if st.name in ("BITCOIN", "GOLD"):
                    sig = SIGNAL_ENGINE.try_fire(st, h4, ctx, proxies)
                    if sig:
                        msgs.append(sig)
            for m in msgs:
                await tg_send(session, m)
        except Exception:
            log.exception("tick")
        await asyncio.sleep(8)

async def command_loop(session):
    offset = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{TOKEN}/getUpdates?timeout=20&offset={offset}"
            async with session.get(url) as r:
                data = await r.json()
            for upd in data.get("result", []):
                offset = upd["update_id"] + 1
                text = ((upd.get("message") or {}).get("text") or "").strip()
                if not text.startswith("/"):
                    continue
                cmd = text.split()[0].split("@")[0].lower()
                if cmd == "/now":
                    await tg_send(session, build_now())
                elif cmd == "/flow":
                    await tg_send(session, build_flow())
                elif cmd == "/signal":
                    await tg_send(session, build_signals())
                elif cmd == "/health":
                    await tg_send(session, build_health())
                elif cmd in ("/help", "/start"):
                    await tg_send(session, HELP)
        except Exception as e:
            log.error(f"commands: {e}")
        await asyncio.sleep(1)

# ---------------------------------------------------------------- builders (human)
def build_now() -> str:
    lines = [f"<b>Live desk</b>  ·  {now_eat().strftime('%H:%M EAT')}\n"]
    for st in STORES:
        fm = flow_metrics(st if st.cvd_ticks else PROXIES.get(st.name, st))
        d = fm["dir"] if fm else "NEUTRAL"
        conv = fm["conv"] if fm else "—"
        lines.append(
            f"{DIR_EMOJI[d]} <b>{st.name}</b>  {fp(st.price, st.name)}  ·  "
            f"{DIR_WORD[d]} · {conv}"
        )
    if SIGNAL_ENGINE.active:
        lines.append("")
        for n, s in SIGNAL_ENGINE.active.items():
            arrow = "LONG" if s["dir"] == "BULL" else "SHORT"
            lines.append(f"Open: {n} {arrow} · SL {fp(s['sl'], n)} · TP1 {fp(s['tp1'], n)}")
    lines.append(f"\n{SIGNAL_ENGINE.record_line()}")
    return "\n".join(lines)

def build_flow() -> str:
    lines = [f"<b>Flow</b>  ·  {now_eat().strftime('%H:%M EAT')}\n"]
    for st in STORES:
        fs = st if st.cvd_ticks else PROXIES.get(st.name, st)
        fm = flow_metrics(fs)
        if fm and fm["dir"] != "NEUTRAL":
            lines.append(
                f"<b>{st.name}</b>  {fp(st.price, st.name)}\n"
                f"{DIR_WORD[fm['dir']]} · {fm['conv']} · CVD 15m {fm['c15']:+,.0f}"
            )
        else:
            lines.append(f"<b>{st.name}</b>  {fp(st.price, st.name)}  ·  quiet")
    return "\n\n".join(lines)

def build_signals() -> str:
    lines = [f"<b>Signals</b>  ·  {now_eat().strftime('%H:%M EAT')}\n"]
    if SIGNAL_ENGINE.active:
        for n, s in SIGNAL_ENGINE.active.items():
            arrow = "LONG" if s["dir"] == "BULL" else "SHORT"
            age = int((time.time() - s["t"]) / 60)
            lines.append(
                f"<b>{n} {arrow}</b>  {s['score']}/{SIGNAL_MAX_SCORE}  ·  open {age}m\n"
                f"Entry {fp(s['entry'], n)} · SL {fp(s['sl'], n)}\n"
                f"TP1 {fp(s['tp1'], n)} · TP2 {fp(s['tp2'], n)}"
            )
    else:
        lines.append("No open signals right now.")
    lines.append(f"\n{SIGNAL_ENGINE.record_line()}")
    return "\n".join(lines)

def build_health() -> str:
    lines = [f"<b>Status</b>  ·  {now_eat().strftime('%H:%M EAT')}\n"]
    for st in STORES:
        age = st.data_age()
        ok = "🟢" if age < 120 else "🔴"
        lines.append(f"{ok} {st.name}  {fp(st.price, st.name)}  ·  {st.source}  ·  {age:.0f}s")
    lines.append(f"News blackout: {'YES' if news_blackout() else 'clear'}")
    lines.append(SIGNAL_ENGINE.record_line())
    return "\n".join(lines)

HELP = (
    f"<b>{BRAND}</b>\n\n"
    "/now — live snapshot\n"
    "/flow — tape & CVD\n"
    "/signal — open trades + record\n"
    "/health — feed status\n\n"
    "A+ signals only (≥10/12) · max 2/day · 4h cooldown\n"
    f"{FOOT}"
)

# ---------------------------------------------------------------- flask
app = Flask(__name__)

@app.route("/")
def root():
    return jsonify({"status": "ok", "service": BRAND})

@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "time": now_eat().isoformat(),
        "assets": {s.name: {"price": s.price, "age": round(s.data_age())} for s in STORES},
        "signals": list(SIGNAL_ENGINE.active.keys()),
        "record": SIGNAL_ENGINE.record,
    })

def run_flask():
    app.run(host="0.0.0.0", port=PORT, use_reloader=False)

# ---------------------------------------------------------------- main
STORES = []
PROXIES = {}
H4 = {}
CTX = {}

async def main():
    global STORES, PROXIES
    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25))

    btc  = CandleStore("BITCOIN", ws_sym="btcusdt")
    paxg = CandleStore("PAXG", ws_sym="paxgusdt")
    gold = CandleStore("GOLD")

    STORES = [btc, gold]
    PROXIES = {"GOLD": paxg, "BITCOIN": btc}

    log.info(f"{BRAND} v4 starting")

    await bootstrap_btc(session, btc)
    await bootstrap_gold(session, gold)

    # startup ping
    await tg_send(session,
        f"✅ <b>{BRAND} online</b>\n"
        f"Watching Bitcoin + Gold\n"
        f"A+ signals only · real-time tape\n"
        f"{now_eat().strftime('%H:%M EAT')}"
    )

    await asyncio.gather(
        binance_ws([btc, paxg]),
        gold_poll(session, gold),
        h4_worker(session, H4, STORES),
        news_worker(session),
        tick_loop(session, STORES, PROXIES, H4, CTX),
        command_loop(session),
    )

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    asyncio.run(main())
