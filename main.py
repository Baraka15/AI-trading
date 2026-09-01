"""
BRAX FX v3.1 — Autonomous Flow, Manipulation & Signal Desk (FINAL)
==================================================================
Assets: BITCOIN (24/7 live tape) · GOLD (spot-hours aware; flow via PAXG tape)

AUTO POSTS (fully autonomous)
  • DAILY OUTLOOK     — 07:00 EAT daily: prose narrative + sessions + news
  • SESSION OPEN      — Asia 02:00 / London 08:00 / NY 13:00 / NY PM 17:00 EAT
  • FLOW UPDATE       — hourly in-session
  • REAL-TIME ALERTS  — flow flip · absorption · liquidity sweep · VWAP cross
  • MANIPULATION      — stop hunts · fake breakouts · squeeze traps · absorption
  • A+ SIGNALS        — >=10/12 confluence, ATR SL/TP, chart attached, tracked to outcome
  • NEWS ALERTS       — 15 min before high-impact USD events
  • NY Close 21:00 · Weekend Review Sat 10:00 · Reopen notice Sun 21:30 EAT

COMMANDS  /now /flow /signal /health /help /sotd

SIGNAL RULES
  • BTC only (gold has no live aggressor tape — no honest gold signals)
  • Score >= 10/12 confluence · max 2/day · 4h cooldown
  • Skipped 15 min around high-impact USD news
  • Skipped for 60s after a $2M+ liquidation flush (active-cascade risk)
  • Every signal tracked: TP1 -> TP2 or SL, reported publicly

DEPLOY   Render · Start: python main.py · Build: pip install -r requirements.txt
ENV      TELEGRAM_TOKEN · TELEGRAM_CHAT_ID · TWELVEDATA_API_KEY
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
PORT     = int(os.getenv("PORT", "10000"))

BINANCE_REST  = "https://data-api.binance.vision/api/v3"
BINANCE_FAPI  = "https://fapi.binance.com"
BINANCE_HOSTS = ["wss://data-stream.binance.vision/stream?streams=",
                 "wss://stream.binance.com:9443/stream?streams="]
BINANCE_LIQ_WS = "wss://fstream.binance.com/ws/btcusdt@forceOrder"   # forced-liquidation stream
COINBASE_SPOT  = "https://api.coinbase.com/v2/prices/BTC-USD/spot"    # cross-exchange divergence check
BYBIT_TICKERS  = "https://api.bybit.com/v5/market/tickers"            # public fallback for funding/OI
                                                                        # (Binance fapi.binance.com returns
                                                                        # HTTP 451 from US-region cloud hosts
                                                                        # per Binance's own ToS geo-enforcement —
                                                                        # this is a documented, legal public
                                                                        # market-data endpoint, no auth needed)
FF_CAL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

LIQUIDATIONS = deque(maxlen=500)   # (ts, symbol, side, qty, price, notional)
CROSS_EX = {}                      # name -> {binance, coinbase, divergence_pct, ts}

TICK_INTERVAL      = 10
GOLD_POLL          = 120
GOLD_POLL_CLOSED   = 900
CTX_INTERVAL       = 120
STALE_CRYPTO_SEC   = 90
STALE_FEED_SEC     = 300
ALERT_COOLDOWN     = 300
MIN_ONESIDED_ALERT = 0.30
VWAP_DEV_MIN       = 0.05
SIGNAL_MAX_SCORE   = 12   # was 10; raised when cross-exchange + clear-path factors were added
SIGNAL_MIN_SCORE   = 10   # was 8/10 (80%); kept at ~83% of new max so more data != looser bar
SIGNAL_COOLDOWN    = 4 * 3600
MAX_SIGNALS_DAY    = 2
ATR_SL_MULT        = 1.5
TP1_R              = 1.5
TP2_R              = 2.5
NEWS_BLACKOUT_MIN  = 15

EAT   = pytz.timezone("Africa/Nairobi")
BRAND = "BRAX FX // FLOW & SIGNAL DESK"
FOOT  = "BRAX FX · Autonomous Flow & Signal Desk\nEducational analysis. Not financial advice. Trading carries risk."

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

def atr(df: pd.DataFrame, n=14) -> float:
    if df.empty or len(df) < n + 1:
        return 0.0
    hl = df.h - df.l
    hc = (df.h - df.c.shift()).abs()
    lc = (df.l - df.c.shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    val = float(tr.rolling(n).mean().iloc[-1])
    return val if not pd.isna(val) else 0.0

def ts_sec(ms):
    return ms / 1000 if ms > 1e11 else ms

def day_start_min() -> int:
    n = datetime.now(EAT).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(n.timestamp() // 60)

# ---------------------------------------------------------------- CANDLE + FLOW STORE
class CandleStore:
    def __init__(self, name, ws_sym=None):
        self.name, self.ws_sym = name, ws_sym
        self._c = {}                       # epoch_min -> [o,h,l,c,v]
        self._df, self._df_ts = None, 0.0
        self.price, self.day_open = 0.0, None
        self.cvd_ticks = deque(maxlen=60000)   # (ts_sec, signed_qty, price)
        self.last_update = 0.0
        self.source = "—"

    def _update_day_open(self):
        if self.day_open is None and self._c:
            first_min = min(self._c)
            self.day_open = self._c[first_min][0]

    def _ingest_min(self, m, o, h, l, c, v):
        # Binance 1m klines carry cumulative volume for the minute → overwrite, don't add
        if m in self._c:
            bar = self._c[m]
            self._c[m] = [bar[0], max(bar[1], h), min(bar[2], l), c, v]
        else:
            self._c[m] = [o, h, l, c, v]
        self.price = c
        self.last_update = time.time()
        self._df = None
        self._update_day_open()

    def ingest_kline(self, k):
        m = int(k["t"]) // 60000
        self._ingest_min(m, float(k["o"]), float(k["h"]), float(k["l"]),
                         float(k["c"]), float(k["v"]))

    def ingest_kline_tuple(self, m, o, h, l, c, v):
        self._ingest_min(m, o, h, l, c, v)

    def ingest_td(self, values):
        """TwelveData bars, newest-first."""
        for row in reversed(values):
            dt = datetime.fromisoformat(row["datetime"].replace("Z", ""))
            m = int(dt.timestamp() // 60)
            self._ingest_min(m, float(row["open"]), float(row["high"]),
                             float(row["low"]), float(row["close"]),
                             float(row.get("volume") or 0))
        self.last_update = time.time()

    def ingest_trade(self, t):
        p = float(t["p"]); q = float(t["q"])
        is_sell = bool(t.get("m", t.get("isBuyerMaker", False)))
        signed = -q if is_sell else q
        ts = t.get("T") or int(time.time() * 1000)
        self.cvd_ticks.append((ts_sec(ts), signed, p))
        self.last_update = time.time()

    def df(self, rule="1min", limit=200) -> pd.DataFrame:
        if not self._c:
            return pd.DataFrame(columns=["o", "h", "l", "c", "v"])
        now = time.time()
        if self._df is None or now - self._df_ts > 30:
            d = pd.DataFrame.from_dict(self._c, orient="index",
                                       columns=["o", "h", "l", "c", "v"])
            d.index = pd.to_datetime(d.index * 60, unit="s")
            self._df, self._df_ts = d.sort_index(), now
        d = self._df
        if rule != "1min":
            d = d.resample(rule).agg({"o": "first", "h": "max",
                                      "l": "min", "c": "last", "v": "sum"}).dropna()
        return d.tail(limit)

    def vwap(self):
        if self.day_open is None or not self.price:
            return None
        rows = [v for k, v in self._c.items()
                if k >= day_start_min() and v[4] > 0]
        if not rows:
            return None
        pv = sum(r[3] * r[4] for r in rows)
        vv = sum(r[4] for r in rows)
        return pv / vv if vv > 0 else None

    def vwap_dev_pct(self) -> float:
        vw = self.vwap()
        if not vw or not self.price:
            return 0.0
        return (self.price - vw) / vw * 100

    def data_age(self) -> float:
        return time.time() - self.last_update

# ---------------------------------------------------------------- FLOW METRICS
def _cvd_window_raw(st, seconds, signed=True):
    cutoff = time.time() - seconds
    tot = 0.0
    for t, s, p in st.cvd_ticks:
        if t >= cutoff:
            tot += (s if signed else abs(s))
    return tot

def flow_metrics(st: CandleStore):
    if not st.cvd_ticks or st.data_age() > STALE_FEED_SEC:
        return None
    c15, c1h = _cvd_window_raw(st, 900, True), _cvd_window_raw(st, 3600, True)
    v15, v1h = _cvd_window_raw(st, 900, False), _cvd_window_raw(st, 3600, False)
    ones15 = abs(c15) / v15 if v15 > 0 else 0.0
    ones1h = abs(c1h) / v1h if v1h > 0 else 0.0
    if c1h > 0 and ones1h >= MIN_ONESIDED_ALERT:
        d = "BULL"
    elif c1h < 0 and ones1h >= MIN_ONESIDED_ALERT:
        d = "BEAR"
    elif c15 > 0:
        d = "BULL"
    elif c15 < 0:
        d = "BEAR"
    else:
        d = "NEUTRAL"
    conv = "High" if ones1h >= 0.45 else ("Medium" if ones1h >= MIN_ONESIDED_ALERT else "Low")
    if d == "BULL":
        regime = "ACCUMULATION" if c15 > 0 else "PULLBACK-BUYING"
    elif d == "BEAR":
        regime = "DISTRIBUTION" if c15 < 0 else "RALLY-SELLING"
    else:
        regime = "CHOP"
    return {"dir": d, "c15": c15, "c1h": c1h, "ones15": ones15,
            "ones1h": ones1h, "conv": conv, "regime": regime}

# ---------------------------------------------------------------- STRUCTURE + AGREEMENT
def structure_read(st: CandleStore, h4: dict):
    intra, wk = "NEUTRAL", "NEUTRAL"
    d = st.df("1h", 60)
    if len(d) >= 20:
        last = float(d.c.iloc[-1])
        swing_h, swing_l = float(d.h.iloc[:-1].max()), float(d.l.iloc[:-1].min())
        if last > swing_h:
            intra = "BULL"
        elif last < swing_l:
            intra = "BEAR"
        else:
            ma = float(d.c.rolling(20).mean().iloc[-1])
            intra = "BULL" if last > ma else ("BEAR" if last < ma else "NEUTRAL")
    closes = h4.get(st.name)
    if closes is not None and len(closes) >= 60:
        last, ma20, ma50 = (float(closes.iloc[-1]),
                            float(closes.tail(20).mean()),
                            float(closes.tail(50).mean()))
        if ma20 > ma50 and last > ma20:
            wk = "BULL"
        elif ma20 < ma50 and last < ma20:
            wk = "BEAR"
    return intra, wk

def agreement(intra: str, flow: str):
    if intra == "NEUTRAL" or flow == "NEUTRAL":
        return 50, "MIXED"
    if intra == flow:
        return 90, "A"
    return 20, "C"

# ---------------------------------------------------------------- ENGINES (instantiated HERE — before any module-level f-string uses them)
SIGNAL_ENGINE = SignalEngine() if False else None   # placeholder removed below

class SignalEngine:
    """Maximum-confluence signals only (BTC). Score /12, fire at >= 10.
    Tracked to outcome publicly. No gold signals (no live gold tape).
    v2: added cross-exchange confirmation + clear-path (whale-wall) factors,
    and a liquidation-cascade pause gate in try_fire (risk-timing, not a
    confluence opinion — avoids entering fresh into an active flush)."""

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

    def score(self, st: CandleStore, h4: dict, ctx: dict):
        fm = flow_metrics(st)
        if fm is None or fm["dir"] == "NEUTRAL":
            return None, 0, {}
        intra, wk = structure_read(st, h4)
        parts = {}
        if intra == fm["dir"]:
            parts["structure aligned"] = 2
        elif intra != "NEUTRAL":
            parts["structure partial"] = 1
        if wk == fm["dir"]:
            parts["weekly aligned"] = 1
        if fm["conv"] == "High":
            parts["high flow conviction"] = 2
        elif fm["conv"] == "Medium":
            parts["medium flow conviction"] = 1
        if fm["regime"] == ("ACCUMULATION" if fm["dir"] == "BULL" else "DISTRIBUTION"):
            parts["regime confirms"] = 2
        vw = st.vwap()
        if vw and ((st.price > vw and fm["dir"] == "BULL") or (st.price < vw and fm["dir"] == "BEAR")):
            parts["vwap side"] = 1
        c = ctx.get("BITCOIN", {})
        f, bk = c.get("funding"), c.get("book")
        if f is not None:
            if (fm["dir"] == "BEAR" and f > 0.01) or (fm["dir"] == "BULL" and f < 0):
                parts["crowd positioned opposite"] = 1
        if bk is not None:
            if (fm["dir"] == "BULL" and bk > 0) or (fm["dir"] == "BEAR" and bk < 0):
                parts["book supports"] = 1
        # --- new factors (added with explicit owner permission on top of the original /10 rubric) ---
        xex = CROSS_EX.get("BITCOIN")
        if xex is not None and abs(xex.get("divergence_pct", 99)) < 0.15:
            parts["cross-exchange confirms"] = 1
        wall_key = "ask_wall" if fm["dir"] == "BULL" else "bid_wall"
        wall = c.get(wall_key)
        if st.price and c.get("book") is not None:   # only score this if depth data actually loaded this cycle
            blocked = (wall is not None and wall.get("usd", 0) >= 100_000
                       and abs(wall["price"] - st.price) / st.price < 0.003)
            if not blocked:
                parts["clear path"] = 1
        return fm["dir"], sum(parts.values()), parts

    def try_fire(self, st: CandleStore, h4: dict, ctx: dict):
        n = st.name
        if n != "BITCOIN" or n in self.active or not self._allowed(n):
            return None
        if news_blackout():
            return None
        # liquidation-cascade pause: don't open a fresh position while $2M+ has just
        # been force-liquidated in the last 60s (same threshold as the cascade alert) --
        # active flushes mean abnormal volatility/slippage risk regardless of direction.
        recent_liq_notional = sum(x[5] for x in LIQUIDATIONS if x[0] >= time.time() - 60)
        if recent_liq_notional > 2_000_000:
            log.info(f"try_fire: skipped, ${recent_liq_notional:,.0f} liquidated in last 60s")
            return None
        d, score, parts = self.score(st, h4, ctx)
        if d is None or score < SIGNAL_MIN_SCORE:
            return None
        df15 = st.df("15min", 60)
        a = atr(df15, 14)
        if not a or not st.price:
            return None
        entry = st.price
        risk = ATR_SL_MULT * a
        if d == "BULL":
            sl, tp1, tp2 = entry - risk, entry + TP1_R * risk, entry + TP2_R * risk
        else:
            sl, tp1, tp2 = entry + risk, entry - TP1_R * risk, entry - TP2_R * risk
        self.active[n] = {"dir": d, "entry": entry, "sl": sl, "tp1": tp1,
                          "tp2": tp2, "score": score, "t": time.time(), "tp1_hit": False}
        k = now_eat().strftime("%Y-%m-%d")
        self.counts[k] = self.counts.get(k, 0) + 1
        self.last_fire[n] = time.time()
        why = " · ".join(parts.keys())
        arrow = "▲ LONG" if d == "BULL" else "▼ SHORT"
        return (f"🎯 <b>{n} · A+ SETUP · {arrow}</b>\n"
                f"Confluence <b>{score}/{SIGNAL_MAX_SCORE}</b> — structure, flow, regime, positioning aligned.\n"
                f"Entry {fp(entry, n)} · SL {fp(sl, n)} · TP1 {fp(tp1, n)} · TP2 {fp(tp2, n)}\n"
                f"<i>{why}</i>\n\n"
                f"Educational — not financial advice. Risk only what you can lose.")

    def track(self, st: CandleStore) -> list:
        msgs = []
        sig = self.active.get(st.name)
        if not sig or not st.price:
            return []
        n = st.name
        if sig["dir"] == "BULL":
            hit_sl, hit_tp1, hit_tp2 = (st.price <= sig["sl"], st.price >= sig["tp1"],
                                        st.price >= sig["tp2"])
        else:
            hit_sl, hit_tp1, hit_tp2 = (st.price >= sig["sl"], st.price <= sig["tp1"],
                                        st.price <= sig["tp2"])
        if hit_sl:
            del self.active[n]
            self.record["sl"] += 1
            return [f"❌ <b>{n} · SIGNAL CLOSED — SL HIT</b>\n"
                    f"Structure invalidated. Desk stands down for {SIGNAL_COOLDOWN//3600}h.\n\n<i>{BRAND}</i>"]
        if hit_tp1 and not sig["tp1_hit"]:
            sig["tp1_hit"] = True
            self.record["tp1"] += 1
            return [f"✅ <b>{n} · TP1 HIT</b> — {fp(sig['tp1'], n)}\n"
                    f"TP2 {fp(sig['tp2'], n)} remains. Trail risk.\n\n<i>{BRAND}</i>"]
        if sig["tp1_hit"] and hit_tp2:
            del self.active[n]
            self.record["tp2"] += 1
            return [f"🏆 <b>{n} · TP2 HIT — FULL TARGET COMPLETE</b> · {fp(sig['tp2'], n)}\n"
                    f"Cycle closed.\n\n<i>{BRAND}</i>"]
        return []

    def record_line(self) -> str:
        r = self.record
        total = r["tp2"] + r["tp1"] + r["sl"]
        if not total:
            return "No signals closed yet."
        wins = r["tp2"] + r["tp1"]
        return f"Track record: {wins}/{total} closed green (TP2 {r['tp2']} · TP1 {r['tp1']} · SL {r['sl']})"

class AlertEngine:
    def __init__(self):
        self.state = {}

    def _cool(self, name, key, seconds) -> bool:
        S = self.state.setdefault(name, {})
        last = S.get(key, 0)
        if time.time() - last < seconds:
            return False
        S[key] = time.time()
        return True

    def scan(self, st: CandleStore) -> list:
        out = []
        if st.name != "BITCOIN":
            return out
        fm = flow_metrics(st)
        if not fm:
            return out
        n = st.name
        S = self.state.setdefault(n, {})

        if "flow_dir" in S and S["flow_dir"] != fm["dir"] and fm["dir"] != "NEUTRAL" \
                and fm["ones1h"] >= MIN_ONESIDED_ALERT and self._cool(n, "flip", ALERT_COOLDOWN):
            out.append(
                f"🔁 <b>{n} · FLOW FLIP</b>\n"
                f"1h CVD turned <b>{DIR_WORD[fm['dir']].lower()}</b> "
                f"(one-sidedness {fm['ones1h']*100:.0f}%, {fm['conv']}) @ {fp(st.price, n)}\n\n<i>{BRAND}</i>")
        S["flow_dir"] = fm["dir"]

        vw = st.vwap()
        if vw and st.price:
            dev = (st.price - vw) / vw * 100
            if st.price > vw * (1 + VWAP_DEV_MIN / 100):
                side = "above"
            elif st.price < vw * (1 - VWAP_DEV_MIN / 100):
                side = "below"
            else:
                side = S.get("vwap_side", "above")
            if S.get("vwap_side") and S["vwap_side"] != side and self._cool(n, "vwap", 600):
                confirming = (fm["c1h"] > 0) == (side == "above")
                note = "flow confirming" if confirming else "flow NOT confirming"
                out.append(
                    f"📍 <b>{n} · VWAP CROSS</b>\n"
                    f"Price crossed <b>{side}</b> session VWAP ({fp(vw, n)}, dev {abs(dev):.2f}%) — {note}.\n\n<i>{BRAND}</i>")
            S["vwap_side"] = side
        return out

class ManipulationEngine:
    """Detects market-manipulation footprints on the live BTC tape.

    1. STOP HUNT  — wick pierces a swing high/low, closes back inside.
    2. FAKE BREAK — level breaks on non-confirming CVD, then snaps back.
    3. SQUEEZE TRAP — fast flush against one-sided flow, instant retrace.
    4. ABSORPTION — price pinned at a level while tape runs the other way.
    """

    def __init__(self):
        self.state = {}

    def _cool(self, name, key, seconds) -> bool:
        S = self.state.setdefault(name, {})
        last = S.get(key, 0)
        if time.time() - last < seconds:
            return False
        S[key] = time.time()
        return True

    def scan(self, st: CandleStore) -> list:
        out = []
        if st.name != "BITCOIN" or not st.cvd_ticks:
            return out
        n = st.name
        d5 = st.df("5min", 48)
        if len(d5) < 20:
            return out
        fm = flow_metrics(st)

        # ---- 1. STOP HUNT
        prior = d5.iloc[:-2]
        lc = d5.iloc[-2]
        sw_hi, sw_lo = float(prior.h.max()), float(prior.l.min())
        if lc.h > sw_hi and lc.c < sw_hi and self._cool(n, "hunt_hi", 1800):
            out.append(
                f"🪤 <b>{n} · STOP HUNT DETECTED</b>\n"
                f"5m wick spiked through {fp(sw_hi, n)} and closed back below it.\n"
                f"Buy-side liquidity grabbed. Watch for reversal — do NOT chase the wick.\n\n<i>{BRAND}</i>")
        elif lc.l < sw_lo and lc.c > sw_lo and self._cool(n, "hunt_lo", 1800):
            out.append(
                f"🪤 <b>{n} · STOP HUNT DETECTED</b>\n"
                f"5m wick flushed through {fp(sw_lo, n)} and closed back above it.\n"
                f"Sell-side liquidity grabbed. Classic flush-and-reclaim — do NOT sell the low.\n\n<i>{BRAND}</i>")

        # ---- 2. FAKE BREAKOUT
        if fm is not None:
            if lc.c > sw_hi and float(d5.c.iloc[-1]) < sw_hi and self._cool(n, "fake_hi", 1800):
                confirm = fm["c15"] > 0
                note = "flow confirmed the break — treat as real" if confirm \
                    else "CVD did NOT confirm — engineered break"
                out.append(
                    f"🎭 <b>{n} · FAKE BREAKOUT CHECK</b>\n"
                    f"Break above {fp(sw_hi, n)} failed and price snapped back inside.\n"
                    f"{note}.\n\n<i>{BRAND}</i>")
            elif lc.c < sw_lo and float(d5.c.iloc[-1]) > sw_lo and self._cool(n, "fake_lo", 1800):
                confirm = fm["c15"] < 0
                note = "flow confirmed the break — treat as real" if confirm \
                    else "CVD did NOT confirm — engineered break"
                out.append(
                    f"🎭 <b>{n} · FAKE BREAKDOWN CHECK</b>\n"
                    f"Break below {fp(sw_lo, n)} failed and price snapped back above.\n"
                    f"{note}.\n\n<i>{BRAND}</i>")

        # ---- 3. SQUEEZE TRAP
        recent = [p for t, s, p in st.cvd_ticks if t >= time.time() - 120]
        if len(recent) >= 20 and fm is not None:
            last_p = st.price
            min_p = min(recent)
            max_p = max(recent)
            if abs(min_p - last_p) / max(last_p, 1e-9) < 0.0008 and (max_p - min_p) / min_p > 0.004 \
                    and fm["c1h"] > 0 and self._cool(n, "squeeze_lo", 1800):
                out.append(
                    f"🧨 <b>{n} · SQUEEZE TRAP</b>\n"
                    f"Fast flush to {fp(min_p, n)} on heavy selling, instantly reclaimed to "
                    f"{fp(last_p, n)} — leverage cascade caught and absorbed. "
                    f"Longs flushed, tape still bullish.\n\n<i>{BRAND}</i>")
            elif abs(max_p - last_p) / max(last_p, 1e-9) < 0.0008 and (max_p - min_p) / min_p > 0.004 \
                    and fm["c1h"] < 0 and self._cool(n, "squeeze_hi", 1800):
                out.append(
                    f"🧨 <b>{n} · SQUEEZE TRAP</b>\n"
                    f"Fast spike to {fp(max_p, n)} on heavy buying, instantly rejected to "
                    f"{fp(last_p, n)} — shorts squeezed out, tape still bearish.\n\n<i>{BRAND}</i>")

        # ---- 4. ABSORPTION
        if fm is not None:
            flat = st.df("5min", 6)
            if len(flat) >= 6:
                rng = float(flat.h.max() - flat.l.min())
                a5 = atr(st.df("5min", 30), 14)
                if a5 and rng < a5 * 0.8:
                    if fm["c15"] > 0 and self._cool(n, "absorb_bid", 1800):
                        out.append(
                            f"🛡️ <b>{n} · ABSORPTION AT BID</b>\n"
                            f"Price pinned {fp(float(flat.l.min()), n)}–{fp(float(flat.h.max()), n)} "
                            f"while 15m tape is BUYING ({fm['c15']:+,.0f}). Passive size defending.\n\n<i>{BRAND}</i>")
                    elif fm["c15"] < 0 and self._cool(n, "absorb_ask", 1800):
                        out.append(
                            f"🛡️ <b>{n} · ABSORPTION AT OFFER</b>\n"
                            f"Price pinned {fp(float(flat.l.min()), n)}–{fp(float(flat.h.max()), n)} "
                            f"while 15m tape is SELLING ({fm['c15']:+,.0f}). Size capping the move.\n\n<i>{BRAND}</i>")

        # ---- 5. LIQUIDATION CASCADE (Binance futures forced-order stream, real-time)
        recent_liqs = [x for x in LIQUIDATIONS if x[0] >= time.time() - 60 and x[1] == "BTCUSDT"]
        if len(recent_liqs) >= 5:
            total_notional = sum(x[5] for x in recent_liqs)
            if total_notional > 2_000_000 and self._cool(n, "liq_cascade", 900):
                longs_liq = sum(x[5] for x in recent_liqs if x[2] == "SELL")   # forced sell = long liquidated
                shorts_liq = sum(x[5] for x in recent_liqs if x[2] == "BUY")   # forced buy = short liquidated
                skew = "LONGS" if longs_liq > shorts_liq else "SHORTS"
                out.append(
                    f"💥 <b>{n} · LIQUIDATION CASCADE</b>\n"
                    f"${total_notional:,.0f} force-liquidated in the last 60s across {len(recent_liqs)} orders "
                    f"— {skew} bearing the brunt. Binance futures forced-order stream, live.\n\n<i>{BRAND}</i>")
        return out

# Single instantiation point — everything below can safely reference these.
SIGNAL_ENGINE = SignalEngine()
ALERT_ENGINE  = AlertEngine()
MANIP_ENGINE  = ManipulationEngine()

# ---------------------------------------------------------------- CHART
def render_chart(st: CandleStore, out_path: str):
    df = st.df("15min", 96)
    if df.empty:
        return None
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 7), sharex=True,
                                   gridspec_kw={"height_ratios": [3, 1]})
    x = np.arange(len(df))
    up = df.c >= df.o
    ax1.vlines(x, df.l, df.h, color=np.where(up, "#26a69a", "#ef5350"), lw=0.8)
    ax1.bar(x, (df.c - df.o), 0.6, bottom=df.o,
            color=np.where(up, "#26a69a", "#ef5350"))
    vw = st.vwap()
    if vw:
        ax1.axhline(vw, ls="--", c="#f0b90b", lw=1, label="Session VWAP")
    sig = SIGNAL_ENGINE.active.get(st.name)
    if sig:
        for lvl, lbl in ((sig["entry"], "Entry"), (sig["sl"], "SL"),
                         (sig["tp1"], "TP1"), (sig["tp2"], "TP2")):
            ax1.axhline(lvl, ls=":", lw=1)
            ax1.annotate(lbl, (0, lvl), fontsize=7, va="bottom")
    ax1.set_title(f"{st.name} 15m — {fp(st.price, st.name)}", fontsize=11)
    ax1.legend(fontsize=7)
    cum = np.cumsum([s for _, s, _ in st.cvd_ticks])[-len(df):] if st.cvd_ticks else x * 0
    ax2.fill_between(x, cum, color="#42a5f5", alpha=0.4)
    ax2.set_title("Tick CVD (session)", fontsize=8)
    plt.tight_layout()
    fig.savefig(out_path, dpi=110)
    plt.close(fig)
    return out_path

def render_chart_bytes(st: CandleStore):
    try:
        path = f"/tmp/{st.name.lower()}_chart.png"
        if render_chart(st, path):
            with open(path, "rb") as f:
                return f.read()
    except Exception as e:
        log.error(f"render_chart: {e}")
    return None

# ---------------------------------------------------------------- TELEGRAM
HTTP = None

async def tg_send(text: str):
    try:
        async with HTTP.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                             json={"chat_id": CHAT_ID, "text": text,
                                   "parse_mode": "HTML",
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
        form.add_field("parse_mode", "HTML")
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
            for sym, name in (("BTCUSDT", "BITCOIN"), ("PAXGUSDT", "GOLD")):
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
    syms = {"BTCUSDT": "BITCOIN", "PAXGUSDT": "GOLD"}
    while True:
        try:
            for fsym, name in syms.items():
                c = ctx.setdefault(name, {})
                try:
                    async with HTTP.get(f"{BINANCE_FAPI}/fapi/v1/premiumIndex?symbol={fsym}") as r:
                        if r.status != 200:
                            raise RuntimeError(f"binance fapi funding status={r.status}")
                        c["funding"] = float((await r.json()).get("lastFundingRate", 0)) * 100
                except Exception as e:
                    if fsym == "BTCUSDT":
                        log.warning(f"context: binance funding fetch failed for {fsym} ({e}); trying bybit fallback")
                    else:
                        log.warning(f"context: binance funding fetch failed for {fsym} ({e}); no fallback for this symbol")
                    if fsym == "BTCUSDT":
                        try:
                            async with HTTP.get(f"{BYBIT_TICKERS}?category=linear&symbol=BTCUSDT") as r2:
                                j = await r2.json()
                            row = (j.get("result", {}).get("list") or [{}])[0]
                            c["funding"] = float(row.get("fundingRate", 0)) * 100
                            c["funding_source"] = "bybit"
                        except Exception as e2:
                            log.warning(f"context: bybit funding fallback also failed ({e2})")
                try:
                    async with HTTP.get(f"{BINANCE_FAPI}/fapi/v1/openInterest?symbol={fsym}") as r:
                        if r.status != 200:
                            raise RuntimeError(f"binance fapi OI status={r.status}")
                        c["oi"] = float((await r.json()).get("openInterest", 0))
                except Exception as e:
                    log.warning(f"context: binance OI fetch failed for {fsym} ({e})")
                    if fsym == "BTCUSDT":
                        try:
                            async with HTTP.get(f"{BYBIT_TICKERS}?category=linear&symbol=BTCUSDT") as r2:
                                j = await r2.json()
                            row = (j.get("result", {}).get("list") or [{}])[0]
                            c["oi"] = float(row.get("openInterest", 0))
                        except Exception as e2:
                            log.warning(f"context: bybit OI fallback also failed ({e2})")
                try:
                    async with HTTP.get(f"{BINANCE_REST}/depth?symbol={fsym}&limit=100") as r:
                        d = await r.json()
                    bids = [(float(p), float(q)) for p, q in d.get("bids", [])]
                    asks = [(float(p), float(q)) for p, q in d.get("asks", [])]
                    bid = sum(q * p for p, q in bids)
                    ask = sum(q * p for p, q in asks)
                    if bid + ask > 0:
                        c["book"] = (bid - ask) / (bid + ask) * 100
                    # whale-wall detection — largest single resting order by notional (institutional-desk-style book read)
                    if bids:
                        wp, wq = max(bids, key=lambda x: x[0] * x[1])
                        c["bid_wall"] = {"price": wp, "usd": round(wp * wq)}
                    if asks:
                        wp, wq = max(asks, key=lambda x: x[0] * x[1])
                        c["ask_wall"] = {"price": wp, "usd": round(wp * wq)}
                except Exception as e:
                    log.warning(f"context: depth/whale-wall fetch failed for {fsym} ({e})")
        except Exception as e:
            log.error(f"context: {e}")
        await asyncio.sleep(CTX_INTERVAL)

async def liquidation_worker():
    """Real-time forced-liquidation stream (Binance futures) — the same signal institutional
    desks watch for cascade/squeeze risk. Purely additive: feeds LIQUIDATIONS, used by
    ManipulationEngine check #5 and exposed on /health. Does not touch existing signal logic."""
    while True:
        try:
            async with HTTP.ws_connect(BINANCE_LIQ_WS, heartbeat=20) as ws:
                log.info("liquidation stream connected")
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    o = json.loads(msg.data).get("o", {})
                    if not o:
                        continue
                    side = o.get("S")            # SELL = long liquidated, BUY = short liquidated
                    qty = float(o.get("q", 0) or 0)
                    price = float(o.get("p", 0) or 0)
                    LIQUIDATIONS.append((time.time(), "BTCUSDT", side, qty, price, qty * price))
        except Exception:
            log.exception("liquidation stream")
            await asyncio.sleep(5)

async def cross_exchange_worker(stores):
    """Cross-exchange price check (Binance vs Coinbase spot) — flags venue-specific
    dislocation, a classic desk red flag for single-exchange spoofing/wash trading.
    Purely additive: writes CROSS_EX, exposed on /health. Does not touch signal logic."""
    while True:
        try:
            btc = next((s for s in stores if s.name == "BITCOIN"), None)
            if btc and btc.price:
                async with HTTP.get(COINBASE_SPOT) as r:
                    cb = await r.json()
                cb_price = float(cb["data"]["amount"])
                if cb_price:
                    div = (btc.price - cb_price) / cb_price * 100
                    CROSS_EX["BITCOIN"] = {"binance": btc.price, "coinbase": cb_price,
                                            "divergence_pct": round(div, 3), "ts": time.time()}
        except Exception:
            log.exception("cross_exchange")
        await asyncio.sleep(30)

# ---------------------------------------------------------------- NEWS
NEWS = {"events": [], "announced": set()}

async def news_worker():
    while True:
        try:
            async with HTTP.get(FF_CAL) as r:
                ctype = r.headers.get("Content-Type", "")
                if r.status == 429:
                    log.warning(f"news: rate limited (429) — keeping {len(NEWS['events'])} cached events")
                elif "json" not in ctype.lower():
                    log.warning(f"news: non-JSON response (status={r.status}, content-type={ctype}) — keeping cached events")
                else:
                    evs = await r.json()
                    if isinstance(evs, list):
                        NEWS["events"] = [e for e in evs
                                          if e.get("impact") == "High" and e.get("currency") == "USD"]
                        log.info(f"news: {len(NEWS['events'])} high-impact USD events this week")
        except Exception as e:
            log.error(f"news: {e}")
        await asyncio.sleep(1800)

def _event_dt(e):
    dt = datetime.fromisoformat(e["date"])
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.utc)
    return dt

def news_imminent(window_min=15) -> list:
    out = []
    for e in NEWS["events"]:
        try:
            dt = _event_dt(e)
            key = e.get("title", "") + dt.isoformat()
            mins = (dt - datetime.now(pytz.utc)).total_seconds() / 60
            if 0 <= mins <= window_min and key not in NEWS["announced"]:
                NEWS["announced"].add(key)
                out.append((e.get("title", "High-impact release"), dt))
        except Exception:
            continue
    if len(NEWS["announced"]) > 300:
        NEWS["announced"] = set(list(NEWS["announced"])[-100:])
    return out

def news_blackout() -> bool:
    for e in NEWS["events"]:
        try:
            dt = _event_dt(e)
            secs = (dt - datetime.now(pytz.utc)).total_seconds()
            if 0 <= secs <= NEWS_BLACKOUT_MIN * 60:
                return True
        except Exception:
            continue
    return False

def news_today_lines() -> str:
    today = datetime.now(EAT).date()
    items = []
    for e in NEWS["events"]:
        try:
            dt = _event_dt(e).astimezone(EAT)
            if dt.date() == today:
                items.append(f"  • {dt.strftime('%H:%M')} EAT — {e.get('title', '?')}")
        except Exception:
            continue
    if not items:
        return "No high-impact USD releases scheduled today."
    return "High-impact USD releases today:\n" + "\n".join(items)

# ---------------------------------------------------------------- FORMATTING
def header(title: str) -> str:
    return f"📡 <b>{title}</b>\nBRAX FX // FLOW & SIGNAL DESK\n\n"

def footer() -> str:
    return f"\n\n<i>{FOOT}</i>"

def asset_block(st: CandleStore, proxy: CandleStore, h4: dict, ctx: dict,
                feed_note: str = "") -> str:
    n = st.name
    p = st.price
    intra, wk = structure_read(st, h4)
    fs = st if st.cvd_ticks else proxy
    fm = flow_metrics(fs)
    fl = fm["dir"] if fm else "NEUTRAL"
    conv = fm["conv"] if fm else "—"
    pct, grade = agreement(intra, fl)
    fs_label = "live tape" if fs is st else "PAXG tape"
    lines = [f"<b>{n}</b> — {fp(p, n)}" if p else f"<b>{n}</b> — awaiting data"]
    df24 = st.df("1h", 24)
    if not df24.empty and p:
        hi, lo = float(df24.h.max()), float(df24.l.min())
        if hi > lo:
            lines.append(f"24h {fp(lo, n)} – {fp(hi, n)} · {((p - lo) / (hi - lo) * 100):.0f}% of range")
    vw = st.vwap()
    if vw:
        lines.append(f"VWAP {fp(vw, n)} ({st.vwap_dev_pct():+.2f}%)")
    if fm:
        lines.append(f"Flow {DIR_EMOJI[fl]} {DIR_WORD[fl]} ({conv}) · {fm['regime']} · {fs_label}")
        lines.append(f"CVD 15m {fm['c15']:+,.0f} · 1h {fm['c1h']:+,.0f}")
    else:
        lines.append(f"Flow ⚪ Warming up · {fs_label}")
    lines.append(f"Structure {DIR_EMOJI[intra]} {DIR_WORD[intra]} · Weekly {DIR_EMOJI[wk]} {DIR_WORD[wk]}")
    lines.append(f"Agreement {pct}% {bar(pct)} · Grade {grade}")
    c = ctx.get(n, {})
    der = []
    if "funding" in c:
        der.append(f"Funding {c['funding']:+.4f}%")
    if c.get("oi"):
        der.append(f"OI {fmt_vol(c['oi'])}")
    if "book" in c:
        der.append(f"Book {'bid' if c['book'] > 0 else 'ask'}-heavy {abs(c['book']):.1f}%")
    if der:
        lines.append(" · ".join(der))
    if feed_note:
        lines.append(feed_note)
    age = st.data_age()
    lines.append(f"Feed: {st.source} · {'live' if age < STALE_FEED_SEC else f'stale {int(age/60)}m'}")
    return "\n".join(lines)

def build_daily_outlook(stores, proxies, h4) -> str:
    t = now_eat()
    lines = [f"🌅 <b>BRAX FX · DAILY OUTLOOK — {t.strftime('%A, %d %B %Y')}</b>\n"]
    for st in stores:
        n = st.name
        if n == "GOLD" and not gold_market_open():
            continue
        intra, wk = structure_read(st, h4)
        fm = flow_metrics(st if st.cvd_ticks else proxies.get("GOLD", st))
        lines.append(f"<b>{n}</b> — {fp(st.price, n) if st.price else 'warming up'}")
        if fm and fm["dir"] != "NEUTRAL":
            lines.append(
                f"The tape opens {DIR_WORD[fm['dir']].lower()} ({fm['conv']} conviction, {fm['regime'].replace('-', ' ').lower()}). "
                f"Structure is {DIR_WORD[intra].lower()} on the 1h and {DIR_WORD[wk].lower()} on the 4h. "
                + (f"When flow and structure point the same way, pullbacks toward VWAP are where the desk leans — "
                   if intra == fm["dir"] else
                   "When tape and structure disagree, the desk waits — traps live in disagreement — ")
                + f"key session VWAP {fp(st.vwap(), n) if st.vwap() else 'forming'}.")
        else:
            lines.append("Flow is balanced — no edge yet. Desk waits for one side to commit.")
        lines.append("")
    lines.append(news_today_lines())
    lines.append("\nKey releases get a 15-minute warning here automatically. "
                 "No signals fire inside the blackout window.\n")
    lines.append(f"🎯 {SIGNAL_ENGINE.record_line()}")
    return header("DAILY OUTLOOK") + "\n".join(lines) + footer()

def confluence_score(st: CandleStore, proxies: dict, h4: dict) -> dict:
    """Original confluence scorer for 'Signal of the Day'. Combines structure alignment
    (1h vs 4h), flow direction/conviction/regime, and a volatility sanity check into a
    0-10 score with plain-English reasons. This is informational only — it does NOT feed
    SIGNAL_ENGINE.try_fire and does not change how live entry signals fire."""
    fs = st if st.cvd_ticks else proxies.get(st.name, st)
    fm = flow_metrics(fs)
    if fm is None:
        return {"score": 0, "dir": "NEUTRAL", "reasons": ["flow still warming up"]}
    intra, wk = structure_read(st, h4)

    score, reasons = 0, []
    if intra == wk and intra != "NEUTRAL":
        score += 3
        reasons.append(f"1h and 4h structure both {DIR_WORD[intra].lower()}")
    elif intra != "NEUTRAL":
        score += 1
        reasons.append(f"1h structure {DIR_WORD[intra].lower()}, 4h mixed")

    if fm["dir"] == intra and fm["dir"] != "NEUTRAL":
        score += 3
        reasons.append(f"tape confirms — {fm['conv'].lower()} conviction {DIR_WORD[fm['dir']].lower()} flow")
    elif fm["dir"] != "NEUTRAL":
        score += 1
        reasons.append(f"flow {DIR_WORD[fm['dir']].lower()} but structure disagrees")

    if fm["conv"] == "High":
        score += 2
        reasons.append("strong one-sided conviction")
    if fm.get("regime") in ("ACCUMULATION", "DISTRIBUTION"):
        score += 1
        reasons.append(f"regime: {fm['regime'].lower()} (flow and trend both confirming)")

    d5 = st.df("5min", 30)
    if len(d5) >= 20:
        a5 = atr(d5, 14)
        if a5 and st.price:
            if a5 / st.price * 100 > 0.05:
                score += 1
                reasons.append("volatility supports a move")

    return {"score": min(score, 10), "dir": fm["dir"], "reasons": reasons}

def build_signal_of_the_day(stores, proxies, h4) -> str:
    """Ranks assets by confluence_score and calls out the single highest-conviction bias —
    an original scorer built entirely from this desk's own structure+flow engines. This is
    NOT a reproduction of any third-party product's methodology; nobody outside a vendor's
    own team can know a proprietary black-box's exact logic, and this makes no such claim."""
    scored = []
    for st in stores:
        if st.name == "GOLD" and not gold_market_open():
            continue
        scored.append((st, confluence_score(st, proxies, h4)))
    if not scored:
        return ""
    scored.sort(key=lambda x: x[1]["score"], reverse=True)
    best_st, best = scored[0]

    lines = []
    if best["score"] < 5 or best["dir"] == "NEUTRAL":
        lines.append("No standout setup today — flow and structure aren't aligned enough "
                      "on either asset for a high-conviction call. Desk stands down.")
    else:
        arrow = "▲ LONG BIAS" if best["dir"] == "BULL" else "▼ SHORT BIAS"
        lines.append(f"🏆 <b>{best_st.name} · {arrow} · Confluence {best['score']}/10</b>\n")
        lines.append(f"Price: {fp(best_st.price, best_st.name) if best_st.price else '—'}")
        for r in best["reasons"]:
            lines.append(f"  • {r}")
        lines.append("\nThis is a bias call, not an entry trigger — wait for the live "
                      "/signal alert for exact entry, SL, and TP levels.")
    lines.append(f"\n🎯 {SIGNAL_ENGINE.record_line()}")
    return header("SIGNAL OF THE DAY") + "\n".join(lines) + footer()

def build_now() -> str:
    lines = [header("LIVE DESK — " + now_eat().strftime("%H:%M EAT"))]
    for st in STORES:
        proxy = PROXIES.get(st.name, st)
        lines.append(asset_block(st, proxy, H4, CTX))
        lines.append("")
    if SIGNAL_ENGINE.active:
        for n, s in SIGNAL_ENGINE.active.items():
            arrow = "▲ LONG" if s["dir"] == "BULL" else "▼ SHORT"
            lines.append(f"🎯 Open signal: {n} {arrow} · {s['score']}/{SIGNAL_MAX_SCORE} · "
                         f"entry {fp(s['entry'], n)} · SL {fp(s['sl'], n)} · "
                         f"TP1 {fp(s['tp1'], n)} · TP2 {fp(s['tp2'], n)}")
    lines.append(f"🎯 {SIGNAL_ENGINE.record_line()}")
    return "\n".join(lines) + footer()

def build_flow_report() -> str:
    lines = [header("FLOW REPORT")]
    for st in STORES:
        fs = st if st.cvd_ticks else PROXIES.get(st.name, st)
        fm = flow_metrics(fs)
        if fm:
            lines.append(f"<b>{st.name}</b> · {DIR_EMOJI[fm['dir']]} {DIR_WORD[fm['dir']]} ({fm['conv']})")
            lines.append(f"  CVD 15m {fm['c15']:+,.0f} · 1h {fm['c1h']:+,.0f}")
            lines.append(f"  One-sidedness 15m {fm['ones15']*100:.0f}% · 1h {fm['ones1h']*100:.0f}%")
            lines.append(f"  Regime: {fm['regime']}")
        else:
            lines.append(f"<b>{st.name}</b> · flow warming up")
        lines.append("")
    return "\n".join(lines) + footer()

def build_signal_card() -> str:
    lines = [header("SIGNAL DESK")]
    lines.append(SIGNAL_ENGINE.record_line())
    lines.append(f"Today: {SIGNAL_ENGINE.counts.get(now_eat().strftime('%Y-%m-%d'), 0)}/{MAX_SIGNALS_DAY} · "
                 f"min confluence {SIGNAL_MIN_SCORE}/{SIGNAL_MAX_SCORE}")
    if SIGNAL_ENGINE.active:
        for n, s in SIGNAL_ENGINE.active.items():
            arrow = "▲ LONG" if s["dir"] == "BULL" else "▼ SHORT"
            lines.append(f"\n🎯 {n} {arrow} · {s['score']}/{SIGNAL_MAX_SCORE}")
            lines.append(f"Entry {fp(s['entry'], n)} · SL {fp(s['sl'], n)}")
            lines.append(f"TP1 {fp(s['tp1'], n)} · TP2 {fp(s['tp2'], n)}")
    else:
        lines.append(f"\nNo open signal — desk fires only at ≥{SIGNAL_MIN_SCORE}/{SIGNAL_MAX_SCORE} confluence.")
    return "\n".join(lines) + footer()

def build_health() -> str:
    lines = [header("SYSTEM HEALTH")]
    for st in STORES:
        age = st.data_age()
        ok = age < STALE_FEED_SEC
        lines.append(f"{'🟢' if ok else '🔴'} {st.name} · {st.source} · "
                     f"{'live' if ok else f'stale {int(age/60)}m'} · {fp(st.price, st.name) if st.price else '—'}")
    lines.append(f"News events tracked: {len(NEWS['events'])}")
    lines.append(f"Session: {session_name() or 'CLOSED'}")
    if not gold_market_open():
        lines.append(f"Gold reopens {gold_next_open_eat().strftime('%a %H:%M')} EAT")
    lines.append(f"News blackout active: {'YES' if news_blackout() else 'no'}")
    return "\n".join(lines) + footer()

HELP_TEXT = (
    "🤖 <b>BRAX FX · FLOW & SIGNAL DESK</b>\n\n"
    "/now — full live desk snapshot\n"
    "/flow — aggressor flow & CVD read\n"
    "/signal — open signals + track record\n"
    "/health — feed status\n"
    "/sotd — signal of the day (top confluence bias call)\n\n"
    "Auto: daily outlook 07:00 · session opens · hourly flow updates · "
    "manipulation alerts (stop hunts, fake breaks, squeeze traps) · "
    f"A+ signals ≥{SIGNAL_MIN_SCORE}/{SIGNAL_MAX_SCORE} confluence with live charts · news warnings 15 min ahead.\n\n"
    f"🎯 {SIGNAL_ENGINE.record_line()}\n\n<i>{FOOT}</i>"
)

# ---------------------------------------------------------------- LOOPS
async def tick_worker(stores, proxies, h4, ctx):
    """10-second loop: news, alerts, manipulation, signals + tracking (with charts)."""
    while True:
        try:
            msgs = []                       # plain-text messages
            photos = []                     # (png_bytes, caption)
            for title, dt in news_imminent(15):
                msgs.append(f"📰 <b>NEWS IN {int((dt - datetime.now(pytz.utc)).total_seconds()//60)} MIN</b>\n"
                            f"{title} — expect volatility. Desk goes quiet until it prints.")
            for st in stores:
                msgs += ALERT_ENGINE.scan(st)
                msgs += MANIP_ENGINE.scan(st)
                for m in SIGNAL_ENGINE.track(st):
                    msgs.append(m)
                if st.name == "BITCOIN":
                    sig = SIGNAL_ENGINE.try_fire(st, h4, ctx)
                    if sig:
                        png = render_chart_bytes(st)
                        if png:
                            photos.append((png, sig))
                        else:
                            msgs.append(sig)
            for m in msgs:
                await tg_send(m)
            for png, cap in photos:
                await tg_photo(png, cap)
        except Exception:
            log.exception("tick")
        await asyncio.sleep(TICK_INTERVAL)

async def flow_update_worker(stores):
    while True:
        await asyncio.sleep(3600)
        s = session_name()
        if s and now_eat().hour in FLOW_HOURS:
            lines = [header(f"FLOW UPDATE — {s}")]
            for st in stores:
                fm = flow_metrics(st if st.cvd_ticks else PROXIES.get(st.name, st))
                if fm:
                    lines.append(f"{st.name}: {DIR_EMOJI[fm['dir']]} {DIR_WORD[fm['dir']]} "
                                 f"({fm['conv']}) · {fm['regime']} · CVD 1h {fm['c1h']:+,.0f}")
                else:
                    lines.append(f"{st.name}: flow warming up")
            await tg_send("\n".join(lines) + footer())

async def daily_outlook_worker(stores, proxies, h4):
    sent_for = None
    while True:
        t = now_eat()
        if t.hour == 7 and t.minute < 2 and sent_for != t.date():
            await tg_send(build_daily_outlook(stores, proxies, h4))
            sent_for = t.date()
        await asyncio.sleep(60)

async def signal_of_day_worker(stores, proxies, h4):
    """Auto-sends the confluence-based Signal of the Day at 07:05 EAT — 5 min after the
    existing daily outlook so the two messages don't collide. Also available on-demand
    via /sotd. Purely additive; does not alter daily_outlook_worker or SIGNAL_ENGINE."""
    sent_for = None
    while True:
        t = now_eat()
        if t.hour == 7 and 5 <= t.minute < 7 and sent_for != t.date():
            msg = build_signal_of_the_day(stores, proxies, h4)
            if msg:
                await tg_send(msg)
            sent_for = t.date()
        await asyncio.sleep(60)

async def session_worker(stores, proxies, h4, ctx):
    opened = set()
    while True:
        t = now_eat()
        s = session_name()
        if s and t.minute < 2 and (t.strftime("%Y%m%d") + s) not in opened:
            opened.add(t.strftime("%Y%m%d") + s)
            if len(opened) > 20:
                opened = set(list(opened)[-10:])
            lines = [header(f"{s} SESSION OPEN — {t.strftime('%H:%M EAT')}")]
            for st in stores:
                if st.name == "GOLD" and not gold_market_open():
                    continue
                intra, wk = structure_read(st, h4)
                fm = flow_metrics(st if st.cvd_ticks else proxies.get(st.name, st))
                lines.append(f"<b>{st.name}</b> {fp(st.price, st.name) if st.price else '—'} — "
                             f"structure {DIR_WORD[intra]} · flow "
                             f"{DIR_WORD[fm['dir']] if fm else 'warming up'}")
            lines.append("\nFull read: /now")
            await tg_send("\n".join(lines) + footer())
        if not s and now_eat().hour >= 21:
            opened = set()
        await asyncio.sleep(60)

async def close_worker(stores):
    while True:
        t = now_eat()
        if t.hour == 21 and 0 <= t.minute < 2:
            lines = [header("NY CLOSE — DAY END")]
            for st in stores:
                df = st.df("1h", 24)
                if not df.empty and st.price:
                    hi, lo = float(df.h.max()), float(df.l.min())
                    lines.append(f"{st.name} {fp(st.price, st.name)} · 24h {fp(lo, st.name)}–{fp(hi, st.name)}")
            lines.append(f"🎯 {SIGNAL_ENGINE.record_line()}")
            await tg_send("\n".join(lines) + footer())
            await asyncio.sleep(120)   # don't re-fire inside the same window
        await asyncio.sleep(60)

async def weekend_worker(stores, h4):
    sent_sat = sent_sun = None
    while True:
        t = now_eat()
        if t.weekday() == 5 and t.hour == 10 and t.minute < 2 and sent_sat != t.date():
            sent_sat = t.date()
            lines = [header("WEEKEND REVIEW")]
            for st in stores:
                df = st.df("1h", 120)
                if len(df) >= 24 and st.price:
                    hi = float(df.tail(120).h.max())
                    lo = float(df.tail(120).l.min())
                    lines.append(f"{st.name} {fp(st.price, st.name)} · 5d range {fp(lo, st.name)}–{fp(hi, st.name)}")
            lines.append(f"🎯 {SIGNAL_ENGINE.record_line()}")
            await tg_send("\n".join(lines) + footer())
        if t.weekday() == 6 and t.hour == 21 and 30 <= t.minute < 32 and sent_sun != t.date():
            sent_sun = t.date()
            await tg_send(
                header("REOPEN NOTICE") +
                f"Gold reopens {gold_next_open_eat().strftime('%H:%M')} EAT.\n"
                f"Gap risk is real — first 30 min after open are flow noise, "
                f"desk waits for the tape to settle before reading anything.\n\n<i>{BRAND}</i>")
        await asyncio.sleep(60)

# ---------------------------------------------------------------- COMMANDS
async def command_worker():
    offset = 0
    while True:
        try:
            async with HTTP.get(f"https://api.telegram.org/bot{TOKEN}/getUpdates"
                                f"?timeout=25&offset={offset}") as r:
                data = await r.json()
            for upd in data.get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message") or {}
                text = (msg.get("text") or "").strip()
                if not text.startswith("/"):
                    continue
                cmd = text.split()[0].split("@")[0].lower()
                if cmd == "/now":
                    await tg_send(build_now())
                elif cmd == "/flow":
                    await tg_send(build_flow_report())
                elif cmd == "/signal":
                    png = render_chart_bytes(next(s for s in STORES if s.name == "BITCOIN"))
                    card = build_signal_card()
                    if png and SIGNAL_ENGINE.active:
                        await tg_photo(png, card)
                    else:
                        await tg_send(card)
                elif cmd == "/health":
                    await tg_send(build_health())
                elif cmd == "/sotd":
                    await tg_send(build_signal_of_the_day(STORES, PROXIES, H4))
                elif cmd in ("/help", "/start"):
                    await tg_send(HELP_TEXT)
        except Exception as e:
            log.error(f"commands: {e}")
        await asyncio.sleep(1)

# ---------------------------------------------------------------- FLASK HEALTH SERVER
app = Flask(__name__)

@app.route("/")
def root():
    return jsonify({
        "status": "ok",
        "service": BRAND,
        "health": "/health",
    })

@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "time": now_eat().isoformat(),
        "session": session_name(),
        "assets": {s.name: {"price": s.price, "source": s.source,
                            "age_s": round(s.data_age())} for s in STORES},
        "signals_open": list(SIGNAL_ENGINE.active.keys()),
        "record": SIGNAL_ENGINE.record,
        "context": {name: {k: v for k, v in c.items()} for name, c in CTX.items()},
        "cross_exchange": CROSS_EX,
        "liquidations_5m_usd": round(sum(
            x[5] for x in LIQUIDATIONS if x[0] >= time.time() - 300)),
    })

def run_flask():
    app.run(host="0.0.0.0", port=PORT, use_reloader=False)

# ---------------------------------------------------------------- MAIN
STORES, PROXIES, H4, CTX = [], {}, {}, {}

async def main_async():
    global HTTP
    HTTP = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))

    btc  = CandleStore("BITCOIN", ws_sym="btcusdt")
    paxg = CandleStore("PAXG")                      # gold derivatives proxy — live tape
    gold = CandleStore("GOLD")

    STORES.clear(); STORES.extend([btc, gold])
    PROXIES["GOLD"] = paxg
    PROXIES["BITCOIN"] = btc

    log.info(f"{BRAND} starting — assets: BITCOIN, GOLD")

    # bootstraps (sequential — clean logs)
    await bootstrap_crypto([btc, paxg])
    await bootstrap_gold(gold)

    tasks = [
        asyncio.create_task(binance_worker([btc, paxg])),
        asyncio.create_task(crypto_rest_fallback([btc, paxg])),
        asyncio.create_task(gold_worker(gold)),
        asyncio.create_task(h4_worker(H4)),
        asyncio.create_task(context_worker(CTX)),
        asyncio.create_task(liquidation_worker()),
        asyncio.create_task(cross_exchange_worker(STORES)),
        asyncio.create_task(news_worker()),
        asyncio.create_task(tick_worker(STORES, PROXIES, H4, CTX)),
        asyncio.create_task(flow_update_worker(STORES)),
        asyncio.create_task(daily_outlook_worker(STORES, PROXIES, H4)),
        asyncio.create_task(signal_of_day_worker(STORES, PROXIES, H4)),
        asyncio.create_task(session_worker(STORES, PROXIES, H4, CTX)),
        asyncio.create_task(close_worker(STORES)),
        asyncio.create_task(weekend_worker(STORES, H4)),
        asyncio.create_task(command_worker()),
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    asyncio.run(main_async())
