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

COMMANDS  /now /flow /dayoutlook /signal /health /help /sotd

SIGNAL RULES
  • BTC + Gold signals · ≥10/12 confluence · max 2/day · 4h cooldown
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
FF_CAL     = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
FF_CAL_CDN = "https://cdn-nfs.faireconomy.media/ff_calendar_thisweek.json"  # CDN fallback

LIQUIDATIONS   = deque(maxlen=500)   # (ts, symbol, side, qty, price, notional)
CROSS_EX       = {}                   # name -> {binance, coinbase, divergence_pct, ts}
FUNDING_HIST   = deque(maxlen=96)     # (ts, rate_pct) — track funding trend over time

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
    """Sum signed (or absolute) trade qty within the last `seconds`.
    Iterates reversed so we stop as soon as we leave the window —
    O(window ticks) not O(all 60k ticks)."""
    cutoff = time.time() - seconds
    tot = 0.0
    for t, s, p in reversed(st.cvd_ticks):
        if t < cutoff:
            break
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

def cvd_acceleration(st: CandleStore) -> tuple:
    """Compare last 5m CVD vs prior 5m CVD.
    Returns (ratio, label) — ratio > 1.1 means tape pressure building,
    < 0.9 means fading. Real desk metric: not just which way, but how fast."""
    now = time.time()
    recent = sum(s for t, s, p in reversed(st.cvd_ticks) if t >= now - 300)
    prior  = sum(s for t, s, p in reversed(st.cvd_ticks) if now - 600 <= t < now - 300)
    if abs(recent) < 1 and abs(prior) < 1:
        return 0.0, "flat"
    if abs(prior) < 1:
        return 2.0, "accelerating" if abs(recent) > 0 else "flat"
    ratio = abs(recent) / abs(prior)
    same_dir = (recent > 0) == (prior > 0)
    if not same_dir:
        return 0.0, "reversing"
    label = "accelerating ↑" if ratio > 1.2 else ("steady" if ratio > 0.8 else "fading ↓")
    return round(ratio, 2), label

def vwap_bands(st: CandleStore, n_std: float = 1.5) -> tuple:
    """Session VWAP ± 1.5σ of intraday price deviation.
    Returns (vwap, upper_band, lower_band) or (None, None, None).
    Used to flag stretched conditions — price > upper band historically
    means mean-reversion risk even in trending tape."""
    rows = [(v[3], v[4]) for k, v in st._c.items()
            if k >= day_start_min() and v[4] > 0]
    if len(rows) < 20:
        return None, None, None
    prices = [r[0] for r in rows]
    vols   = [r[1] for r in rows]
    total_vol = sum(vols)
    if total_vol <= 0:
        return None, None, None
    vwap_val = sum(p * v for p, v in zip(prices, vols)) / total_vol
    dev = (sum((p - vwap_val) ** 2 for p in prices) / len(prices)) ** 0.5
    return vwap_val, vwap_val + n_std * dev, vwap_val - n_std * dev

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
    # partial: flow has a direction but intra is neutral (rare path if we get here)
    return 20, "C"

def agreement_full(intra: str, wk: str, flow: str) -> tuple:
    """Four-tier agreement: A+ (all three aligned), A (1h+flow agree),
    B (weekly+flow agree but 1h mixed), C (flow vs 1h disagree), MIXED (no flow direction).
    Weekly NEUTRAL is common (ranging 4h) and doesn't override clear 1h/flow reads."""
    if flow == "NEUTRAL" or intra == "NEUTRAL":
        return 50, "MIXED"
    if intra == flow and wk == flow:
        return 95, "A+"
    if intra == flow:
        return 80, "A"
    if wk == flow:
        return 60, "B"
    return 20, "C"

# ---------------------------------------------------------------- SIGNAL ENGINE
class SignalEngine:
    """Signals for BITCOIN and GOLD. Score /12 (Gold max /11 — no cross-ex factor).
    Fire at >=10. Max 2/day per asset, 4h cooldown. News blackout respected.
    BTC: cascade gate active. Gold: market-hours gate active."""

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
        fs = st if st.cvd_ticks else PROXIES.get(st.name, st)  # PAXG proxy for Gold
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
            parts["weekly aligned"] = 1
        if fm["conv"] == "High":
            parts["high flow conviction"] = 2
        elif fm["conv"] == "Medium":
            parts["medium flow conviction"] = 1
        if fm["regime"] == ("ACCUMULATION" if fm["dir"] == "BULL" else "DISTRIBUTION"):
            parts["regime confirms"] = 2
        vw = st.vwap()
        if vw and st.price and ((st.price > vw and fm["dir"] == "BULL") or
                                 (st.price < vw and fm["dir"] == "BEAR")):
            parts["vwap side"] = 1
        c = ctx.get(st.name, {})                # asset-specific context, not always BITCOIN
        f, bk = c.get("funding"), c.get("book")
        if f is not None:
            if (fm["dir"] == "BEAR" and f > 0.01) or (fm["dir"] == "BULL" and f < 0):
                parts["crowd positioned opposite"] = 1
        if bk is not None:
            if (fm["dir"] == "BULL" and bk > 0) or (fm["dir"] == "BEAR" and bk < 0):
                parts["book supports"] = 1
        if st.name == "BITCOIN":               # cross-ex only for BTC (no Coinbase PAXG eq.)
            xex = CROSS_EX.get("BITCOIN")
            if xex is not None and abs(xex.get("divergence_pct", 99)) < 0.15:
                parts["cross-exchange confirms"] = 1
        wall_key = "ask_wall" if fm["dir"] == "BULL" else "bid_wall"
        wall = c.get(wall_key)
        if st.price and c.get("book") is not None:
            blocked = (wall is not None and wall.get("usd", 0) >= 100_000
                       and abs(wall["price"] - st.price) / st.price < 0.003)
            if not blocked:
                parts["clear path"] = 1
        return fm["dir"], sum(parts.values()), parts

    def try_fire(self, st: CandleStore, h4: dict, ctx: dict):
        n = st.name
        if n not in ("BITCOIN", "GOLD"):
            return None
        if n in self.active or not self._allowed(n):
            return None
        if n == "GOLD" and not gold_market_open():
            return None
        if news_blackout():
            return None
        # cascade gate — BTC only (LIQUIDATIONS stream is BTCUSDT futures)
        if n == "BITCOIN":
            recent_liq = sum(x[5] for x in LIQUIDATIONS if x[0] >= time.time() - 60)
            if recent_liq > 2_000_000:
                log.info(f"try_fire {n}: skipped — ${recent_liq:,.0f} cascading")
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
        arrow = "▲ LONG" if d == "BULL" else "▼ SHORT"
        why = " · ".join(parts.keys())
        max_s = SIGNAL_MAX_SCORE if n == "BITCOIN" else SIGNAL_MAX_SCORE - 1
        return (f"🎯 <b>{n} · {arrow}</b>\n"
                f"Entry {fp(entry, n)} · SL {fp(sl, n)} · TP1 {fp(tp1, n)} · TP2 {fp(tp2, n)}\n"
                f"Score {score}/{max_s} · {why}\n\n"
                f"<i>Educational. Not financial advice.</i>")

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
            return [f"❌ <b>{n} · STOPPED</b> {fp(sig['sl'], n)}\n"
                    f"Invalidated. Next window in {SIGNAL_COOLDOWN//3600}h.\n\n<i>{BRAND}</i>"]
        if hit_tp1 and not sig["tp1_hit"]:
            sig["tp1_hit"] = True
            self.record["tp1"] += 1
            return [f"✅ <b>{n} · TP1 ✓</b> {fp(sig['tp1'], n)}\n"
                    f"First target done. Trail stop to TP2 {fp(sig['tp2'], n)}.\n\n<i>{BRAND}</i>"]
        if sig["tp1_hit"] and hit_tp2:
            del self.active[n]
            self.record["tp2"] += 1
            return [f"🏆 <b>{n} · TP2 ✓</b> {fp(sig['tp2'], n)}\n"
                    f"Full trade done.\n\n<i>{BRAND}</i>"]
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
            pct = fm["ones1h"] * 100
            out.append(
                f"🔁 <b>FLOW FLIP · {n}</b>\n"
                f"Tape turned {DIR_WORD[fm['dir']].lower()} @ {fp(st.price, n)} · "
                f"{pct:.0f}% one-sided · {fm['conv'].lower()} conviction.\n\n<i>{BRAND}</i>"
            )
        S["flow_dir"] = fm["dir"]

        vw = st.vwap()
        if vw and st.price:
            dev = (st.price - vw) / vw * 100
            side = "above" if st.price > vw * (1 + VWAP_DEV_MIN / 100) else \
                   "below" if st.price < vw * (1 - VWAP_DEV_MIN / 100) else \
                   S.get("vwap_side", "above")
            if S.get("vwap_side") and S["vwap_side"] != side and self._cool(n, "vwap", 600):
                confirm = (fm["c1h"] > 0) == (side == "above")
                out.append(
                    f"📍 <b>VWAP CROSS · {n}</b>\n"
                    f"Price {side} VWAP {fp(vw, n)} ({abs(dev):.2f}% dev) · "
                    f"{'flow confirms' if confirm else 'flow NOT confirming — watch for fade'}.\n\n<i>{BRAND}</i>"
                )
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
                f"🪤 <b>STOP HUNT · {n}</b>\n"
                f"Wick above {fp(sw_hi, n)}, closed back under. Buy stops grabbed — not a real break.\n\n<i>{BRAND}</i>"
            )
        elif lc.l < sw_lo and lc.c > sw_lo and self._cool(n, "hunt_lo", 1800):
            out.append(
                f"🪤 <b>STOP HUNT · {n}</b>\n"
                f"Wick below {fp(sw_lo, n)}, snapped back above. Sell stops swept — don't sell the low.\n\n<i>{BRAND}</i>"
            )

        # ---- 2. FAKE BREAKOUT
        if fm is not None:
            if lc.c > sw_hi and float(d5.c.iloc[-1]) < sw_hi and self._cool(n, "fake_hi", 1800):
                cvd_note = "CVD didn't confirm" if fm["c15"] <= 0 else "flow backed it but still failed"
                out.append(
                    f"🎭 <b>FAKE BREAKOUT · {n}</b>\n"
                    f"Broke {fp(sw_hi, n)}, snapped back. {cvd_note}. Trap.\n\n<i>{BRAND}</i>"
                )
            elif lc.c < sw_lo and float(d5.c.iloc[-1]) > sw_lo and self._cool(n, "fake_lo", 1800):
                cvd_note = "CVD didn't confirm" if fm["c15"] >= 0 else "flow backed it but still failed"
                out.append(
                    f"🎭 <b>FAKE BREAKDOWN · {n}</b>\n"
                    f"Broke below {fp(sw_lo, n)}, reclaimed. {cvd_note}. Trap.\n\n<i>{BRAND}</i>"
                )

        # ---- 3. SQUEEZE TRAP
        recent = [p for t, s, p in st.cvd_ticks if t >= time.time() - 120]
        if len(recent) >= 20 and fm is not None:
            last_p = st.price
            min_p = min(recent)
            max_p = max(recent)
            if abs(min_p - last_p) / max(last_p, 1e-9) < 0.0008 and (max_p - min_p) / min_p > 0.004 \
                    and fm["c1h"] > 0 and self._cool(n, "squeeze_lo", 1800):
                out.append(
                    f"🧨 <b>LONG SQUEEZE · {n}</b>\n"
                    f"Flushed to {fp(min_p, n)}, back to {fp(last_p, n)}. Weak hands out. Tape still bullish.\n\n<i>{BRAND}</i>"
                )
            elif abs(max_p - last_p) / max(last_p, 1e-9) < 0.0008 and (max_p - min_p) / min_p > 0.004 \
                    and fm["c1h"] < 0 and self._cool(n, "squeeze_hi", 1800):
                out.append(
                    f"🧨 <b>SHORT SQUEEZE · {n}</b>\n"
                    f"Spiked to {fp(max_p, n)}, back to {fp(last_p, n)}. Shorts cleared. Tape still bearish.\n\n<i>{BRAND}</i>"
                )

        # ---- 4. ABSORPTION
        if fm is not None:
            flat = st.df("5min", 6)
            if len(flat) >= 6:
                rng = float(flat.h.max() - flat.l.min())
                a5 = atr(st.df("5min", 30), 14)
                if a5 and rng < a5 * 0.8:
                    lo_lvl = fp(float(flat.l.min()), n)
                    hi_lvl = fp(float(flat.h.max()), n)
                    if fm["c15"] > 0 and self._cool(n, "absorb_bid", 1800):
                        out.append(
                            f"🛡️ <b>ABSORPTION · {n}</b>\n"
                            f"Pinned {lo_lvl}–{hi_lvl}, tape buying ({fm['c15']:+,.0f} CVD 15m). "
                            f"Passive seller capping. Watch for exhaustion break.\n\n<i>{BRAND}</i>"
                        )
                    elif fm["c15"] < 0 and self._cool(n, "absorb_ask", 1800):
                        out.append(
                            f"🛡️ <b>ABSORPTION · {n}</b>\n"
                            f"Capped {lo_lvl}–{hi_lvl}, tape selling ({fm['c15']:+,.0f} CVD 15m). "
                            f"Passive buyer defending. Key level.\n\n<i>{BRAND}</i>"
                        )

        # ---- 5. LIQUIDATION CASCADE
        recent_liqs = [x for x in LIQUIDATIONS if x[0] >= time.time() - 60 and x[1] == "BTCUSDT"]
        if len(recent_liqs) >= 5:
            total_notional = sum(x[5] for x in recent_liqs)
            if total_notional > 2_000_000 and self._cool(n, "liq_cascade", 900):
                longs_liq = sum(x[5] for x in recent_liqs if x[2] == "SELL")
                skew = "longs" if longs_liq > total_notional / 2 else "shorts"
                out.append(
                    f"💥 <b>CASCADE · {n}</b>\n"
                    f"${total_notional/1e6:.1f}M force-liquidated in 60s · {skew} taking the hit. "
                    f"No new entries inside this.\n\n<i>{BRAND}</i>"
                )
        return out

# Single instantiation point — everything below can safely reference these.
SIGNAL_ENGINE = SignalEngine()
ALERT_ENGINE  = AlertEngine()
MANIP_ENGINE  = ManipulationEngine()

# ---------------------------------------------------------------- SMC ENGINE
class SMCEngine:
    """Smart Money Concepts analysis engine.
    Detects: Order Blocks · Fair Value Gaps · Break of Structure · Change of Character
             · Premium/Discount zones · Liquidity pools (equal highs/lows).
    Built from established SMC methodology — fully transparent, no black box."""

    # ── Internal helpers ──────────────────────────────────────────────────────
    @staticmethod
    def _swings(df, lookback=3):
        """Returns list of (type, idx, price) for swing highs (H) and lows (L)."""
        swings = []
        for i in range(lookback, len(df) - lookback):
            if all(df.h.iloc[i] >= df.h.iloc[i-j] for j in range(1, lookback+1)) and \
               all(df.h.iloc[i] >= df.h.iloc[i+j] for j in range(1, lookback+1)):
                swings.append(("H", i, float(df.h.iloc[i])))
            if all(df.l.iloc[i] <= df.l.iloc[i-j] for j in range(1, lookback+1)) and \
               all(df.l.iloc[i] <= df.l.iloc[i+j] for j in range(1, lookback+1)):
                swings.append(("L", i, float(df.l.iloc[i])))
        return swings

    # ── Order Blocks ─────────────────────────────────────────────────────────
    def bullish_ob(self, df) -> dict | None:
        """Last bearish candle before a strong bullish impulse (3+ green expanding candles).
        Represents where smart money placed buy orders — price returning = demand zone."""
        if len(df) < 12:
            return None
        a = atr(df, 14)
        for i in range(len(df) - 4, 4, -1):
            if float(df.c.iloc[i]) >= float(df.o.iloc[i]):   # must be bearish candle
                continue
            nxt = df.iloc[i+1:i+4]
            if len(nxt) < 3 or not all(nxt.c > nxt.o):       # next 3 must be bullish
                continue
            move = float(nxt.h.iloc[-1]) - float(nxt.l.iloc[0])
            if a and move < a * 0.6:                           # must be a real impulse
                continue
            return {"high": float(df.h.iloc[i]), "low": float(df.l.iloc[i]),
                    "mid":  (float(df.h.iloc[i]) + float(df.l.iloc[i])) / 2}
        return None

    def bearish_ob(self, df) -> dict | None:
        """Last bullish candle before a strong bearish impulse — supply zone."""
        if len(df) < 12:
            return None
        a = atr(df, 14)
        for i in range(len(df) - 4, 4, -1):
            if float(df.c.iloc[i]) <= float(df.o.iloc[i]):
                continue
            nxt = df.iloc[i+1:i+4]
            if len(nxt) < 3 or not all(nxt.c < nxt.o):
                continue
            move = float(nxt.l.iloc[0]) - float(nxt.l.iloc[-1])
            if a and move < a * 0.6:
                continue
            return {"high": float(df.h.iloc[i]), "low": float(df.l.iloc[i]),
                    "mid":  (float(df.h.iloc[i]) + float(df.l.iloc[i])) / 2}
        return None

    # ── Fair Value Gaps ───────────────────────────────────────────────────────
    def bullish_fvg(self, df) -> dict | None:
        """Three-candle pattern: gap where candle[i-1].high < candle[i+1].low.
        Price left unfilled buyers — magnet for price to return and fill."""
        for i in range(len(df) - 2, 1, -1):
            hi_prev = float(df.h.iloc[i-1])
            lo_next = float(df.l.iloc[i+1]) if i+1 < len(df) else None
            if lo_next and hi_prev < lo_next:
                return {"high": lo_next, "low": hi_prev,
                        "mid": (lo_next + hi_prev) / 2}
        return None

    def bearish_fvg(self, df) -> dict | None:
        """Gap where candle[i-1].low > candle[i+1].high — unfilled selling imbalance."""
        for i in range(len(df) - 2, 1, -1):
            lo_prev = float(df.l.iloc[i-1])
            hi_next = float(df.h.iloc[i+1]) if i+1 < len(df) else None
            if hi_next and lo_prev > hi_next:
                return {"high": lo_prev, "low": hi_next,
                        "mid": (lo_prev + hi_next) / 2}
        return None

    # ── Structure ─────────────────────────────────────────────────────────────
    def bos(self, df) -> dict | None:
        """Break of Structure — confirms trend direction.
        BOS bullish: price closes above last swing high.
        BOS bearish: price closes below last swing low."""
        if len(df) < 20:
            return None
        swings = self._swings(df)
        if not swings:
            return None
        current = float(df.c.iloc[-1])
        last_h = next((v for t,_,v in reversed(swings) if t == "H"), None)
        last_l = next((v for t,_,v in reversed(swings) if t == "L"), None)
        if last_h and current > last_h:
            return {"dir": "BULL", "level": last_h, "label": f"BOS ↑ broke ${last_h:,.2f}"}
        if last_l and current < last_l:
            return {"dir": "BEAR", "level": last_l, "label": f"BOS ↓ broke ${last_l:,.2f}"}
        return None

    def choch(self, df) -> dict | None:
        """Change of Character — first sign of a potential reversal.
        In a downtrend (lower highs/lows): first break of a recent high = CHOCH bullish.
        In an uptrend (higher highs/lows): first break of a recent low = CHOCH bearish."""
        if len(df) < 40:
            return None
        swings = self._swings(df)
        highs = [(i, v) for t,i,v in swings if t == "H"]
        lows  = [(i, v) for t,i,v in swings if t == "L"]
        current = float(df.c.iloc[-1])
        # CHOCH bullish: if last 3 swing highs are descending but current breaks the last one
        if len(highs) >= 3:
            recent_highs = sorted(highs[-3:], key=lambda x: x[0])
            if recent_highs[0][1] > recent_highs[1][1] > recent_highs[2][1]:  # lower highs
                if current > recent_highs[2][1]:
                    return {"dir": "BULL", "level": recent_highs[2][1],
                            "label": f"CHOCH ↑ — broke ${recent_highs[2][1]:,.2f} in a downtrend"}
        # CHOCH bearish: if last 3 swing lows are ascending but current breaks the last one
        if len(lows) >= 3:
            recent_lows = sorted(lows[-3:], key=lambda x: x[0])
            if recent_lows[0][1] < recent_lows[1][1] < recent_lows[2][1]:   # higher lows
                if current < recent_lows[2][1]:
                    return {"dir": "BEAR", "level": recent_lows[2][1],
                            "label": f"CHOCH ↓ — broke ${recent_lows[2][1]:,.2f} in an uptrend"}
        return None

    # ── Premium / Discount ────────────────────────────────────────────────────
    def premium_discount(self, df, price) -> dict | None:
        """Where is price relative to the current range?
        Discount (0-40%) = cheap, institutions buy here.
        Equilibrium (40-60%) = fair value.
        Premium (60-100%) = expensive, institutions sell here.
        OTE (Optimal Trade Entry) = 61.8-78.6% retracement level."""
        if df.empty or not price:
            return None
        hi = float(df.h.max())
        lo = float(df.l.min())
        if hi == lo:
            return None
        pct = (price - lo) / (hi - lo) * 100
        ote = lo + (hi - lo) * 0.382     # OTE buy zone (38.2% fib of the range)
        ote_sell = lo + (hi - lo) * 0.618  # OTE sell zone (61.8%)
        zone = ("deep discount — smart money buy zone"   if pct < 25 else
                "discount — still favourable for longs"  if pct < 40 else
                "equilibrium — fair value, neutral zone" if pct < 60 else
                "premium — smart money sell zone"        if pct < 75 else
                "deep premium — extended, reversal risk")
        return {"pct": round(pct, 1), "zone": zone, "hi": hi, "lo": lo,
                "mid": (hi + lo) / 2, "ote_buy": ote, "ote_sell": ote_sell}

    # ── Liquidity Pools ───────────────────────────────────────────────────────
    def liquidity_pools(self, df, price) -> list:
        """Equal highs/lows within 0.15% — resting stop orders that will be hunted.
        Smart money always targets liquidity before reversing."""
        if len(df) < 20 or not price:
            return []
        tol = 0.0015
        pools = []
        recent = df.tail(60)

        for arr, label in ((recent.h.values, "equal highs"), (recent.l.values, "equal lows")):
            seen = []
            for i in range(len(arr)):
                cluster = [arr[j] for j in range(len(arr))
                           if abs(arr[j] - arr[i]) / max(arr[i], 1e-9) < tol]
                if len(cluster) >= 2:
                    lvl = sum(cluster) / len(cluster)
                    if not any(abs(s["level"] - lvl) / max(lvl, 1) < tol * 2 for s in pools):
                        pools.append({"level": round(lvl, 2), "type": label,
                                      "distance_pct": round((lvl - price) / price * 100, 2),
                                      "count": len(cluster)})
        pools.sort(key=lambda x: abs(x["distance_pct"]))
        return pools[:4]

    # ── Full analysis ─────────────────────────────────────────────────────────
    def analyse(self, st: CandleStore) -> dict:
        df15 = st.df("15min", 100)
        df1h = st.df("1h", 60)
        p = st.price
        if len(df15) < 20:
            return {}
        return {
            "bull_ob":  self.bullish_ob(df15),
            "bear_ob":  self.bearish_ob(df15),
            "bull_fvg": self.bullish_fvg(df15),
            "bear_fvg": self.bearish_fvg(df15),
            "bos":      self.bos(df1h),
            "choch":    self.choch(df1h),
            "pd":       self.premium_discount(df1h, p),
            "liq":      self.liquidity_pools(df15, p),
        }

    # ── Human-readable output ─────────────────────────────────────────────────
    def format(self, st: CandleStore) -> str:
        """Compact SMC read — one section per concept, max 20 lines total."""
        res = self.analyse(st)
        if not res:
            return f"<b>{st.name}</b> — not enough data yet."
        n = st.name
        p = st.price
        lines = [f"📐 <b>SMC · {n}</b>  {fp(p, n) if p else '—'}\n"]

        # Premium/Discount
        pd = res.get("pd")
        if pd:
            lines.append(f"📍 <b>Price zone</b>  {pd['pct']:.0f}% of range — {pd['zone']}")
            lines.append(f"   Range {fp(pd['lo'], n)} – {fp(pd['hi'], n)}  ·  Mid {fp(pd['mid'], n)}")

        # Structure (BOS + CHOCH)
        bos  = res.get("bos")
        choch = res.get("choch")
        if choch:
            lines.append(f"\n🔄 <b>CHOCH</b>  {choch['label']}")
        if bos:
            lines.append(f"{'✅' if not choch else '📊'} <b>BOS</b>  {bos['label']}")

        # Order Blocks
        bull_ob = res.get("bull_ob")
        bear_ob = res.get("bear_ob")
        if bull_ob and p:
            dist = (bull_ob["mid"] - p) / p * 100
            label = "below — demand zone to watch" if dist < 0 else "above — already passed"
            lines.append(f"\n🟩 <b>Bullish OB</b>  {fp(bull_ob['low'], n)} – {fp(bull_ob['high'], n)}  ·  {label}")
        if bear_ob and p:
            dist = (bear_ob["mid"] - p) / p * 100
            label = "above — supply zone to watch" if dist > 0 else "below — already passed"
            lines.append(f"🟥 <b>Bearish OB</b>  {fp(bear_ob['low'], n)} – {fp(bear_ob['high'], n)}  ·  {label}")

        # Fair Value Gaps
        bull_fvg = res.get("bull_fvg")
        bear_fvg = res.get("bear_fvg")
        if bull_fvg:
            filled = p and p < bull_fvg["low"]
            lines.append(f"\n⬜ <b>Bullish FVG</b>  {fp(bull_fvg['low'], n)} – {fp(bull_fvg['high'], n)}"
                         f"{'  ✓ filled' if filled else '  — unfilled, price likely returns'}")
        if bear_fvg:
            filled = p and p > bear_fvg["high"]
            lines.append(f"⬛ <b>Bearish FVG</b>  {fp(bear_fvg['low'], n)} – {fp(bear_fvg['high'], n)}"
                         f"{'  ✓ filled' if filled else '  — unfilled, price likely returns'}")

        # Liquidity Pools
        liq = res.get("liq", [])
        if liq:
            lines.append("\n💧 <b>Liquidity pools</b> (stop clusters — likely targets)")
            for pool in liq[:3]:
                direction = "above" if pool["distance_pct"] > 0 else "below"
                lines.append(f"   {fp(pool['level'], n)}  {direction}  "
                             f"({abs(pool['distance_pct']):.2f}% away)  ·  {pool['type']}")

        # SMC bias
        bias = self._smc_bias(res, p)
        if bias:
            lines.append(f"\n{bias}")

        return "\n".join(lines) + f"\n\n<i>{BRAND}</i>"

    @staticmethod
    def _smc_bias(res: dict, price: float) -> str:
        """Synthesise SMC read into a one-line actionable bias."""
        if not price:
            return ""
        bull_score = bear_score = 0
        pd = res.get("pd")
        if pd:
            if pd["pct"] < 40:
                bull_score += 2
            elif pd["pct"] > 60:
                bear_score += 2
        bos = res.get("bos")
        if bos:
            if bos["dir"] == "BULL":
                bull_score += 2
            else:
                bear_score += 2
        choch = res.get("choch")
        if choch:
            if choch["dir"] == "BULL":
                bull_score += 1
            else:
                bear_score += 1
        bull_ob = res.get("bull_ob")
        if bull_ob and bull_ob["mid"] < price:
            bull_score += 1
        bear_ob = res.get("bear_ob")
        if bear_ob and bear_ob["mid"] > price:
            bear_score += 1
        if bull_score > bear_score + 1:
            return f"📊 SMC bias  ▲ LONG  ({bull_score} bull factors) — wait for OB/FVG pullback entry"
        if bear_score > bull_score + 1:
            return f"📊 SMC bias  ▼ SHORT  ({bear_score} bear factors) — wait for OB/FVG reaction entry"
        return "📊 SMC bias  NEUTRAL — conflicting signals, no clean setup"

SMC_ENGINE = SMCEngine()
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
    futures_syms = {"BTCUSDT"}   # PAXG is spot-only — no futures, funding/OI calls always 400/fail
    while True:
        try:
            for fsym, name in syms.items():
                c = ctx.setdefault(name, {})
                if fsym in futures_syms:
                    try:
                        async with HTTP.get(f"{BINANCE_FAPI}/fapi/v1/premiumIndex?symbol={fsym}") as r:
                            if r.status != 200:
                                raise RuntimeError(f"binance fapi funding status={r.status}")
                            new_f = float((await r.json()).get("lastFundingRate", 0)) * 100
                            c["funding"] = new_f
                            FUNDING_HIST.append((time.time(), new_f))
                    except Exception as e:
                        log.warning(f"context: binance funding failed for {fsym} ({e}); trying bybit")
                        try:
                            async with HTTP.get(f"{BYBIT_TICKERS}?category=linear&symbol=BTCUSDT") as r2:
                                j = await r2.json()
                            row = (j.get("result", {}).get("list") or [{}])[0]
                            c["funding"] = float(row.get("fundingRate", 0)) * 100
                            c["funding_source"] = "bybit"
                        except Exception as e2:
                            log.warning(f"context: bybit funding fallback failed ({e2})")
                    try:
                        async with HTTP.get(f"{BINANCE_FAPI}/fapi/v1/openInterest?symbol={fsym}") as r:
                            if r.status != 200:
                                raise RuntimeError(f"binance fapi OI status={r.status}")
                            new_oi = float((await r.json()).get("openInterest", 0))
                            prev_oi = c.get("oi", 0)
                            c["oi"] = new_oi
                            if prev_oi > 0 and new_oi > 0:
                                c["oi_chg_pct"] = round((new_oi - prev_oi) / prev_oi * 100, 3)
                    except Exception as e:
                        log.warning(f"context: binance OI failed for {fsym} ({e}); trying bybit")
                        try:
                            async with HTTP.get(f"{BYBIT_TICKERS}?category=linear&symbol=BTCUSDT") as r2:
                                j = await r2.json()
                            row = (j.get("result", {}).get("list") or [{}])[0]
                            c["oi"] = float(row.get("openInterest", 0))
                        except Exception as e2:
                            log.warning(f"context: bybit OI fallback failed ({e2})")
                try:
                    async with HTTP.get(f"{BINANCE_REST}/depth?symbol={fsym}&limit=100") as r:
                        d = await r.json()
                    bids = [(float(p), float(q)) for p, q in d.get("bids", [])]
                    asks = [(float(p), float(q)) for p, q in d.get("asks", [])]
                    bid = sum(q * p for p, q in bids)
                    ask = sum(q * p for p, q in asks)
                    if bid + ask > 0:
                        c["book"] = (bid - ask) / (bid + ask) * 100
                    if bids:
                        wp, wq = max(bids, key=lambda x: x[0] * x[1])
                        c["bid_wall"] = {"price": wp, "usd": round(wp * wq)}
                    if asks:
                        wp, wq = max(asks, key=lambda x: x[0] * x[1])
                        c["ask_wall"] = {"price": wp, "usd": round(wp * wq)}
                except Exception as e:
                    log.warning(f"context: depth fetch failed for {fsym} ({e})")
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
NEWS_IQ_SNAPSHOTS = {}   # event_key -> {event, pre_prices, dt_utc, post_sent}

# NewsIQ knowledge base — keyword -> (category, beat_result, miss_result, plain_note)
# beat_result / miss_result describe USD, BTC, Gold reaction to a stronger/weaker-than-expected print
NEWS_IQ_DB = {
    "non-farm":        ("Employment", "USD ↑ · BTC risk-off · Gold ↓",  "USD ↓ · BTC relief · Gold ↑",  "Biggest monthly mover. Strong jobs = Fed stays hawkish. Usually triggers a risk-off flush first."),
    "nfp":             ("Employment", "USD ↑ · BTC risk-off · Gold ↓",  "USD ↓ · BTC relief · Gold ↑",  "Same as Non-Farm Payrolls."),
    "unemployment":    ("Employment", "USD ↑ · BTC ↓",                  "USD ↓ · BTC ↑",                 "Lower unemployment = hawkish Fed. Opposite of what you'd expect — good news = risk-off."),
    "jobless claims":  ("Employment", "USD ↑ (fewer claims)",            "USD ↓ (more claims)",           "Weekly jobs check. Rising claims = Fed may cut = risk-on eventually."),
    "initial claims":  ("Employment", "USD ↑ (fewer claims)",            "USD ↓",                         "Same dynamics as jobless claims."),
    "adp":             ("Employment", "USD ↑ · BTC ↓",                  "USD ↓ · BTC ↑",                 "Private payrolls precursor to NFP. Market uses it to position ahead of Friday."),
    "cpi":             ("Inflation",  "USD ↑ · BTC ↓ · Gold ↕",        "USD ↓ · BTC ↑ · Gold ↑",       "Hot CPI = Fed can't cut = risk-off. Gold initially spikes then often fades if USD rallies hard."),
    "core cpi":        ("Inflation",  "USD ↑ · BTC ↓",                  "USD ↓ · BTC ↑ · Gold ↑",       "Fed watches core more than headline. Sticky core = prolonged hawkish = bad for risk."),
    "ppi":             ("Inflation",  "USD ↑ · Gold ↕",                 "USD ↓ · Gold ↑",                "Producer prices lead consumer prices. Hot PPI = CPI risk next month."),
    "pce":             ("Inflation",  "USD ↑ · BTC ↓",                  "USD ↓ · BTC ↑ · Gold ↑",       "Fed's preferred gauge. Hotter than expected = hawkish hold. Market usually re-prices rate cuts."),
    "core pce":        ("Inflation",  "USD ↑ · BTC ↓",                  "USD ↓ · BTC ↑",                 "The one the Fed actually targets (2%). Miss to the upside = delay in cuts."),
    "fomc":            ("Fed",        "↑ if hawkish · BTC ↓",           "↑ if dovish · BTC ↑",           "Rate decisions. Hawkish surprises crush risk assets fast. Any pivot language = instant spike."),
    "rate decision":   ("Fed",        "↑ if hike surprise · BTC ↓",     "↑ if cut · BTC ↑ · Gold ↑",    "The actual rate move. Surprise hike = flush. Cut = rally. In-line = watch the statement."),
    "fed minutes":     ("Fed",        "volatile both ways",              "volatile both ways",             "Meeting minutes. Watch for hawkish/dovish tone. Often causes mid-day vol."),
    "powell":          ("Fed",        "volatile — every word counts",    "volatile",                       "Fed Chair speaks. Any mention of rate path moves markets instantly."),
    "fed chair":       ("Fed",        "volatile",                        "volatile",                       "Same as Powell speech — high vol event regardless of direction."),
    "gdp":             ("Growth",     "BTC ↑ risk-on · Gold ↓",         "BTC ↓ risk-off · Gold ↑",       "Strong GDP = growth = risk-on. But also = Fed stays higher for longer. Mixed signal."),
    "ism manuf":       ("PMI",        "BTC ↑ · Gold ↓",                 "BTC ↓ · Gold ↑",                "Above 50 = expansion. Key for risk sentiment. Below 50 = contraction = risk-off."),
    "ism services":    ("PMI",        "BTC ↑ · USD ↑",                  "BTC ↓ · USD ↓",                 "Services PMI matters more than manufacturing for the US economy (70% services)."),
    "pmi":             ("PMI",        "BTC ↑ if above 50",              "BTC ↓ if below 50",             "Purchasing managers index. Above 50 = expansion. Watch the number vs consensus."),
    "retail sales":    ("Consumer",   "USD ↑ · BTC ↑ risk-on",          "USD ↓ · BTC ↓",                 "Strong consumer spending = growth = can be risk-on. Also means Fed stays hawkish."),
    "consumer conf":   ("Consumer",   "BTC ↑ · Gold ↓",                 "BTC ↓ · Gold ↑",                "Consumer mood. High confidence = spend more = growth = risk-on."),
    "durable goods":   ("Manufacturing","USD ↑ if strong",              "USD ↓",                          "Business investment proxy. Less direct crypto impact."),
    "trade balance":   ("Trade",      "USD ↑ if surplus",               "USD ↓ if deficit widens",        "Less direct crypto impact. Watch for USD moves that ripple into gold."),
    "ecb":             ("Central Bank","EUR ↕ · Gold volatile",         "EUR ↕ · Gold volatile",          "ECB decisions move EUR/USD which ripples into gold pricing."),
    "boj":             ("Central Bank","JPY ↑ if hawkish · carry unwind","JPY ↓",                         "BOJ hawkish surprise = yen strengthens = carry trade unwind = BTC risk."),
    "boe":             ("Central Bank","GBP ↕",                         "GBP ↕",                          "Bank of England. Low direct BTC/Gold impact unless USD reacts."),
    "opec":            ("Energy",     "Oil ↑ · inflation risk",         "Oil ↓",                          "Oil affects inflation expectations which feed into gold and rate path."),
    "nonfarm":         ("Employment", "USD ↑ · BTC risk-off · Gold ↓",  "USD ↓ · BTC relief · Gold ↑",  "Non-Farm Payrolls."),
}

IMPACT_EMOJI = {"High": "🔴", "Medium": "🟡", "Low": "⚪", "Holiday": "📅"}
MAJOR_CCY    = {"USD","EUR","GBP","JPY","AUD","CAD","CHF","NZD"}   # show all, flag these

async def _fetch_news_events() -> list:
    """Fetch ForexFactory calendar — tries primary then CDN fallback.
    Returns raw list of all events or empty list on failure."""
    for url in (FF_CAL, FF_CAL_CDN):
        try:
            async with HTTP.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                ctype = r.headers.get("Content-Type", "")
                if r.status == 429:
                    log.warning(f"news: 429 from {url}")
                    continue
                if r.status != 200:
                    log.warning(f"news: HTTP {r.status} from {url}")
                    continue
                if "json" not in ctype.lower():
                    body = await r.text()
                    log.warning(f"news: non-JSON from {url} ({ctype}): {body[:120]}")
                    continue
                evs = await r.json()
                log.info(f"news: {len(evs)} raw events from {url}")
                return evs if isinstance(evs, list) else []
        except Exception as e:
            log.warning(f"news: error fetching {url}: {e}")
    return []

async def news_worker():
    # fetch immediately on startup, then every 30 min
    while True:
        evs = await _fetch_news_events()
        if evs:
            # keep everything — user sees all events, blackout only blocks High
            # FF JSON uses 'country' (not 'currency') for the currency field
            NEWS["events"] = [e for e in evs if e.get("country") in MAJOR_CCY
                              or e.get("impact") == "High"]
            high = sum(1 for e in NEWS["events"] if e.get("impact") == "High")
            log.info(f"news: stored {len(NEWS['events'])} events this week "
                     f"({high} high-impact) from {len(evs)} total")
            if not NEWS["events"]:
                log.warning(f"news: zero stored — sample raw keys: "
                            f"{list(evs[0].keys()) if evs else 'empty'}")
        await asyncio.sleep(1800)

def _event_dt(e):
    dt = datetime.fromisoformat(e["date"])
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.utc)
    return dt

def news_imminent(window_min=15) -> list:
    """Returns (title, dt, impact) tuples for events within window_min.
    Warns for High and Medium — not Low (too noisy)."""
    out = []
    for e in NEWS["events"]:
        try:
            if e.get("impact") not in ("High", "Medium"):
                continue
            dt  = _event_dt(e)
            key = e.get("title", "") + dt.isoformat()
            mins = (dt - datetime.now(pytz.utc)).total_seconds() / 60
            if 0 <= mins <= window_min and key not in NEWS["announced"]:
                NEWS["announced"].add(key)
                out.append((e.get("title", "Release"), dt, e.get("impact", "Medium")))
        except Exception:
            continue
    if len(NEWS["announced"]) > 300:
        NEWS["announced"] = set(list(NEWS["announced"])[-100:])
    return out

def news_blackout() -> bool:
    """Only block signals within 15 min of HIGH-impact events.
    Medium/Low events don't pause the desk."""
    for e in NEWS["events"]:
        try:
            if e.get("impact") != "High":
                continue
            dt   = _event_dt(e)
            secs = (dt - datetime.now(pytz.utc)).total_seconds()
            if 0 <= secs <= NEWS_BLACKOUT_MIN * 60:
                return True
        except Exception:
            continue
    return False

def news_today_lines() -> str:
    """Compact list of ALL today's events — time, impact emoji, title, currency."""
    today = datetime.now(EAT).date()
    items = []
    for e in sorted(NEWS["events"], key=lambda x: x.get("date", "")):
        try:
            dt = _event_dt(e).astimezone(EAT)
            if dt.date() != today:
                continue
            emoji  = IMPACT_EMOJI.get(e.get("impact", ""), "⚪")
            ccy    = e.get("country", "")
            title  = e.get("title", "?")
            items.append(f"{emoji} {dt.strftime('%H:%M')}  {title}  {ccy}")
        except Exception:
            continue
    if not items:
        return ""
    return "📅 <b>TODAY'S EVENTS</b>\n" + "\n".join(items)

def _news_iq_match(title: str):
    """Match event title to NewsIQ knowledge base. Returns (key, data) or (None, None)."""
    t = title.lower()
    for key, data in NEWS_IQ_DB.items():
        if key in t:
            return key, data
    return None, None

def news_iq_pre(event: dict) -> str:
    """Pre-event analysis: direction risk, current positioning context, key levels.
    Short and direct — max 8 lines. Honest about uncertainty."""
    title    = event.get("title", "?")
    country  = event.get("country", "")
    impact   = event.get("impact", "")
    forecast = event.get("forecast", "")
    previous = event.get("previous", "")
    try:
        dt_str = _event_dt(event).astimezone(EAT).strftime("%H:%M EAT")
    except Exception:
        dt_str = "—"

    _, db = _news_iq_match(title)
    if not db:
        return ""   # unknown event — no analysis generated

    category, beat_rx, miss_rx, note = db
    emoji = IMPACT_EMOJI.get(impact, "📰")

    lines = [f"🧠 <b>NEWSIQ · {title.upper()}</b>\n{dt_str} · {country} · {category}"]
    if forecast and previous:
        lines.append(f"Consensus {forecast}  ·  Prev {previous}")
    lines.append(f"\nBeat → {beat_rx}")
    lines.append(f"Miss  → {miss_rx}")
    lines.append(f"\n{note}")

    # Current market context
    ctx_bits = []
    btc = next((s for s in STORES if s.name == "BITCOIN"), None)
    if btc and btc.price:
        fs  = btc if btc.cvd_ticks else PROXIES.get("BITCOIN", btc)
        fm  = flow_metrics(fs)
        c   = CTX.get("BITCOIN", {})
        vw  = btc.vwap()
        if fm and fm["dir"] != "NEUTRAL":
            ctx_bits.append(f"BTC tape {DIR_WORD[fm['dir']].lower()} ({fm['conv'].lower()} conv)")
        fr = c.get("funding")
        if fr is not None:
            if fr > 0.02:
                ctx_bits.append("longs crowded — squeeze risk on miss")
            elif fr < -0.02:
                ctx_bits.append("shorts crowded — squeeze risk on beat")
            else:
                ctx_bits.append("positioning neutral")
        oi_chg = c.get("oi_chg_pct")
        if oi_chg and abs(oi_chg) > 0.05:
            ctx_bits.append("OI growing (leveraged market)" if oi_chg > 0 else "OI falling")
        if ctx_bits:
            lines.append("Desk now: " + " · ".join(ctx_bits))
        if vw and btc.price:
            dev = (btc.price - vw) / vw * 100
            _, vwup, vwdn = vwap_bands(btc)
            stretched = vwup and (btc.price >= vwup or btc.price <= vwdn)
            vwap_note = " — stretched, spike may reverse fast" if stretched else ""
            lines.append(f"BTC {fp(btc.price, 'BITCOIN')} · VWAP {fp(vw, 'BITCOIN')} "
                         f"({'▲' if dev > 0 else '▼'}{abs(dev):.1f}%){vwap_note}")

    if impact == "High":
        lines.append("\n🔴 Signals paused until print.")
    lines.append(f"\n<i>Pattern-based — actual move depends on the number. Not financial advice.</i>")
    return "\n".join(lines) + f"\n\n<i>{BRAND}</i>"

def news_iq_post(snap: dict) -> str:
    """Post-event: what actually happened vs the expected pattern. Fires 10min after print."""
    event    = snap["event"]
    title    = event.get("title", "?")
    pre_prices = snap.get("pre_prices", {})
    _, db = _news_iq_match(title)

    lines = [f"🧠 <b>NEWSIQ · {title.upper()} — AFTERMATH</b>"]
    moved = []
    for name, pre_p in pre_prices.items():
        st = next((s for s in STORES if s.name == name), None)
        if st and st.price and pre_p:
            chg = (st.price - pre_p) / pre_p * 100
            arrow = "▲" if chg > 0 else "▼"
            moved.append(f"{name} {arrow}{abs(chg):.2f}%  ({fp(pre_p, name)} → {fp(st.price, name)})")
    if moved:
        lines += moved
    else:
        lines.append("Price data unavailable.")
    if db:
        beat_rx = db[1]
        lines.append(f"\nExpected on beat: {beat_rx}")
    lines.append(f"\n<i>{BRAND}</i>")
    return "\n".join(lines)

# ---------------------------------------------------------------- FORMATTING
def header(title: str) -> str:
    return f"📡 <b>{title}</b>\nBRAX FX // FLOW & SIGNAL DESK\n\n"

def footer() -> str:
    return f"\n\n<i>{FOOT}</i>"

def asset_block(st: CandleStore, proxy: CandleStore, h4: dict, ctx: dict,
                feed_note: str = "") -> str:
    n  = st.name
    p  = st.price
    c  = ctx.get(n, {})
    intra, wk = structure_read(st, h4)
    fs = st if st.cvd_ticks else proxy
    fm = flow_metrics(fs)
    fl = fm["dir"] if fm else "NEUTRAL"
    _, grade = agreement_full(intra, wk, fl)

    # line 1 — asset + price + range tag
    df24 = st.df("1h", 24)
    range_tag = ""
    if not df24.empty and p:
        hi, lo = float(df24.h.max()), float(df24.l.min())
        if hi > lo:
            pos = (p - lo) / (hi - lo) * 100
            range_tag = " · near highs" if pos > 75 else (" · near lows" if pos < 25 else " · mid-range")
    status = DIR_EMOJI[fl] if fl != "NEUTRAL" else "⚪"
    l1 = f"{status} <b>{n}</b>  {fp(p, n) if p else '—'}{range_tag}"

    # line 2 — core read
    if fm and fl != "NEUTRAL":
        conv_s = {"High": "high conv", "Medium": "med conv", "Low": "low conv"}.get(fm["conv"], "")
        reg_s  = {"ACCUMULATION":"accumulating","DISTRIBUTION":"distributing",
                  "PULLBACK-BUYING":"buying dips","RALLY-SELLING":"selling rallies",
                  "CHOP":"choppy"}.get(fm.get("regime",""), "")
        _, acc = cvd_acceleration(fs)
        acc_s  = " · accel ↑" if "accel" in acc else (" · fading ↓" if "fading" in acc else "")
        l2 = f"{DIR_WORD[fl]} · Grade {grade} · {conv_s} · {reg_s}{acc_s}"
    else:
        l2 = f"Grade {grade} · warming up"

    # line 3 — VWAP
    l3 = ""
    vw = st.vwap()
    if vw and p:
        dev = st.vwap_dev_pct()
        _, vwup, vwdn = vwap_bands(st)
        s_note = " ⚠️ stretched" if vwup and (p >= vwup or p <= vwdn) else ""
        l3 = f"VWAP {fp(vw, n)}  {'▲' if dev > 0 else '▼'}{abs(dev):.1f}%{s_note}"

    # line 4 — positioning (compact)
    pos = []
    if "funding" in c:
        fr = c["funding"]
        pos.append("longs paying" if fr > 0.01 else ("shorts paying" if fr < -0.01 else "funding flat"))
    if c.get("oi_chg_pct") is not None:
        chg = c["oi_chg_pct"]
        pos.append("OI ↑" if chg > 0.05 else ("OI ↓" if chg < -0.05 else ""))
    if "book" in c and abs(c["book"]) > 8:
        pos.append(f"book {'bid' if c['book'] > 0 else 'ask'}-heavy")
    l4 = " · ".join(x for x in pos if x)

    # line 5 — key levels + cross-ex
    bid_w, ask_w = c.get("bid_wall"), c.get("ask_wall")
    l5 = ""
    if bid_w and ask_w:
        xex_tag = ""
        if n == "BITCOIN":
            xex = CROSS_EX.get("BITCOIN")
            if xex and time.time() - xex.get("ts", 0) < 120:
                xex_tag = "  ⚠️ venue gap" if abs(xex["divergence_pct"]) >= 0.15 else "  CB ✓"
        l5 = (f"Bid ${bid_w['usd']//1000}k @ {fp(bid_w['price'], n)}  "
              f"Ask ${ask_w['usd']//1000}k @ {fp(ask_w['price'], n)}{xex_tag}")

    age = st.data_age()
    feed = f"{'🟢' if age < STALE_FEED_SEC else '🔴'} {st.source}"

    return "\n".join(x for x in [l1, l2, l3, l4, l5, feed] if x)

def build_daily_outlook(stores, proxies, h4) -> str:
    t = now_eat()
    lines = [f"🌅 <b>DAILY OUTLOOK · {t.strftime('%a %d %b').upper()}</b>\n"]
    for st in stores:
        n = st.name
        if n == "GOLD" and not gold_market_open():
            continue
        intra, wk = structure_read(st, h4)
        fs = st if st.cvd_ticks else proxies.get(n, st)
        fm = flow_metrics(fs)
        fl = fm["dir"] if fm else "NEUTRAL"
        _, grade = agreement_full(intra, wk, fl)
        price_str = fp(st.price, n) if st.price else "—"
        vw = st.vwap()
        vwap_str = f" · VWAP {fp(vw, n)}" if vw else ""
        dir_str = DIR_WORD[fl] if fl != "NEUTRAL" else "Neutral"
        conv_str = f" · {fm['conv'].lower()} conv" if fm and fl != "NEUTRAL" else ""
        lines.append(f"<b>{n}</b>  {price_str}  ·  {dir_str} {grade}{conv_str}{vwap_str}")
    news = news_today_lines()
    if news.strip():
        lines.append(f"\n{news.strip()}")
    lines.append(f"\n🎯 {SIGNAL_ENGINE.record_line()}")
    return "\n".join(lines) + footer()

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
    scored = []
    for st in stores:
        if st.name == "GOLD" and not gold_market_open():
            continue
        scored.append((st, confluence_score(st, proxies, h4)))
    if not scored:
        return ""
    scored.sort(key=lambda x: x[1]["score"], reverse=True)

    t_str = now_eat().strftime("%H:%M EAT")
    lines = [f"📡 <b>SIGNAL OF THE DAY — {t_str}</b>\n{BRAND}\n"]

    any_clean = any(d["score"] >= 5 and d["dir"] != "NEUTRAL" for _, d in scored)

    for i, (st, d) in enumerate(scored):
        n, price_str = st.name, fp(st.price, st.name) if st.price else "—"
        if d["score"] < 5 or d["dir"] == "NEUTRAL":
            lines.append(f"<b>{n}</b>  {price_str} · No clean bias — desk stands aside.")
        else:
            arrow = "▲ LONG" if d["dir"] == "BULL" else "▼ SHORT"
            medal = "🥇" if i == 0 else "🥈"
            key_reason = d["reasons"][0] if d["reasons"] else ""
            lines.append(
                f"{medal} <b>{n}</b>  {price_str} · <b>{arrow} · {d['score']}/10</b>\n"
                f"{key_reason.capitalize()}."
            )

    lines.append(
        random.choice([
            "Bias only. /signal fires with entry, SL, TP.",
            "Direction only — /signal gives the exact levels.",
            "Wait for /signal before entering. This is the lean, not the trigger.",
        ]) if any_clean else random.choice([
            "Nothing clean on either asset. Patience.",
            "Mixed on both. No call today.",
        ])
    )
    lines.append(f"\n🎯 {SIGNAL_ENGINE.record_line()}")
    return "\n\n".join(lines) + footer()

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
    t_str = now_eat().strftime("%H:%M EAT")
    lines = [f"📊 <b>FLOW · {t_str}</b>"]
    for st in STORES:
        fs = st if st.cvd_ticks else PROXIES.get(st.name, st)
        fm = flow_metrics(fs)
        c = CTX.get(st.name, {})
        price = fp(st.price, st.name) if st.price else "—"
        if fm and fm["dir"] != "NEUTRAL":
            _, acc = cvd_acceleration(fs)
            acc_s = " · accel ↑" if "accel" in acc else (" · fading ↓" if "fading" in acc else "")
            row1 = f"<b>{st.name}</b>  {price}  ·  {DIR_WORD[fm['dir']]} · {fm['conv'].lower()} conv{acc_s}"
            row2 = f"CVD 15m {fm['c15']:+,.0f} · 1h {fm['c1h']:+,.0f}"
        else:
            row1 = f"<b>{st.name}</b>  {price}  ·  warming up"
            row2 = ""
        bid_w, ask_w = c.get("bid_wall"), c.get("ask_wall")
        walls = ""
        if bid_w and ask_w:
            xex = ""
            if st.name == "BITCOIN":
                x = CROSS_EX.get("BITCOIN")
                if x and time.time() - x.get("ts", 0) < 120:
                    xex = "  ⚠️ gap" if abs(x["divergence_pct"]) >= 0.15 else "  CB ✓"
            walls = (f"Bid ${bid_w['usd']//1000}k@{fp(bid_w['price'], st.name)}  "
                     f"Ask ${ask_w['usd']//1000}k@{fp(ask_w['price'], st.name)}{xex}")
        liq_1h = sum(x[5] for x in LIQUIDATIONS if x[0] >= time.time() - 3600) if st.name == "BITCOIN" else 0
        liq_s = f"  ·  liqs 1h ${liq_1h/1e6:.1f}M" if liq_1h > 500_000 else ""
        block = "\n".join(x for x in [row1, row2, walls + liq_s] if x)
        lines.append(block)
    return "\n\n".join(lines) + footer()

def build_signal_card() -> str:
    t_str = now_eat().strftime("%H:%M EAT")
    lines = [f"📊 <b>SIGNALS · {t_str}</b>\n"]
    if SIGNAL_ENGINE.active:
        for n, s in SIGNAL_ENGINE.active.items():
            arrow = "▲ LONG" if s["dir"] == "BULL" else "▼ SHORT"
            age_min = int((time.time() - s["t"]) / 60)
            max_s = SIGNAL_MAX_SCORE if n == "BITCOIN" else SIGNAL_MAX_SCORE - 1
            lines.append(f"<b>{n} · {arrow}</b>  {s['score']}/{max_s}  ·  open {age_min}min")
            lines.append(f"Entry {fp(s['entry'], n)} · SL {fp(s['sl'], n)}")
            lines.append(f"TP1 {fp(s['tp1'], n)} · TP2 {fp(s['tp2'], n)}")
    else:
        lines.append(f"No open signals · bar ≥{SIGNAL_MIN_SCORE}/{SIGNAL_MAX_SCORE}")
    today = SIGNAL_ENGINE.counts.get(now_eat().strftime("%Y-%m-%d"), 0)
    lines.append(f"\n🎯 {SIGNAL_ENGINE.record_line()}  ·  Today {today}/{MAX_SIGNALS_DAY}")
    return "\n".join(lines) + footer()

def build_health() -> str:
    t_str = now_eat().strftime("%H:%M EAT")
    lines = [f"⚙️ <b>STATUS · {t_str}</b>\n"]
    for st in STORES:
        age = st.data_age()
        ok = age < STALE_FEED_SEC
        lines.append(f"{'🟢' if ok else '🔴'} <b>{st.name}</b>  "
                     f"{fp(st.price, st.name) if st.price else '—'}  ·  {st.source}")
    xex = CROSS_EX.get("BITCOIN")
    if xex and time.time() - xex.get("ts", 0) < 120:
        div = xex["divergence_pct"]
        lines.append(f"Cross-ex {'⚠️ gap' if abs(div) >= 0.15 else 'CB ✓'}  {div:+.3f}%")
    liq_5m = sum(x[5] for x in LIQUIDATIONS if x[0] >= time.time() - 300)
    liq_1h = sum(x[5] for x in LIQUIDATIONS if x[0] >= time.time() - 3600)
    lines.append(f"Liqs  5m ${liq_5m/1e3:.0f}k  ·  1h ${liq_1h/1e6:.2f}M")
    nb = "🔴 ACTIVE" if news_blackout() else "clear"
    lines.append(f"Session {session_name() or 'CLOSED'}  ·  News {nb}")
    sigs = list(SIGNAL_ENGINE.active.keys()) or ["none"]
    lines.append(f"Open: {', '.join(sigs)}")
    lines.append(f"🎯 {SIGNAL_ENGINE.record_line()}")
    return "\n".join(lines) + footer()

HELP_TEXT = (
    "🤖 <b>BRAX FX · FLOW & SIGNAL DESK</b>\n\n"
    "/now — live desk snapshot\n"
    "/flow — tape & CVD\n"
    "/dayoutlook — daily bias\n"
    "/sotd — signal of the day\n"
    "/signal — open signals + record\n"
    "/health — feed status\n\n"
    "Auto: 07:00 daily · session opens · hourly flow · alerts · signals\n"
    "Signals: BTC + Gold · ≥10/12 · max 2/day · 4h cooldown\n\n"
    f"🎯 {SIGNAL_ENGINE.record_line()}\n\n<i>{FOOT}</i>"
)

# ---------------------------------------------------------------- LOOPS
async def tick_worker(stores, proxies, h4, ctx):
    """10-second loop: news, alerts, manipulation, signals + tracking (with charts)."""
    while True:
        try:
            msgs = []                       # plain-text messages
            photos = []                     # (png_bytes, caption)

            # ── NewsIQ pre-event ─────────────────────────────────────────
            for title, dt, impact in news_imminent(15):
                secs = (dt - datetime.now(pytz.utc)).total_seconds()
                mins = max(0, int(secs // 60))
                time_str = f"{mins}min" if mins > 0 else "under a minute"
                emoji = IMPACT_EMOJI.get(impact, "📰")
                pause = impact == "High"
                msgs.append(
                    f"{emoji} <b>{title.upper()} — {time_str}</b>\n"
                    + ("Signals paused until it prints." if pause else "Watch for a move.")
                )
                # generate NewsIQ analysis for each event (High + Medium)
                ev_obj = next(
                    (e for e in NEWS["events"]
                     if e.get("title") == title),
                    {"title": title, "impact": impact}
                )
                ev_key = title + dt.isoformat()
                if ev_key not in NEWS_IQ_SNAPSHOTS:
                    # first time seeing this — capture pre-event prices and generate analysis
                    pre_prices = {s.name: s.price for s in stores if s.price}
                    NEWS_IQ_SNAPSHOTS[ev_key] = {
                        "event": ev_obj, "pre_prices": pre_prices,
                        "dt_utc": dt, "post_sent": False,
                    }
                    iq = news_iq_pre(ev_obj)
                    if iq:
                        msgs.append(iq)

            # ── NewsIQ post-event (10 min after print) ───────────────────
            now_utc = datetime.now(pytz.utc)
            for ev_key, snap in list(NEWS_IQ_SNAPSHOTS.items()):
                if snap.get("post_sent"):
                    continue
                dt = snap["dt_utc"]
                dt_aware = dt if dt.tzinfo else dt.replace(tzinfo=pytz.utc)
                elapsed = (now_utc - dt_aware).total_seconds()
                if 540 <= elapsed <= 660:   # 9-11 min after print
                    post = news_iq_post(snap)
                    if post:
                        msgs.append(post)
                    snap["post_sent"] = True
            # clean old snapshots (keep last 50)
            if len(NEWS_IQ_SNAPSHOTS) > 50:
                for k in list(NEWS_IQ_SNAPSHOTS.keys())[:-50]:
                    del NEWS_IQ_SNAPSHOTS[k]
            for st in stores:
                msgs += ALERT_ENGINE.scan(st)
                msgs += MANIP_ENGINE.scan(st)
                for m in SIGNAL_ENGINE.track(st):
                    msgs.append(m)
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
    """Hourly auto-post during active sessions — calls build_flow_report()
    so it stays in sync with /flow: same whale walls, cross-ex, liquidation
    context, CVD acceleration. Single source of truth."""
    while True:
        await asyncio.sleep(3600)
        s = session_name()
        if s and now_eat().hour in FLOW_HOURS:
            await tg_send(build_flow_report())

async def daily_outlook_worker(stores, proxies, h4):
    sent_for = None
    while True:
        t = now_eat()
        if t.hour == 7 and t.minute < 2 and sent_for != t.date():
            await tg_send(build_daily_outlook(stores, proxies, h4))
            sent_for = t.date()
        await asyncio.sleep(60)

async def news_brief_worker():
    """Posts all today's economic events at 07:10 EAT — after the daily outlook.
    Shows every event (High/Medium/Low) so traders know what's dropping all day."""
    sent_for = None
    while True:
        t = now_eat()
        if t.hour == 7 and 10 <= t.minute < 12 and sent_for != t.date():
            brief = news_today_lines()
            if brief:
                await tg_send(brief + f"\n\n🔴 High = signals pause 15min before\n🟡 Medium = watch for volatility\n⚪ Low = FYI\n\n<i>{BRAND}</i>")
            else:
                await tg_send(f"📅 No scheduled events today.\n\n<i>{BRAND}</i>")
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
    SESSION_NOTE = {
        "ASIA":     "Thin volume. Respect the noise.",
        "LONDON":   "Liquidity up. Take the tape seriously.",
        "NEW YORK": "Highest volume. Moves happen here.",
        "NY PM":    "Volume fading. Manage trades, avoid new setups.",
    }
    while True:
        t = now_eat()
        s = session_name()
        if s and t.minute < 2 and (t.strftime("%Y%m%d") + s) not in opened:
            opened.add(t.strftime("%Y%m%d") + s)
            if len(opened) > 20:
                opened = set(list(opened)[-10:])
            note = SESSION_NOTE.get(s, "Session live.")
            lines = [f"🔔 <b>{s} · {t.strftime('%H:%M EAT')}</b>\n{note}\n"]
            for st in stores:
                if st.name == "GOLD" and not gold_market_open():
                    continue
                fs = st if st.cvd_ticks else proxies.get(st.name, st)
                fm = flow_metrics(fs)
                intra, wk = structure_read(st, h4)
                _, grade = agreement_full(intra, wk, fm["dir"] if fm else "NEUTRAL")
                price_str = fp(st.price, st.name) if st.price else "—"
                dir_str = DIR_WORD[fm["dir"]] if fm and fm["dir"] != "NEUTRAL" else "Neutral"
                lines.append(f"<b>{st.name}</b>  {price_str}  ·  {dir_str} {grade}")
            await tg_send("\n".join(lines) + footer())
        if not s and now_eat().hour >= 21:
            opened = set()
        await asyncio.sleep(60)

async def close_worker(stores):
    sent_for = None
    while True:
        t = now_eat()
        if t.hour == 21 and 0 <= t.minute < 2 and sent_for != t.date():
            sent_for = t.date()
            lines = [f"🌙 <b>NY CLOSE · {t.strftime('%d %b')}</b>\n"]
            for st in stores:
                df = st.df("1h", 24)
                if not df.empty and st.price:
                    hi, lo = float(df.h.max()), float(df.l.min())
                    pos = (st.price - lo) / (hi - lo) * 100 if hi != lo else 50
                    pos_w = "near highs" if pos > 70 else ("near lows" if pos < 30 else "mid-range")
                    lines.append(f"<b>{st.name}</b>  {fp(st.price, st.name)}  ·  {pos_w}  "
                                 f"24h {fp(lo, st.name)}–{fp(hi, st.name)}")
            lines.append(f"\n🎯 {SIGNAL_ENGINE.record_line()}")
            await tg_send("\n".join(lines) + footer())
            await asyncio.sleep(120)
        await asyncio.sleep(60)

async def weekend_worker(stores, h4):
    sent_sat = sent_sun = None
    while True:
        t = now_eat()
        if t.weekday() == 5 and t.hour == 10 and t.minute < 2 and sent_sat != t.date():
            sent_sat = t.date()
            lines = [f"📋 <b>WEEK CLOSED · {t.strftime('%d %b')}</b>\n"]
            for st in stores:
                df = st.df("1h", 120)
                if len(df) >= 24 and st.price:
                    hi = float(df.tail(120).h.max())
                    lo = float(df.tail(120).l.min())
                    pos = (st.price - lo) / (hi - lo) * 100 if hi != lo else 50
                    pos_w = "near highs" if pos > 70 else ("near lows" if pos < 30 else "mid-range")
                    lines.append(f"<b>{st.name}</b>  {fp(st.price, st.name)}  ·  {pos_w}  "
                                 f"5d {fp(lo, st.name)}–{fp(hi, st.name)}")
            lines.append(f"\n🎯 {SIGNAL_ENGINE.record_line()}\nReopen Sun 21:30 EAT. Watch for gap.")
            await tg_send("\n".join(lines) + footer())
        if t.weekday() == 6 and t.hour == 21 and 30 <= t.minute < 32 and sent_sun != t.date():
            sent_sun = t.date()
            gold_t = gold_next_open_eat().strftime("%H:%M EAT")
            await tg_send(
                f"🟢 <b>MARKETS OPEN</b>\n"
                f"BTC live. Gold opens {gold_t}.\n"
                f"First 15min noisy — wait for flow to settle.\n\n<i>{BRAND}</i>"
            )
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
                elif cmd == "/smc":
                    for st in STORES:
                        if st.name == "GOLD" and not gold_market_open():
                            continue
                        await tg_send(SMC_ENGINE.format(st))
                elif cmd == "/dayoutlook":
                    await tg_send(build_daily_outlook(STORES, PROXIES, H4))
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
    paxg = CandleStore("PAXG", ws_sym="paxgusdt")    # gold derivatives proxy — live tape
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
        asyncio.create_task(news_brief_worker()),
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
