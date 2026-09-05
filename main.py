"""
BRAX APEX DESK  ·  Real-Time Zone Intelligence System
=====================================================
Proprietary multi-layer signal engine built around
dynamic zone geometry + probabilistic reversal scoring.

Design goals
  • High selectivity (few, high-quality signals)
  • Wide protective stops + asymmetric reward
  • Multi-timeframe regime awareness
  • Real-time price intelligence
  • Human desk narrative
  • Full outcome tracking

This is not a profit guarantee.
It is a disciplined, real-time decision support system.
"""

from __future__ import annotations
import asyncio, os, json, time, math, logging, random, hashlib
from collections import deque, defaultdict
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime, timezone, timedelta
from threading import Thread
from enum import Enum

import aiohttp
import pytz
from flask import Flask, jsonify

# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("APEX")

# ═══════════════════════════════════════════════════════════════
# ENVIRONMENT
# ═══════════════════════════════════════════════════════════════
TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TD_KEY  = os.getenv("TWELVEDATA_API_KEY", "")
PORT    = int(os.getenv("PORT", "10000"))

if not all([TOKEN, CHAT_ID, TD_KEY]):
    raise ValueError("TELEGRAM_TOKEN · TELEGRAM_CHAT_ID · TWELVEDATA_API_KEY required")

EAT = pytz.timezone("Africa/Nairobi")
BRAND = "BRAX APEX"
FOOT  = "Educational decision support. Not financial advice. Risk capital only."

# ═══════════════════════════════════════════════════════════════
# CORE PARAMETERS — Zone Engine (Main Logic)
# ═══════════════════════════════════════════════════════════════
ASSETS = {
    "XAU/USD": {"internal": "XAUUSD", "kind": "metal"},
    "BTC/USD": {"internal": "BTCUSD", "kind": "crypto"},
}

# Probability & zone geometry
PROB_THRESHOLD     = 0.76
MIN_TOUCHES        = 3
MIN_ZONE_CONF      = 0.58
ZONE_EPS_ATR       = 0.36
ZONE_DECAY         = 0.00022
ZONE_DEAD_SEC      = 2100
SWING_K            = 3
ATR_LEN            = 32
RSI_LEN            = 14
BUFFER_LEN         = 720

# Risk geometry (wide stops, asymmetric targets)
SL_ATR             = 3.15
TP1_ATR            = 5.8
TP2_ATR            = 9.4
COOLDOWN_SEC       = 780
MAX_SIGNALS_DAY    = 3

# Multi-layer filters
HTF_FAST           = 21
HTF_SLOW           = 55
REGIME_LOOKBACK    = 80
VOL_EXPAND_RATIO   = 1.25
MIN_RR             = 2.4

# Timing
POLL_SEC           = 10
SEED_BARS          = 90

# ═══════════════════════════════════════════════════════════════
# TYPES
# ═══════════════════════════════════════════════════════════════
class Regime(str, Enum):
    TREND_UP = "TREND_UP"
    TREND_DN = "TREND_DN"
    RANGE    = "RANGE"
    EXPAND   = "EXPAND"
    QUIET    = "QUIET"

class Bias(str, Enum):
    BUY  = "BUY"
    SELL = "SELL"

@dataclass
class Zone:
    center: float
    width: float
    touches: int = 0
    confidence: float = 0.55
    polarity: str = "NEUTRAL"
    last_touch: float = field(default_factory=time.time)
    anchors: List[float] = field(default_factory=list)
    strength: float = 0.4
    age_bars: int = 0

@dataclass
class Signal:
    symbol: str
    bias: Bias
    entry: float
    sl: float
    tp1: float
    tp2: float
    prob: float
    zone: Zone
    regime: Regime
    rsi: float
    atr: float
    htf: str
    reasons: List[str]
    ts: float = field(default_factory=time.time)
    tp1_hit: bool = False
    id: str = ""

    def __post_init__(self):
        if not self.id:
            raw = f"{self.symbol}{self.bias}{self.entry:.2f}{int(self.ts)}"
            self.id = hashlib.md5(raw.encode()).hexdigest()[:10]

# ═══════════════════════════════════════════════════════════════
# GLOBAL STATE
# ═══════════════════════════════════════════════════════════════
prices: Dict[str, deque] = {}
zones: Dict[str, List[Zone]] = {}
atr_cache: Dict[str, float] = {}
rsi_cache: Dict[str, float] = {}
htf_cache: Dict[str, str] = {}
regime_cache: Dict[str, Regime] = {}
last_price: Dict[str, float] = {}
last_signal_t: Dict[str, float] = {}
last_hash: Dict[str, str] = {}
active: Dict[str, Signal] = {}
day_count: Dict[str, int] = defaultdict(int)
record = {"tp2": 0, "tp1": 0, "sl": 0, "total": 0}
signals_fired = 0
boot_ts = time.time()

def _init_state():
    for meta in ASSETS.values():
        s = meta["internal"]
        prices[s] = deque(maxlen=BUFFER_LEN)
        zones[s] = []
        atr_cache[s] = 0.0
        rsi_cache[s] = 50.0
        htf_cache[s] = "NEUTRAL"
        regime_cache[s] = Regime.RANGE
        last_price[s] = 0.0
        last_signal_t[s] = 0.0
        last_hash[s] = ""

_init_state()

# ═══════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════
def now_eat() -> datetime:
    return datetime.now(EAT)

def fp(x: float, symbol: str) -> str:
    if not x: return "—"
    return f"${x:,.0f}" if "BTC" in symbol else f"${x:,.2f}"

def day_key() -> str:
    return now_eat().strftime("%Y-%m-%d")

# ═══════════════════════════════════════════════════════════════
# LAYER 1 — PURE PRICE INTELLIGENCE
# ═══════════════════════════════════════════════════════════════
def calc_atr(series: List[float], n: int = ATR_LEN) -> float:
    if len(series) < n + 1: return 0.0
    trs = [abs(series[i] - series[i-1]) for i in range(1, len(series))]
    if len(trs) < n: return sum(trs) / max(len(trs), 1)
    return sum(trs[-n:]) / n

def calc_rsi(series: List[float], n: int = RSI_LEN) -> float:
    if len(series) < n + 2: return 50.0
    deltas = [series[i] - series[i-1] for i in range(1, len(series))]
    gains = [max(d, 0.0) for d in deltas]
    losses = [max(-d, 0.0) for d in deltas]
    ag = sum(gains[-n:]) / n
    al = sum(losses[-n:]) / n
    if al <= 1e-12: return 72.0 if ag > 0 else 50.0
    rs = ag / al
    return 100.0 - (100.0 / (1.0 + rs))

def detect_swing(buf: deque, k: int = SWING_K) -> Optional[Tuple[str, float]]:
    if len(buf) < 2 * k + 1: return None
    p = list(buf)
    mid_i = -k - 1
    mid = p[mid_i]
    left, right = p[-2*k-1:mid_i], p[-k:]
    if mid > max(left) and mid > max(right): return ("HIGH", mid)
    if mid < min(left) and mid < min(right): return ("LOW", mid)
    return None

def ema(series: List[float], span: int) -> float:
    if len(series) < span: return series[-1] if series else 0.0
    alpha = 2 / (span + 1)
    val = series[-span]
    for x in series[-span+1:]:
        val = alpha * x + (1 - alpha) * val
    return val

# ═══════════════════════════════════════════════════════════════
# LAYER 2 — ZONE GEOMETRY ENGINE (MAIN LOGIC)
# ═══════════════════════════════════════════════════════════════
def zone_attach_or_create(sym: str, anchor: float, atr: float, swing: Optional[str]):
    attached = False
    for z in zones[sym]:
        if abs(anchor - z.center) < ZONE_EPS_ATR * atr:
            z.anchors.append(anchor)
            if len(z.anchors) > 14:
                z.anchors = z.anchors[-14:]
            z.center = sum(z.anchors) / len(z.anchors)
            var = sum((a - z.center) ** 2 for a in z.anchors) / len(z.anchors)
            z.width = math.sqrt(var) + 0.14 * atr
            z.touches += 1
            z.confidence = min(1.0, z.confidence + 0.115)
            z.strength = z.confidence * min(1.0, z.touches / 6.5)
            if swing == "HIGH": z.polarity = "RESISTANCE"
            elif swing == "LOW": z.polarity = "SUPPORT"
            z.last_touch = time.time()
            attached = True
            break
    if not attached:
        pol = "RESISTANCE" if swing == "HIGH" else "SUPPORT" if swing == "LOW" else "NEUTRAL"
        zones[sym].append(Zone(
            center=anchor, width=0.26 * atr, anchors=[anchor],
            polarity=pol, strength=0.38, confidence=0.55
        ))

def zone_merge(sym: str, atr: float):
    if len(zones[sym]) < 2: return
    zones[sym].sort(key=lambda z: z.center)
    i = 0
    while i < len(zones[sym]) - 1:
        a, b = zones[sym][i], zones[sym][i+1]
        if abs(a.center - b.center) < (a.width + b.width) * 0.52:
            anchors = a.anchors + b.anchors
            center = sum(anchors) / len(anchors)
            var = sum((x - center)**2 for x in anchors) / len(anchors)
            width = math.sqrt(var) + 0.14 * atr
            touches = a.touches + b.touches
            conf = (a.confidence + b.confidence) / 2
            pol = a.polarity if a.polarity == b.polarity else "NEUTRAL"
            strength = conf * min(1.0, touches / 6.5)
            zones[sym][i] = Zone(center, width, touches, conf, pol,
                                 max(a.last_touch, b.last_touch), anchors, strength)
            del zones[sym][i+1]
        else:
            i += 1

def zone_interact(sym: str, price: float, atr: float):
    now = time.time()
    for z in zones[sym]:
        age = now - z.last_touch
        z.confidence *= math.exp(-ZONE_DECAY * age)
        z.strength = z.confidence * min(1.0, z.touches / 6.5)
        z.age_bars += 1
        if abs(price - z.center) < z.width and age > 9:
            z.touches += 1
            z.last_touch = now
            z.confidence = min(1.0, z.confidence + 0.13)
            z.strength = z.confidence * min(1.0, z.touches / 6.5)

def zone_prune(sym: str):
    now = time.time()
    zones[sym] = [
        z for z in zones[sym]
        if z.confidence > 0.24 and (now - z.last_touch) < ZONE_DEAD_SEC
    ]

# ═══════════════════════════════════════════════════════════════
# LAYER 3 — REVERSAL PROBABILITY (MAIN SCORING)
# ═══════════════════════════════════════════════════════════════
def reversal_probability(sym: str, price: float, atr: float) -> List[Dict]:
    out = []
    series = list(prices[sym])
    if len(series) < 40 or atr <= 0: return out

    # Micro structure
    vel = [series[i] - series[i-1] for i in range(-7, 0)]
    v_mean = sum(vel) / len(vel)
    v_norm = abs(v_mean) / (atr + 1e-9)
    exhaustion = max(0.0, 1.0 - min(v_norm, 1.55) / 1.55)

    # Compression
    short = series[-16:]
    long_ = series[-55:]
    s_std = (sum((x - sum(short)/len(short))**2 for x in short) / len(short))**0.5
    l_std = (sum((x - sum(long_)/len(long_))**2 for x in long_) / len(long_))**0.5 + 1e-9
    compression = max(0.0, 1.0 - s_std / l_std)

    rsi = rsi_cache[sym]
    trend = htf_cache[sym]
    regime = regime_cache[sym]

    for z in zones[sym]:
        if z.touches < MIN_TOUCHES or z.confidence < MIN_ZONE_CONF:
            continue
        dist = abs(price - z.center)
        if dist > z.width * 1.12:
            continue

        impulse = math.exp(-dist / (atr * 0.92))

        approach = 0.0
        rsi_term = 0.0
        if z.polarity == "RESISTANCE":
            approach = 0.145 * v_norm if v_mean > 0 else -0.055
            rsi_term = 0.12 if rsi > 66 else (-0.08 if rsi < 36 else 0.0)
        elif z.polarity == "SUPPORT":
            approach = 0.145 * v_norm if v_mean < 0 else -0.055
            rsi_term = 0.12 if rsi < 34 else (-0.08 if rsi > 64 else 0.0)
        else:
            continue

        # Higher-timeframe alignment
        htf_term = 0.0
        if z.polarity == "SUPPORT" and trend == "BULL":
            htf_term = 0.125
        elif z.polarity == "RESISTANCE" and trend == "BEAR":
            htf_term = 0.125
        elif z.polarity == "SUPPORT" and trend == "BEAR":
            htf_term = -0.16
        elif z.polarity == "RESISTANCE" and trend == "BULL":
            htf_term = -0.16

        # Regime bonus / penalty
        reg_term = 0.0
        if regime == Regime.EXPAND:
            reg_term = 0.06
        elif regime == Regime.QUIET:
            reg_term = -0.08
        elif regime in (Regime.TREND_UP, Regime.TREND_DN):
            # prefer continuation alignment
            if (regime == Regime.TREND_UP and z.polarity == "SUPPORT") or \
               (regime == Regime.TREND_DN and z.polarity == "RESISTANCE"):
                reg_term = 0.09
            else:
                reg_term = -0.05

        strength_term = 0.295 * z.strength

        raw = (strength_term + 0.155 * exhaustion + 0.115 * compression +
               0.135 * impulse + approach + rsi_term + htf_term + reg_term)
        prob = max(0.0, min(1.0, raw))
        out.append({"zone": z, "probability": prob})
    return out

# ═══════════════════════════════════════════════════════════════
# LAYER 4 — REGIME + HTF
# ═══════════════════════════════════════════════════════════════
def update_htf(sym: str):
    series = list(prices[sym])
    if len(series) < HTF_SLOW + 5: return
    fast = ema(series, HTF_FAST)
    slow = ema(series, HTF_SLOW)
    if fast > slow * 1.00055:
        htf_cache[sym] = "BULL"
    elif fast < slow * 0.99945:
        htf_cache[sym] = "BEAR"
    else:
        htf_cache[sym] = "NEUTRAL"

def update_regime(sym: str):
    series = list(prices[sym])
    if len(series) < REGIME_LOOKBACK:
        regime_cache[sym] = Regime.RANGE
        return
    window = series[-REGIME_LOOKBACK:]
    ret = (window[-1] - window[0]) / (window[0] + 1e-9)
    vol = calc_atr(window, 20) / (window[-1] + 1e-9)
    # expansion detection
    recent_vol = calc_atr(series[-25:], 12) / (series[-1] + 1e-9)
    base_vol = calc_atr(series[-80:], 30) / (series[-1] + 1e-9) + 1e-9
    expand = recent_vol / base_vol

    if expand > VOL_EXPAND_RATIO:
        regime_cache[sym] = Regime.EXPAND
    elif abs(ret) < 0.004 and vol < 0.0018:
        regime_cache[sym] = Regime.QUIET
    elif ret > 0.012 and htf_cache[sym] == "BULL":
        regime_cache[sym] = Regime.TREND_UP
    elif ret < -0.012 and htf_cache[sym] == "BEAR":
        regime_cache[sym] = Regime.TREND_DN
    else:
        regime_cache[sym] = Regime.RANGE

# ═══════════════════════════════════════════════════════════════
# LAYER 5 — SIGNAL CONSTRUCTION + NARRATIVE
# ═══════════════════════════════════════════════════════════════
def build_reasons(z: Zone, prob: float, regime: Regime, htf: str, rsi: float) -> List[str]:
    reasons = []
    if z.touches >= 5:
        reasons.append(f"well-tested zone ({z.touches} touches)")
    elif z.touches >= 3:
        reasons.append(f"developing zone ({z.touches} touches)")
    if z.strength > 0.7:
        reasons.append("high zone strength")
    if regime == Regime.EXPAND:
        reasons.append("volatility expansion")
    if htf != "NEUTRAL":
        reasons.append(f"HTF {htf.lower()}")
    if z.polarity == "SUPPORT" and rsi < 40:
        reasons.append("RSI supporting long")
    if z.polarity == "RESISTANCE" and rsi > 60:
        reasons.append("RSI supporting short")
    if prob >= 0.82:
        reasons.append("elevated probability")
    return reasons or ["zone reaction"]

def narrative(sig: Signal) -> str:
    openers = [
        "Putting this on the board.",
        "Zone reaction looks clean.",
        "Taking this one.",
        "Confluence is acceptable here.",
        "Flow and geometry align.",
    ]
    conf = "high conviction" if sig.prob >= 0.82 else "solid setup"
    rr1 = abs(sig.tp1 - sig.entry) / max(abs(sig.entry - sig.sl), 1e-9)
    rr2 = abs(sig.tp2 - sig.entry) / max(abs(sig.entry - sig.sl), 1e-9)
    why = " · ".join(sig.reasons[:3])

    return (
        f"{'🟢' if sig.bias == Bias.BUY else '🔴'} <b>{sig.symbol} — {sig.bias.value}</b>\n\n"
        f"{random.choice(openers)}\n\n"
        f"Entry   {fp(sig.entry, sig.symbol)}\n"
        f"Stop    {fp(sig.sl, sig.symbol)}\n"
        f"TP1     {fp(sig.tp1, sig.symbol)}\n"
        f"TP2     {fp(sig.tp2, sig.symbol)}\n\n"
        f"R:R  1:{rr1:.1f} → 1:{rr2:.1f}\n"
        f"Probability {sig.prob*100:.0f}% · {conf}\n"
        f"Regime: {sig.regime.value}\n"
        f"{why}\n\n"
        f"<i>Tracking live. Prefer holding through the first target.</i>\n"
        f"{now_eat().strftime('%H:%M EAT')} · id {sig.id}"
    )

# ═══════════════════════════════════════════════════════════════
# LAYER 6 — EXECUTION GATE + TRACKING
# ═══════════════════════════════════════════════════════════════
def can_fire(sym: str) -> bool:
    if time.time() - last_signal_t.get(sym, 0) < COOLDOWN_SEC:
        return False
    if day_count[day_key()] >= MAX_SIGNALS_DAY:
        return False
    if sym in active:
        return False
    return True

def open_signal(sym: str, bias: Bias, price: float, atr: float,
                zone: Zone, prob: float) -> Optional[Signal]:
    if bias == Bias.BUY:
        sl  = price - SL_ATR * atr
        tp1 = price + TP1_ATR * atr
        tp2 = price + TP2_ATR * atr
    else:
        sl  = price + SL_ATR * atr
        tp1 = price - TP1_ATR * atr
        tp2 = price - TP2_ATR * atr

    # enforce minimum R:R
    rr = abs(tp1 - price) / max(abs(price - sl), 1e-9)
    if rr < MIN_RR:
        return None

    sig = Signal(
        symbol=sym, bias=bias, entry=price, sl=sl, tp1=tp1, tp2=tp2,
        prob=prob, zone=zone, regime=regime_cache[sym],
        rsi=rsi_cache[sym], atr=atr, htf=htf_cache[sym],
        reasons=build_reasons(zone, prob, regime_cache[sym], htf_cache[sym], rsi_cache[sym])
    )
    return sig

def track_active(sym: str, price: float) -> List[str]:
    msgs = []
    sig = active.get(sym)
    if not sig: return msgs

    if sig.bias == Bias.BUY:
        hit_sl  = price <= sig.sl
        hit_tp1 = price >= sig.tp1
        hit_tp2 = price >= sig.tp2
    else:
        hit_sl  = price >= sig.sl
        hit_tp1 = price <= sig.tp1
        hit_tp2 = price <= sig.tp2

    if hit_sl:
        del active[sym]
        record["sl"] += 1
        record["total"] += 1
        msgs.append(
            f"❌ <b>{sym} stopped</b> @ {fp(sig.sl, sym)}\n"
            f"Setup invalidated. Waiting for next clean geometry."
        )
    elif hit_tp1 and not sig.tp1_hit:
        sig.tp1_hit = True
        record["tp1"] += 1
        msgs.append(
            f"✅ <b>{sym} TP1 reached</b> {fp(sig.tp1, sym)}\n"
            f"Trailing remainder toward TP2 {fp(sig.tp2, sym)}."
        )
    elif sig.tp1_hit and hit_tp2:
        del active[sym]
        record["tp2"] += 1
        record["total"] += 1
        msgs.append(
            f"🏆 <b>{sym} full target</b> — TP2 {fp(sig.tp2, sym)}\n"
            f"Trade complete. Resetting focus."
        )
    return msgs

def record_line() -> str:
    t = record["total"]
    if t == 0: return "No closed signals yet."
    wins = record["tp2"] + record["tp1"]
    return f"Record {wins}/{t}  (TP2 {record['tp2']} · TP1 {record['tp1']} · SL {record['sl']})"

# ═══════════════════════════════════════════════════════════════
# DATA LAYER
# ═══════════════════════════════════════════════════════════════
async def td_price(session: aiohttp.ClientSession, td_sym: str) -> Optional[float]:
    try:
        url = f"https://api.twelvedata.com/price?symbol={td_sym}&apikey={TD_KEY}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=9)) as r:
            d = await r.json()
        if "price" in d:
            return float(d["price"])
        if "code" in d:
            log.debug(f"TD {td_sym}: {d.get('message')}")
    except Exception as e:
        log.debug(f"price err {td_sym}: {e}")
    return None

async def seed_history(session: aiohttp.ClientSession):
    log.info("Seeding price history…")
    for td_sym, meta in ASSETS.items():
        sym = meta["internal"]
        try:
            url = (f"https://api.twelvedata.com/time_series"
                   f"?symbol={td_sym}&interval=1min&outputsize={SEED_BARS}&apikey={TD_KEY}")
            async with session.get(url) as r:
                d = await r.json()
            vals = d.get("values") or []
            for row in reversed(vals):
                prices[sym].append(float(row["close"]))
            if vals:
                last_price[sym] = float(vals[0]["close"])
                log.info(f"  {sym}: {len(vals)} bars · last {last_price[sym]:.2f}")
        except Exception as e:
            log.warning(f"  seed {sym}: {e}")
        await asyncio.sleep(1.1)

# ═══════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════
async def tg(session: aiohttp.ClientSession, text: str):
    try:
        async with session.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={
                "chat_id": CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status != 200:
                log.warning(f"TG HTTP {r.status}")
    except Exception as e:
        log.error(f"TG: {e}")

# ═══════════════════════════════════════════════════════════════
# MAIN REAL-TIME LOOP
# ═══════════════════════════════════════════════════════════════
async def apex_loop(session: aiohttp.ClientSession):
    global signals_fired
    log.info("APEX engine online — zone geometry is primary")
    while True:
        try:
            for td_sym, meta in ASSETS.items():
                sym = meta["internal"]
                px = await td_price(session, td_sym)
                if px is None or px <= 0:
                    continue

                prices[sym].append(px)
                last_price[sym] = px
                series = list(prices[sym])

                atr = calc_atr(series)
                atr_cache[sym] = atr
                rsi_cache[sym] = calc_rsi(series)
                if atr <= 0 or len(series) < 45:
                    continue

                update_htf(sym)
                update_regime(sym)

                # Zone engine (MAIN)
                swing = detect_swing(prices[sym])
                if swing:
                    zone_attach_or_create(sym, swing[1], atr, swing[0])
                    zone_merge(sym, atr)
                zone_interact(sym, px, atr)
                zone_prune(sym)

                # Track existing
                for m in track_active(sym, px):
                    await tg(session, m)

                # Score + fire
                if not can_fire(sym):
                    continue

                candidates = reversal_probability(sym, px, atr)
                for c in candidates:
                    if c["probability"] < PROB_THRESHOLD:
                        continue
                    z = c["zone"]
                    bias = Bias.BUY if z.polarity == "SUPPORT" else \
                           Bias.SELL if z.polarity == "RESISTANCE" else None
                    if bias is None:
                        continue

                    h = f"{sym}-{bias.value}-{round(z.center, 2)}"
                    if last_hash.get(sym) == h:
                        continue

                    sig = open_signal(sym, bias, px, atr, z, c["probability"])
                    if sig is None:
                        continue

                    last_signal_t[sym] = time.time()
                    last_hash[sym] = h
                    day_count[day_key()] += 1
                    signals_fired += 1
                    active[sym] = sig

                    await tg(session, narrative(sig))
                    log.info(
                        f"FIRE #{signals_fired} {bias.value} {sym} @ {px:.2f} "
                        f"prob={c['probability']:.3f} regime={regime_cache[sym].value}"
                    )

            await asyncio.sleep(POLL_SEC)
        except Exception as e:
            log.error(f"apex loop: {e}", exc_info=True)
            await asyncio.sleep(12)

# ═══════════════════════════════════════════════════════════════
# DESK COMMANDS + SCHEDULE
# ═══════════════════════════════════════════════════════════════
def build_now() -> str:
    lines = [f"<b>{BRAND} · Live</b>  ·  {now_eat().strftime('%H:%M EAT')}\n"]
    for meta in ASSETS.values():
        s = meta["internal"]
        lines.append(
            f"{'🟢' if htf_cache[s]=='BULL' else '🔴' if htf_cache[s]=='BEAR' else '⚪'} "
            f"<b>{s}</b>  {fp(last_price[s], s)}  ·  {regime_cache[s].value}  ·  "
            f"zones {len(zones[s])}"
        )
    if active:
        lines.append("")
        for s, sig in active.items():
            lines.append(
                f"Open {s} {sig.bias.value} · SL {fp(sig.sl, s)} · TP1 {fp(sig.tp1, s)}"
            )
    lines.append(f"\n{record_line()}")
    return "\n".join(lines)

def build_outlook() -> str:
    lines = [f"🌅 <b>Daily Outlook</b>  ·  {now_eat().strftime('%a %d %b')}\n"]
    for meta in ASSETS.values():
        s = meta["internal"]
        lines.append(
            f"<b>{s}</b>  {fp(last_price[s], s)}\n"
            f"HTF {htf_cache[s]} · Regime {regime_cache[s].value} · "
            f"{len(zones[s])} active zones"
        )
    lines.append(f"\n{record_line()}\n\n<i>{FOOT}</i>")
    return "\n".join(lines)

def build_health() -> str:
    uptime = int(time.time() - boot_ts)
    lines = [
        f"<b>System Health</b>  ·  {now_eat().strftime('%H:%M EAT')}",
        f"Uptime {uptime//3600}h {(uptime%3600)//60}m",
        f"Signals fired (session): {signals_fired}",
        f"Open: {list(active.keys()) or 'none'}",
        record_line(),
    ]
    for meta in ASSETS.values():
        s = meta["internal"]
        lines.append(f"{s}: px {fp(last_price[s], s)} · ATR {atr_cache[s]:.3f} · RSI {rsi_cache[s]:.1f}")
    return "\n".join(lines)

HELP = (
    f"<b>{BRAND}</b>\n\n"
    "Primary engine: Dynamic Zone Geometry + Reversal Probability\n\n"
    "/now — live snapshot\n"
    "/outlook — daily bias\n"
    "/health — system status\n"
    "/help — this message\n\n"
    f"Threshold {int(PROB_THRESHOLD*100)}% · Max {MAX_SIGNALS_DAY}/day · Wide stops\n"
    f"{FOOT}"
)

async def command_loop(session: aiohttp.ClientSession):
    offset = 0
    while True:
        try:
            async with session.get(
                f"https://api.telegram.org/bot{TOKEN}/getUpdates?timeout=20&offset={offset}"
            ) as r:
                data = await r.json()
            for u in data.get("result", []):
                offset = u["update_id"] + 1
                text = ((u.get("message") or {}).get("text") or "").strip()
                if not text.startswith("/"): continue
                cmd = text.split()[0].split("@")[0].lower()
                if cmd == "/now":
                    await tg(session, build_now())
                elif cmd in ("/outlook", "/dayoutlook"):
                    await tg(session, build_outlook())
                elif cmd == "/health":
                    await tg(session, build_health())
                elif cmd in ("/help", "/start"):
                    await tg(session, HELP)
        except Exception as e:
            log.error(f"commands: {e}")
        await asyncio.sleep(1)

async def schedule_loop(session: aiohttp.ClientSession):
    sent = None
    while True:
        t = now_eat()
        if t.hour == 7 and t.minute < 2 and sent != t.date():
            await tg(session, build_outlook())
            sent = t.date()
        await asyncio.sleep(25)

# ═══════════════════════════════════════════════════════════════
# HTTP HEALTH
# ═══════════════════════════════════════════════════════════════
app = Flask(__name__)

@app.route("/")
def root():
    return jsonify({
        "status": "ok",
        "system": BRAND,
        "engine": "Zone Geometry + Reversal Probability",
        "signals_fired": signals_fired,
        "open": list(active.keys()),
        "record": record,
    })

@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "time": now_eat().isoformat(),
        "prices": last_price,
        "regimes": {k: v.value for k, v in regime_cache.items()},
        "zones": {k: len(v) for k, v in zones.items()},
        "active": list(active.keys()),
        "record": record,
    })

def run_flask():
    app.run(host="0.0.0.0", port=PORT, use_reloader=False)

# ═══════════════════════════════════════════════════════════════
# BOOT
# ═══════════════════════════════════════════════════════════════
async def main():
    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=22))
    log.info(f"{BRAND} booting — Zone Engine is primary logic")
    await seed_history(session)
    await tg(session,
        f"✅ <b>{BRAND} online</b>\n\n"
        f"Primary logic: Dynamic Zone Geometry + Reversal Probability\n"
        f"Wide protective stops · Asymmetric targets · Regime filters\n"
        f"Selective by design.\n\n"
        f"{now_eat().strftime('%H:%M EAT')}"
    )
    await asyncio.gather(
        apex_loop(session),
        command_loop(session),
        schedule_loop(session),
    )

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    asyncio.run(main())
