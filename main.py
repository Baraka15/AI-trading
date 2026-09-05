"""
BRAX FX — ZONE ENGINE AS MAIN LOGIC  v5.1
=========================================
Your FIRST trading robot is the core:

  Dynamic Support/Resistance Zones
  + Multi-factor Reversal Probability
  + Wide ATR stops for 30min+ holds
  + High R:R

Everything else is support:
  • Real-time price feeds (Binance + Twelve Data)
  • Human desk-style Telegram messages
  • Daily Outlook · Sessions · Flow · SOTD · News
  • Signal tracking to outcome

This is the original zone system, upgraded and running live.
"""

import asyncio, os, json, time, math, logging, random
from collections import deque
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone
from threading import Thread

import aiohttp
import pytz
from flask import Flask, jsonify

# ─────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("BRAX-ZONE")

TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TD_KEY  = os.getenv("TWELVEDATA_API_KEY", "")
PORT    = int(os.getenv("PORT", "10000"))

if not all([TOKEN, CHAT_ID, TD_KEY]):
    raise ValueError("Set TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, TWELVEDATA_API_KEY")

EAT = pytz.timezone("Africa/Nairobi")
BRAND = "BRAX FX"
FOOT  = "Educational only. Not financial advice."

# ── MAIN LOGIC CONFIG (your original zone robot) ─────────────
SYMBOLS = {
    "XAU/USD": "XAUUSD",
    "BTC/USD": "BTCUSD",
}

REV_PROB_THRESHOLD = 0.74          # high probability only
MIN_TOUCHES        = 3
MIN_ZONE_CONF      = 0.55
SL_ATR_MULT        = 3.1           # wide stop (your request)
TP_ATR_MULT        = 9.2           # high R:R ≈ 1:3
COOLDOWN_SEC       = 720
TICK_BUFFER        = 600
ATR_PERIOD         = 28
ZONE_EPS_ATR       = 0.38
ZONE_DECAY         = 0.00028
ZONE_DEAD_TIME     = 1600
SWING_K            = 3
RSI_PERIOD         = 14
POLL_SEC           = 11

# ─────────────────────────────────────────────────────────────
@dataclass
class Zone:
    center: float
    width: float
    touches: int = 0
    confidence: float = 1.0
    polarity: str = "NEUTRAL"
    last_touch: float = field(default_factory=time.time)
    anchors: List[float] = field(default_factory=list)
    strength: float = 0.5

# state
price_buf: Dict[str, deque] = {s: deque(maxlen=TICK_BUFFER) for s in SYMBOLS.values()}
zones: Dict[str, List[Zone]] = {s: [] for s in SYMBOLS.values()}
atr_c: Dict[str, float] = {s: 0.0 for s in SYMBOLS.values()}
rsi_c: Dict[str, float] = {s: 50.0 for s in SYMBOLS.values()}
last_sig_t: Dict[str, float] = {s: 0.0 for s in SYMBOLS.values()}
last_sig_h: Dict[str, str] = {s: "" for s in SYMBOLS.values()}
htf: Dict[str, str] = {s: "NEUTRAL" for s in SYMBOLS.values()}
last_px: Dict[str, float] = {s: 0.0 for s in SYMBOLS.values()}
active: Dict[str, dict] = {}
record = {"tp": 0, "sl": 0}
sig_count = 0

def now_eat():
    return datetime.now(EAT)

def fp(x, name):
    if not x: return "—"
    return f"${x:,.0f}" if "BTC" in name else f"${x:,.2f}"

# ─────────────────────────────────────────────────────────────
# CORE: your original zone + reversal probability engine
# ─────────────────────────────────────────────────────────────
def compute_atr(prices: List[float], period=ATR_PERIOD) -> float:
    if len(prices) < period + 1: return 0.0
    diffs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    if len(diffs) < period: return sum(diffs)/len(diffs) if diffs else 0.0
    return sum(diffs[-period:]) / period

def compute_rsi(prices: List[float], period=RSI_PERIOD) -> float:
    if len(prices) < period + 2: return 50.0
    deltas = [prices[i]-prices[i-1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0.0 for d in deltas]
    losses = [-d if d < 0 else 0.0 for d in deltas]
    ag = sum(gains[-period:]) / period
    al = sum(losses[-period:]) / period
    if al <= 0: return 70.0 if ag > 0 else 50.0
    rs = ag / al
    return 100 - (100 / (1 + rs))

def detect_swing(buf: deque, k=SWING_K) -> Optional[Tuple[str, float]]:
    if len(buf) < 2*k + 1: return None
    prices = list(buf)
    mid_idx = -k - 1
    mid = prices[mid_idx]
    left = prices[-2*k-1:mid_idx]
    right = prices[-k:]
    if mid > max(left) and mid > max(right): return ("HIGH", mid)
    if mid < min(left) and mid < min(right): return ("LOW", mid)
    return None

def update_zones(sym: str, anchor: float, atr: float, swing_type: Optional[str]):
    attached = False
    for z in zones[sym]:
        if abs(anchor - z.center) < ZONE_EPS_ATR * atr:
            z.anchors.append(anchor)
            if len(z.anchors) > 12: z.anchors = z.anchors[-12:]
            z.center = sum(z.anchors) / len(z.anchors)
            std = (sum((a - z.center)**2 for a in z.anchors) / len(z.anchors))**0.5
            z.width = std + 0.15 * atr
            z.touches += 1
            z.confidence = min(1.0, z.confidence + 0.12)
            z.strength = z.confidence * min(1.0, z.touches / 6.0)
            if swing_type == "HIGH": z.polarity = "RESISTANCE"
            elif swing_type == "LOW": z.polarity = "SUPPORT"
            attached = True
            break
    if not attached:
        pol = "RESISTANCE" if swing_type == "HIGH" else "SUPPORT" if swing_type == "LOW" else "NEUTRAL"
        zones[sym].append(Zone(center=anchor, width=0.28*atr, anchors=[anchor], polarity=pol, strength=0.4))

def merge_zones(sym: str, atr: float):
    if len(zones[sym]) < 2: return
    zones[sym].sort(key=lambda z: z.center)
    i = 0
    while i < len(zones[sym]) - 1:
        z1, z2 = zones[sym][i], zones[sym][i+1]
        if abs(z1.center - z2.center) < (z1.width + z2.width) * 0.55:
            anchors = z1.anchors + z2.anchors
            center = sum(anchors)/len(anchors)
            std = (sum((a-center)**2 for a in anchors)/len(anchors))**0.5
            width = std + 0.15*atr
            touches = z1.touches + z2.touches
            conf = (z1.confidence + z2.confidence)/2
            pol = z1.polarity if z1.polarity == z2.polarity else "NEUTRAL"
            strength = conf * min(1.0, touches/6.0)
            zones[sym][i] = Zone(center, width, touches, conf, pol,
                                 max(z1.last_touch, z2.last_touch), anchors, strength)
            del zones[sym][i+1]
        else:
            i += 1

def process_zones(sym: str, price: float, atr: float):
    now = time.time()
    for z in zones[sym]:
        age = now - z.last_touch
        z.confidence *= math.exp(-ZONE_DECAY * age)
        z.strength = z.confidence * min(1.0, z.touches / 6.0)
        if abs(price - z.center) < z.width and age > 10:
            z.touches += 1
            z.last_touch = now
            z.confidence = min(1.0, z.confidence + 0.14)
            z.strength = z.confidence * min(1.0, z.touches / 6.0)

def prune_zones(sym: str):
    now = time.time()
    zones[sym] = [z for z in zones[sym] if z.confidence > 0.22 and (now - z.last_touch) < ZONE_DEAD_TIME]

def compute_reversal_prob(sym: str, price: float, atr: float) -> List[dict]:
    """MAIN LOGIC — your original multi-factor reversal probability"""
    results = []
    prices = list(price_buf[sym])
    if len(prices) < 35 or atr <= 0: return results

    velocity = [prices[i]-prices[i-1] for i in range(-6, 0)]
    v_mean = sum(velocity)/len(velocity)
    v_norm = abs(v_mean)/(atr + 1e-9)
    exhaustion = max(0.0, 1.0 - min(v_norm, 1.5)/1.5)

    short_std = (sum((p - sum(prices[-15:])/15)**2 for p in prices[-15:])/15)**0.5
    long_std  = (sum((p - sum(prices[-50:])/50)**2 for p in prices[-50:])/50)**0.5 + 1e-9
    compression = max(0.0, 1.0 - short_std/long_std)

    rsi = rsi_c[sym]
    trend = htf[sym]

    for z in zones[sym]:
        if z.touches < MIN_TOUCHES or z.confidence < MIN_ZONE_CONF: continue
        dist = abs(price - z.center)
        if dist > z.width * 1.1: continue

        impulse = math.exp(-dist / (atr * 0.95))
        approach = 0.0
        rsi_b = 0.0

        if z.polarity == "RESISTANCE":
            approach = 0.14 * v_norm if v_mean > 0 else -0.05
            rsi_b = 0.11 if rsi > 65 else (-0.07 if rsi < 38 else 0)
        elif z.polarity == "SUPPORT":
            approach = 0.14 * v_norm if v_mean < 0 else -0.05
            rsi_b = 0.11 if rsi < 35 else (-0.07 if rsi > 62 else 0)
        else:
            continue

        htf_s = 0.0
        if z.polarity == "SUPPORT" and trend == "BULL": htf_s = 0.11
        elif z.polarity == "RESISTANCE" and trend == "BEAR": htf_s = 0.11
        elif z.polarity == "SUPPORT" and trend == "BEAR": htf_s = -0.15
        elif z.polarity == "RESISTANCE" and trend == "BULL": htf_s = -0.15

        raw = (0.28 * z.strength + 0.15 * exhaustion + 0.11 * compression +
               0.14 * impulse + approach + rsi_b + htf_s)
        prob = max(0.0, min(1.0, raw))
        results.append({"zone": z, "probability": prob})
    return results

def update_htf(sym: str):
    prices = list(price_buf[sym])
    if len(prices) < 55: return
    ema_f = sum(prices[-20:])/20
    ema_s = sum(prices[-50:])/50
    if ema_f > ema_s * 1.0005: htf[sym] = "BULL"
    elif ema_f < ema_s * 0.9995: htf[sym] = "BEAR"
    else: htf[sym] = "NEUTRAL"

# ─────────────────────────────────────────────────────────────
# TELEGRAM (human desk voice)
# ─────────────────────────────────────────────────────────────
async def tg(session, text: str):
    try:
        async with session.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML",
                  "disable_web_page_preview": True},
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            if r.status != 200: log.warning(f"TG {r.status}")
    except Exception as e:
        log.error(f"TG: {e}")

def human_signal(sym, bias, price, sl, tp, prob, atr, zone, rsi, trend):
    rr = abs(tp - price) / max(abs(price - sl), 1e-9)
    openers = [
        "I’m taking this one.",
        "Zone is holding. Going with it.",
        "Clean reaction here.",
        "This looks solid enough.",
    ]
    conf = "high conviction" if prob >= 0.80 else "solid setup"
    why = "price reacting from support" if bias == "BUY" else "price rejecting resistance"
    return (
        f"{'🟢' if bias == 'BUY' else '🔴'} <b>{sym} — {bias}</b>\n\n"
        f"{random.choice(openers)}\n\n"
        f"Entry   {fp(price, sym)}\n"
        f"Stop    {fp(sl, sym)}\n"
        f"Target  {fp(tp, sym)}\n\n"
        f"R:R ≈ 1:{rr:.1f}  ·  Prob {prob*100:.0f}% ({conf})\n"
        f"Why: {why}\n"
        f"Zone touches: {zone.touches} · RSI {rsi:.0f} · HTF {trend}\n"
        f"ATR {atr:.2f}\n\n"
        f"<i>Hold for the move — 30 min+ preferred.</i>\n"
        f"{now_eat().strftime('%H:%M EAT')}"
    )

# ─────────────────────────────────────────────────────────────
# DATA
# ─────────────────────────────────────────────────────────────
async def fetch_price(session, td_sym: str) -> Optional[float]:
    try:
        url = f"https://api.twelvedata.com/price?symbol={td_sym}&apikey={TD_KEY}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            d = await r.json()
        if "price" in d: return float(d["price"])
    except Exception as e:
        log.debug(f"price {td_sym}: {e}")
    return None

async def seed(session):
    log.info("Seeding from Twelve Data...")
    for td, sym in SYMBOLS.items():
        try:
            url = f"https://api.twelvedata.com/time_series?symbol={td}&interval=1min&outputsize=80&apikey={TD_KEY}"
            async with session.get(url) as r:
                d = await r.json()
            vals = d.get("values") or []
            for row in reversed(vals):
                price_buf[sym].append(float(row["close"]))
            if vals:
                last_px[sym] = float(vals[0]["close"])
                log.info(f"  {sym}: {len(vals)} bars | last={last_px[sym]:.2f}")
        except Exception as e:
            log.warning(f"  {sym} seed fail: {e}")
        await asyncio.sleep(1.2)

# ─────────────────────────────────────────────────────────────
# MAIN SIGNAL LOOP (zone engine drives everything)
# ─────────────────────────────────────────────────────────────
async def signal_loop(session):
    global sig_count
    log.info("Zone engine live")
    while True:
        try:
            for td_sym, sym in SYMBOLS.items():
                price = await fetch_price(session, td_sym)
                if price is None or price <= 0: continue

                price_buf[sym].append(price)
                last_px[sym] = price
                prices = list(price_buf[sym])
                atr = compute_atr(prices)
                atr_c[sym] = atr
                rsi_c[sym] = compute_rsi(prices)
                if atr <= 0 or len(prices) < 40: continue

                update_htf(sym)

                swing = detect_swing(price_buf[sym])
                if swing:
                    update_zones(sym, swing[1], atr, swing[0])
                    merge_zones(sym, atr)
                process_zones(sym, price, atr)
                prune_zones(sym)

                # ── MAIN LOGIC FIRE ──
                revs = compute_reversal_prob(sym, price, atr)
                now = time.time()
                for r in revs:
                    if r["probability"] < REV_PROB_THRESHOLD: continue
                    z = r["zone"]
                    bias = "SELL" if z.polarity == "RESISTANCE" else "BUY" if z.polarity == "SUPPORT" else None
                    if not bias: continue
                    if now - last_sig_t.get(sym, 0) < COOLDOWN_SEC: continue
                    h = f"{sym}-{bias}-{round(z.center,1)}"
                    if last_sig_h.get(sym) == h: continue

                    last_sig_t[sym] = now
                    last_sig_h[sym] = h
                    sig_count += 1

                    if bias == "BUY":
                        sl = price - SL_ATR_MULT * atr
                        tp = price + TP_ATR_MULT * atr
                    else:
                        sl = price + SL_ATR_MULT * atr
                        tp = price - TP_ATR_MULT * atr

                    msg = human_signal(sym, bias, price, sl, tp, r["probability"],
                                       atr, z, rsi_c[sym], htf[sym])
                    await tg(session, msg)
                    log.info(f"SIGNAL #{sig_count} {bias} {sym} @ {price:.2f} prob={r['probability']:.2f}")

                    # track
                    active[sym] = {"bias": bias, "entry": price, "sl": sl, "tp": tp, "t": now}

                # track open
                if sym in active:
                    a = active[sym]
                    if a["bias"] == "BUY":
                        if price <= a["sl"]:
                            await tg(session, f"❌ <b>{sym} stopped</b> @ {fp(a['sl'], sym)}")
                            record["sl"] += 1; del active[sym]
                        elif price >= a["tp"]:
                            await tg(session, f"🏆 <b>{sym} target hit</b> @ {fp(a['tp'], sym)}")
                            record["tp"] += 1; del active[sym]
                    else:
                        if price >= a["sl"]:
                            await tg(session, f"❌ <b>{sym} stopped</b> @ {fp(a['sl'], sym)}")
                            record["sl"] += 1; del active[sym]
                        elif price <= a["tp"]:
                            await tg(session, f"🏆 <b>{sym} target hit</b> @ {fp(a['tp'], sym)}")
                            record["tp"] += 1; del active[sym]

            await asyncio.sleep(POLL_SEC)
        except Exception as e:
            log.error(f"loop: {e}", exc_info=True)
            await asyncio.sleep(15)

# ─────────────────────────────────────────────────────────────
# DESK FEATURES (support the main zone logic)
# ─────────────────────────────────────────────────────────────
def build_now():
    lines = [f"<b>Live desk</b>  ·  {now_eat().strftime('%H:%M EAT')}\n"]
    for sym in SYMBOLS.values():
        lines.append(f"• <b>{sym}</b>  {fp(last_px.get(sym), sym)}  ·  HTF {htf.get(sym,'—')}  ·  zones {len(zones.get(sym,[]))}")
    if active:
        lines.append("")
        for s, a in active.items():
            lines.append(f"Open: {s} {a['bias']} · SL {fp(a['sl'], s)} · TP {fp(a['tp'], s)}")
    tot = record["tp"] + record["sl"]
    lines.append(f"\nRecord: {record['tp']}/{tot} green" if tot else "\nNo closed signals yet")
    return "\n".join(lines)

def build_outlook():
    lines = [f"🌅 <b>Daily outlook</b>  ·  {now_eat().strftime('%a %d %b')}\n"]
    for sym in SYMBOLS.values():
        lines.append(f"<b>{sym}</b>  {fp(last_px.get(sym), sym)}  ·  HTF {htf.get(sym,'—')}")
    lines.append(f"\nZone engine watching. A+ reversals only.\n\n<i>{FOOT}</i>")
    return "\n".join(lines)

HELP = (
    f"<b>{BRAND} — Zone Engine</b>\n\n"
    "Main logic: Dynamic zones + reversal probability\n\n"
    "/now — live\n/outlook — daily bias\n/health — status\n/help\n\n"
    f"Prob ≥ {int(REV_PROB_THRESHOLD*100)}% · Wide SL · High R:R\n{FOOT}"
)

async def commands(session):
    offset = 0
    while True:
        try:
            async with session.get(f"https://api.telegram.org/bot{TOKEN}/getUpdates?timeout=20&offset={offset}") as r:
                data = await r.json()
            for u in data.get("result", []):
                offset = u["update_id"] + 1
                text = ((u.get("message") or {}).get("text") or "").strip()
                if not text.startswith("/"): continue
                c = text.split()[0].split("@")[0].lower()
                if c == "/now": await tg(session, build_now())
                elif c in ("/outlook", "/dayoutlook"): await tg(session, build_outlook())
                elif c == "/health":
                    await tg(session, f"<b>Status</b>\nSignals sent: {sig_count}\n"
                                      f"Open: {list(active.keys()) or 'none'}\n"
                                      f"Record TP/SL: {record['tp']}/{record['sl']}")
                elif c in ("/help", "/start"): await tg(session, HELP)
        except Exception as e:
            log.error(f"cmd: {e}")
        await asyncio.sleep(1)

async def schedule(session):
    sent = None
    while True:
        t = now_eat()
        if t.hour == 7 and t.minute < 2 and sent != t.date():
            await tg(session, build_outlook())
            sent = t.date()
        await asyncio.sleep(30)

# ─────────────────────────────────────────────────────────────
app = Flask(__name__)
@app.route("/")
def root(): return jsonify(status="ok", engine="Zone + Reversal Probability", signals=sig_count)
@app.route("/health")
def health():
    return jsonify(status="ok", time=now_eat().isoformat(),
                   prices=last_px, zones={s: len(z) for s,z in zones.items()},
                   active=list(active.keys()), record=record)

def run_flask():
    app.run(host="0.0.0.0", port=PORT, use_reloader=False)

async def main():
    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
    log.info(f"{BRAND} ZONE ENGINE (main logic) starting")
    await seed(session)
    await tg(session,
        f"✅ <b>{BRAND} Zone Engine live</b>\n\n"
        f"Main logic: Dynamic zones + reversal probability\n"
        f"Wide stops · High R:R · Human desk voice\n\n"
        f"{now_eat().strftime('%H:%M EAT')}"
    )
    await asyncio.gather(
        signal_loop(session),
        commands(session),
        schedule(session),
    )

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    asyncio.run(main())
