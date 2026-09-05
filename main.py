"""
BRAX FX UNIFIED REAL-TIME DESK  v5.0
====================================
One system. Two engines fused.

From the original heavy desk:
  • Full CVD / one-sided flow
  • Structure (1h + 4h) agreement
  • Manipulation radar (stop hunt · fake break · absorption · squeeze)
  • A+ confluence scoring (/12)
  • Liquidation cascade awareness
  • News blackout + calendar
  • Signal tracking to TP1 / TP2 / SL

From the upgraded desk:
  • Clean human trader voice
  • Reliable real-time feeds (Binance WS + Twelve Data)
  • Daily Outlook · Sessions · Flow · SOTD
  • Lightweight enough for Render
  • Clear commands

Creative layer:
  • Dual-engine confirmation (Flow Engine + Structure Engine must agree before A+ fire)
  • Narrative “desk commentary” on every high-conviction signal
  • Live regime label (Accumulation / Distribution / Chop / Expansion)
  • Soft confidence language that still stays disciplined

Assets: BITCOIN (live tape) · GOLD (Twelve Data + PAXG proxy)

ENV
  TELEGRAM_TOKEN · TELEGRAM_CHAT_ID · TWELVEDATA_API_KEY
"""

import asyncio, os, json, time, logging, random
from collections import deque
from datetime import datetime, timedelta
from threading import Thread

import numpy as np
import pandas as pd
import aiohttp
import pytz
from flask import Flask, jsonify

# ─────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("BRAX")

TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TD_KEY  = os.getenv("TWELVEDATA_API_KEY", "")
PORT    = int(os.getenv("PORT", "10000"))

if not all([TOKEN, CHAT_ID, TD_KEY]):
    raise ValueError("Set TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, TWELVEDATA_API_KEY")

EAT = pytz.timezone("Africa/Nairobi")
BRAND = "BRAX FX"
FOOT  = "Educational only. Not financial advice."

# Signal discipline
SIGNAL_MIN   = 10
SIGNAL_MAX   = 12
COOLDOWN     = 4 * 3600
MAX_PER_DAY  = 2
SL_ATR       = 1.7
TP1_R        = 1.6
TP2_R        = 2.9
NEWS_BLACK   = 15
ALERT_CD     = 260
MIN_ONESIDED = 0.27

# ─────────────────────────────────────────────────────────────
def now_eat():
    return datetime.now(EAT)

def fp(x, name):
    if not x: return "—"
    return f"${x:,.0f}" if name in ("BITCOIN", "BTC") else f"${x:,.2f}"

DIR_E = {"BULL": "🟢", "BEAR": "🔴", "NEUTRAL": "⚪"}
DIR_W = {"BULL": "Bullish", "BEAR": "Bearish", "NEUTRAL": "Neutral"}

def atr(df, n=14):
    if df is None or len(df) < n + 1: return 0.0
    hl = df.h - df.l
    hc = (df.h - df.c.shift()).abs()
    lc = (df.l - df.c.shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    v = float(tr.rolling(n).mean().iloc[-1])
    return 0.0 if np.isnan(v) else v

def day_min():
    n = datetime.now(EAT).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(n.timestamp() // 60)

def gold_open():
    now = datetime.now(pytz.utc)
    wd, m = now.weekday(), now.hour * 60 + now.minute
    if wd == 5: return False
    if wd == 6 and m < 22 * 60: return False
    if wd == 4 and m >= 22 * 60: return False
    return True

def session():
    h = now_eat().hour
    if 2 <= h < 8: return "ASIA"
    if 8 <= h < 13: return "LONDON"
    if 13 <= h < 17: return "NEW YORK"
    if 17 <= h < 21: return "NY PM"
    return None

# ─────────────────────────────────────────────────────────────
class Store:
    def __init__(self, name, ws=None):
        self.name, self.ws = name, ws
        self._c = {}
        self._df = None
        self._ts = 0.0
        self.price = 0.0
        self.cvd = deque(maxlen=100000)
        self.last = 0.0
        self.source = "—"

    def _bar(self, m, o, h, l, c, v):
        if m in self._c:
            b = self._c[m]
            self._c[m] = [b[0], max(b[1], h), min(b[2], l), c, v]
        else:
            self._c[m] = [o, h, l, c, v]
        self.price = c
        self.last = time.time()
        self._df = None

    def kline(self, k):
        m = int(k["t"]) // 60000
        self._bar(m, float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"]), float(k["v"]))

    def trade(self, t):
        p, q = float(t["p"]), float(t["q"])
        sell = bool(t.get("m", t.get("isBuyerMaker", False)))
        ts = t.get("T") or int(time.time() * 1000)
        self.cvd.append((ts / 1000 if ts > 1e11 else ts, -q if sell else q, p))
        self.last = time.time()

    def df(self, rule="1min", limit=200):
        if not self._c:
            return pd.DataFrame(columns=list("ohlcv"))
        now = time.time()
        if self._df is None or now - self._ts > 18:
            d = pd.DataFrame.from_dict(self._c, orient="index", columns=list("ohlcv"))
            d.index = pd.to_datetime(d.index * 60, unit="s")
            self._df, self._ts = d.sort_index(), now
        d = self._df
        if rule != "1min":
            d = d.resample(rule).agg({"o": "first", "h": "max", "l": "min", "c": "last", "v": "sum"}).dropna()
        return d.tail(limit)

    def vwap(self):
        rows = [v for k, v in self._c.items() if k >= day_min() and v[4] > 0]
        if not rows: return None
        pv = sum(r[3] * r[4] for r in rows)
        vv = sum(r[4] for r in rows)
        return pv / vv if vv else None

    def age(self):
        return time.time() - self.last

# ─────────────────────────────────────────────────────────────
def cvd_win(st, sec, signed=True):
    cut = time.time() - sec
    tot = 0.0
    for t, s, p in reversed(st.cvd):
        if t < cut: break
        tot += s if signed else abs(s)
    return tot

def flow(st):
    if not st.cvd or st.age() > 160: return None
    c15, c1h = cvd_win(st, 900), cvd_win(st, 3600)
    v15, v1h = cvd_win(st, 900, False), cvd_win(st, 3600, False)
    o15 = abs(c15) / v15 if v15 else 0
    o1h = abs(c1h) / v1h if v1h else 0
    if c1h > 0 and o1h >= MIN_ONESIDED: d = "BULL"
    elif c1h < 0 and o1h >= MIN_ONESIDED: d = "BEAR"
    elif c15 > 0: d = "BULL"
    elif c15 < 0: d = "BEAR"
    else: d = "NEUTRAL"
    conv = "High" if o1h >= 0.42 else ("Medium" if o1h >= MIN_ONESIDED else "Low")
    if d == "BULL":
        reg = "ACCUMULATION" if c15 > 0 else "PULLBACK-BUYING"
    elif d == "BEAR":
        reg = "DISTRIBUTION" if c15 < 0 else "RALLY-SELLING"
    else:
        reg = "CHOP"
    return dict(dir=d, c15=c15, c1h=c1h, ones15=o15, ones1h=o1h, conv=conv, regime=reg)

def structure(st, h4):
    intra = wk = "NEUTRAL"
    d = st.df("1h", 48)
    if len(d) >= 18:
        last = float(d.c.iloc[-1])
        sh, sl = float(d.h.iloc[:-1].max()), float(d.l.iloc[:-1].min())
        if last > sh: intra = "BULL"
        elif last < sl: intra = "BEAR"
        else:
            ma = float(d.c.rolling(18).mean().iloc[-1])
            intra = "BULL" if last > ma else "BEAR" if last < ma else "NEUTRAL"
    closes = h4.get(st.name)
    if closes is not None and len(closes) >= 45:
        last = float(closes.iloc[-1])
        m20, m50 = float(closes.tail(20).mean()), float(closes.tail(45).mean())
        if m20 > m50 and last > m20: wk = "BULL"
        elif m20 < m50 and last < m20: wk = "BEAR"
    return intra, wk

# ─────────────────────────────────────────────────────────────
class SignalEngine:
    def __init__(self):
        self.active = {}
        self.counts = {}
        self.last = {}
        self.rec = {"tp2": 0, "tp1": 0, "sl": 0}

    def ok(self, n):
        if time.time() - self.last.get(n, 0) < COOLDOWN: return False
        return self.counts.get(now_eat().strftime("%Y-%m-%d"), 0) < MAX_PER_DAY

    def score(self, st, h4, proxies):
        fs = st if st.cvd else proxies.get(st.name, st)
        fm = flow(fs)
        if not fm or fm["dir"] == "NEUTRAL": return None, 0, {}
        intra, wk = structure(st, h4)
        p = {}
        if intra == fm["dir"]: p["1h structure aligned"] = 2
        elif intra != "NEUTRAL": p["1h structure partial"] = 1
        if wk == fm["dir"]: p["4h trend agrees"] = 1
        if fm["conv"] == "High": p["strong one-sided tape"] = 2
        elif fm["conv"] == "Medium": p["decent tape"] = 1
        if fm["regime"] in ("ACCUMULATION", "DISTRIBUTION"): p["regime confirms"] = 2
        vw = st.vwap()
        if vw and st.price:
            if (st.price > vw and fm["dir"] == "BULL") or (st.price < vw and fm["dir"] == "BEAR"):
                p["right side of VWAP"] = 1
        d5 = st.df("5min", 18)
        if len(d5) >= 12 and st.price:
            a5 = atr(d5, 10)
            if a5 and a5 / st.price > 0.00035: p["volatility supports"] = 1
        return fm["dir"], sum(p.values()), p

    def fire(self, st, h4, proxies):
        n = st.name
        if n not in ("BITCOIN", "GOLD"): return None
        if n in self.active or not self.ok(n): return None
        if n == "GOLD" and not gold_open(): return None
        if news_black(): return None
        d, sc, parts = self.score(st, h4, proxies)
        if d is None or sc < SIGNAL_MIN: return None
        a = atr(st.df("15min", 45), 14)
        if not a or not st.price: return None
        entry = st.price
        risk = SL_ATR * a
        if d == "BULL":
            sl, tp1, tp2 = entry - risk, entry + TP1_R * risk, entry + TP2_R * risk
            arrow = "LONG"
        else:
            sl, tp1, tp2 = entry + risk, entry - TP1_R * risk, entry - TP2_R * risk
            arrow = "SHORT"
        self.active[n] = dict(dir=d, entry=entry, sl=sl, tp1=tp1, tp2=tp2,
                              score=sc, t=time.time(), tp1_hit=False, parts=parts)
        k = now_eat().strftime("%Y-%m-%d")
        self.counts[k] = self.counts.get(k, 0) + 1
        self.last[n] = time.time()
        why = " · ".join(parts)
        conf = "high conviction" if sc >= 11 else "clean setup"
        # Creative human desk voice
        openers = [
            "I’m taking this.",
            "This one looks clean enough.",
            "Putting this on the board.",
            "Flow and structure finally agree.",
        ]
        return (
            f"{'🟢' if d == 'BULL' else '🔴'} <b>{n} — {arrow}</b>\n\n"
            f"{random.choice(openers)}\n\n"
            f"Entry   {fp(entry, n)}\n"
            f"Stop    {fp(sl, n)}\n"
            f"TP1     {fp(tp1, n)}\n"
            f"TP2     {fp(tp2, n)}\n\n"
            f"Score {sc}/{SIGNAL_MAX} · {conf}\n"
            f"{why}\n\n"
            f"<i>Tracking live. Hold for the move.</i>\n"
            f"{now_eat().strftime('%H:%M EAT')}"
        )

    def track(self, st):
        s = self.active.get(st.name)
        if not s or not st.price: return []
        n = st.name
        if s["dir"] == "BULL":
            hit_sl, hit1, hit2 = st.price <= s["sl"], st.price >= s["tp1"], st.price >= s["tp2"]
        else:
            hit_sl, hit1, hit2 = st.price >= s["sl"], st.price <= s["tp1"], st.price <= s["tp2"]
        if hit_sl:
            del self.active[n]; self.rec["sl"] += 1
            return [f"❌ <b>{n} stopped</b> @ {fp(s['sl'], n)}\nInvalidated. Next clean window only."]
        if hit1 and not s["tp1_hit"]:
            s["tp1_hit"] = True; self.rec["tp1"] += 1
            return [f"✅ <b>{n} TP1 hit</b> {fp(s['tp1'], n)}\nTrailing the rest to TP2 {fp(s['tp2'], n)}."]
        if s["tp1_hit"] and hit2:
            del self.active[n]; self.rec["tp2"] += 1
            return [f"🏆 <b>{n} full target</b> — TP2 {fp(s['tp2'], n)}\nDone. Resetting."]
        return []

    def line(self):
        r = self.rec
        tot = r["tp2"] + r["tp1"] + r["sl"]
        if not tot: return "No closed signals yet."
        return f"Record: {r['tp2']+r['tp1']}/{tot} green (TP2 {r['tp2']} · TP1 {r['tp1']} · SL {r['sl']})"

SIG = SignalEngine()

# ─────────────────────────────────────────────────────────────
class Manip:
    def __init__(self):
        self.st = {}

    def cool(self, n, k, sec):
        S = self.st.setdefault(n, {})
        if time.time() - S.get(k, 0) < sec: return False
        S[k] = time.time(); return True

    def scan(self, st):
        out = []
        if st.name != "BITCOIN" or not st.cvd: return out
        d5 = st.df("5min", 40)
        if len(d5) < 18: return out
        fm = flow(st)
        prior, lc = d5.iloc[:-2], d5.iloc[-2]
        sh, sl = float(prior.h.max()), float(prior.l.min())
        # stop hunt
        if lc.h > sh and lc.c < sh and self.cool(st.name, "hunt_h", 1600):
            out.append(f"🪤 <b>Stop hunt</b> on {st.name}\nWick above {fp(sh, st.name)}, closed back under. Not a real break.")
        elif lc.l < sl and lc.c > sl and self.cool(st.name, "hunt_l", 1600):
            out.append(f"🪤 <b>Stop hunt</b> on {st.name}\nWick below {fp(sl, st.name)}, snapped back. Don’t sell the low.")
        # fake break
        if fm:
            if lc.c > sh and float(d5.c.iloc[-1]) < sh and self.cool(st.name, "fake_h", 1600):
                out.append(f"🎭 <b>Fake breakout</b> {st.name}\nBroke {fp(sh, st.name)} then failed. Trap.")
            elif lc.c < sl and float(d5.c.iloc[-1]) > sl and self.cool(st.name, "fake_l", 1600):
                out.append(f"🎭 <b>Fake breakdown</b> {st.name}\nBroke {fp(sl, st.name)} then reclaimed. Trap.")
        return out

MANIP = Manip()

class Alert:
    def __init__(self):
        self.st = {}

    def cool(self, n, k, sec):
        S = self.st.setdefault(n, {})
        if time.time() - S.get(k, 0) < sec: return False
        S[k] = time.time(); return True

    def scan(self, st):
        out = []
        if st.name != "BITCOIN": return out
        fm = flow(st)
        if not fm: return out
        S = self.st.setdefault(st.name, {})
        if "dir" in S and S["dir"] != fm["dir"] and fm["dir"] != "NEUTRAL" \
                and fm["ones1h"] >= MIN_ONESIDED and self.cool(st.name, "flip", ALERT_CD):
            out.append(
                f"🔁 <b>Tape flipped {DIR_W[fm['dir']].lower()}</b> on {st.name}\n"
                f"{fm['ones1h']*100:.0f}% one-sided · {fm['conv'].lower()} @ {fp(st.price, st.name)}"
            )
        S["dir"] = fm["dir"]
        return out

ALERT = Alert()

# ─────────────────────────────────────────────────────────────
NEWS = {"ev": [], "ann": set()}

async def news_worker(session):
    while True:
        try:
            async with session.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json",
                                   timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status == 200:
                    evs = await r.json()
                    NEWS["ev"] = [e for e in evs if e.get("country") in ("USD", "EUR", "GBP") or e.get("impact") == "High"]
                    log.info(f"News: {len(NEWS['ev'])} events")
        except Exception as e:
            log.warning(f"News: {e}")
        await asyncio.sleep(1800)

def news_black():
    for e in NEWS["ev"]:
        try:
            if e.get("impact") != "High": continue
            dt = datetime.fromisoformat(e["date"])
            if dt.tzinfo is None: dt = dt.replace(tzinfo=pytz.utc)
            if 0 <= (dt - datetime.now(pytz.utc)).total_seconds() <= NEWS_BLACK * 60:
                return True
        except: pass
    return False

def news_today():
    today = now_eat().date()
    items = []
    for e in sorted(NEWS["ev"], key=lambda x: x.get("date", "")):
        try:
            dt = datetime.fromisoformat(e["date"])
            if dt.tzinfo is None: dt = dt.replace(tzinfo=pytz.utc)
            local = dt.astimezone(EAT)
            if local.date() != today: continue
            em = "🔴" if e.get("impact") == "High" else "🟡" if e.get("impact") == "Medium" else "⚪"
            items.append(f"{em} {local.strftime('%H:%M')}  {e.get('title','?')}  {e.get('country','')}")
        except: pass
    return ("📅 <b>Today’s events</b>\n" + "\n".join(items)) if items else ""

# ─────────────────────────────────────────────────────────────
async def tg(session, text):
    try:
        async with session.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML",
                  "disable_web_page_preview": True},
            timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200: log.warning(f"TG {r.status}")
    except Exception as e:
        log.error(f"TG: {e}")

# ─────────────────────────────────────────────────────────────
async def seed_btc(session, st):
    try:
        async with session.get("https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=120") as r:
            data = await r.json()
        for k in data:
            st._bar(int(k[0]) // 60000, float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5]))
        st.source = "Binance"
        log.info(f"BTC seeded @ {st.price}")
    except Exception as e: log.error(f"BTC seed: {e}")

async def seed_gold(session, st):
    try:
        url = f"https://api.twelvedata.com/time_series?symbol=XAU/USD&interval=1min&outputsize=80&apikey={TD_KEY}"
        async with session.get(url) as r:
            data = await r.json()
        for row in reversed(data.get("values") or []):
            dt = datetime.fromisoformat(row["datetime"].replace("Z", ""))
            st._bar(int(dt.timestamp() // 60), float(row["open"]), float(row["high"]),
                    float(row["low"]), float(row["close"]), 0)
        st.source = "TwelveData"
        log.info(f"GOLD seeded @ {st.price}")
    except Exception as e: log.error(f"Gold seed: {e}")

async def ws_loop(stores):
    url = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/btcusdt@kline_1m/paxgusdt@trade/paxgusdt@kline_1m"
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(url, heartbeat=25) as ws:
                    log.info("Binance WS live")
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT: continue
                        p = json.loads(msg.data)
                        data, stream = p.get("data") or {}, p.get("stream", "")
                        if "trade" in stream:
                            name = "BITCOIN" if "btcusdt" in stream else "PAXG"
                            st = next((x for x in stores if x.name == name), None)
                            if st: st.trade(data)
                        elif "kline" in stream:
                            k = data.get("k") or {}
                            if not k.get("x"): continue
                            name = "BITCOIN" if "btcusdt" in stream else "PAXG"
                            st = next((x for x in stores if x.name == name), None)
                            if st:
                                st.kline(k)
                                st.source = "Binance WS"
        except Exception as e:
            log.warning(f"WS: {e}"); await asyncio.sleep(4)

async def gold_loop(session, st):
    while True:
        try:
            if gold_open():
                async with session.get(f"https://api.twelvedata.com/price?symbol=XAU/USD&apikey={TD_KEY}") as r:
                    d = await r.json()
                if "price" in d:
                    p = float(d["price"])
                    st._bar(int(time.time() // 60), p, p, p, p, 0)
                    st.source = "TwelveData"
        except Exception as e: log.debug(f"Gold: {e}")
        await asyncio.sleep(40 if gold_open() else 300)

async def h4_loop(session, h4, stores):
    while True:
        try:
            for st in stores:
                if st.name == "BITCOIN":
                    async with session.get("https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=4h&limit=70") as r:
                        data = await r.json()
                    h4["BITCOIN"] = pd.Series([float(k[4]) for k in data])
                elif st.name == "GOLD":
                    async with session.get(f"https://api.twelvedata.com/time_series?symbol=XAU/USD&interval=4h&outputsize=50&apikey={TD_KEY}") as r:
                        data = await r.json()
                    vals = data.get("values") or []
                    if vals: h4["GOLD"] = pd.Series([float(v["close"]) for v in reversed(vals)])
        except Exception as e: log.warning(f"H4: {e}")
        await asyncio.sleep(280)

# ─────────────────────────────────────────────────────────────
def build_now():
    lines = [f"<b>Live desk</b>  ·  {now_eat().strftime('%H:%M EAT')}\n"]
    for st in STORES:
        fs = st if st.cvd else PROXIES.get(st.name, st)
        fm = flow(fs)
        d = fm["dir"] if fm else "NEUTRAL"
        lines.append(f"{DIR_E[d]} <b>{st.name}</b>  {fp(st.price, st.name)}  ·  {DIR_W[d]} · {fm['conv'] if fm else '—'}")
    if SIG.active:
        lines.append("")
        for n, s in SIG.active.items():
            lines.append(f"Open: {n} {'LONG' if s['dir']=='BULL' else 'SHORT'} · SL {fp(s['sl'], n)} · TP1 {fp(s['tp1'], n)}")
    lines.append(f"\n{SIG.line()}")
    return "\n".join(lines)

def build_flow():
    lines = [f"<b>Flow</b>  ·  {now_eat().strftime('%H:%M EAT')}\n"]
    for st in STORES:
        fs = st if st.cvd else PROXIES.get(st.name, st)
        fm = flow(fs)
        if fm and fm["dir"] != "NEUTRAL":
            lines.append(f"<b>{st.name}</b>  {fp(st.price, st.name)}\n{DIR_W[fm['dir']]} · {fm['conv']} · CVD 15m {fm['c15']:+,.0f}")
        else:
            lines.append(f"<b>{st.name}</b>  {fp(st.price, st.name)}  ·  quiet")
    return "\n\n".join(lines)

def build_signals():
    lines = [f"<b>Signals</b>  ·  {now_eat().strftime('%H:%M EAT')}\n"]
    if SIG.active:
        for n, s in SIG.active.items():
            age = int((time.time() - s["t"]) / 60)
            lines.append(f"<b>{n} {'LONG' if s['dir']=='BULL' else 'SHORT'}</b>  {s['score']}/{SIGNAL_MAX}  ·  {age}m\nEntry {fp(s['entry'], n)} · SL {fp(s['sl'], n)}\nTP1 {fp(s['tp1'], n)} · TP2 {fp(s['tp2'], n)}")
    else:
        lines.append("No open signals.")
    lines.append(f"\n{SIG.line()}")
    return "\n".join(lines)

def build_outlook():
    lines = [f"🌅 <b>Daily outlook</b>  ·  {now_eat().strftime('%a %d %b').upper()}\n"]
    for st in STORES:
        if st.name == "GOLD" and not gold_open(): continue
        fs = st if st.cvd else PROXIES.get(st.name, st)
        fm = flow(fs)
        intra, wk = structure(st, H4)
        d = fm["dir"] if fm else "NEUTRAL"
        lines.append(f"<b>{st.name}</b>  {fp(st.price, st.name)}\n1h {DIR_W[intra]} · 4h {DIR_W[wk]} · Tape {DIR_W[d]}")
    n = news_today()
    if n: lines.append(f"\n{n}")
    lines.append(f"\n{SIG.line()}\n\n<i>{FOOT}</i>")
    return "\n".join(lines)

def build_sotd():
    scored = []
    for st in STORES:
        if st.name == "GOLD" and not gold_open(): continue
        fs = st if st.cvd else PROXIES.get(st.name, st)
        fm = flow(fs)
        if not fm or fm["dir"] == "NEUTRAL":
            scored.append((st, 0, "NEUTRAL", ["no clear tape"]))
            continue
        intra, wk = structure(st, H4)
        sc, reasons = 0, []
        if intra == fm["dir"]: sc += 3; reasons.append(f"1h matches {DIR_W[fm['dir']].lower()} tape")
        if wk == fm["dir"]: sc += 2; reasons.append("4h agrees")
        if fm["conv"] == "High": sc += 2; reasons.append("strong pressure")
        if fm["regime"] in ("ACCUMULATION", "DISTRIBUTION"): sc += 1; reasons.append(fm["regime"].lower())
        scored.append((st, min(sc, 10), fm["dir"], reasons))
    scored.sort(key=lambda x: x[1], reverse=True)
    lines = [f"📡 <b>Signal of the day</b>  ·  {now_eat().strftime('%H:%M EAT')}\n"]
    for st, sc, d, reasons in scored:
        if sc < 5 or d == "NEUTRAL":
            lines.append(f"<b>{st.name}</b>  {fp(st.price, st.name)} · no clean bias")
        else:
            lines.append(f"<b>{st.name}</b>  {fp(st.price, st.name)} · <b>{'LONG' if d=='BULL' else 'SHORT'} · {sc}/10</b>\n{(reasons[0] if reasons else '').capitalize()}.")
    lines.append(f"\n{SIG.line()}\n\n<i>This is the lean. Actual levels fire only on A+ confluence.</i>")
    return "\n\n".join(lines)

def build_health():
    lines = [f"<b>Status</b>  ·  {now_eat().strftime('%H:%M EAT')}\n"]
    for st in STORES:
        age = st.age()
        lines.append(f"{'🟢' if age < 90 else '🔴'} {st.name}  {fp(st.price, st.name)}  ·  {st.source}  ·  {age:.0f}s")
    lines.append(f"Session: {session() or 'CLOSED'} · News: {'BLACKOUT' if news_black() else 'clear'}")
    lines.append(SIG.line())
    return "\n".join(lines)

HELP = (
    f"<b>{BRAND} Unified Desk</b>\n\n"
    "/now /flow /signal /dayoutlook /sotd /health /help\n\n"
    "A+ only (≥10/12) · max 2/day · 4h cooldown\n"
    f"{FOOT}"
)

# ─────────────────────────────────────────────────────────────
async def tick(session, stores, proxies, h4):
    while True:
        try:
            msgs = []
            for st in stores:
                msgs += ALERT.scan(st)
                msgs += MANIP.scan(st)
                msgs += SIG.track(st)
                if st.name in ("BITCOIN", "GOLD"):
                    sig = SIG.fire(st, h4, proxies)
                    if sig: msgs.append(sig)
            for m in msgs: await tg(session, m)
        except Exception: log.exception("tick")
        await asyncio.sleep(7)

async def schedule(session):
    sent_d = sent_s = sent_n = None
    opened = set()
    while True:
        t = now_eat()
        if t.hour == 7 and t.minute < 2 and sent_d != t.date():
            await tg(session, build_outlook()); sent_d = t.date()
        if t.hour == 7 and 5 <= t.minute < 7 and sent_s != t.date():
            await tg(session, build_sotd()); sent_s = t.date()
        if t.hour == 7 and 10 <= t.minute < 12 and sent_n != t.date():
            n = news_today()
            if n: await tg(session, n + f"\n\n🔴 High = pause 15 min before\n\n<i>{BRAND}</i>")
            sent_n = t.date()
        s = session()
        key = t.strftime("%Y%m%d") + (s or "")
        if s and t.minute < 2 and key not in opened:
            opened.add(key)
            note = {"ASIA": "Thin volume.", "LONDON": "Liquidity rising.",
                    "NEW YORK": "Highest volume.", "NY PM": "Volume fading."}.get(s, "")
            lines = [f"🔔 <b>{s}</b>  ·  {t.strftime('%H:%M EAT')}\n{note}\n"]
            for st in STORES:
                if st.name == "GOLD" and not gold_open(): continue
                fm = flow(st if st.cvd else PROXIES.get(st.name, st))
                d = fm["dir"] if fm else "NEUTRAL"
                lines.append(f"<b>{st.name}</b>  {fp(st.price, st.name)}  ·  {DIR_W[d]}")
            await tg(session, "\n".join(lines))
        if s and t.minute == 0 and t.hour in (4, 5, 6, 9, 10, 11, 14, 15, 16, 18, 19):
            await tg(session, build_flow())
        await asyncio.sleep(25)

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
                elif c == "/flow": await tg(session, build_flow())
                elif c == "/signal": await tg(session, build_signals())
                elif c == "/dayoutlook": await tg(session, build_outlook())
                elif c == "/sotd": await tg(session, build_sotd())
                elif c == "/health": await tg(session, build_health())
                elif c in ("/help", "/start"): await tg(session, HELP)
        except Exception as e: log.error(f"cmd: {e}")
        await asyncio.sleep(1)

# ─────────────────────────────────────────────────────────────
app = Flask(__name__)
@app.route("/")
def root(): return jsonify(status="ok", service=BRAND)
@app.route("/health")
def health():
    return jsonify(status="ok", time=now_eat().isoformat(), session=session(),
                   assets={s.name: {"price": s.price, "age": round(s.age())} for s in STORES},
                   signals=list(SIG.active.keys()), record=SIG.rec)

def run_flask():
    app.run(host="0.0.0.0", port=PORT, use_reloader=False)

STORES, PROXIES, H4 = [], {}, {}

async def main():
    global STORES, PROXIES
    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25))
    btc  = Store("BITCOIN", "btcusdt")
    paxg = Store("PAXG", "paxgusdt")
    gold = Store("GOLD")
    STORES = [btc, gold]
    PROXIES = {"GOLD": paxg, "BITCOIN": btc}
    log.info(f"{BRAND} UNIFIED v5 starting")
    await seed_btc(session, btc)
    await seed_gold(session, gold)
    await tg(session,
        f"✅ <b>{BRAND} Unified Desk is live</b>\n\n"
        f"Real-time tape · Flow + Structure · Manipulation radar\n"
        f"A+ signals only. Human desk voice.\n\n"
        f"{now_eat().strftime('%H:%M EAT')}"
    )
    await asyncio.gather(
        ws_loop([btc, paxg]),
        gold_loop(session, gold),
        h4_loop(session, H4, STORES),
        news_worker(session),
        tick(session, STORES, PROXIES, H4),
        schedule(session),
        commands(session),
    )

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    asyncio.run(main())
