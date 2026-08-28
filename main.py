import asyncio
import os
import logging
from datetime import datetime
from dataclasses import dataclass
from typing import Optional
import numpy as np
import pandas as pd
import aiohttp
from gtts import gTTS
import pytz
from flask import Flask
from threading import Thread

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("BraxAdvanced")

TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TD_KEY = os.getenv("TWELVEDATA_API_KEY")

if not TOKEN or not CHAT_ID or not TD_KEY:
    raise ValueError("Missing required environment variables")

EAT = pytz.timezone("Africa/Nairobi")
SIGNAL_INTERVAL = 900

app = Flask(__name__)

@app.route("/")
def health():
    return "BRAX ADVANCED DESK - ONLINE", 200

class TwelveDataFeed:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=18))

    async def get_quote(self, symbol: str) -> float:
        url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_KEY}"
        try:
            async with self.session.get(url) as r:
                data = await r.json()
                if data.get("status") == "error":
                    logger.warning(f"Quote error {symbol}: {data.get('message')}")
                    return 0.0
                price = data.get("close") or data.get("price")
                return float(price) if price else 0.0
        except Exception as e:
            logger.error(f"Quote {symbol}: {e}")
            return 0.0

    async def get_candles(self, symbol: str) -> pd.DataFrame:
        # Try 5min first, then 15min as fallback
        for interval, size in [("5min", 80), ("15min", 50)]:
            url = (
                f"https://api.twelvedata.com/time_series"
                f"?symbol={symbol}&interval={interval}&outputsize={size}&apikey={TD_KEY}"
            )
            try:
                async with self.session.get(url) as r:
                    data = await r.json()
                    if data.get("status") == "error":
                        logger.warning(f"Candles {interval} {symbol}: {data.get('message')}")
                        continue
                    if "values" not in data or not data["values"]:
                        continue

                    df = pd.DataFrame(data["values"])
                    df = df.rename(columns={"open": "o", "high": "h", "low": "l", "close": "c", "volume": "v"})
                    for col in ["o", "h", "l", "c"]:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    df["v"] = pd.to_numeric(df.get("v", 0), errors="coerce").fillna(0)
                    df = df.dropna(subset=["o", "h", "l", "c"])
                    df = df.iloc[::-1].reset_index(drop=True)
                    if len(df) >= 15:
                        logger.info(f"Got {len(df)} candles for {symbol} ({interval})")
                        return df
            except Exception as e:
                logger.error(f"Candles error {symbol}: {e}")
        return pd.DataFrame()

def calc_rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss
    val = 100 - (100 / (1 + rs.iloc[-1]))
    return float(val) if not np.isnan(val) else 50.0

@dataclass
class Signal:
    name: str
    price: float
    direction: str
    conviction: int
    session: str
    regime: str
    cvd: float
    entry: float
    sl: float
    t1: float
    t2: float
    day_bias: str
    atr: float
    rsi: float
    ema9: float
    ema21: float
    change_pct: float
    next_15min: str
    has_structure: bool

async def analyze(feed: TwelveDataFeed, symbol: str, name: str) -> Signal:
    price = await feed.get_quote(symbol)
    df = await feed.get_candles(symbol)

    now = datetime.now(EAT)
    h = now.hour + now.minute / 60
    if 13 <= h < 17:
        sess = "NY KILLZONE"
    elif 8 <= h < 13:
        sess = "LONDON"
    elif 2 <= h < 8:
        sess = "ASIAN"
    else:
        sess = "OFF-HOURS"

    # Fallback if no candle data
    if price <= 0:
        return Signal(
            name=name, price=0.0, direction="NEUTRAL", conviction=1,
            session=sess, regime="NO PRICE DATA", cvd=0.0,
            entry=0, sl=0, t1=0, t2=0, day_bias="NEUTRAL",
            atr=0, rsi=50, ema9=0, ema21=0, change_pct=0,
            next_15min="No price data available.", has_structure=False
        )

    if df.empty or len(df) < 15:
        # We have price but no structure → simple briefing
        return Signal(
            name=name, price=price, direction="NEUTRAL", conviction=2,
            session=sess, regime="LIMITED DATA", cvd=0.0,
            entry=price, sl=price, t1=price, t2=price,
            day_bias="NEUTRAL", atr=0.0, rsi=50.0,
            ema9=price, ema21=price, change_pct=0.0,
            next_15min=f"Price is at {price:.1f}. Waiting for clearer structure. Expect range-bound action in the next 15 minutes.",
            has_structure=False
        )

    # Full analysis
    tr = np.maximum(
        df["h"] - df["l"],
        np.maximum(abs(df["h"] - df["c"].shift()), abs(df["l"] - df["c"].shift()))
    )
    atr = float(tr.rolling(14).mean().iloc[-1])

    rsi = calc_rsi(df["c"])
    ema9 = float(df["c"].ewm(span=9).mean().iloc[-1])
    ema21 = float(df["c"].ewm(span=21).mean().iloc[-1])

    df["buy_vol"] = np.where(df["c"] > df["o"], df["v"], 0)
    df["sell_vol"] = np.where(df["c"] < df["o"], df["v"], 0)
    cvd = float(df["buy_vol"].tail(12).sum() - df["sell_vol"].tail(12).sum())

    vol_mean = float(df["v"].tail(20).mean()) or 1.0
    regime = "TRENDING" if abs(cvd) > vol_mean * 0.5 else "RANGING / COMPRESSING"

    score = 0
    bull = bear = 0

    if cvd > 0:
        bull += 1
        score += 2 if abs(cvd) > vol_mean * 0.3 else 1
    else:
        bear += 1
        score += 2 if abs(cvd) > vol_mean * 0.3 else 1

    if ema9 > ema21:
        bull += 1
        score += 1
    else:
        bear += 1
        score += 1

    if rsi < 33:
        bull += 1
        score += 1
    elif rsi > 67:
        bear += 1
        score += 1

    if sess in ("LONDON", "NY KILLZONE"):
        score += 1

    direction = "BUY" if bull > bear else "SELL"
    if abs(bull - bear) <= 1:
        score = max(0, score - 2)

    if direction == "BUY":
        entry = price - atr * 0.12
        sl = entry - atr * 1.35
        t1 = entry + atr * 2.1
        t2 = entry + atr * 3.4
    else:
        entry = price + atr * 0.12
        sl = entry + atr * 1.35
        t1 = entry - atr * 2.1
        t2 = entry - atr * 3.4

    day_bias = "BULLISH" if ema9 > ema21 else "BEARISH"
    change_pct = ((price - float(df["o"].iloc[0])) / float(df["o"].iloc[0])) * 100 if float(df["o"].iloc[0]) > 0 else 0.0

    # Next 15 min outlook
    if regime.startswith("RANGING"):
        next_15 = (
            f"Expect continued consolidation. "
            f"Key short-term levels: {price - atr*0.5:.1f} support and {price + atr*0.5:.1f} resistance. "
            f"Breakout risk is elevated."
        )
    elif direction == "SELL" and score >= 5:
        next_15 = f"Short-term pressure remains to the downside. More likely to test {t1:.1f} than to rally strongly."
    elif direction == "BUY" and score >= 5:
        next_15 = f"Short-term bias higher. More likely to push toward {t1:.1f} if momentum continues."
    else:
        next_15 = f"Mixed conditions. Expect choppy action between {price - atr*0.4:.1f} and {price + atr*0.4:.1f} over the next 15 minutes."

    return Signal(
        name=name, price=price, direction=direction, conviction=min(score, 10),
        session=sess, regime=regime, cvd=cvd,
        entry=entry, sl=sl, t1=t1, t2=t2,
        day_bias=day_bias, atr=atr, rsi=rsi,
        ema9=ema9, ema21=ema21, change_pct=change_pct,
        next_15min=next_15, has_structure=True
    )

def make_recap(sig: Signal) -> str:
    time_str = datetime.now(EAT).strftime("%H:%M EAT")

    flow = "Volume delta relatively balanced."
    if sig.cvd > 0:
        flow = "Buying pressure in recent volume delta."
    elif sig.cvd < 0:
        flow = "Selling pressure in recent volume delta."

    structure_line = f"EMA9/21: {'Bullish' if sig.ema9 > sig.ema21 else 'Bearish'} | RSI: {sig.rsi:.1f} | ATR: {sig.atr:.2f}"
    if not sig.has_structure:
        structure_line = "Structure data limited"

    return f"""
<b>BRAX ADVANCED DESK — {sig.name}</b>
<code>{time_str} | {sig.session}</code>

<b>Current Price:</b> ${sig.price:.2f}  ({sig.change_pct:+.2f}%)
<b>Day Bias:</b> {sig.day_bias}
<b>Regime:</b> {sig.regime}

<b>ORDER FLOW</b>
CVD Proxy: <b>{sig.cvd:.1f}</b>
→ {flow}

<b>STRUCTURE</b>
{structure_line}

<b>KEY LEVELS</b>
Entry Zone: <b>${sig.entry:.2f}</b>
Invalidation: ${sig.sl:.2f}
Target 1: ${sig.t1:.2f} | Target 2: ${sig.t2:.2f}

<b>NEXT 15 MINUTES OUTLOOK</b>
{sig.next_15min}

<b>DESK VIEW</b>
{sig.direction} bias — Conviction <b>{sig.conviction}/10</b>

<code>Voice briefing follows • Next update in 15 minutes</code>
""".strip()

def make_voice(sig: Signal, path: str):
    t = datetime.now(EAT).strftime("%I:%M %p")
    text = (
        f"Advanced desk briefing at {t}. "
        f"{sig.name} is trading at {sig.price:.1f}. "
        f"Session is {sig.session}. Day bias is {sig.day_bias}. "
        f"Current view is {sig.direction} with conviction {sig.conviction} out of 10. "
        f"For the next fifteen minutes: {sig.next_15min}"
    )
    gTTS(text=text, lang='en', slow=False).save(path)

class Alerts:
    def __init__(self):
        self.base = f"https://api.telegram.org/bot{TOKEN}"

    async def text(self, msg: str, session):
        try:
            async with session.post(f"{self.base}/sendMessage", json={
                "chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"
            }, timeout=20):
                pass
        except Exception as e:
            logger.error(f"Text: {e}")

    async def voice(self, path: str, session):
        try:
            with open(path, 'rb') as f:
                data = aiohttp.FormData()
                data.add_field("chat_id", CHAT_ID)
                data.add_field("voice", f, filename="briefing.mp3")
                async with session.post(f"{self.base}/sendVoice", data=data, timeout=40):
                    pass
        except Exception as e:
            logger.error(f"Voice: {e}")

async def main_loop():
    feed = TwelveDataFeed()
    await feed.start()
    alerts = Alerts()

    await alerts.text(
        "<b>BRAX ADVANCED DESK RESTARTED</b>\n"
        "More resilient candle handling activated.\n"
        "First update in \~20 seconds...",
        feed.session
    )

    assets = [("XAU/USD", "GOLD"), ("BTC/USD", "BITCOIN")]

    await asyncio.sleep(20)

    for symbol, name in assets:
        try:
            sig = await analyze(feed, symbol, name)
            await alerts.text(make_recap(sig), feed.session)
            path = f"/tmp/{name.lower()}.mp3"
            make_voice(sig, path)
            await alerts.voice(path, feed.session)
            logger.info(f"Update sent → {name} | ${sig.price:.2f}")
        except Exception as e:
            logger.error(f"First {name}: {e}")

    while True:
        try:
            await asyncio.sleep(SIGNAL_INTERVAL)
            for symbol, name in assets:
                try:
                    sig = await analyze(feed, symbol, name)
                    await alerts.text(make_recap(sig), feed.session)
                    path = f"/tmp/{name.lower()}.mp3"
                    make_voice(sig, path)
                    await alerts.voice(path, feed.session)
                except Exception as e:
                    logger.error(f"Cycle {name}: {e}")
        except Exception as e:
            logger.error(f"Loop: {e}")
            await asyncio.sleep(20)

def run_flask():
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    asyncio.run(main_loop())
