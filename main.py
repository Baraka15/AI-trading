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
logger = logging.getLogger("BraxSmart")

TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TD_KEY = os.getenv("TWELVEDATA_API_KEY")

if not TOKEN or not CHAT_ID or not TD_KEY:
    raise ValueError("Missing TELEGRAM_TOKEN, TELEGRAM_CHAT_ID or TWELVEDATA_API_KEY")

EAT = pytz.timezone("Africa/Nairobi")
SIGNAL_INTERVAL = 900  # 15 minutes

app = Flask(__name__)

@app.route("/")
def health():
    return "BRAX SMART DESK - ONLINE", 200

class TwelveDataFeed:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.last_candle_attempt = 0

    async def start(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))

    async def get_quote(self, symbol: str) -> float:
        url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_KEY}"
        try:
            async with self.session.get(url) as r:
                data = await r.json()
                if data.get("status") == "error":
                    logger.warning(f"Quote {symbol}: {data.get('message')}")
                    return 0.0
                price = data.get("close") or data.get("price")
                return float(price) if price else 0.0
        except Exception as e:
            logger.error(f"Quote error {symbol}: {e}")
            return 0.0

    async def get_candles(self, symbol: str) -> pd.DataFrame:
        # Only try candles every other cycle to save credits
        now = datetime.now().timestamp()
        if now - self.last_candle_attempt < 800:  # roughly every 13+ minutes
            return pd.DataFrame()

        self.last_candle_attempt = now

        url = (
            f"https://api.twelvedata.com/time_series"
            f"?symbol={symbol}&interval=15min&outputsize=40&apikey={TD_KEY}"
        )
        try:
            async with self.session.get(url) as r:
                data = await r.json()
                if data.get("status") == "error":
                    logger.warning(f"Candles {symbol}: {data.get('message')}")
                    return pd.DataFrame()
                if "values" not in data or not data["values"]:
                    return pd.DataFrame()

                df = pd.DataFrame(data["values"])
                df = df.rename(columns={"open": "o", "high": "h", "low": "l", "close": "c", "volume": "v"})
                for col in ["o", "h", "l", "c"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                df["v"] = pd.to_numeric(df.get("v", 0), errors="coerce").fillna(0)
                df = df.dropna(subset=["o", "h", "l", "c"])
                df = df.iloc[::-1].reset_index(drop=True)
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
    entry: float
    sl: float
    t1: float
    day_bias: str
    rsi: float
    atr: float
    next_15min: str
    full_data: bool

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

    if price <= 0:
        return Signal(
            name=name, price=0.0, direction="NEUTRAL", conviction=1,
            session=sess, regime="NO PRICE", entry=0, sl=0, t1=0,
            day_bias="NEUTRAL", rsi=50, atr=0,
            next_15min="No live price available.", full_data=False
        )

    # ----- Limited data path (most common on free tier) -----
    if df.empty or len(df) < 12:
        return Signal(
            name=name,
            price=price,
            direction="NEUTRAL",
            conviction=2,
            session=sess,
            regime="PRICE ONLY",
            entry=price,
            sl=price,
            t1=price,
            day_bias="NEUTRAL",
            rsi=50.0,
            atr=0.0,
            next_15min=(
                f"{name} is currently at ${price:.1f}. "
                f"Full structure data is temporarily limited. "
                f"Expect range-bound or low-momentum action over the next 15 minutes. "
                f"Wait for clearer directional confirmation."
            ),
            full_data=False
        )

    # ----- Full data path -----
    tr = np.maximum(
        df["h"] - df["l"],
        np.maximum(abs(df["h"] - df["c"].shift()), abs(df["l"] - df["c"].shift()))
    )
    atr = float(tr.rolling(14).mean().iloc[-1])
    rsi = calc_rsi(df["c"])
    ema9 = float(df["c"].ewm(span=9).mean().iloc[-1])
    ema21 = float(df["c"].ewm(span=21).mean().iloc[-1])

    day_bias = "BULLISH" if ema9 > ema21 else "BEARISH"
    direction = "BUY" if ema9 > ema21 and rsi < 60 else "SELL" if ema9 < ema21 and rsi > 40 else "NEUTRAL"

    score = 3
    if abs(ema9 - ema21) / price > 0.001:
        score += 1
    if rsi < 35 or rsi > 65:
        score += 1
    if sess in ("LONDON", "NY KILLZONE"):
        score += 1

    if direction == "BUY":
        entry = price - atr * 0.15
        sl = entry - atr * 1.3
        t1 = entry + atr * 2.0
    elif direction == "SELL":
        entry = price + atr * 0.15
        sl = entry + atr * 1.3
        t1 = entry - atr * 2.0
    else:
        entry = sl = t1 = price

    if direction == "NEUTRAL":
        next_15 = (
            f"Mixed structure. Expect choppy movement around {price:.1f} "
            f"in the next 15 minutes. Key zone: {price-atr*0.5:.1f} – {price+atr*0.5:.1f}."
        )
    elif direction == "BUY":
        next_15 = f"Short-term bias higher. More likely to work toward {t1:.1f} if momentum holds."
    else:
        next_15 = f"Short-term bias lower. More likely to test {t1:.1f} than to rally strongly."

    return Signal(
        name=name, price=price, direction=direction, conviction=min(score, 10),
        session=sess, regime="STRUCTURE AVAILABLE", entry=entry, sl=sl, t1=t1,
        day_bias=day_bias, rsi=rsi, atr=atr,
        next_15min=next_15, full_data=True
    )

def make_recap(sig: Signal) -> str:
    time_str = datetime.now(EAT).strftime("%H:%M EAT")

    if not sig.full_data:
        return f"""
<b>BRAX SMART DESK — {sig.name}</b>
<code>{time_str} | {sig.session}</code>

<b>Live Price:</b> ${sig.price:.2f}

<b>Status:</b> Full candle structure temporarily limited (free-tier constraint)

<b>NEXT 15 MINUTES</b>
{sig.next_15min}

<b>DESK VIEW</b>
Neutral / Observing — Conviction {sig.conviction}/10

<code>Voice briefing follows • Next update in 15 minutes</code>
""".strip()

    return f"""
<b>BRAX SMART DESK — {sig.name}</b>
<code>{time_str} | {sig.session}</code>

<b>Live Price:</b> ${sig.price:.2f}
<b>Day Bias:</b> {sig.day_bias}
<b>Regime:</b> {sig.regime}

<b>STRUCTURE</b>
RSI: {sig.rsi:.1f} | ATR: {sig.atr:.2f}

<b>KEY LEVELS</b>
Entry: <b>${sig.entry:.2f}</b>
Stop: ${sig.sl:.2f}
Target: ${sig.t1:.2f}

<b>NEXT 15 MINUTES OUTLOOK</b>
{sig.next_15min}

<b>DESK VIEW</b>
{sig.direction} — Conviction <b>{sig.conviction}/10</b>

<code>Voice briefing follows • Next update in 15 minutes</code>
""".strip()

def make_voice(sig: Signal, path: str):
    t = datetime.now(EAT).strftime("%I:%M %p")
    if not sig.full_data:
        text = (
            f"Smart desk update at {t}. "
            f"{sig.name} is trading at {sig.price:.1f}. "
            f"Full structure data is currently limited. "
            f"{sig.next_15min}"
        )
    else:
        text = (
            f"Smart desk briefing at {t}. "
            f"{sig.name} is at {sig.price:.1f}. "
            f"Day bias is {sig.day_bias}. "
            f"Current view is {sig.direction} with conviction {sig.conviction}. "
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
            logger.error(f"Text error: {e}")

    async def voice(self, path: str, session):
        try:
            with open(path, 'rb') as f:
                data = aiohttp.FormData()
                data.add_field("chat_id", CHAT_ID)
                data.add_field("voice", f, filename="briefing.mp3")
                async with session.post(f"{self.base}/sendVoice", data=data, timeout=40):
                    pass
        except Exception as e:
            logger.error(f"Voice error: {e}")

async def main_loop():
    feed = TwelveDataFeed()
    await feed.start()
    alerts = Alerts()

    await alerts.text(
        "<b>BRAX SMART DESK ONLINE</b>\n"
        "Optimized for free-tier limits.\n"
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
                    logger.info(f"Sent → {name} | ${sig.price:.2f} | full={sig.full_data}")
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
