import asyncio
import os
import logging
from datetime import datetime
from dataclasses import dataclass
from typing import Optional
import numpy as np
import pandas as pd
import aiohttp
import pytz
from flask import Flask
from threading import Thread

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("BraxTwelve")

TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TD_KEY = os.getenv("TWELVEDATA_API_KEY")

if not TOKEN or not CHAT_ID or not TD_KEY:
    raise ValueError("TELEGRAM_TOKEN, TELEGRAM_CHAT_ID and TWELVEDATA_API_KEY must be set")

EAT = pytz.timezone("Africa/Nairobi")
SIGNAL_INTERVAL = 900  # 15 minutes

app = Flask(__name__)

@app.route("/")
def health():
    return "BRAX TWELVEDATA DESK - ONLINE", 200

# ------------------------------------------------------------
# Twelve Data Feed
# ------------------------------------------------------------
class TwelveDataFeed:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.base = "https://api.twelvedata.com"

    async def start(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))

    async def get_quote(self, symbol: str) -> float:
        url = f"{self.base}/quote?symbol={symbol}&apikey={TD_KEY}"
        try:
            async with self.session.get(url) as r:
                data = await r.json()
                if data.get("status") == "error":
                    logger.warning(f"TwelveData quote error {symbol}: {data.get('message')}")
                    return 0.0
                price = data.get("close") or data.get("price")
                return float(price) if price else 0.0
        except Exception as e:
            logger.error(f"Quote {symbol}: {e}")
            return 0.0

    async def get_candles(self, symbol: str, interval: str = "5min", outputsize: int = 80) -> pd.DataFrame:
        url = (
            f"{self.base}/time_series"
            f"?symbol={symbol}&interval={interval}&outputsize={outputsize}&apikey={TD_KEY}"
        )
        try:
            async with self.session.get(url) as r:
                data = await r.json()
                if data.get("status") == "error":
                    logger.warning(f"TwelveData candles error {symbol}: {data.get('message')}")
                    return pd.DataFrame()
                if "values" not in data:
                    return pd.DataFrame()

                df = pd.DataFrame(data["values"])
                df = df.rename(columns={
                    "open": "o", "high": "h", "low": "l", "close": "c", "volume": "v"
                })
                for col in ["o", "h", "l", "c"]:
                    df[col] = df[col].astype(float)
                if "v" in df.columns:
                    df["v"] = df["v"].astype(float)
                else:
                    df["v"] = 0.0
                # Twelve Data returns newest first → reverse to oldest first
                df = df.iloc[::-1].reset_index(drop=True)
                return df
        except Exception as e:
            logger.error(f"Candles {symbol}: {e}")
            return pd.DataFrame()

# ------------------------------------------------------------
# Analysis
# ------------------------------------------------------------
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
    df: pd.DataFrame

def calc_rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss
    val = 100 - (100 / (1 + rs.iloc[-1]))
    return float(val) if not np.isnan(val) else 50.0

async def analyze(feed: TwelveDataFeed, symbol: str, name: str) -> Signal:
    price = await feed.get_quote(symbol)
    df = await feed.get_candles(symbol, "5min", 90)

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

    if price <= 0 or df.empty or len(df) < 20:
        return Signal(
            name=name, price=price, direction="NEUTRAL", conviction=1,
            session=sess, regime="NO DATA", cvd=0.0,
            entry=price, sl=price, t1=price, t2=price,
            day_bias="NEUTRAL", atr=0.0, rsi=50.0,
            ema9=price, ema21=price, change_pct=0.0, df=df
        )

    # ATR
    tr = np.maximum(
        df["h"] - df["l"],
        np.maximum(abs(df["h"] - df["c"].shift()), abs(df["l"] - df["c"].shift()))
    )
    atr = float(tr.rolling(14).mean().iloc[-1])

    rsi = calc_rsi(df["c"])
    ema9 = float(df["c"].ewm(span=9).mean().iloc[-1])
    ema21 = float(df["c"].ewm(span=21).mean().iloc[-1])

    # Volume delta proxy
    df["buy_vol"] = np.where(df["c"] > df["o"], df["v"], 0)
    df["sell_vol"] = np.where(df["c"] < df["o"], df["v"], 0)
    cvd = float(df["buy_vol"].tail(12).sum() - df["sell_vol"].tail(12).sum())

    vol_mean = float(df["v"].tail(20).mean()) or 1.0
    regime = "TRENDING" if abs(cvd) > vol_mean * 0.55 else "RANGING / COMPRESSING"

    # Scoring
    score = 0
    bull = bear = 0

    if cvd > 0:
        bull += 1
        score += 2 if abs(cvd) > vol_mean * 0.35 else 1
    else:
        bear += 1
        score += 2 if abs(cvd) > vol_mean * 0.35 else 1

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
        entry = price - atr * 0.15
        sl = entry - atr * 1.4
        t1 = entry + atr * 2.2
        t2 = entry + atr * 3.5
    else:
        entry = price + atr * 0.15
        sl = entry + atr * 1.4
        t1 = entry - atr * 2.2
        t2 = entry - atr * 3.5

    day_bias = "BULLISH" if ema9 > ema21 else "BEARISH"
    change_pct = ((price - float(df["o"].iloc[0])) / float(df["o"].iloc[0])) * 100

    return Signal(
        name=name, price=price, direction=direction, conviction=min(score, 10),
        session=sess, regime=regime, cvd=cvd,
        entry=entry, sl=sl, t1=t1, t2=t2,
        day_bias=day_bias, atr=atr, rsi=rsi,
        ema9=ema9, ema21=ema21, change_pct=change_pct, df=df
    )

def make_recap(sig: Signal) -> str:
    time_str = datetime.now(EAT).strftime("%H:%M EAT")

    if sig.cvd > 0:
        flow = "Buying pressure visible in recent volume delta."
    elif sig.cvd < 0:
        flow = "Selling pressure visible in recent volume delta."
    else:
        flow = "Volume delta relatively balanced."

    if sig.conviction >= 7:
        expect = f"High conviction {sig.direction}. Looking for continuation."
    elif sig.conviction >= 5:
        expect = f"Moderate {sig.direction} bias. Prefer confirmation."
    else:
        expect = "Low conviction. Prefer patience."

    if "RANGING" in sig.regime:
        expect += " Compression present — breakout risk elevated."

    return f"""
<b>BRAX INSTITUTIONAL DESK — {sig.name}</b>
<code>{time_str} | {sig.session} | Source: Twelve Data</code>

<b>Price:</b> ${sig.price:.2f}  ({sig.change_pct:+.2f}%)
<b>Day Bias:</b> {sig.day_bias}
<b>Regime:</b> {sig.regime}

<b>ORDER FLOW</b>
CVD Proxy: <b>{sig.cvd:.1f}</b>
→ {flow}

<b>STRUCTURE</b>
EMA9/21: {"Bullish" if sig.ema9 > sig.ema21 else "Bearish"} | RSI: {sig.rsi:.1f} | ATR: {sig.atr:.2f}

<b>LEVELS</b>
Entry: <b>${sig.entry:.2f}</b>
SL: ${sig.sl:.2f}
T1: ${sig.t1:.2f} | T2: ${sig.t2:.2f}

<b>DESK VIEW</b>
{sig.direction} — Conviction <b>{sig.conviction}/10</b>
{expect}

<code>Next update in 15 minutes</code>
""".strip()

class Alerts:
    def __init__(self):
        self.base = f"https://api.telegram.org/bot{TOKEN}"

    async def text(self, msg: str, session):
        try:
            async with session.post(f"{self.base}/sendMessage", json={
                "chat_id": CHAT_ID,
                "text": msg,
                "parse_mode": "HTML"
            }, timeout=20):
                pass
        except Exception as e:
            logger.error(f"Telegram error: {e}")

async def main_loop():
    feed = TwelveDataFeed()
    await feed.start()
    alerts = Alerts()

    await alerts.text(
        "<b>BRAX TWELVEDATA DESK ONLINE</b>\n"
        "Using real XAU/USD + BTC/USD from Twelve Data.\n"
        "First recaps in \~25 seconds...",
        feed.session
    )

    assets = [
        ("XAU/USD", "GOLD"),
        ("BTC/USD", "BITCOIN"),
    ]

    await asyncio.sleep(25)

    await alerts.text("<b>Sending first Twelve Data recaps...</b>", feed.session)

    for symbol, name in assets:
        try:
            sig = await analyze(feed, symbol, name)
            await alerts.text(make_recap(sig), feed.session)
            logger.info(f"First → {name} | ${sig.price:.2f}")
        except Exception as e:
            logger.error(f"First {name}: {e}")
            await alerts.text(f"<b>{name}</b>\nError: {str(e)[:160]}", feed.session)

    while True:
        try:
            await asyncio.sleep(SIGNAL_INTERVAL)
            for symbol, name in assets:
                try:
                    sig = await analyze(feed, symbol, name)
                    await alerts.text(make_recap(sig), feed.session)
                    logger.info(f"Recap → {name} | {sig.direction} | {sig.conviction}/10")
                except Exception as e:
                    logger.error(f"Cycle {name}: {e}")
        except Exception as e:
            logger.error(f"Main loop: {e}")
            await asyncio.sleep(20)

def run_flask():
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    asyncio.run(main_loop())
