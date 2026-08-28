import asyncio
import os
import json
import logging
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, Tuple
import numpy as np
import pandas as pd
import aiohttp
from urllib.parse import quote
import pytz
from flask import Flask
from threading import Thread

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("BraxAllTick")

TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
ALLTICK_TOKEN = os.getenv("ALLTICK_TOKEN")

if not TOKEN or not CHAT_ID or not ALLTICK_TOKEN:
    raise ValueError("TELEGRAM_TOKEN, TELEGRAM_CHAT_ID and ALLTICK_TOKEN must be set")

EAT = pytz.timezone("Africa/Nairobi")
SIGNAL_INTERVAL = 900  # 15 minutes

app = Flask(__name__)

@app.route("/")
def health():
    return "BRAX ALLTICK DESK - ONLINE", 200

# ------------------------------------------------------------
# AllTick Data Engine
# ------------------------------------------------------------
class AllTickFeed:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.base = "https://quote.alltick.co/quote-b-api"

    async def start(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))

    async def get_price(self, code: str) -> float:
        """Get latest trade price"""
        query = {
            "trace": f"price-{datetime.now().timestamp()}",
            "data": {
                "symbol_list": [{"code": code}]
            }
        }
        q = quote(json.dumps(query))
        url = f"{self.base}/trade-tick?token={ALLTICK_TOKEN}&query={q}"

        try:
            async with self.session.get(url) as r:
                data = await r.json()
                logger.info(f"AllTick price response ({code}): {str(data)[:300]}")

                # Different possible response structures
                if "data" in data:
                    d = data["data"]
                    if isinstance(d, list) and len(d) > 0:
                        item = d[0]
                        if "price" in item:
                            return float(item["price"])
                        if "tick_list" in item and item["tick_list"]:
                            return float(item["tick_list"][0].get("price", 0))
                    if isinstance(d, dict):
                        if "price" in d:
                            return float(d["price"])
                        if "tick_list" in d and d["tick_list"]:
                            return float(d["tick_list"][0].get("price", 0))
        except Exception as e:
            logger.error(f"AllTick price error {code}: {e}")
        return 0.0

    async def get_candles(self, code: str, kline_type: int = 2, limit: int = 80) -> pd.DataFrame:
        """
        kline_type:
        1 = 1min, 2 = 5min, 3 = 15min, 5 = 1h, 8 = 1day
        """
        query = {
            "trace": f"kline-{datetime.now().timestamp()}",
            "data": {
                "code": code,
                "kline_type": kline_type,
                "kline_timestamp_end": 0,
                "query_kline_num": limit,
                "adjust_type": 0
            }
        }
        q = quote(json.dumps(query))
        url = f"{self.base}/kline?token={ALLTICK_TOKEN}&query={q}"

        try:
            async with self.session.get(url) as r:
                data = await r.json()
                logger.info(f"AllTick kline response ({code}): {str(data)[:400]}")

                if "data" in data and "kline_list" in data["data"]:
                    klines = data["data"]["kline_list"]
                    records = []
                    for k in klines:
                        records.append({
                            "o": float(k.get("open", 0)),
                            "h": float(k.get("high", 0)),
                            "l": float(k.get("low", 0)),
                            "c": float(k.get("close", 0)),
                            "v": float(k.get("volume", 0))
                        })
                    df = pd.DataFrame(records)
                    if not df.empty:
                        # AllTick usually returns newest first → reverse
                        df = df.iloc[::-1].reset_index(drop=True)
                    return df
        except Exception as e:
            logger.error(f"AllTick kline error {code}: {e}")
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

async def analyze(feed: AllTickFeed, code: str, name: str) -> Signal:
    price = await feed.get_price(code)
    df = await feed.get_candles(code, kline_type=2, limit=80)  # 5min

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

    if price <= 0 or df.empty or len(df) < 15:
        return Signal(
            name=name, price=price, direction="NEUTRAL", conviction=1,
            session=sess, regime="NO DATA FROM ALLTICK", cvd=0.0,
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
    regime = "TRENDING" if abs(cvd) > vol_mean * 0.5 else "RANGING / COMPRESSING"

    # Scoring
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
<code>{time_str} | {sig.session} | Source: AllTick</code>

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
    feed = AllTickFeed()
    await feed.start()
    alerts = Alerts()

    await alerts.text(
        "<b>BRAX ALLTICK DESK ONLINE</b>\n"
        "Using AllTick as primary data source.\n"
        "First recaps in \~25 seconds...",
        feed.session
    )

    assets = [
        ("XAUUSD", "GOLD"),
        ("BTCUSDT", "BITCOIN"),
    ]

    await asyncio.sleep(25)

    await alerts.text("<b>Sending first AllTick recaps now...</b>", feed.session)

    for code, name in assets:
        try:
            sig = await analyze(feed, code, name)
            await alerts.text(make_recap(sig), feed.session)
            logger.info(f"First recap → {name} | ${sig.price:.2f}")
        except Exception as e:
            logger.error(f"First cycle {name}: {e}")
            await alerts.text(f"<b>{name}</b>\nError: {str(e)[:180]}", feed.session)

    # Normal 15-minute loop
    while True:
        try:
            await asyncio.sleep(SIGNAL_INTERVAL)
            for code, name in assets:
                try:
                    sig = await analyze(feed, code, name)
                    await alerts.text(make_recap(sig), feed.session)
                    logger.info(f"Recap → {name} | {sig.direction} | Conv {sig.conviction}")
                except Exception as e:
                    logger.error(f"Cycle {name}: {e}")
                    await alerts.text(f"<b>{name}</b>\nTemporary error: {str(e)[:120]}", feed.session)
        except Exception as e:
            logger.error(f"Main loop error: {e}")
            await asyncio.sleep(20)

def run_flask():
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    asyncio.run(main_loop())
