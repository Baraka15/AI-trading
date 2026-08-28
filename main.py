import asyncio
import os
import json
import logging
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
import numpy as np
import pandas as pd
import aiohttp
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import pytz
from flask import Flask
from threading import Thread
from urllib.parse import quote

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("BraxMulti")

TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TD_KEY = os.getenv("TWELVEDATA_API_KEY", "")
ALLTICK_TOKEN = os.getenv("ALLTICK_TOKEN", "")

if not TOKEN or not CHAT_ID:
    raise ValueError("TELEGRAM_TOKEN and TELEGRAM_CHAT_ID must be set")

EAT = pytz.timezone("Africa/Nairobi")
SIGNAL_INTERVAL = 900

app = Flask(__name__)

@app.route("/")
def health():
    return "BRAX MULTI-SOURCE INSTITUTIONAL DESK - ONLINE", 200

# ------------------------------------------------------------
# Multi-Source Data Engine
# ------------------------------------------------------------
class MultiSourceFeed:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.last_source = "None"

    async def start(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12))

    async def get_price_and_candles(self, asset: str) -> Tuple[float, pd.DataFrame, str]:
        """
        asset: "GOLD" or "BTC"
        Returns: (price, df_5m, source_name)
        """
        # 1. Try Twelve Data
        if TD_KEY:
            price, df = await self._from_twelvedata(asset)
            if price > 0 and not df.empty:
                self.last_source = "TwelveData"
                return price, df, "TwelveData"

        # 2. Try AllTick
        if ALLTICK_TOKEN:
            price, df = await self._from_alltick(asset)
            if price > 0 and not df.empty:
                self.last_source = "AllTick"
                return price, df, "AllTick"

        # 3. Fallback to Binance
        price, df = await self._from_binance(asset)
        self.last_source = "Binance"
        return price, df, "Binance"

    async def _from_twelvedata(self, asset: str) -> Tuple[float, pd.DataFrame]:
        symbol = "XAU/USD" if asset == "GOLD" else "BTC/USD"
        try:
            # Quote
            url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_KEY}"
            async with self.session.get(url) as r:
                data = await r.json()
                if data.get("status") == "error":
                    return 0.0, pd.DataFrame()
                price = float(data.get("close") or data.get("price") or 0)

            # Candles
            url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=5min&outputsize=80&apikey={TD_KEY}"
            async with self.session.get(url) as r:
                data = await r.json()
                if "values" not in data:
                    return price, pd.DataFrame()
                df = pd.DataFrame(data["values"])
                df = df.rename(columns={"open":"o","high":"h","low":"l","close":"c","volume":"v"})
                for col in ["o","h","l","c"]:
                    df[col] = df[col].astype(float)
                df["v"] = df.get("v", 0).astype(float)
                df = df.iloc[::-1].reset_index(drop=True)
                return price, df
        except Exception as e:
            logger.warning(f"TwelveData {asset}: {e}")
            return 0.0, pd.DataFrame()

    async def _from_alltick(self, asset: str) -> Tuple[float, pd.DataFrame]:
        code = "XAUUSD" if asset == "GOLD" else "BTCUSDT"
        try:
            # Latest price
            query = {
                "trace": f"brax-{datetime.now().timestamp()}",
                "data": {"symbol_list": [{"code": code}]}
            }
            q = quote(json.dumps(query))
            url = f"https://quote.alltick.co/quote-b-api/trade-tick?token={ALLTICK_TOKEN}&query={q}"
            async with self.session.get(url) as r:
                data = await r.json()
                price = 0.0
                if "data" in data and data["data"]:
                    # structure can vary
                    items = data["data"] if isinstance(data["data"], list) else [data["data"]]
                    for item in items:
                        if "price" in item:
                            price = float(item["price"])
                            break
                        if "tick_list" in item and item["tick_list"]:
                            price = float(item["tick_list"][0].get("price", 0))
                            break

            # Klines 5min
            query = {
                "trace": f"brax-kline-{datetime.now().timestamp()}",
                "data": {
                    "code": code,
                    "kline_type": 2,          # 2 = 5min
                    "kline_timestamp_end": 0,
                    "query_kline_num": 80,
                    "adjust_type": 0
                }
            }
            q = quote(json.dumps(query))
            url = f"https://quote.alltick.co/quote-b-api/kline?token={ALLTICK_TOKEN}&query={q}"
            async with self.session.get(url) as r:
                data = await r.json()
                df = pd.DataFrame()
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
                        df = df.iloc[::-1].reset_index(drop=True)
                return price, df
        except Exception as e:
            logger.warning(f"AllTick {asset}: {e}")
            return 0.0, pd.DataFrame()

    async def _from_binance(self, asset: str) -> Tuple[float, pd.DataFrame]:
        symbol = "PAXGUSDT" if asset == "GOLD" else "BTCUSDT"
        try:
            # Price
            async with self.session.get(f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}") as r:
                data = await r.json()
                price = float(data["price"])
                if asset == "GOLD":
                    price -= 2.8

            # Candles
            async with self.session.get(
                f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=5m&limit=80"
            ) as r:
                data = await r.json()
                df = pd.DataFrame(data, columns=[
                    't','o','h','l','c','v','ct','qav','n','tbb','tbq','x'
                ])
                for col in ['o','h','l','c','v']:
                    df[col] = df[col].astype(float)
                if asset == "GOLD":
                    df[['o','h','l','c']] -= 2.8
                return price, df
        except Exception as e:
            logger.error(f"Binance {asset}: {e}")
            return 0.0, pd.DataFrame()

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
    source: str
    df: pd.DataFrame

def calc_rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss
    val = 100 - (100 / (1 + rs.iloc[-1]))
    return float(val) if not np.isnan(val) else 50.0

async def analyze(feed: MultiSourceFeed, asset: str, name: str) -> Signal:
    price, df, source = await feed.get_price_and_candles(asset)

    now = datetime.now(EAT)
    h = now.hour + now.minute / 60
    if 13 <= h < 17: sess = "NY KILLZONE"
    elif 8 <= h < 13: sess = "LONDON"
    elif 2 <= h < 8: sess = "ASIAN"
    else: sess = "OFF-HOURS"

    if price <= 0 or df.empty or len(df) < 20:
        return Signal(
            name=name, price=price, direction="NEUTRAL", conviction=1,
            session=sess, regime="NO DATA", cvd=0.0,
            entry=price, sl=price, t1=price, t2=price,
            day_bias="NEUTRAL", atr=0.0, rsi=50.0,
            ema9=price, ema21=price, change_pct=0.0,
            source=source, df=df
        )

    tr = np.maximum(df["h"]-df["l"], np.maximum(abs(df["h"]-df["c"].shift()), abs(df["l"]-df["c"].shift())))
    atr = float(tr.rolling(14).mean().iloc[-1])

    rsi = calc_rsi(df["c"])
    ema9 = float(df["c"].ewm(span=9).mean().iloc[-1])
    ema21 = float(df["c"].ewm(span=21).mean().iloc[-1])

    df["buy_vol"] = np.where(df["c"] > df["o"], df["v"], 0)
    df["sell_vol"] = np.where(df["c"] < df["o"], df["v"], 0)
    cvd = float(df["buy_vol"].tail(12).sum() - df["sell_vol"].tail(12).sum())

    vol_mean = float(df["v"].tail(20).mean()) or 1.0
    regime = "TRENDING" if abs(cvd) > vol_mean * 0.55 else "RANGING / COMPRESSING"

    score = 0
    bull = bear = 0

    if cvd > 0: bull += 1; score += 2 if abs(cvd) > vol_mean*0.35 else 1
    else: bear += 1; score += 2 if abs(cvd) > vol_mean*0.35 else 1

    if ema9 > ema21: bull += 1; score += 1
    else: bear += 1; score += 1

    if rsi < 33: bull += 1; score += 1
    elif rsi > 67: bear += 1; score += 1

    if sess in ("LONDON", "NY KILLZONE"): score += 1

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
        ema9=ema9, ema21=ema21, change_pct=change_pct,
        source=source, df=df
    )

def make_recap(sig: Signal) -> str:
    time_str = datetime.now(EAT).strftime("%H:%M EAT")

    flow = "Volume delta balanced."
    if sig.cvd > 0:
        flow = "Buying pressure visible in recent volume delta."
    elif sig.cvd < 0:
        flow = "Selling pressure visible in recent volume delta."

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
<code>{time_str} | {sig.session} | Source: {sig.source}</code>

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
                "chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"
            }, timeout=20):
                pass
        except Exception as e:
            logger.error(f"Telegram: {e}")

async def main_loop():
    feed = MultiSourceFeed()
    await feed.start()
    alerts = Alerts()

    await alerts.text(
        "<b>BRAX MULTI-SOURCE DESK ONLINE</b>\n"
        "Sources: TwelveData → AllTick → Binance\n"
        "First recaps in \~25 seconds...",
        feed.session
    )

    assets = [("GOLD", "GOLD"), ("BTC", "BITCOIN")]

    await asyncio.sleep(25)

    await alerts.text("<b>Sending first multi-source recaps...</b>", feed.session)

    for asset, name in assets:
        try:
            sig = await analyze(feed, asset, name)
            await alerts.text(make_recap(sig), feed.session)
            logger.info(f"First → {name} | ${sig.price:.2f} | {sig.source}")
        except Exception as e:
            await alerts.text(f"<b>{name}</b>\nError: {str(e)[:150]}", feed.session)

    while True:
        try:
            await asyncio.sleep(SIGNAL_INTERVAL)
            for asset, name in assets:
                try:
                    sig = await analyze(feed, asset, name)
                    await alerts.text(make_recap(sig), feed.session)
                    logger.info(f"Recap → {name} | {sig.direction} | {sig.source}")
                except Exception as e:
                    logger.error(f"{name}: {e}")
        except Exception as e:
            logger.error(f"Loop: {e}")
            await asyncio.sleep(20)

def run_flask():
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    asyncio.run(main_loop())
