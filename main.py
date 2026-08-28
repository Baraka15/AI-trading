import asyncio
import os
import logging
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Optional
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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("BraxDesk")

TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TD_KEY = os.getenv("TWELVEDATA_API_KEY")
ALLTICK_TOKEN = os.getenv("ALLTICK_TOKEN")

if not TOKEN or not CHAT_ID or not TD_KEY:
    raise ValueError("TELEGRAM_TOKEN, TELEGRAM_CHAT_ID and TWELVEDATA_API_KEY must be set")

EAT = pytz.timezone("Africa/Nairobi")
SIGNAL_INTERVAL = 900  # 15 minutes

app = Flask(__name__)

@app.route("/")
def health():
    return "BRAX INSTITUTIONAL DESK (TwelveData) - ONLINE", 200

# ------------------------------------------------------------
# Data Layer - Twelve Data Primary
# ------------------------------------------------------------
class MarketData:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.cache: Dict[str, dict] = {
            "XAU/USD": {"price": 0.0, "cvd": 0.0, "imbalance": 0.5},
            "BTC/USD": {"price": 0.0, "cvd": 0.0, "imbalance": 0.5},
        }

    async def start(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))

    async def get_quote(self, symbol: str) -> float:
        url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_KEY}"
        try:
            async with self.session.get(url) as r:
                if r.status == 200:
                    data = await r.json()
                    if "close" in data:
                        price = float(data["close"])
                        self.cache[symbol]["price"] = price
                        return price
                    if "price" in data:
                        price = float(data["price"])
                        self.cache[symbol]["price"] = price
                        return price
        except Exception as e:
            logger.warning(f"Quote {symbol}: {e}")
        return self.cache[symbol]["price"]

    async def get_candles(self, symbol: str, interval: str = "5min", outputsize: int = 80) -> pd.DataFrame:
        url = (
            f"https://api.twelvedata.com/time_series?"
            f"symbol={symbol}&interval={interval}&outputsize={outputsize}&apikey={TD_KEY}"
        )
        try:
            async with self.session.get(url) as r:
                if r.status == 200:
                    data = await r.json()
                    if "values" in data:
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
                        df = df.iloc[::-1].reset_index(drop=True)  # oldest → newest
                        return df
        except Exception as e:
            logger.error(f"Candles {symbol}: {e}")
        return pd.DataFrame()

# ------------------------------------------------------------
# Analysis
# ------------------------------------------------------------
@dataclass
class Signal:
    symbol: str
    name: str
    price: float
    direction: str
    conviction: int
    session: str
    regime: str
    cvd_proxy: float
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

async def analyze(md: MarketData, symbol: str, name: str) -> Signal:
    price = await md.get_quote(symbol)
    df5 = await md.get_candles(symbol, "5min", 90)
    df1h = await md.get_candles(symbol, "1h", 40)

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

    if df5.empty or len(df5) < 20 or price <= 0:
        return Signal(
            symbol=symbol, name=name, price=price or 0.0,
            direction="NEUTRAL", conviction=2, session=sess,
            regime="DATA LOADING", cvd_proxy=0.0,
            entry=price, sl=price, t1=price, t2=price,
            day_bias="NEUTRAL", atr=0.0, rsi=50.0,
            ema9=price, ema21=price, change_pct=0.0, df=df5
        )

    # ATR
    tr = np.maximum(
        df5["h"] - df5["l"],
        np.maximum(abs(df5["h"] - df5["c"].shift()), abs(df5["l"] - df5["c"].shift()))
    )
    atr = float(tr.rolling(14).mean().iloc[-1])

    rsi = calc_rsi(df5["c"])
    ema9 = float(df5["c"].ewm(span=9).mean().iloc[-1])
    ema21 = float(df5["c"].ewm(span=21).mean().iloc[-1])
    ema9_1h = float(df1h["c"].ewm(span=9).mean().iloc[-1]) if not df1h.empty else price
    ema21_1h = float(df1h["c"].ewm(span=21).mean().iloc[-1]) if not df1h.empty else price

    # Simple volume delta proxy (close vs open)
    df5["buy_vol"] = np.where(df5["c"] > df5["o"], df5["v"], 0)
    df5["sell_vol"] = np.where(df5["c"] < df5["o"], df5["v"], 0)
    cvd_proxy = float(df5["buy_vol"].tail(12).sum() - df5["sell_vol"].tail(12).sum())

    vol_mean = float(df5["v"].tail(20).mean()) or 1.0
    regime = "TRENDING" if abs(cvd_proxy) > vol_mean * 0.6 else "RANGING / COMPRESSING"

    # Scoring
    score = 0
    bull = bear = 0

    if cvd_proxy > 0:
        bull += 1
        score += 2 if abs(cvd_proxy) > vol_mean * 0.4 else 1
    else:
        bear += 1
        score += 2 if abs(cvd_proxy) > vol_mean * 0.4 else 1

    if ema9 > ema21:
        bull += 1
        score += 1
    else:
        bear += 1
        score += 1

    if ema9_1h > ema21_1h:
        bull += 1
        score += 2
    else:
        bear += 1
        score += 2

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

    day_bias = "BULLISH" if ema9_1h > ema21_1h else "BEARISH"
    change_pct = ((price - float(df5["o"].iloc[0])) / float(df5["o"].iloc[0])) * 100

    return Signal(
        symbol=symbol, name=name, price=price, direction=direction,
        conviction=min(score, 10), session=sess, regime=regime,
        cvd_proxy=cvd_proxy, entry=entry, sl=sl, t1=t1, t2=t2,
        day_bias=day_bias, atr=atr, rsi=rsi, ema9=ema9, ema21=ema21,
        change_pct=change_pct, df=df5
    )

# ------------------------------------------------------------
# Institutional Recap
# ------------------------------------------------------------
def make_recap(sig: Signal) -> str:
    time_str = datetime.now(EAT).strftime("%H:%M EAT")

    if sig.cvd_proxy > 0:
        flow = "Buying pressure visible in recent candles (volume delta positive)."
    elif sig.cvd_proxy < 0:
        flow = "Selling pressure visible in recent candles (volume delta negative)."
    else:
        flow = "Volume delta relatively balanced."

    if sig.conviction >= 7:
        expect = f"High conviction {sig.direction}. Looking for continuation if structure holds."
    elif sig.conviction >= 5:
        expect = f"Moderate {sig.direction} bias. Prefer confirmation or pullback into value."
    else:
        expect = "Low conviction environment. Prefer patience until clearer directional edge appears."

    if "RANGING" in sig.regime:
        expect += " Market is compressing — elevated breakout risk."

    return f"""
<b>BRAX INSTITUTIONAL DESK — {sig.name}</b>
<code>{time_str} | {sig.session}</code>

<b>Price:</b> ${sig.price:.2f}  ({sig.change_pct:+.2f}%)
<b>Day Bias:</b> {sig.day_bias}
<b>Regime:</b> {sig.regime}

<b>ORDER FLOW / VOLUME DELTA</b>
CVD Proxy: <b>{sig.cvd_proxy:.1f}</b>
→ {flow}

<b>STRUCTURE</b>
EMA9/21 (5m): {"Bullish" if sig.ema9 > sig.ema21 else "Bearish"}
RSI (5m): {sig.rsi:.1f} | ATR: {sig.atr:.2f}

<b>KEY LEVELS</b>
Entry Zone: <b>${sig.entry:.2f}</b>
Invalidation: ${sig.sl:.2f}
T1: ${sig.t1:.2f} | T2: ${sig.t2:.2f}

<b>DESK VIEW</b>
{sig.direction} bias — Conviction <b>{sig.conviction}/10</b>
{expect}

<code>Data: Twelve Data | Next update in 15 minutes</code>
""".strip()

# ------------------------------------------------------------
# Telegram
# ------------------------------------------------------------
class Alerts:
    def __init__(self):
        self.base = f"https://api.telegram.org/bot{TOKEN}"

    async def text(self, msg: str, session: aiohttp.ClientSession):
        try:
            async with session.post(f"{self.base}/sendMessage", json={
                "chat_id": CHAT_ID,
                "text": msg,
                "parse_mode": "HTML"
            }, timeout=20):
                pass
        except Exception as e:
            logger.error(f"Telegram error: {e}")

# ------------------------------------------------------------
# Main Loop
# ------------------------------------------------------------
async def main_loop():
    md = MarketData()
    await md.start()
    alerts = Alerts()

    await alerts.text(
        "<b>BRAX INSTITUTIONAL DESK ONLINE</b>\n"
        "Source: <b>Twelve Data</b> (real XAU/USD + BTC/USD)\n"
        "First full recaps in \~30 seconds...",
        md.session
    )

    assets = [
        ("XAU/USD", "GOLD"),
        ("BTC/USD", "BITCOIN"),
    ]

    # First update after data has time to arrive
    await asyncio.sleep(30)

    await alerts.text("<b>Sending first institutional recaps now...</b>", md.session)

    for symbol, name in assets:
        try:
            sig = await analyze(md, symbol, name)
            recap = make_recap(sig)
            await alerts.text(recap, md.session)
            logger.info(f"FIRST RECAP → {name} | ${sig.price:.2f}")
        except Exception as e:
            logger.error(f"First cycle {name}: {e}")
            await alerts.text(f"<b>{name}</b>\nFirst cycle error: {str(e)[:150]}", md.session)

    # Normal 15-minute loop
    while True:
        try:
            await asyncio.sleep(SIGNAL_INTERVAL)

            for symbol, name in assets:
                try:
                    sig = await analyze(md, symbol, name)
                    recap = make_recap(sig)
                    await alerts.text(recap, md.session)
                    logger.info(f"Recap → {name} | {sig.direction} | Conv {sig.conviction}")
                except Exception as e:
                    logger.error(f"Cycle {name}: {e}")
                    await alerts.text(f"<b>{name}</b>\nTemporary error: {str(e)[:120]}", md.session)

        except Exception as e:
            logger.error(f"Main loop: {e}")
            await asyncio.sleep(20)

def run_flask():
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    asyncio.run(main_loop())
