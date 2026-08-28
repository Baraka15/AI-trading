import asyncio
import os
import logging
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Optional
import numpy as np
import pandas as pd
import aiohttp
from gtts import gTTS
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
logger = logging.getLogger("BraxInstitutional")

TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not TOKEN or not CHAT_ID:
    raise ValueError("TELEGRAM_TOKEN and TELEGRAM_CHAT_ID must be set")

EAT = pytz.timezone("Africa/Nairobi")
SIGNAL_INTERVAL = 900          # 15 minutes after the first update

app = Flask(__name__)

@app.route("/")
def health():
    return "BRAX INSTITUTIONAL ORDER FLOW DESK - ONLINE", 200

@dataclass
class OrderFlowState:
    symbol: str
    cvd: float = 0.0
    imbalance: float = 0.5
    large_buy: int = 0
    large_sell: int = 0
    last_price: float = 0.0

class LiveFeed:
    def __init__(self):
        self.states: Dict[str, OrderFlowState] = {
            "PAXGUSDT": OrderFlowState("PAXGUSDT"),
            "BTCUSDT": OrderFlowState("BTCUSDT"),
        }
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12))
        asyncio.create_task(self._poll_loop())

    async def _poll_loop(self):
        while True:
            for symbol in list(self.states.keys()):
                await self._update(symbol)
            await asyncio.sleep(5)

    async def _update(self, symbol: str):
        state = self.states[symbol]
        try:
            # Live price
            async with self.session.get(f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}") as r:
                if r.status == 200:
                    data = await r.json()
                    price = float(data["price"])
                    state.last_price = price - 2.8 if symbol == "PAXGUSDT" else price

            # Recent aggressive trades → CVD
            async with self.session.get(f"https://api.binance.com/api/v3/aggTrades?symbol={symbol}&limit=80") as r:
                if r.status == 200:
                    trades = await r.json()
                    cvd = 0.0
                    lb = ls = 0
                    for t in trades:
                        qty = float(t["q"])
                        if t["m"]:
                            cvd -= qty
                            if qty > (1.0 if symbol == "BTCUSDT" else 0.13):
                                ls += 1
                        else:
                            cvd += qty
                            if qty > (1.0 if symbol == "BTCUSDT" else 0.13):
                                lb += 1
                    state.cvd = cvd
                    state.large_buy = lb
                    state.large_sell = ls

            # Order book imbalance
            async with self.session.get(f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=20") as r:
                if r.status == 200:
                    book = await r.json()
                    bids = sum(float(b[1]) for b in book["bids"])
                    asks = sum(float(a[1]) for a in book["asks"])
                    total = bids + asks
                    if total > 0:
                        state.imbalance = bids / total
        except Exception as e:
            logger.warning(f"Feed update {symbol}: {e}")

    async def get_candles(self, symbol: str, interval: str = "5m", limit: int = 100) -> pd.DataFrame:
        try:
            url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
            async with self.session.get(url) as r:
                if r.status == 200:
                    data = await r.json()
                    df = pd.DataFrame(data, columns=[
                        't','o','h','l','c','v','ct','qav','n','tbb','tbq','x'
                    ])
                    for col in ['o','h','l','c','v']:
                        df[col] = df[col].astype(float)
                    if symbol == "PAXGUSDT":
                        df[['o','h','l','c']] -= 2.8
                    return df
        except Exception as e:
            logger.error(f"Candles {symbol}: {e}")
        return pd.DataFrame()

@dataclass
class Signal:
    symbol: str
    name: str
    price: float
    direction: str
    conviction: int
    session: str
    regime: str
    cvd: float
    imbalance: float
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
    large_delta: int = 0

def calc_rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss
    val = 100 - (100 / (1 + rs.iloc[-1]))
    return float(val) if not np.isnan(val) else 50.0

async def analyze(feed: LiveFeed, symbol: str, name: str) -> Optional[Signal]:
    state = feed.states[symbol]
    price = state.last_price
    if price <= 0:
        return None

    df5 = await feed.get_candles(symbol, "5m", 90)
    df1h = await feed.get_candles(symbol, "1h", 48)
    df4h = await feed.get_candles(symbol, "4h", 30)

    if df5.empty or len(df5) < 25:
        # Fallback so we never stay completely silent
        return Signal(
            symbol=symbol, name=name, price=price, direction="NEUTRAL",
            conviction=2, session="LIMITED DATA", regime="UNKNOWN",
            cvd=state.cvd, imbalance=state.imbalance,
            entry=price, sl=price, t1=price, t2=price,
            day_bias="NEUTRAL", atr=0.0, rsi=50.0, ema9=price, ema21=price,
            change_pct=0.0, df=df5, large_delta=0
        )

    # ATR
    tr = np.maximum(
        df5['h'] - df5['l'],
        np.maximum(abs(df5['h'] - df5['c'].shift()), abs(df5['l'] - df5['c'].shift()))
    )
    atr = float(tr.rolling(14).mean().iloc[-1])

    # Session
    now = datetime.now(EAT)
    h = now.hour + now.minute / 60
    if 13 <= h < 17:
        sess = "NY KILLZONE"
    elif 8 <= h < 13:
        sess = "LONDON"
    elif 2 <= h < 8:
        sess = "ASIAN"
    else:
        sess = "OFF-HOURS / LOW LIQUIDITY"

    rsi = calc_rsi(df5['c'])
    ema9 = float(df5['c'].ewm(span=9).mean().iloc[-1])
    ema21 = float(df5['c'].ewm(span=21).mean().iloc[-1])
    ema9_1h = float(df1h['c'].ewm(span=9).mean().iloc[-1]) if not df1h.empty else price
    ema21_1h = float(df1h['c'].ewm(span=21).mean().iloc[-1]) if not df1h.empty else price
    ema9_4h = float(df4h['c'].ewm(span=9).mean().iloc[-1]) if not df4h.empty else price

    cvd = state.cvd
    imb = state.imbalance
    large_delta = state.large_buy - state.large_sell
    vol_mean = float(df5['v'].tail(20).mean()) or 1.0
    regime = "TRENDING" if abs(cvd) > vol_mean * 0.55 else "RANGING / COMPRESSING"

    # Conviction engine
    score = 0
    bull = 0
    bear = 0

    if cvd > 0:
        bull += 1
        score += 2 if abs(cvd) > vol_mean * 0.35 else 1
    else:
        bear += 1
        score += 2 if abs(cvd) > vol_mean * 0.35 else 1

    if imb > 0.57:
        bull += 1
        score += 2
    elif imb < 0.43:
        bear += 1
        score += 2

    if large_delta >= 2:
        bull += 1
        score += 1
    elif large_delta <= -2:
        bear += 1
        score += 1

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

    if ema9_4h > price:
        bull += 1
    else:
        bear += 1

    if rsi < 32:
        bull += 1
        score += 1
    elif rsi > 68:
        bear += 1
        score += 1

    if sess in ("LONDON", "NY KILLZONE"):
        score += 1

    direction = "BUY" if bull > bear else "SELL"
    if abs(bull - bear) <= 1:
        score = max(0, score - 2)

    if direction == "BUY":
        entry = price - atr * 0.14
        sl = entry - atr * 1.45
        t1 = entry + atr * 2.25
        t2 = entry + atr * 3.6
    else:
        entry = price + atr * 0.14
        sl = entry + atr * 1.45
        t1 = entry - atr * 2.25
        t2 = entry - atr * 3.6

    day_bias = "BULLISH" if ema9_1h > ema21_1h else "BEARISH"
    change_pct = ((price - float(df5['o'].iloc[0])) / float(df5['o'].iloc[0])) * 100

    return Signal(
        symbol=symbol, name=name, price=price, direction=direction,
        conviction=min(score, 10), session=sess, regime=regime,
        cvd=cvd, imbalance=imb, entry=entry, sl=sl, t1=t1, t2=t2,
        day_bias=day_bias, atr=atr, rsi=rsi, ema9=ema9, ema21=ema21,
        change_pct=change_pct, df=df5, large_delta=large_delta
    )

def make_institutional_recap(sig: Signal) -> str:
    time_str = datetime.now(EAT).strftime("%H:%M EAT")

    # Flow interpretation
    if sig.cvd > 0 and sig.imbalance > 0.55:
        flow_text = "Clear buying aggression with a supportive order book. Buyers currently control the tape."
    elif sig.cvd < 0 and sig.imbalance < 0.45:
        flow_text = "Selling pressure dominant. Order book is leaning toward the sellers."
    elif abs(sig.cvd) < 8:
        flow_text = "Order flow is relatively balanced. No decisive aggression from either side."
    else:
        flow_text = "Notable order-flow imbalance present. Watching for absorption or continuation."

    # Desk expectation
    if sig.conviction >= 7:
        expect = f"High conviction {sig.direction} setup. Looking for continuation toward {sig.t1:.1f} if structure remains intact."
    elif sig.conviction >= 5:
        expect = f"Moderate {sig.direction} bias. Prefer waiting for a pullback into value or clearer flow confirmation."
    else:
        expect = "Low conviction environment. Prefer staying flat or reducing size until a clearer directional edge appears."

    if "RANGING" in sig.regime or "COMPRESSING" in sig.regime:
        expect += " Market is compressing — elevated breakout risk in either direction."

    return f"""
<b>BRAX INSTITUTIONAL DESK — {sig.name}</b>
<code>{time_str} | {sig.session}</code>

<b>Price:</b> ${sig.price:.2f}   ({sig.change_pct:+.2f}% from open)
<b>Day Bias:</b> {sig.day_bias}
<b>Regime:</b> {sig.regime}

<b>LIVE ORDER FLOW</b>
• CVD: <b>{sig.cvd:.1f}</b>
• Book Imbalance: <b>{sig.imbalance:.2f}</b>
• Large Trade Delta: {sig.large_delta}
→ {flow_text}

<b>STRUCTURE</b>
• 5m EMA9/21: {"Bullish cross" if sig.ema9 > sig.ema21 else "Bearish cross"}
• RSI (5m): {sig.rsi:.1f}
• ATR: {sig.atr:.2f}

<b>KEY LEVELS</b>
• Preferred Entry Zone: <b>${sig.entry:.2f}</b>
• Invalidation (SL): ${sig.sl:.2f}
• Target 1: ${sig.t1:.2f}
• Target 2: ${sig.t2:.2f}

<b>DESK VIEW / EXPECTATION</b>
{sig.direction} bias — Conviction <b>{sig.conviction}/10</b>

{expect}

<code>Next full desk update in 15 minutes</code>
""".strip()

def make_chart(sig: Signal, path: str):
    if sig.df.empty or len(sig.df) < 10:
        return
    df = sig.df.tail(60).reset_index(drop=True)
    fig, ax = plt.subplots(figsize=(13, 7), facecolor='#0d1117')
    ax.set_facecolor('#0d1117')

    for i, row in df.iterrows():
        color = '#00e676' if row['c'] >= row['o'] else '#ff1744'
        ax.plot([i, i], [row['l'], row['h']], color=color, lw=1.1)
        body = max(abs(row['c'] - row['o']), 0.05)
        ax.add_patch(patches.Rectangle(
            (i - 0.35, min(row['o'], row['c'])), 0.7, body,
            facecolor=color, edgecolor=color
        ))

    ax.axhline(sig.entry, color='#ffc107', lw=1.8, label=f'ENTRY {sig.entry:.2f}')
    ax.axhline(sig.sl, color='#f44336', ls='--', lw=1.4, label=f'SL {sig.sl:.2f}')
    ax.axhline(sig.t1, color='#00e676', ls='--', lw=1.4, label=f'T1 {sig.t1:.2f}')
    ax.axhline(sig.t2, color='#69f0ae', ls=':', lw=1.2, label=f'T2 {sig.t2:.2f}')

    title = f"{sig.name} | ${sig.price:.2f} | {sig.direction} (Conviction {sig.conviction}/10) | {sig.session}"
    ax.set_title(title, color='white', fontsize=12, weight='bold')
    ax.tick_params(colors='#8b949e')
    for spine in ax.spines.values():
        spine.set_color('#30363d')
    ax.grid(True, color='#21262d', lw=0.5)
    ax.legend(loc='upper left', facecolor='#161b22', labelcolor='#c9d1d9', fontsize=8)
    plt.tight_layout()
    plt.savefig(path, dpi=140, facecolor='#0d1117')
    plt.close()

class Alerts:
    def __init__(self):
        self.base = f"https://api.telegram.org/bot{TOKEN}"

    async def text(self, msg: str, session: aiohttp.ClientSession):
        try:
            async with session.post(f"{self.base}/sendMessage", json={
                "chat_id": CHAT_ID,
                "text": msg,
                "parse_mode": "HTML"
            }, timeout=15):
                pass
        except Exception as e:
            logger.error(f"Text send error: {e}")

    async def photo(self, path: str, caption: str, session: aiohttp.ClientSession):
        try:
            with open(path, 'rb') as f:
                data = aiohttp.FormData()
                data.add_field("chat_id", CHAT_ID)
                data.add_field("caption", caption)
                data.add_field("parse_mode", "HTML")
                data.add_field("photo", f, filename="desk.png")
                async with session.post(f"{self.base}/sendPhoto", data=data, timeout=40):
                    pass
        except Exception as e:
            logger.error(f"Photo send error: {e}")

async def main_loop():
    feed = LiveFeed()
    await feed.start()
    alerts = Alerts()

    await alerts.text(
        "<b>BRAX INSTITUTIONAL DESK ONLINE</b>\n"
        "First full market recaps will arrive in \~20 seconds...\n"
        "Then every 15 minutes thereafter.",
        feed.session
    )

    assets = [("PAXGUSDT", "GOLD"), ("BTCUSDT", "BITCOIN")]
    logger.info("Institutional desk started — first update in 20s")

    # ========== FIRST UPDATE AFTER \~20 SECONDS ==========
    await asyncio.sleep(20)

    for symbol, name in assets:
        try:
            sig = await analyze(feed, symbol, name)
            if sig:
                recap = make_institutional_recap(sig)
                await alerts.text(recap, feed.session)
                logger.info(f"FIRST RECAP SENT → {name}")

                if sig.conviction >= 7 and not sig.df.empty:
                    chart_path = f"/tmp/{symbol}_first.png"
                    make_chart(sig, chart_path)
                    await alerts.photo(chart_path, f"<b>HIGH CONVICTION</b> {name}", feed.session)
        except Exception as e:
            logger.error(f"First update error {name}: {e}")
            await alerts.text(f"<b>{name}</b>\nTemporary issue on first cycle. Continuing...", feed.session)

    # ========== NORMAL 15-MINUTE LOOP ==========
    while True:
        try:
            await asyncio.sleep(SIGNAL_INTERVAL)

            for symbol, name in assets:
                sig = await analyze(feed, symbol, name)
                if not sig:
                    await alerts.text(f"<b>{name}</b>\nData temporarily unavailable. Next cycle in 15 min.", feed.session)
                    continue

                recap = make_institutional_recap(sig)
                await alerts.text(recap, feed.session)
                logger.info(f"Recap sent → {name} | {sig.direction} | Conv {sig.conviction}")

                if sig.conviction >= 7 and not sig.df.empty:
                    chart_path = f"/tmp/{symbol}.png"
                    make_chart(sig, chart_path)
                    await alerts.photo(chart_path, f"<b>HIGH CONVICTION SETUP</b>\n{name} {sig.direction}", feed.session)

        except Exception as e:
            logger.error(f"Main loop error: {e}")
            try:
                await alerts.text(f"<b>Desk temporary error</b>\n{str(e)[:180]}\nResuming...", feed.session)
            except:
                pass
            await asyncio.sleep(30)

def run_flask():
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    asyncio.run(main_loop())
