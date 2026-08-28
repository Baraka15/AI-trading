import asyncio
import os
import json
import logging
from datetime import datetime
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional, List
import numpy as np
import pandas as pd
import aiohttp
import websockets
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
logger = logging.getLogger("BraxOrderFlow")

TOKEN = os.getenv("8747660197:AAEqz0C7bg2ntLm_Hf0r4o7NuXVicSK7P5M")
CHAT_ID = os.getenv("7168775421")
if not TOKEN or not CHAT_ID:
    raise ValueError("TELEGRAM_TOKEN and TELEGRAM_CHAT_ID must be set")

EAT = pytz.timezone("Africa/Nairobi")
SIGNAL_INTERVAL = int(os.getenv("SIGNAL_INTERVAL", 180))
MIN_CONVICTION = int(os.getenv("MIN_CONVICTION", 7))

app = Flask(__name__)

@app.route("/")
def health():
    return "BRAX REAL-TIME ORDER FLOW ENGINE - ONLINE", 200

# ---------------------------------------------------------------------------
# Live Order Flow State
# ---------------------------------------------------------------------------
@dataclass
class OrderFlowState:
    symbol: str
    cvd: float = 0.0
    buy_vol: float = 0.0
    sell_vol: float = 0.0
    imbalance: float = 0.0          # bid_vol / (bid_vol + ask_vol)  → >0.55 bullish
    large_buy: int = 0
    large_sell: int = 0
    last_price: float = 0.0
    trades: Deque = field(default_factory=lambda: deque(maxlen=800))
    last_update: float = 0.0

    def reset_window(self):
        """Soft reset every evaluation cycle so CVD is recent"""
        self.cvd *= 0.35
        self.buy_vol *= 0.35
        self.sell_vol *= 0.35
        self.large_buy = max(0, self.large_buy - 1)
        self.large_sell = max(0, self.large_sell - 1)

# ---------------------------------------------------------------------------
# Real-time WebSocket feed
# ---------------------------------------------------------------------------
class LiveFeed:
    def __init__(self):
        self.states: Dict[str, OrderFlowState] = {
            "PAXGUSDT": OrderFlowState("PAXGUSDT"),
            "BTCUSDT": OrderFlowState("BTCUSDT"),
        }
        self.session: Optional[aiohttp.ClientSession] = None
        self.running = True

    async def start(self):
        self.session = aiohttp.ClientSession()
        asyncio.create_task(self._trade_stream("PAXGUSDT"))
        asyncio.create_task(self._trade_stream("BTCUSDT"))
        asyncio.create_task(self._depth_loop())

    async def _trade_stream(self, symbol: str):
        url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@aggTrade"
        while self.running:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    logger.info(f"Trade stream connected: {symbol}")
                    async for msg in ws:
                        data = json.loads(msg)
                        price = float(data["p"])
                        qty = float(data["q"])
                        is_buyer_maker = data["m"]  # True = sell aggression

                        state = self.states[symbol]
                        state.last_price = price - 2.8 if symbol == "PAXGUSDT" else price
                        state.trades.append((price, qty, is_buyer_maker))

                        if is_buyer_maker:
                            state.sell_vol += qty
                            state.cvd -= qty
                            if qty > (0.8 if symbol == "BTCUSDT" else 0.15):
                                state.large_sell += 1
                        else:
                            state.buy_vol += qty
                            state.cvd += qty
                            if qty > (0.8 if symbol == "BTCUSDT" else 0.15):
                                state.large_buy += 1

                        state.last_update = asyncio.get_event_loop().time()
            except Exception as e:
                logger.error(f"Trade WS {symbol}: {e}")
                await asyncio.sleep(3)

    async def _depth_loop(self):
        """Poll order book every 2–3 seconds for imbalance"""
        while self.running:
            for symbol in self.states:
                try:
                    url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=20"
                    async with self.session.get(url, timeout=5) as resp:
                        if resp.status == 200:
                            book = await resp.json()
                            bids = sum(float(b[1]) for b in book["bids"])
                            asks = sum(float(a[1]) for a in book["asks"])
                            total = bids + asks
                            if total > 0:
                                self.states[symbol].imbalance = bids / total
                except Exception as e:
                    logger.debug(f"Depth error {symbol}: {e}")
            await asyncio.sleep(2.5)

    async def get_candles(self, symbol: str, interval: str, limit: int = 100) -> pd.DataFrame:
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
        try:
            async with self.session.get(url, timeout=8) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    df = pd.DataFrame(data, columns=[
                        't','o','h','l','c','v','ct','qav','n','tbb','tbq','x'
                    ])
                    for col in ['o','h','l','c','v']:
                        df[col] = df[col].astype(float)
                    if symbol == "PAXGUSDT":
                        df[['o','h','l','c']] -= 2.8
                    return df
        except Exception as e:
            logger.error(f"Klines {symbol}: {e}")
        return pd.DataFrame()

# ---------------------------------------------------------------------------
# Analysis with real order-flow conviction
# ---------------------------------------------------------------------------
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
    notes: str
    df: pd.DataFrame

class OrderFlowAnalyzer:
    @staticmethod
    def _rsi(close: pd.Series, period: int = 14) -> float:
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(period).mean()
        loss = (-delta.clip(upper=0)).rolling(period).mean()
        rs = gain / loss
        return float(100 - (100 / (1 + rs.iloc[-1])))

    @staticmethod
    def analyze(feed: LiveFeed, symbol: str, name: str) -> Optional[Signal]:
        state = feed.states[symbol]
        if state.last_price <= 0:
            return None

        # Pull multi-TF
        df5 = asyncio.get_event_loop().run_until_complete(
            feed.get_candles(symbol, "5m", 80)
        ) if False else None  # will be awaited properly below

        return None  # placeholder – real method is async

async def full_analyze(feed: LiveFeed, symbol: str, name: str) -> Optional[Signal]:
    state = feed.states[symbol]
    if state.last_price <= 0 or (asyncio.get_event_loop().time() - state.last_update) > 30:
        return None

    df5 = await feed.get_candles(symbol, "5m", 80)
    df15 = await feed.get_candles(symbol, "15m", 50)
    df1h = await feed.get_candles(symbol, "1h", 40)

    if df5.empty or len(df5) < 30:
        return None

    price = state.last_price

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
        sess = "OFF-HOURS"

    # RSI + EMA
    rsi = OrderFlowAnalyzer._rsi(df5['c'])
    ema9 = float(df5['c'].ewm(span=9).mean().iloc[-1])
    ema21 = float(df5['c'].ewm(span=21).mean().iloc[-1])
    ema9_1h = float(df1h['c'].ewm(span=9).mean().iloc[-1]) if not df1h.empty else price
    ema21_1h = float(df1h['c'].ewm(span=21).mean().iloc[-1]) if not df1h.empty else price

    # Order-flow metrics
    cvd = state.cvd
    imb = state.imbalance
    large_delta = state.large_buy - state.large_sell

    # Regime
    vol_mean = float(df5['v'].tail(20).mean())
    regime = "TRENDING" if abs(cvd) > vol_mean * 0.6 else "RANGING"

    # ----- Conviction scoring (0-10) -----
    score = 0
    bull = 0
    bear = 0

    # 1. CVD strength
    if cvd > 0:
        bull += 1
        score += 2 if abs(cvd) > vol_mean * 0.4 else 1
    else:
        bear += 1
        score += 2 if abs(cvd) > vol_mean * 0.4 else 1

    # 2. Order book imbalance
    if imb > 0.58:
        bull += 1
        score += 2
    elif imb < 0.42:
        bear += 1
        score += 2

    # 3. Large trade aggression
    if large_delta > 2:
        bull += 1
        score += 1
    elif large_delta < -2:
        bear += 1
        score += 1

    # 4. EMA structure
    if ema9 > ema21:
        bull += 1
        score += 1
    else:
        bear += 1
        score += 1

    # 5. Higher TF
    if ema9_1h > ema21_1h:
        bull += 1
        score += 2
    else:
        bear += 1
        score += 2

    # 6. RSI extreme
    if rsi < 32:
        bull += 1
        score += 1
    elif rsi > 68:
        bear += 1
        score += 1

    # 7. Session quality
    if sess in ("LONDON", "NY KILLZONE"):
        score += 1

    direction = "BUY" if bull > bear else "SELL"
    if abs(bull - bear) <= 1:
        score = max(0, score - 2)

    # Levels
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

    day_bias = "BULLISH" if ema9_1h > ema21_1h else "BEARISH"

    notes = (f"CVD {cvd:.1f} | Imb {imb:.2f} | LargeΔ {large_delta} | "
             f"RSI {rsi:.0f} | {regime}")

    return Signal(
        symbol=symbol,
        name=name,
        price=price,
        direction=direction,
        conviction=min(score, 10),
        session=sess,
        regime=regime,
        cvd=cvd,
        imbalance=imb,
        entry=entry,
        sl=sl,
        t1=t1,
        t2=t2,
        day_bias=day_bias,
        atr=atr,
        rsi=rsi,
        notes=notes,
        df=df5
    )

# ---------------------------------------------------------------------------
# Chart & Voice
# ---------------------------------------------------------------------------
def make_chart(sig: Signal, path: str):
    df = sig.df.tail(55).reset_index(drop=True)
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

    title = (f"{sig.name} | ${sig.price:.2f} | {sig.direction} "
             f"(Conviction {sig.conviction}/10) | {sig.session}")
    ax.set_title(title, color='white', fontsize=12, weight='bold')
    ax.tick_params(colors='#8b949e')
    for s in ax.spines.values():
        s.set_color('#30363d')
    ax.grid(True, color='#21262d', lw=0.5)
    ax.legend(loc='upper left', facecolor='#161b22', labelcolor='#c9d1d9', fontsize=8)
    plt.tight_layout()
    plt.savefig(path, dpi=140, facecolor='#0d1117')
    plt.close()

def make_voice(sig: Signal, path: str):
    t = datetime.now(EAT).strftime("%I:%M %p")
    text = (
        f"Real-time order flow briefing at {t}. "
        f"{sig.name} trading at {sig.price:.1f}. "
        f"Session {sig.session}. Day bias {sig.day_bias}. "
        f"Live CVD is {sig.cvd:.0f}, order book imbalance {sig.imbalance:.2f}. "
        f"Model conviction {sig.conviction} out of 10 giving a clear {sig.direction} bias. "
        f"Entry zone {sig.entry:.1f}, stop {sig.sl:.1f}, first target {sig.t1:.1f}. "
        f"Execute only with proper risk."
    )
    gTTS(text=text, lang='en', slow=False).save(path)

# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------
class Alerts:
    def __init__(self):
        self.base = f"https://api.telegram.org/bot{TOKEN}"

    async def photo(self, path: str, caption: str, session: aiohttp.ClientSession):
        try:
            with open(path, 'rb') as f:
                data = aiohttp.FormData()
                data.add_field("chat_id", CHAT_ID)
                data.add_field("caption", caption)
                data.add_field("parse_mode", "HTML")
                data.add_field("photo", f, filename="of.png")
                async with session.post(f"{self.base}/sendPhoto", data=data, timeout=40):
                    pass
        except Exception as e:
            logger.error(f"Photo: {e}")

    async def voice(self, path: str, session: aiohttp.ClientSession):
        try:
            with open(path, 'rb') as f:
                data = aiohttp.FormData()
                data.add_field("chat_id", CHAT_ID)
                data.add_field("voice", f, filename="of.mp3")
                async with session.post(f"{self.base}/sendVoice", data=data, timeout=40):
                    pass
        except Exception as e:
            logger.error(f"Voice: {e}")

    async def text(self, msg: str, session: aiohttp.ClientSession):
        try:
            payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}
            async with session.post(f"{self.base}/sendMessage", json=payload, timeout=15):
                pass
        except Exception as e:
            logger.error(f"Text: {e}")

# ---------------------------------------------------------------------------
# Main real-time loop
# ---------------------------------------------------------------------------
async def main_loop():
    feed = LiveFeed()
    await feed.start()
    alerts = Alerts()

    logger.info("Real-time Order Flow Engine started")
    logger.info(f"Evaluation every {SIGNAL_INTERVAL}s | Min conviction {MIN_CONVICTION}")

    assets = [("PAXGUSDT", "GOLD"), ("BTCUSDT", "BITCOIN")]

    while True:
        try:
            for symbol, name in assets:
                sig = await full_analyze(feed, symbol, name)
                if not sig:
                    continue

                # Soft reset CVD window so it stays recent
                feed.states[symbol].reset_window()

                logger.info(
                    f"{name} | {sig.direction} | Conv {sig.conviction} | "
                    f"CVD {sig.cvd:.0f} | Imb {sig.imbalance:.2f} | {sig.session}"
                )

                outlook = (
                    f"<b>{name} LIVE ORDER FLOW</b>\n"
                    f"Price: <b>${sig.price:.2f}</b>\n"
                    f"Direction: {sig.direction} | Conviction: {sig.conviction}/10\n"
                    f"Day Bias: {sig.day_bias} | Session: {sig.session}\n"
                    f"{sig.notes}"
                )

                if sig.conviction >= MIN_CONVICTION:
                    chart = f"/tmp/{symbol}_of.png"
                    voice = f"/tmp/{symbol}_of.mp3"
                    make_chart(sig, chart)
                    make_voice(sig, voice)

                    caption = (
                        f"<b>BRAX ORDER-FLOW SIGNAL | {sig.name}</b>\n\n"
                        f"<b>{sig.direction}</b>  •  Conviction <b>{sig.conviction}/10</b>\n"
                        f"Entry: <b>${sig.entry:.2f}</b>\n"
                        f"SL: ${sig.sl:.2f}   T1: ${sig.t1:.2f}   T2: ${sig.t2:.2f}\n"
                        f"Day Bias: {sig.day_bias} | {sig.session}\n\n"
                        f"<i>{sig.notes}</i>\n"
                        f"<i>Live trade + order-book data. Voice follows.</i>"
                    )
                    await alerts.photo(chart, caption, feed.session)
                    await alerts.voice(voice, feed.session)
                elif sig.conviction >= 5:
                    await alerts.text(outlook, feed.session)

            await asyncio.sleep(SIGNAL_INTERVAL)

        except Exception as e:
            logger.error(f"Loop error: {e}")
            await asyncio.sleep(10)

def run_flask():
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    asyncio.run(main_loop())
