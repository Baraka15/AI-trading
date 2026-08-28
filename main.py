# BRAX QUANTITATIVE ALPHA ENGINE - INSTITUTIONAL GRADE XAUUSD
# Architecture: Event-Driven Async, Vectorized Stats, Tick-Level Order Flow

import asyncio
import os
import time
import logging
from datetime import datetime
from dataclasses import dataclass
import pytz
import aiohttp
import numpy as np
import pandas as pd
from scipy.stats import linregress
from flask import Flask
from threading import Thread
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as patches

# --- CONFIGURATION & LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("BraxQuant")

TOKEN = os.getenv("TELEGRAM_TOKEN", "YOUR_TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID")
TWELVE_KEY = os.getenv("TWELVEDATA_API_KEY", "YOUR_TWELVE_KEY")
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/paxgusdt@trade"
EAT = pytz.timezone("Africa/Nairobi")

app = Flask(__name__)
@app.route("/")
def health_check():
    return "BRAX QUANT ENGINE V2.0 - ACTIVE", 200

# --- DATA STRUCTURES ---
@dataclass
class MarketState:
    symbol: str
    price: float
    bid: float
    ask: float
    volume_delta: float
    vwap: float
    hurst_exponent: float
    regime: str
    timestamp: float

# --- INSTITUTIONAL MATH & QUANT MODELS ---
class QuantMath:
    @staticmethod
    def calc_hurst_exponent(ts_data: np.array, max_lag=20) -> float:
        """Determines if market is trending (H > 0.5) or mean-reverting (H < 0.5)"""
        lags = range(2, max_lag)
        tau = [np.sqrt(np.std(np.subtract(ts_data[lag:], ts_data[:-lag]))) for lag in lags]
        poly = np.polyfit(np.log(lags), np.log(tau), 1)
        return poly[0] * 2.0

    @staticmethod
    def calc_vwap(df: pd.DataFrame) -> pd.Series:
        """Calculates true Volume Weighted Average Price"""
        q = df['vol']
        p = df['close']
        return (p * q).cumsum() / q.cumsum()

    @staticmethod
    def identify_smart_money_zones(df: pd.DataFrame) -> tuple:
        """Vectorized FVG (Fair Value Gap) and Liquidity Sweep detection"""
        df['fvg_bull'] = (df['low'] > df['high'].shift(2)) & (df['close'] > df['open'])
        df['fvg_bear'] = (df['high'] < df['low'].shift(2)) & (df['close'] < df['open'])
        
        # Rolling liquidity pools (Swing Highs/Lows)
        df['swing_high'] = df['high'] == df['high'].rolling(window=15, center=True).max()
        df['swing_low'] = df['low'] == df['low'].rolling(window=15, center=True).min()
        
        recent_bsl = df[df['swing_high']]['high'].last_valid_index()
        recent_ssl = df[df['swing_low']]['low'].last_valid_index()
        
        return df, recent_bsl, recent_ssl

# --- ASYNC DATA INGESTION ENGINE ---
class DataFeed:
    def __init__(self):
        self.session = None
        self.tick_price = 0.0
        self.df_5m = pd.DataFrame()

    async def initialize(self):
        self.session = aiohttp.ClientSession()
        asyncio.create_task(self.binance_tick_stream())

    async def binance_tick_stream(self):
        """Zero-latency WebSocket connection for real-time order execution matching"""
        import json, websockets
        while True:
            try:
                async with websockets.connect(BINANCE_WS_URL) as ws:
                    logger.info("Connected to tick-level WebSocket feed.")
                    async for message in ws:
                        data = json.loads(message)
                        self.tick_price = float(data['p']) - 2.8 # XAU offset adjustment
            except Exception as e:
                logger.error(f"WebSocket disconnected, reconnecting... {e}")
                await asyncio.sleep(1)

    async def fetch_historical_async(self):
        """Asynchronous REST fetch for multi-timeframe alignment"""
        url = f"https://api.binance.com/api/v3/klines?symbol=PAXGUSDT&interval=5m&limit=100"
        try:
            async with self.session.get(url, timeout=5) as response:
                data = await response.json()
                df = pd.DataFrame(data, columns=['t', 'o', 'h', 'l', 'c', 'v', 'ct', 'qav', 'num_trades', 'tbbav', 'tbqav', 'ignore'])
                for col in ['o', 'h', 'l', 'c', 'v']:
                    df[col] = df[col].astype(float)
                # Apply XAU offset
                df[['o', 'h', 'l', 'c']] = df[['o', 'h', 'l', 'c']] - 2.8
                df.rename(columns={'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'vol'}, inplace=True)
                self.df_5m = df
                return df
        except Exception as e:
            logger.error(f"Async REST Error: {e}")
            return None

# --- TELEGRAM ASYNC DISPATCHER ---
class TelegramNotifier:
    def __init__(self, token, chat_id):
        self.url = f"https://api.telegram.org/bot{token}"
        self.chat_id = chat_id
        
    async def send_text(self, text, session):
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}
        try:
            await session.post(f"{self.url}/sendMessage", data=payload)
        except Exception as e:
            logger.error(f"TG Text Error: {e}")

# --- EXECUTION & LOGIC LOOP ---
async def institutional_alpha_loop():
    feed = DataFeed()
    await feed.initialize()
    notifier = TelegramNotifier(TOKEN, CHAT_ID)
    
    # Wait for initial buffer
    await asyncio.sleep(3)
    
    while True:
        try:
            ts_start = time.time()
            
            # 1. Ingest Data
            df = await feed.fetch_historical_async()
            current_price = feed.tick_price
            
            if df is None or current_price == 0.0:
                await asyncio.sleep(1)
                continue
                
            # 2. Vectorized Quant Analysis
            df['vwap'] = QuantMath.calc_vwap(df)
            hurst = QuantMath.calc_hurst_exponent(df['close'].values)
            regime = "TRENDING" if hurst > 0.55 else "MEAN_REVERTING" if hurst < 0.45 else "CHOP"
            
            df, bsl_idx, ssl_idx = QuantMath.identify_smart_money_zones(df)
            
            # 3. Dynamic Order Flow (CVD Approximation via volume direction)
            df['buy_vol'] = np.where(df['close'] > df['open'], df['vol'], 0)
            df['sell_vol'] = np.where(df['close'] < df['open'], df['vol'], 0)
            cvd = (df['buy_vol'].sum() - df['sell_vol'].sum())
            
            # 4. ATR & Dynamic Risk Sizing
            df['tr'] = df['high'] - df['low']
            atr = df['tr'].rolling(14).mean().iloc[-1]
            
            # Signal Generation Logic
            score = 0
            direction = "WAIT"
            premium_zone = current_price > df['vwap'].iloc[-1] + (atr * 1.5)
            discount_zone = current_price < df['vwap'].iloc[-1] - (atr * 1.5)
            
            if regime == "MEAN_REVERTING":
                if premium_zone and cvd < 0:
                    score += 4
                    direction = "SELL"
                elif discount_zone and cvd > 0:
                    score += 4
                    direction = "BUY"
            
            # 5. Output Execution Matrix
            latency = (time.time() - ts_start) * 1000
            
            if score >= 4:
                entry = current_price
                sl = entry + (atr * 2) if direction == "SELL" else entry - (atr * 2)
                tp = entry - (atr * 4) if direction == "SELL" else entry + (atr * 4)
                
                msg = (f"⚡ <b>INSTITUTIONAL EXECUTION FIRED</b> ⚡\n\n"
                       f"<b>Asset:</b> XAUUSD (Tick-Matched)\n"
                       f"<b>Action:</b> {direction} @ ${entry:.2f}\n"
                       f"<b>Latency:</b> {latency:.2f}ms\n\n"
                       f"<b>Quant Data:</b>\n"
                       f"Hurst Exponent: {hurst:.3f} [{regime}]\n"
                       f"VWAP Variance: ${abs(current_price - df['vwap'].iloc[-1]):.2f}\n"
                       f"Volume Delta (CVD): {cvd:.2f}\n\n"
                       f"<b>Risk:</b>\n"
                       f"SL: ${sl:.2f} | TP: ${tp:.2f} | ATR: {atr:.2f}")
                
                async with aiohttp.ClientSession() as session:
                    await notifier.send_text(msg, session)
                    
                await asyncio.sleep(300) # Throttle after execution
                
            await asyncio.sleep(1) # 1-second quant loop evaluation
            
        except Exception as e:
            logger.error(f"Alpha Loop Error: {e}")
            await asyncio.sleep(5)

def run_flask():
    app.run(host="0.0.0.0", port=10000, use_reloader=False)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    asyncio.run(institutional_alpha_loop())
