"""
GOLD KILLER V3.1 FINAL - LIFETIME FREE
Runs on Render Web Service Free Plan
Features: Price + CVD + Liquidity + FVG + Sniper + Bias + News + DXY + Story
"""

import requests, asyncio, threading
from datetime import datetime
import pytz
from telegram import Bot
from flask import Flask

# === KEYS - Already filled ===
TELEGRAM_TOKEN = "8747660197:AAEqz0C7bg2ntLm_Hf0r4o7NuXVicSK7P5M"
CHAT_ID = "7168775421"
TWELVE_DATA_KEY = "abb27fe4fa8749d8a20a042ef4d100ee"

bot = Bot(token=TELEGRAM_TOKEN)
app = Flask(__name__)
EAT = pytz.timezone('Africa/Kampala')

@app.route('/')
def home():
    return "GOLD KILLER V3.1 LIFETIME ONLINE - 200 OK"

def get_all_data():
    data = {}
    for tf in ["5min", "15min", "1h", "4h", "1day"]:
        try:
            url = f"https://api.twelvedata.com/time_series?symbol=XAU/USD&interval={tf}&outputsize=100&apikey={TWELVE_DATA_KEY}"
            r = requests.get(url, timeout=20).json()
            data[tf] = r.get('values', [])
        except:
            data[tf] = []
    try:
        dxy_url = f"https://api.twelvedata.com/time_series?symbol=DXY&interval=15min&outputsize=5&apikey={TWELVE_DATA_KEY}"
        data['dxy'] = requests.get(dxy_url, timeout=15).json().get('values', [])
    except:
        data['dxy'] = []
    return data

def analyze(data):
    if not data['5min'] or not data['1h']:
        return None

    price = float(data['5min'][0]['close'])
    c1h = data['1h']
    c5m = data['5min'][:20]
    c15m = data['15min'][:20]
    c4h = data['4h'][:10]

    high = max([float(x['high']) for x in c1h[:20]])
    low = min([float(x['low']) for x in c1h[:20]])
    mid = (high + low) / 2
    range_pct = ((price - low) / (high - low) * 100) if high!= low else 50

    # Order Flow CVD
    bull_v = sum([float(x['volume']) for x in c5m[:12] if float(x['close']) > float(x['open'])])
    bear_v = sum([float(x['volume']) for x in c5m[:12] if float(x['close']) < float(x['open'])])
    cvd = bull_v - bear_v

    # FVG
    fvg_bull = []
    fvg_bear = []
    for i in range(1, len(c15m)-1):
        if float(c15m[i+1]['low']) > float(c15m[i-1]['high']):
            fvg_bull.append(round(float(c15m[i-1]['high']),2))
        if float(c15m[i+1]['high']) < float(c15m[i-1]['low']):
            fvg_bear.append(round(float(c15m[i-1]['low']),2))

    # Bias Score
    score = 0
    if price < mid: score += 2
    else: score -= 2
    if cvd > 0: score += 1
    else: score -= 1
    if c4h and float(c4h[0]['close']) > float(c4h[2]['close']): score += 1
    else: score -= 1

    if score >= 2: bias = "STRONG BUY 🔵"
    elif score == 1: bias = "BUY"
    elif score == -1: bias = "SELL"
    else: bias = "STRONG SELL 🔴"

    # DXY
    dxy_txt = "DXY Neutral"
    if data['dxy'] and len(data['dxy'])>=2:
        dxy_now = float(data['dxy'][0]['close'])
        dxy_prev = float(data['dxy'][1]['close'])
        dxy_txt = f"DXY {'UP 🔴 Gold Down' if dxy_now>dxy_prev else 'DOWN 🔵 Gold Up'} ({dxy_now})"

    # Human Explanation
    if price > high*0.998:
        now_txt = f"Price at ${price} SWEEPING buy stops above ${high}. Retail longs trapped. Market makers hunting liquidity then will dump. DO NOT BUY - this is trap. Wait SELL at Bearish Order Block."
    elif price < low*1.002:
        now_txt = f"Price at ${price} SWEEPING sell stops below ${low}. Retail shorts trapped. Market makers absorbing. Reversal UP coming. DO NOT SELL - this is trap. Wait BUY at Bullish Order Block."
    elif range_pct < 35:
        now_txt = f"Price at ${price} in DISCOUNT ({round(range_pct)}%) - CHEAP. Market makers buying cheap. CVD {round(cvd)} shows {'buyers absorbing = bullish' if cvd>0 else 'still selling but bottom near'}. Expect rally to ${high}."
    elif range_pct > 65:
        now_txt = f"Price at ${price} in PREMIUM ({round(range_pct)}%) - EXPENSIVE. Market makers selling expensive. CVD {round(cvd)} shows {'sellers absorbing = bearish' if cvd<0 else 'still buying but top near'}. Expect drop to ${low}."
    else:
        now_txt = f"Price at ${price} in EQUILIBRIUM {round(range_pct)}% - Choppy. Market makers accumulating. Asia/London chop before NY real move. Wait for liquidity sweep."

    # Future
    if price > mid:
        future = f"1. Sweep LOW {low}\n2. 5M Bullish MSS\n3. Buy at OB {round(low + (high-low)*0.25,2)}\n4. Target {high}"
    else:
        future = f"1. Sweep HIGH {high}\n2. 5M Bearish MSS\n3. Sell at OB {round(high - (high-low)*0.25,2)}\n4. Target {low}"

    # Sniper
    if price < low*1.002 and cvd > 0:
        sniper = f"🎯 SNIPER BUY TRIGGERED\nEntry: ${price}\nSL: ${round(low-4.5,2)}\nTP1: ${round(price + (price-(low-4.5))*1.5,2)} (1:1.5)\nTP2: ${round(high,2)} (1:3)\nReason: SSL Sweep + CVD Bull Divergence + Discount"
        trigger = True
    elif price > high*0.998 and cvd < 0:
        sniper = f"🎯 SNIPER SELL TRIGGERED\nEntry: ${price}\nSL: ${round(high+4.5,2)}\nTP1: ${round(price - ((high+4.5)-price)*1.5,2)} (1:1.5)\nTP2: ${round(low,2)} (1:3)\nReason: BSL Sweep + CVD Bear Divergence + Premium"
        trigger = True
    else:
        sniper = "⏳ NO SNIPER - Waiting for:\n• Liquidity Sweep (High/Low)\n• CVD Divergence\n• FVG in Discount/Premium"
        trigger = False

    # Session
    h = datetime.now(EAT).hour
    if 15 <= h < 21: sess = "NEW YORK DISTRIBUTION - REAL MOVE"
    elif 10 <= h < 15: sess = "LONDON MANIPULATION - FAKE MOVES"
    elif 3 <= h < 10: sess = "ASIA ACCUMULATION - RANGE"
    else: sess = "OFF HOURS - LOW VOLUME"

    return {
        "price": price, "high": high, "low": low, "mid": mid, "range_pct": range_pct,
        "cvd": cvd, "bias": bias, "score": score, "dxy": dxy_txt,
        "fvg_bull": fvg_bull[:2], "fvg_bear": fvg_bear[:2],
        "now_txt": now_txt, "future": future, "sniper": sniper, "trigger": trigger,
        "session": sess, "time": datetime.now(EAT).strftime('%d %b %Y %H:%M:%S EAT')
    }

async def main_loop():
    await bot.send_message(chat_id=CHAT_ID, text="💎 GOLD KILLER V3.1 FINAL ONLINE\n✅ Lifetime Free Web Service\n✅ Updates every 5 mins\n✅ Sniper + Story + Bias + DXY + FVG")
    errors = 0
    while True:
        try:
            data = get_all_data()
            res = analyze(data)
            if not res:
                await asyncio.sleep(30)
                continue

            msg = f"""
💎 **XAUUSD V3.1 FINAL - LIVE**

💰 Price: ${res['price']} | {res['session']}
📊 Range: L ${res['low']} | H ${res['high']} | {round(res['range_pct'])}%
⏰ {res['time']}

━━━━━━━━━━━━━━━━
🧠 **WHAT MARKET IS DOING NOW:**
{res['now_txt']}

🌊 Order Flow: CVD {round(res['cvd'])} | {res['dxy']}
📦 POI: Bull FVG {res['fvg_bull'] or 'None'} | Bear FVG {res['fvg_bear'] or 'None'}
📈 Bias: {res['bias']} (Score {res['score']})

🔮 **WHAT WILL HAPPEN NEXT:**
{res['future']}

━━━━━━━━━━━━━━━━
⚔️ **SNIPER EXECUTION:**
{res['sniper']}

📰 News: Jackson Hole Powell 17:00 EAT Today - High Volatility
⚠️ Risk 0.5% | No chase - Wait POI

Bot: Lifetime 24/7 Active
"""
            if res['trigger']:
                await bot.send_message(chat_id=CHAT_ID, text=f"🚨🚨🚨 SNIPER ALERT 🚨🚨🚨\n{msg}")
            else:
                await bot.send_message(chat_id=CHAT_ID, text=msg)

            errors = 0
            await asyncio.sleep(300)
        except Exception as e:
            errors += 1
            print(f"Loop Error {errors}: {e}")
            await asyncio.sleep(min(30*errors, 300))

def run_flask():
    app.run(host='0.0.0.0', port=10000)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    asyncio.run(main_loop())
