"""
GOLD KILLER PRO V4.2 - AUTO CHART IMAGE + ICT + ORDER FLOW
Sends chart photo every update
"""
import requests, asyncio, threading, os
from datetime import datetime
import pytz
from telegram import Bot
from flask import Flask
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

TELEGRAM_TOKEN = "8747660197:AAEqz0C7bg2ntLm_Hf0r4o7NuXVicSK7P5M"
CHAT_ID = "7168775421"
TWELVE_DATA_KEY = "abb27fe4fa8749d8a20a042ef4d100ee"

bot = Bot(token=TELEGRAM_TOKEN)
app = Flask(__name__)
EAT = pytz.timezone('Africa/Kampala')

@app.route('/')
def home(): return "PRO V4.2 CHART ONLINE"

def fetch(tf, size=100):
    try:
        url = f"https://api.twelvedata.com/time_series?symbol=XAU/USD&interval={tf}&outputsize={size}&apikey={TWELVE_DATA_KEY}"
        return requests.get(url, timeout=20).json().get('values', [])
    except: return []

def fetch_dxy():
    try:
        url = f"https://api.twelvedata.com/time_series?symbol=DXY&interval=15min&outputsize=10&apikey={TWELVE_DATA_KEY}"
        return requests.get(url, timeout=15).json().get('values', [])
    except: return []

def generate_chart(d5, bsl, ssl, h_mid, fvg_b, fvg_s, ote_top, ote_bot, price):
    try:
        # Take last 80 candles
        candles = d5[:80][::-1]
        times = list(range(len(candles)))
        opens = [float(c['open']) for c in candles]
        closes = [float(c['close']) for c in candles]
        highs = [float(c['high']) for c in candles]
        lows = [float(c['low']) for c in candles]

        fig, ax = plt.subplots(figsize=(12,6), facecolor='#0e0e0e')
        ax.set_facecolor('#0e0e0e')

        # Plot candles as lines + bodies
        for i in range(len(candles)):
            color = '#00ff88' if closes[i] >= opens[i] else '#ff3344'
            ax.plot([times[i], times[i]], [lows[i], highs[i]], color=color, linewidth=1, alpha=0.7)
            ax.plot([times[i], times[i]], [opens[i], closes[i]], color=color, linewidth=3)

        # LEVELS
        ax.axhline(bsl, color='#ffcc00', linestyle='--', linewidth=1.2, label=f'BSL {bsl}')
        ax.axhline(ssl, color='#ffcc00', linestyle='--', linewidth=1.2, label=f'SSL {ssl}')
        ax.axhline(h_mid, color='#8888ff', linestyle=':', linewidth=1, label=f'50% {round(h_mid,2)}')
        ax.axhline(price, color='white', linestyle='-', linewidth=0.8, alpha=0.5, label=f'NOW {price}')

        if fvg_b:
            ax.axhspan(fvg_b-1, fvg_b+1, color='#00ff88', alpha=0.15)
            ax.axhline(fvg_b, color='#00ff88', linestyle='-', linewidth=1, label=f'Bull FVG {round(fvg_b,2)}')
        if fvg_s:
            ax.axhspan(fvg_s-1, fvg_s+1, color='#ff3344', alpha=0.15)
            ax.axhline(fvg_s, color='#ff3344', linestyle='-', linewidth=1, label=f'Bear FVG {round(fvg_s,2)}')

        # OTE Zone
        ax.axhspan(ote_bot, ote_top, color='#ffaa00', alpha=0.1)
        ax.axhline(ote_top, color='#ffaa00', linestyle=':', linewidth=0.8)
        ax.axhline(ote_bot, color='#ffaa00', linestyle=':', linewidth=0.8, label=f'OTE {round(ote_bot,2)}-{round(ote_top,2)}')

        ax.set_title(f'XAUUSD 5M PRO V4.2 | ${price} | BSL {bsl} SSL {ssl}', color='white', fontsize=12, fontweight='bold')
        ax.tick_params(colors='white')
        ax.legend(facecolor='#1a1a1a', edgecolor='white', labelcolor='white', fontsize=7, loc='upper left')
        ax.grid(True, alpha=0.15, color='white')

        plt.tight_layout()
        path = "/tmp/gold_pro_chart.png"
        plt.savefig(path, dpi=150, facecolor='#0e0e0e')
        plt.close()
        return path
    except Exception as e:
        print(f"Chart err {e}")
        return None

def pro_analysis():
    d5 = fetch("5min", 80)
    d15 = fetch("15min", 80)
    d1h = fetch("1h", 100)
    d1d = fetch("1day", 20)
    dxy = fetch_dxy()
    if not d5 or not d1h: return None

    price = float(d5[0]['close'])
    h_high = max([float(x['high']) for x in d1h[:24]])
    h_low = min([float(x['low']) for x in d1h[:24]])
    h_mid = (h_high+h_low)/2
    fib_range = h_high - h_low
    ote_top = h_high - fib_range*0.62
    ote_bot = h_high - fib_range*0.79

    # FVG simple
    fvg_b, fvg_s = None, None
    for i in range(1, len(d15)-1):
        try:
            if float(d15[i+1]['low']) > float(d15[i-1]['high']):
                fvg_b = (float(d15[i-1]['high'])+float(d15[i+1]['low']))/2
                break
        except: pass
    for i in range(1, len(d15)-1):
        try:
            if float(d15[i+1]['high']) < float(d15[i-1]['low']):
                fvg_s = (float(d15[i-1]['low'])+float(d15[i+1]['high']))/2
                break
        except: pass

    vol_bull = sum([float(x['volume']) for x in d5[:20] if float(x['close'])>float(x['open'])])
    vol_bear = sum([float(x['volume']) for x in d5[:20] if float(x['close'])<float(x['open'])])
    cvd = vol_bull - vol_bear
    delta = (cvd/(vol_bull+vol_bear)*100) if (vol_bull+vol_bear)>0 else 0
    price_chg = float(d5[0]['close']) - float(d5[10]['close'])
    vol_10 = sum([float(x['volume']) for x in d5[:10]])
    effort = "ABSORPTION" if abs(price_chg)<2 and vol_10>sum([float(x['volume']) for x in d5[10:20]])*1.5 else "TRENDING"

    is_discount = price < h_mid
    in_ote = ote_bot <= price <= ote_top
    score=50
    if is_discount: score+=15
    else: score-=15
    if cvd>0: score+=10
    else: score-=10
    if in_ote and is_discount: score+=15
    if effort=="ABSORPTION" and price>h_mid and cvd<0: score-=20
    if effort=="ABSORPTION" and price<h_mid and cvd>0: score+=20

    if score>=75: bias="A++ STRONG BUY"
    elif score>=60: bias="A BUY"
    elif score>=45: bias="B NEUTRAL BUY"
    elif score>=35: bias="B NEUTRAL SELL"
    elif score>=20: bias="A SELL"
    else: bias="A++ STRONG SELL"

    h_eat = datetime.now(EAT).hour
    kz="🔥 NY KZ" if 15 <= h_eat < 17 else "LONDON KZ" if 10 <= h_eat < 13 else "OFF KZ"

    dxy_txt="DXY Flat"
    dxy_bias="NEUTRAL"
    if dxy and len(dxy)>=2:
        chg=float(dxy[0]['close'])-float(dxy[1]['close'])
        dxy_bias="DXY UP = GOLD SELL" if chg>0 else "DXY DOWN = GOLD BUY"
        dxy_txt=f"DXY {float(dxy[0]['close'])} {'↑' if chg>0 else '↓'}"

    if is_discount and fvg_b and cvd>0 and price <= fvg_b*1.002:
        sniper=f"🎯 A++ BUY ACTIVE Entry {round(fvg_b,2)} SL {round(h_low-3,2)} TP1 {round(h_mid,2)} TP2 {round(h_high,2)} Score {score}"
        trig=True
    elif not is_discount and fvg_s and cvd<0 and price >= fvg_s*0.998:
        sniper=f"🎯 A++ SELL ACTIVE Entry {round(fvg_s,2)} SL {round(h_high+3,2)} TP1 {round(h_mid,2)} TP2 {round(h_low,2)} Score {score}"
        trig=True
    else:
        sniper=f"⏳ WAIT Score {score}/100 - OTE:{'YES' if in_ote else 'NO'} FVG:{bool(fvg_b or fvg_s)} CVD:{round(delta)}% {kz}"
        trig=False

    chart_path = generate_chart(d5, h_high, h_low, h_mid, fvg_b, fvg_s, ote_top, ote_bot, price)

    return {"price":price,"bias":bias,"score":score,"sniper":sniper,"trig":trig,"chart":chart_path,"time":datetime.now(EAT).strftime('%H:%M:%S EAT'),"dxy":dxy_txt,"dxy_bias":dxy_bias,"bsl":h_high,"ssl":h_low,"h_mid":h_mid,"fvg_b":fvg_b,"fvg_s":fvg_s,"cvd":cvd,"delta":delta,"kz":kz}

async def loop():
    await bot.send_message(chat_id=CHAT_ID, text="🏦 PRO V4.2 CHART MODE ONLINE\nNow sending marked chart photo every 3 mins 🔥")
    while True:
        try:
            r=pro_analysis()
            if not r:
                await asyncio.sleep(30); continue
            caption=f"""
🏦 **PRO V4.2 + CHART**

💰 ${r['price']} | {r['bias']} ({r['score']}/100)
⏰ {r['time']} | {r['kz']} | {r['dxy']} | {r['dxy_bias']}
CVD {round(r['cvd'])} ({round(r['delta'])}%) | BSL {r['bsl']} SSL {r['ssl']}

⚔️ {r['sniper']}

Yellow = BSL/SSL | Green = Bull FVG | Red = Bear FVG | Orange = OTE 62-79%
"""
            if r['chart'] and os.path.exists(r['chart']):
                with open(r['chart'], 'rb') as photo:
                    if r['trig']:
                        await bot.send_photo(chat_id=CHAT_ID, photo=photo, caption=f"🚨 A++ {r['score']}/100 🚨\n{caption}")
                    else:
                        await bot.send_photo(chat_id=CHAT_ID, photo=photo, caption=caption)
            else:
                await bot.send_message(chat_id=CHAT_ID, text=caption)
        except Exception as e:
            print(f"ERR {e}")
            await asyncio.sleep(10)
        await asyncio.sleep(180)

def run_flask(): app.run(host='0.0.0.0', port=10000)

if __name__=="__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    asyncio.run(loop())
