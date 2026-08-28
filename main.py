import requests, asyncio, threading, os
from telegram import Bot
from flask import Flask
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

TELEGRAM_TOKEN = "8747660197:AAEqz0C7bg2ntLm_Hf0r4o7NuXVicSK7P5M"
CHAT_ID = "7168775421"
TWELVE_DATA_KEY = "abb27fe4fa8749d8a20a042ef4d100ee"

bot = Bot(token=TELEGRAM_TOKEN)
app = Flask(__name__)

@app.route('/')
def home(): return "CHART TEST ONLINE"

def fetch():
    url = f"https://api.twelvedata.com/time_series?symbol=XAU/USD&interval=5min&outputsize=80&apikey={TWELVE_DATA_KEY}"
    try:
        return requests.get(url, timeout=20).json().get('values', [])
    except: return []

async def loop():
    await bot.send_message(chat_id=CHAT_ID, text="🧪 CHART TEST STARTING - will send chart in 10 sec")
    await asyncio.sleep(5)
    
    while True:
        try:
            data = fetch()
            if not data:
                await bot.send_message(chat_id=CHAT_ID, text="❌ API fetch failed - TwelveData limit?")
                await asyncio.sleep(60)
                continue

            candles = data[:80][::-1]
            closes = [float(c['close']) for c in candles]
            times = list(range(len(candles)))
            price = closes[-1]

            # SIMPLE CHART
            fig, ax = plt.subplots(figsize=(10,5), facecolor='black')
            ax.set_facecolor('black')
            ax.plot(times, closes, color='#00ff88', linewidth=2, label=f'XAUUSD ${price}')
            ax.set_title(f'GOLD TEST CHART ${price}', color='white')
            ax.tick_params(colors='white')
            ax.legend()
            ax.grid(True, alpha=0.2)
            path = "/tmp/test_chart.png"
            plt.savefig(path, dpi=120, facecolor='black')
            plt.close()

            with open(path, 'rb') as p:
                await bot.send_photo(chat_id=CHAT_ID, photo=p, caption=f"✅ CHART WORKING\nPrice ${price}\nIf you see this, Render + matplotlib works. Now we deploy PRO V4.2")

        except Exception as e:
            await bot.send_message(chat_id=CHAT_ID, text=f"❌ CHART ERROR: {e}")
            print(e)
        
        await asyncio.sleep(180)

def run_flask(): app.run(host='0.0.0.0', port=10000)

if __name__=="__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    asyncio.run(loop())
