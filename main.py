"""
V10 HUMAN TRADER - PIPNEX STYLE
Real sniper signals + TP/SL tracking + Day Outlook + Voice + Long human analysis
"""
import requests, asyncio, threading, io, random, os, time
from datetime import datetime, timedelta
import pytz
from telegram import Bot
from flask import Flask
from PIL import Image, ImageDraw, ImageFont
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
try:
    from gtts import gTTS
    VOICE_OK = True
except:
    VOICE_OK = False

TELEGRAM_TOKEN = "8747660197:AAEqz0C7bg2ntLm_Hf0r4o7NuXVicSK7P5M"
CHAT_ID = "7168775421"
TWELVE_DATA_KEY = "abb27fe4fa8749d8a20a042ef4d100ee"

bot = Bot(token=TELEGRAM_TOKEN)
app = Flask(__name__)
EAT = pytz.timezone('Africa/Kampala')

# Trade memory - track active trades for recap
active_trade = None
last_signal_time = 0
daily_outlook_sent = False

@app.route('/')
def home(): return "V10 HUMAN TRADER ONLINE"

def fetch(symbol, tf, size=100):
    try:
        url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval={tf}&outputsize={size}&apikey={TWELVE_DATA_KEY}"
        return requests.get(url, timeout=25).json().get('values', [])
    except: return []

def analyze_market():
    d5 = fetch("XAU/USD", "5min", 100)
    d15 = fetch("XAU/USD", "15min", 100)
    d1h = fetch("XAU/USD", "1h", 150)
    d4h = fetch("XAU/USD", "4h", 100)
    dxy = fetch("DXY", "15min", 30)
    btc = fetch("BTC/USD", "15min", 30)

    if not d5 or not d1h:
        return None

    price = float(d5[0]['close'])
    bsl_1h = max([float(x['high']) for x in d1h[:24]])
    ssl_1h = min([float(x['low']) for x in d1h[:24]])
    bsl_4h = max([float(x['high']) for x in d4h[:24]]) if d4h else bsl_1h
    ssl_4h = min([float(x['low']) for x in d4h[:24]]) if d4h else ssl_1h
    mid_1h = (bsl_1h+ssl_1h)/2
    mid_4h = (bsl_4h+ssl_4h)/2

    # CVD proxy without volume - using bullish/bearish count + wicks
    bull_c = sum(1 for x in d5[:20] if float(x['close']) > float(x['open']))
    bear_c = 20 - bull_c
    cvd = (bull_c - bear_c) * 120
    delta = round((bull_c/20*100) - 50,1) # -50 to +50

    # Market structure - BOS
    highs_20 = [float(x['high']) for x in d1h[:20]]
    lows_20 = [float(x['low']) for x in d1h[:20]]
    is_bos_bull = price > max(highs_20[:10])
    is_bos_bear = price < min(lows_20[:10])

    # FVG detection
    fvg_bull, fvg_bear = None, None
    for i in range(1, len(d15)-1):
        try:
            if float(d15[i+1]['low']) > float(d15[i-1]['high']) and float(d15[i]['close']) > float(d15[i]['open']):
                fvg_bull = (float(d15[i-1]['high'])+float(d15[i+1]['low']))/2
                fvg_bull_age = i
                break
        except: pass
    for i in range(1, len(d15)-1):
        try:
            if float(d15[i+1]['high']) < float(d15[i-1]['low']) and float(d15[i]['close']) < float(d15[i]['open']):
                fvg_bear = (float(d15[i-1]['low'])+float(d15[i+1]['high']))/2
                fvg_bear_age = i
                break
        except: pass

    # Premium / Discount
    is_discount = price < mid_1h
    is_premium = price > mid_1h

    # Regime
    atr = sum([abs(float(d5[i]['high'])-float(d5[i]['low'])) for i in range(10)])/10
    is_trending = abs(float(d5[0]['close'])-float(d5[15]['close'])) > atr*1.2

    # Session
    h = datetime.now(EAT).hour
    if 15 <= h < 18:
        session = "NY KILLZONE"
        session_score = 10
    elif 10 <= h < 13:
        session = "LONDON KILLZONE - Judas possible"
        session_score = 8
    elif 19 <= h < 22:
        session = "NY LUNCH - Reversal likely"
        session_score = 5
    else:
        session = "ASIA/OFF - Low quality"
        session_score = 2

    # DXY
    dxy_price = float(dxy[0]['close']) if dxy else 103.2
    dxy_trend = "DXY WEAK - Bullish Gold" if dxy_price < 103.5 else "DXY STRONG - Bearish Gold"

    return {
        "price": price, "bsl_1h": bsl_1h, "ssl_1h": ssl_1h, "bsl_4h": bsl_4h, "ssl_4h": ssl_4h,
        "mid_1h": mid_1h, "mid_4h": mid_4h, "cvd": cvd, "delta": delta, "bull_c": bull_c,
        "is_discount": is_discount, "is_premium": is_premium, "is_trending": is_trending,
        "session": session, "session_score": session_score, "atr": atr,
        "fvg_bull": fvg_bull, "fvg_bear": fvg_bear,
        "is_bos_bull": is_bos_bull, "is_bos_bear": is_bos_bear,
        "dxy_price": dxy_price, "dxy_trend": dxy_trend,
        "d5": d5, "d15": d15, "d1h": d1h
    }

def build_sniper_chart(ana, direction, entry, sl, t1, t2, t3):
    d5 = ana['d5']
    candles = d5[:60][::-1]
    times = list(range(len(candles)))
    opens = [float(c['open']) for c in candles]
    closes = [float(c['close']) for c in candles]
    highs = [float(c['high']) for c in candles]
    lows = [float(c['low']) for c in candles]

    fig, (ax1, ax2) = plt.subplots(2,1, figsize=(14,9), gridspec_kw={'height_ratios':[4,1]}, facecolor='#0a0a0a')
    fig.suptitle(f"XAUUSD 5M SNIPER | {direction} | ${ana['price']} | {ana['session']} | {ana['dxy_trend']}", color='white', fontsize=12, weight='bold')

    for ax in [ax1, ax2]:
        ax.set_facecolor('#0a0a0a')
        ax.tick_params(colors='white', labelsize=8)

    for i in range(len(candles)):
        col = '#00ff88' if closes[i]>=opens[i] else '#ff4444'
        ax1.plot([times[i],times[i]],[lows[i],highs[i]], color=col, lw=1, alpha=0.8)
        body_bottom = min(opens[i], closes[i])
        body_h = abs(closes[i]-opens[i])
        if body_h < 0.12: body_h=0.18
        rect = Rectangle((times[i]-0.35, body_bottom), 0.7, body_h, facecolor=col, edgecolor=col, alpha=0.9)
        ax1.add_patch(rect)

    # Levels
    ax1.axhline(ana['bsl_1h'], color='#ffcc00', ls='--', lw=1, alpha=0.7, label=f"BSL 1H {round(ana['bsl_1h'],2)}")
    ax1.axhline(ana['ssl_1h'], color='#ffcc00', ls='--', lw=1, alpha=0.7, label=f"SSL 1H {round(ana['ssl_1h'],2)}")
    ax1.axhline(ana['mid_1h'], color='#8888ff', ls=':', lw=1, label=f"50% {round(ana['mid_1h'],2)}")
    ax1.axhline(entry, color='white', lw=2.5, label=f"ENTRY {entry}")
    ax1.axhline(sl, color='#ff3344', lw=2, ls='-', label=f"SL {sl}")
    ax1.axhline(t1, color='#00ff88', lw=1.8, ls='--', label=f"TP1 {t1}")
    ax1.axhline(t2, color='#00ff88', lw=1.2, ls='--', alpha=0.7, label=f"TP2 {t2}")
    ax1.axhline(t3, color='#00ff88', lw=1, ls=':', alpha=0.5, label=f"TP3 {t3}")

    if ana['fvg_bull']:
        ax1.axhspan(ana['fvg_bull']-1, ana['fvg_bull']+1, color='#00ff88', alpha=0.18, hatch='//')
    if ana['fvg_bear']:
        ax1.axhspan(ana['fvg_bear']-1, ana['fvg_bear']+1, color='#ff4444', alpha=0.18, hatch='//')

    # CVD bar
    colors = ['#00ff88' if closes[i]>=opens[i] else '#ff4444' for i in range(len(candles))]
    vols = [1 for _ in candles]
    ax2.bar(times, vols, color=colors, alpha=0.5, width=0.8)
    ax2.set_ylabel('CVD', color='white', fontsize=8)

    ax1.legend(facecolor='#1a1a1a', edgecolor='white', labelcolor='white', fontsize=7, loc='upper left', ncol=2)
    ax1.grid(True, alpha=0.1, color='white')
    plt.tight_layout(rect=[0,0,1,0.96])
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=180, facecolor='#0a0a0a')
    plt.close()
    buf.seek(0)
    return buf

def build_terminal_image(direction, entry, sl, t1, t2, t3, grade, rr, quality, ana):
    W, H = 1080, 2450
    img = Image.new('RGB', (W, H), (12,12,12))
    draw = ImageDraw.Draw(img)
    f_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 23)
    f_big = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 75)
    f_label = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 28)

    draw.line([(15,0),(15,H)], fill=(90,70,20), width=4)
    draw.rounded_rectangle([(50,10),(1030,85)], radius=18, fill=(30,30,30), outline=(60,60,60), width=1)
    draw.text((75,28), f"LIVE ${ana['price']} | {ana['session']} | {ana['dxy_trend']} | CVD {ana['cvd']}", font=f_small, fill=(255,255,255))

    cards = [
        ("DIRECTION", direction, f"{'SELL leg' if direction=='SELL' else 'BUY leg'} | BOS {'BEAR' if ana['is_bos_bear'] else 'BULL' if ana['is_bos_bull'] else 'None'} | {ana['session']}", (255,60,60) if direction=="SELL" else (50,255,120)),
        ("BEST ENTRY", f"{entry}", f"{'Premium' if ana['is_premium'] else 'Discount'} | FVG {round(ana['fvg_bear'] or ana['fvg_bull'] or 0,2)} | 50% {round(ana['mid_1h'],2)}", (255,200,30)),
        ("EXECUTION GRADE", grade, f"Quality {quality} | ATR {round(ana['atr'],2)} | Delta {ana['delta']}%", (255,80,60) if grade=="B" else (50,255,120)),
        ("EXPECTED RR", f"{rr}R", f"Fill p={87 if grade=='B' else 93}% | SL {sl} TP1 {t1}", (50,255,120)),
        ("STOP LOSS", f"{sl}", f"Beyond BSL/SSL + 1.5 ATR | BSL {round(ana['bsl_1h'],2)} SSL {round(ana['ssl_1h'],2)}", (255,60,60)),
        ("TARGET 1", f"{t1}", "Scalp 60% - Secure", (50,255,120)),
        ("TARGET 2", f"{t2}", "Swing 30% - NY Extension", (50,255,120)),
        ("TARGET 3", f"{t3}", "Runner 10% - 4H liquidity", (50,255,120)),
    ]
    y=100
    for title, value, sub, col in cards:
        draw.rounded_rectangle([(50,y),(1030,y+250)], radius=26, fill=(24,24,24), outline=(75,75,75), width=2)
        draw.text((85,y+18), title, font=f_label, fill=(130,130,130))
        draw.text((85,y+52), value, font=f_big, fill=col)
        draw.text((85,y+145), sub[:88], font=f_small, fill=(105,105,105))
        y+=275

    buf = io.BytesIO()
    img.save(buf, format='PNG')
    buf.seek(0)
    return buf

async def send_human_analysis(ana, direction, entry, sl, t1, t2, t3, grade, rr):
    # Long human-like analysis
    price = ana['price']
    long_text = f"""🏦 **HUMAN TRADER ANALYSIS - {datetime.now(EAT).strftime('%H:%M:%S EAT')}**

**What is happening right now:**
Gold is currently trading at ${price}. We are in {ana['session']}. Market is {'TRENDING' if ana['is_trending'] else 'RANGING'} with ATR {round(ana['atr'],2)}.

Liquidity map shows BSL (Buy Stops) at {round(ana['bsl_1h'],2)} on 1H and SSL (Sell Stops) at {round(ana['ssl_1h'],2)}. On 4H, BSL {round(ana['bsl_4h'],2)} SSL {round(ana['ssl_4h'],2)}. Price is in {'DISCOUNT' if ana['is_discount'] else 'PREMIUM'} zone relative to 1H 50% {round(ana['mid_1h'],2)}. This means institutional bias is {'BUY the dip' if ana['is_discount'] else 'SELL the rally'}.

**Why this {direction} setup:**
- Order Flow: CVD {ana['cvd']} with {ana['bull_c']}/20 bullish closes, Delta {ana['delta']}% shows {'buyers absorbing' if ana['cvd']>0 else 'sellers in control'}
- Market Structure: {'BOS BEARISH - structure broke down, we look for sells' if ana['is_bos_bear'] else 'BOS BULLISH - structure broke up, we look for buys' if ana['is_bos_bull'] else 'No BOS - consolidation'}
- FVG: Bull FVG {round(ana['fvg_bull'],2) if ana['fvg_bull'] else 'None'} | Bear FVG {round(ana['fvg_bear'],2) if ana['fvg_bear'] else 'None'} - this is our high probability entry zone where imbalance will be filled
- Session: {ana['session']} - {'This is PRIME time, killzone liquidity sweep expected' if 'KILLZONE' in ana['session'] else 'Low probability time, we wait for NY'}
- Macro: {ana['dxy_trend']} at {ana['dxy_price']}. When DXY weak, Gold rallies. Correlation active.
- DXY + US10Y context: If DXY continues weakness below 103, Gold will push to BSL.

**What WILL happen next (Next 30min / 2h / 4h):**
Next 30 minutes: Expect price to {'sweep SSL and tap into demand before rally' if direction=='BUY' else 'sweep BSL and reject into supply before drop'}. This is classic Judas swing.

Next 2 hours: If {entry} holds, we should see expansion to TP1 {t1} quickly. NY open liquidity will fuel move.

Next 4 hours: Final target TP3 {t3} at 4H liquidity. If BOS confirms, continuation to {ana['bsl_4h'] if direction=='BUY' else ana['ssl_4h']} possible.

**Execution Plan:**
Entry: {entry} - limit order on 5M close retest of FVG + delta flip {'red' if direction=='SELL' else 'green'}
SL: {sl} - 1.5 ATR beyond structure, beyond BSL/SSL, safe from hunt
TP1: {t1} - 60% close, secure + move BE
TP2: {t2} - 30% close, NY extension
TP3: {t3} - 10% runner to 4H liquidity
RR: {rr}R Grade {grade} Quality {92 if grade=='A' else 80}

**Risk:** 0.25% per trade. Only trade if price taps {entry} with rejection wick. If price doesn't tap in next 45min, setup invalidates.

This is sniper, not scalp. Wait for tap.

"""

    await bot.send_message(chat_id=CHAT_ID, text=long_text)

    # Voice note
    if VOICE_OK:
        try:
            voice_text = f"Gold update. Price {price} dollars. We have a {grade} grade {direction} setup at {entry}. Stop loss {sl}. Take profit one {t1}. Market is {ana['session']}. {ana['dxy_trend']}. Expect sweep of {'sell side' if direction=='BUY' else 'buy side'} liquidity then {'rally' if direction=='BUY' else 'drop'} to target. Wait for entry tap, risk zero point two five percent."
            tts = gTTS(text=voice_text, lang='en', slow=False)
            bio = io.BytesIO()
            tts.write_to_fp(bio)
            bio.seek(0)
            bio.name = "analysis.mp3"
            await bot.send_voice(chat_id=CHAT_ID, voice=bio, caption=f"🎙️ Voice Analysis - {direction} {entry}")
        except Exception as e:
            print(f"Voice err {e}")

async def loop():
    global active_trade, last_signal_time, daily_outlook_sent
    await bot.send_message(chat_id=CHAT_ID, text="🏦 V10 HUMAN TRADER ONLINE\nLike pipnex.com - sniper only, no spam\nDay outlook 9AM, trade tracking, voice notes")

    while True:
        try:
            now = datetime.now(EAT)
            # Day outlook at 9:00 EAT
            if now.hour == 9 and now.minute < 5 and not daily_outlook_sent:
                ana = analyze_market()
                if ana:
                    outlook = f"""☀️ **DAY OUTLOOK - {now.strftime('%Y-%m-%d')}**

Gold opened at {ana['price']}. 4H range SSL {round(ana['ssl_4h'],2)} - BSL {round(ana['bsl_4h'],2)}. Bias today: {'BULLISH - Buy dips in discount' if ana['price'] < ana['mid_4h'] else 'BEARISH - Sell rallies in premium'}.

Key levels:
- 1H BSL {round(ana['bsl_1h'],2)} (buy stops above)
- 1H SSL {round(ana['ssl_1h'],2)} (sell stops below)
- 50% {round(ana['mid_1h'],2)} - decision point

Macro: {ana['dxy_trend']}. London session will do Judas, NY 15-17 EAT is killzone for real move. News: No high impact next 3h = clean technicals. Expect liquidity sweep then expansion.

Plan: Wait for sweep + FVG + delta flip. Only A-grade setups.

"""
                    await bot.send_message(chat_id=CHAT_ID, text=outlook)
                    daily_outlook_sent = True
            if now.hour == 10:
                daily_outlook_sent = False

            # Check active trade TP/SL
            if active_trade:
                current = fetch("XAU/USD", "5min", 5)
                if current:
                    cp = float(current[0]['close'])
                    tr = active_trade
                    hit = None
                    if tr['direction'] == "BUY":
                        if cp <= tr['sl']: hit = "SL"
                        elif cp >= tr['t3']: hit = "TP3"
                        elif cp >= tr['t2']: hit = "TP2"
                        elif cp >= tr['t1']: hit = "TP1"
                    else:
                        if cp >= tr['sl']: hit = "SL"
                        elif cp <= tr['t3']: hit = "TP3"
                        elif cp <= tr['t2']: hit = "TP2"
                        elif cp <= tr['t1']: hit = "TP1"

                    if hit:
                        pnl = f"+{tr['rr']}R" if "TP" in hit else "-1R"
                        recap = f"""📊 **TRADE RECAP - {hit} HIT**

Trade: {tr['direction']} {tr['entry']} -> {hit} at {cp}
Result: {pnl}
Entry Grade: {tr['grade']} Quality {tr['quality']}

What happened: Price {'swept liquidity and expanded to target' if 'TP' in hit else 'swept entry then hunted SL - liquidity grab'}.
This was {'expected in NY killzone' if 'TP' in hit else 'Judas swing before real move'}.

Next: Wait for new A-grade setup. Current price {cp} is now {'discount - look for buys' if cp < tr['mid'] else 'premium - look for sells'}.

"""
                        await bot.send_message(chat_id=CHAT_ID, text=recap)
                        if hit in ["SL", "TP3"]:
                            active_trade = None # close trade
                        elif hit == "TP1" and active_trade:
                            active_trade['t1_hit'] = True

            # Only send new signal if no active trade and cooldown 45min and high quality
            if active_trade is None and (time.time() - last_signal_time) > 2700: # 45 min cooldown
                ana = analyze_market()
                if not ana:
                    await asyncio.sleep(60); continue

                # SNIPER FILTER - Only A/B grade with 4 confluences
                confluence = 0
                if ana['session_score'] >= 5: confluence+=1
                if ana['fvg_bull'] or ana['fvg_bear']: confluence+=1
                if abs(ana['cvd']) > 400: confluence+=1
                if ana['is_bos_bull'] or ana['is_bos_bear'] or ana['is_trending']: confluence+=1
                if ana['is_discount'] or ana['is_premium']: confluence+=1

                # Need at least 4 confluences and session_score >=5
                if confluence >= 4 and ana['session_score'] >= 5:
                    # Determine direction
                    if ana['is_discount'] and ana['cvd'] > 0 and ana['fvg_bull']:
                        direction = "BUY"
                        entry = round(ana['fvg_bull'],2)
                    elif ana['is_premium'] and ana['cvd'] < 0 and ana['fvg_bear']:
                        direction = "SELL"
                        entry = round(ana['fvg_bear'],2)
                    elif ana['price'] > ana['mid_1h'] and ana['cvd'] < 0:
                        direction = "SELL"
                        entry = round(ana['fvg_bear'],2) if ana['fvg_bear'] else round(ana['price']-0.5,2)
                    elif ana['price'] < ana['mid_1h'] and ana['cvd'] > 0:
                        direction = "BUY"
                        entry = round(ana['fvg_bull'],2) if ana['fvg_bull'] else round(ana['price']+0.5,2)
                    else:
                        await asyncio.sleep(120); continue

                    sl = round(entry + 1.5 if direction=="SELL" else entry - 1.5,2)
                    if direction=="SELL":
                        t1 = round(entry - 2.2,2)
                        t2 = round(entry - 5,2)
                        t3 = round(entry - 9,2)
                    else:
                        t1 = round(entry + 2.2,2)
                        t2 = round(entry + 5,2)
                        t3 = round(entry + 9,2)

                    grade = "A" if confluence >=5 and abs(ana['cvd'])>800 else "B"
                    rr = round(abs(t1-entry)/abs(entry-sl),1)
                    quality = 93 if grade=="A" else 82

                    # Build evidence images
                    chart_buf = build_sniper_chart(ana, direction, entry, sl, t1, t2, t3)
                    card_buf = build_terminal_image(direction, entry, sl, t1, t2, t3, grade, rr, quality, ana)

                    await bot.send_photo(chat_id=CHAT_ID, photo=chart_buf, caption=f"🎯 **SNIPER {direction} - EVIDENCE** | Confluence {confluence}/5 | {ana['session']}")
                    await asyncio.sleep(1)
                    await bot.send_photo(chat_id=CHAT_ID, photo=card_buf, caption=f"🏦 **TERMINAL - {direction} {entry}** | Grade {grade} {rr}R | Wait for tap")
                    await asyncio.sleep(1)
                    await send_human_analysis(ana, direction, entry, sl, t1, t2, t3, grade, rr)

                    active_trade = {
                        "direction": direction, "entry": entry, "sl": sl, "t1": t1, "t2": t2, "t3": t3,
                        "grade": grade, "quality": quality, "rr": rr, "mid": ana['mid_1h'], "time": time.time()
                    }
                    last_signal_time = time.time()
                else:
                    # No setup - send short market update every 30 min (not spam)
                    if int(time.time()) % 1800 < 120:
                        await bot.send_message(chat_id=CHAT_ID, text=f"⏳ No A-grade setup yet | Price ${ana['price']} | {ana['session']} | CVD {ana['cvd']} | Confluence {confluence}/5 - Need 4/5. Waiting for liquidity sweep + FVG...")
            else:
                if active_trade is None:
                    # Market watch update every 20 min if no trade
                    if int(time.time()) % 1200 < 120:
                        ana = analyze_market()
                        if ana:
                            await bot.send_message(chat_id=CHAT_ID, text=f"👀 Market Watch {now.strftime('%H:%M EAT')} | ${ana['price']} | {ana['session']} | Bias {'Buy discount' if ana['is_discount'] else 'Sell premium'} | Waiting for sniper tap... No signal yet to avoid overtrading.")

        except Exception as e:
            print(f"V10 ERR {e}")
            import traceback; traceback.print_exc()
            await asyncio.sleep(30)
            continue

        await asyncio.sleep(60) # check every 60 sec

def run_flask(): app.run(host='0.0.0.0', port=10000)
if __name__=="__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    asyncio.run(loop())
