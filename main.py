import os, csv, asyncio, tempfile, threading, io, uuid
from dotenv import load_dotenv
from apify_client import ApifyClient
from flask import Flask, render_template_string, request, send_file, jsonify
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler, CallbackQueryHandler
)

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
APIFY_TOKEN = os.getenv("APIFY_API_TOKEN")

# ══════════════════════════════════════════════
#   SHARED APIFY SCRAPER
# ══════════════════════════════════════════════
def scrape(location, keyword):
    client = ApifyClient(APIFY_TOKEN)
    run = client.actor("compass/crawler-google-places").call(run_input={
        "searchStringsArray": [f"{keyword} in {location}"],
        "maxCrawledPlacesPerSearch": 50,
        "language": "en",
        "includeHistogram": False,
        "includeOpeningHours": False,
        "includePeopleAlsoSearchFor": False,
    })
    leads = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        leads.append({
            "name":      item.get("title", "N/A"),
            "phone":     item.get("phone", "N/A"),
            "email":     item.get("email", "N/A"),
            "address":   item.get("address", "N/A"),
            "category":  item.get("categoryName", "N/A"),
            "rating":    item.get("totalScore", "N/A"),
            "reviews":   item.get("reviewsCount", "N/A"),
            "website":   item.get("website", "N/A"),
            "maps_link": item.get("url", "N/A"),
        })
    return leads

# ══════════════════════════════════════════════
#   WEB DASHBOARD (FLASK)
# ══════════════════════════════════════════════
flask_app = Flask(__name__)
jobs = {}

HTML = """
<!DOCTYPE html>
<html lang="bn">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Maps Lead Generator</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: sans-serif; background: #f0f4f8; min-height: 100vh; display: flex; flex-direction: column; align-items: center; padding: 2rem 1rem; }
  .card { background: white; border-radius: 12px; padding: 2rem; width: 100%; max-width: 500px; box-shadow: 0 2px 12px rgba(0,0,0,0.08); }
  h1 { font-size: 1.4rem; margin-bottom: 0.3rem; color: #1a1a2e; }
  p.sub { color: #666; font-size: 0.9rem; margin-bottom: 1.5rem; }
  label { font-size: 0.85rem; color: #444; display: block; margin-bottom: 0.3rem; }
  input { width: 100%; padding: 0.6rem 0.8rem; border: 1px solid #ddd; border-radius: 8px; font-size: 0.95rem; margin-bottom: 1rem; outline: none; }
  input:focus { border-color: #4f46e5; }
  button { width: 100%; padding: 0.75rem; background: #4f46e5; color: white; border: none; border-radius: 8px; font-size: 1rem; cursor: pointer; }
  button:hover { background: #4338ca; }
  button:disabled { background: #a5b4fc; cursor: not-allowed; }
  .status { margin-top: 1.2rem; padding: 0.8rem 1rem; border-radius: 8px; font-size: 0.9rem; display: none; }
  .status.running { background: #fef9c3; color: #854d0e; display: block; }
  .status.done { background: #dcfce7; color: #166534; display: block; }
  .status.error { background: #fee2e2; color: #991b1b; display: block; }
  .dl-btn { margin-top: 0.8rem; display: none; width: 100%; padding: 0.65rem; background: #16a34a; color: white; border: none; border-radius: 8px; font-size: 0.95rem; cursor: pointer; }
  .dl-btn:hover { background: #15803d; }
</style>
</head>
<body>
<div class="card">
  <h1>🗺️ Maps Lead Generator</h1>
  <p class="sub">Google Maps থেকে business leads বের করো</p>

  <label>📍 Location</label>
  <input id="loc" placeholder="যেমন: Gulshan Dhaka" />

  <label>🔍 Keyword</label>
  <input id="kw" placeholder="যেমন: restaurant" />

  <button id="btn" onclick="startScrape()">শুরু করো</button>

  <div class="status" id="status">⏳ Scraping চলছে... ৩–৫ মিনিট লাগবে</div>
  <button class="dl-btn" id="dl">📥 CSV Download করো</button>
</div>

<script>
let jobId = null;
let poll = null;

async function startScrape() {
  const loc = document.getElementById('loc').value.trim();
  const kw  = document.getElementById('kw').value.trim();
  if (!loc || !kw) { alert('Location আর Keyword দাও!'); return; }

  document.getElementById('btn').disabled = true;
  document.getElementById('dl').style.display = 'none';
  const st = document.getElementById('status');
  st.className = 'status running';
  st.textContent = '⏳ Scraping শুরু হয়েছে... ৩–৫ মিনিট লাগবে';

  const res = await fetch('/start', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({location: loc, keyword: kw})
  });
  const data = await res.json();
  jobId = data.job_id;

  poll = setInterval(checkStatus, 5000);
}

async function checkStatus() {
  if (!jobId) return;
  const res = await fetch(`/status/${jobId}`);
  const data = await res.json();
  const st = document.getElementById('status');

  if (data.status === 'done') {
    clearInterval(poll);
    st.className = 'status done';
    st.textContent = `✅ সম্পন্ন! ${data.count} টি lead পাওয়া গেছে।`;
    const dl = document.getElementById('dl');
    dl.style.display = 'block';
    dl.onclick = () => window.location = `/download/${jobId}`;
    document.getElementById('btn').disabled = false;
  } else if (data.status === 'error') {
    clearInterval(poll);
    st.className = 'status error';
    st.textContent = '❌ Error হয়েছে। আবার চেষ্টা করো।';
    document.getElementById('btn').disabled = false;
  }
}
</script>
</body>
</html>
"""

def run_scrape_flask(job_id, location, keyword):
    try:
        jobs[job_id] = {'status': 'running', 'leads': []}
        leads = scrape(location, keyword)
        jobs[job_id] = {'status': 'done', 'leads': leads, 'count': len(leads)}
    except Exception as e:
        jobs[job_id] = {'status': 'error', 'error': str(e)}

@flask_app.route('/')
def index():
    return render_template_string(HTML)

@flask_app.route('/start', methods=['POST'])
def start_job():
    data = request.json
    job_id = str(uuid.uuid4())[:8]
    t = threading.Thread(target=run_scrape_flask, args=(job_id, data['location'], data['keyword']))
    t.daemon = True
    t.start()
    return jsonify({'job_id': job_id})

@flask_app.route('/status/<job_id>')
def status(job_id):
    job = jobs.get(job_id, {'status': 'not_found'})
    return jsonify({'status': job['status'], 'count': job.get('count', 0)})

@flask_app.route('/download/<job_id>')
def download(job_id):
    job = jobs.get(job_id)
    if not job or job['status'] != 'done':
        return "Not ready", 400
    leads = job['leads']
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=leads[0].keys())
    writer.writeheader()
    writer.writerows(leads)
    out.seek(0)
    return send_file(
        io.BytesIO(out.getvalue().encode('utf-8-sig')),
        mimetype='text/csv',
        as_attachment=True,
        download_name='leads.csv'
    )

# ══════════════════════════════════════════════
#   TELEGRAM BOT 
# ══════════════════════════════════════════════
LOCATION, KEYWORD, CONFIRM = range(3)
store = {}

def to_csv(leads):
    tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8-sig', newline='')
    writer = csv.DictWriter(tmp, fieldnames=leads[0].keys())
    writer.writeheader()
    writer.writerows(leads)
    tmp.close()
    return tmp.name

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 *Google Maps Lead Bot*\n\nশুরু করতে /generate লেখো।", parse_mode='Markdown')

async def gen_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📍 Location দাও:\nউদাহরণ: `Gulshan Dhaka`", parse_mode='Markdown')
    return LOCATION

async def get_loc(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    store[update.message.from_user.id] = {'location': update.message.text.strip()}
    await update.message.reply_text("🔍 Keyword দাও:\nউদাহরণ: `restaurant`", parse_mode='Markdown')
    return KEYWORD

async def get_kw(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.message.from_user.id
    store[uid]['keyword'] = update.message.text.strip()
    loc, kw = store[uid]['location'], store[uid]['keyword']
    kb = [[InlineKeyboardButton("✅ শুরু", callback_data="go"), InlineKeyboardButton("❌ বাতিল", callback_data="no")]]
    await update.message.reply_text(f"📍 `{loc}` → 🔍 `{kw}`\n\nশুরু করবো?", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))
    return CONFIRM

async def confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    if q.data == "no":
        await q.edit_message_text("❌ বাতিল।")
        return ConversationHandler.END

    loc, kw = store[uid]['location'], store[uid]['keyword']
    msg = await q.edit_message_text(f"⏳ *Scraping চলছে...*\n📍 {loc} → 🔍 {kw}\n\n_৩–৫ মিনিট লাগতে পারে_", parse_mode='Markdown')
    
    try:
        loop = asyncio.get_event_loop()
        leads = await loop.run_in_executor(None, scrape, loc, kw)
        
        if not leads:
            await ctx.bot.edit_message_text(chat_id=q.message.chat_id, message_id=msg.message_id, text="😔 কোনো result নেই।")
            return ConversationHandler.END

        path = to_csv(leads)
        em = sum(1 for l in leads if str(l.get('email','')) not in ('N/A','','None'))
        ph = sum(1 for l in leads if str(l.get('phone','')) not in ('N/A','','None'))

        await ctx.bot.edit_message_text(chat_id=q.message.chat_id, message_id=msg.message_id, text="✅ হয়ে গেছে! পাঠাচ্ছি...")
        with open(path, 'rb') as f:
            await ctx.bot.send_document(
                chat_id=q.message.chat_id, document=f,
                filename=f"leads_{loc}_{kw}.csv".replace(' ', '_'),
                caption=f"🎯 *{loc}* — *{kw}*\n📊 Total: *{len(leads)}* | 📧 Email: *{em}* | 📞 Phone: *{ph}*\n\nনতুন search → /generate",
                parse_mode='Markdown'
            )
        os.unlink(path)
    except Exception as e:
        await ctx.bot.edit_message_text(chat_id=q.message.chat_id, message_id=msg.message_id, text=f"❌ Error: `{e}`", parse_mode='Markdown')
    return ConversationHandler.END

async def cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ বাতিল।")
    return ConversationHandler.END

def run_telegram_bot():
    """Bot কে আলাদা থ্রেডে চালানোর জন্য"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    app = Application.builder().token(TOKEN).build()
    conv = ConversationHandler(
        entry_points=[CommandHandler("generate", gen_start)],
        states={
            LOCATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_loc)],
            KEYWORD:  [MessageHandler(filters.TEXT & ~filters.COMMAND, get_kw)],
            CONFIRM:  [CallbackQueryHandler(confirm)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_message=False,
    )
    app.add_handler(CommandHandler("start", start))
    app.add_handler(conv)
    print("✅ Telegram Bot চালু হয়েছে!")
    
    # drop_pending_updates=True দিলে আগের আটকে থাকা conflict মেসেজগুলো ডিলিট হয়ে ফ্রেশভাবে শুরু হবে
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

# ══════════════════════════════════════════════
#   MAIN RUNNER (Starts Both)
# ══════════════════════════════════════════════
if __name__ == "__main__":
    # 1. Telegram Bot কে ব্যাকগ্রাউন্ডে চালু করো
    bot_thread = threading.Thread(target=run_telegram_bot, daemon=True)
    bot_thread.start()

    # 2. Flask Dashboard কে মেইন থ্রেডে চালু করো (Render এর Port Error সলভ করতে)
    port = int(os.environ.get("PORT", 10000))
    print(f"✅ Web Dashboard চালু হয়েছে! Port: {port}")
    flask_app.run(host='0.0.0.0', port=port)
