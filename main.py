import os, csv, asyncio, tempfile, threading, io, uuid, re, time
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from apify_client import ApifyClient
from flask import Flask, render_template_string, request, send_file, jsonify
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler, CallbackQueryHandler
)

load_dotenv()

# Global Config
CONFIG = {
    "TELEGRAM_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN"),
    "APIFY_TOKEN": os.getenv("APIFY_API_TOKEN", "")
}

# ══════════════════════════════════════════════
#   DEEP EMAIL EXTRACTOR (PYTHON FALLBACK)
# ══════════════════════════════════════════════
EMAIL_REGEX = r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

def extract_email_from_website(url):
    if not url or url == "N/A": return "N/A"
    try:
        # Check homepage
        r = requests.get(url, headers=HEADERS, timeout=8)
        emails = re.findall(EMAIL_REGEX, r.text)
        if emails: return emails[0]
        
        # Check contact page
        soup = BeautifulSoup(r.text, 'html.parser')
        contact_link = None
        for a in soup.find_all('a', href=True):
            if 'contact' in a['href'].lower():
                contact_link = a['href']
                break
        
        if contact_link:
            if not contact_link.startswith('http'):
                contact_link = url.rstrip('/') + '/' + contact_link.lstrip('/')
            r2 = requests.get(contact_link, headers=HEADERS, timeout=8)
            emails2 = re.findall(EMAIL_REGEX, r2.text)
            if emails2: return emails2[0]
    except:
        pass
    return "N/A"

# ══════════════════════════════════════════════
#   ADVANCED SCRAPER WITH FILTERS
# ══════════════════════════════════════════════
def scrape_advanced(location, keyword, max_leads=50, max_rating=None, min_reviews=None):
    if not CONFIG["APIFY_TOKEN"]:
        raise Exception("Apify API Token is missing! Please set it in settings.")
        
    client = ApifyClient(CONFIG["APIFY_TOKEN"])
    run = client.actor("compass/crawler-google-places").call(run_input={
        "searchStringsArray": [f"{keyword} in {location}"],
        "maxCrawledPlacesPerSearch": int(max_leads),
        "language": "en",
    })
    
    leads = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        rating = item.get("totalScore", 0)
        reviews = item.get("reviewsCount", 0)
        
        # Apply Filters
        if max_rating and rating and float(rating) > float(max_rating): continue
        if min_reviews and reviews and int(reviews) < int(min_reviews): continue
        
        website = item.get("website", "N/A")
        email = item.get("email")
        
        # Deep Email Extraction if Apify fails
        if not email and website != "N/A":
            email = extract_email_from_website(website)
            time.sleep(0.5) # Be polite to servers
            
        leads.append({
            "Name": item.get("title", "N/A"),
            "Phone": item.get("phone", "N/A"),
            "Email": email if email else "N/A",
            "Address": item.get("address", "N/A"),
            "Category": item.get("categoryName", "N/A"),
            "Rating": rating if rating else "N/A",
            "Reviews": reviews if reviews else "N/A",
            "Website": website,
            "Maps_Link": item.get("url", "N/A"),
        })
    return leads

# ══════════════════════════════════════════════
#   WEB DASHBOARD (FLASK + TAILWIND CSS)
# ══════════════════════════════════════════════
flask_app = Flask(__name__)
jobs = {}

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Pro Lead Gen Agent</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
</head>
<body class="bg-gray-50 text-gray-800 font-sans antialiased">
    <div class="max-w-5xl mx-auto p-4 sm:p-6 lg:p-8">
        <!-- Header -->
        <header class="flex justify-between items-center bg-white p-4 rounded-xl shadow-sm mb-6">
            <div class="flex items-center gap-3">
                <div class="bg-indigo-600 text-white p-3 rounded-lg"><i class="fa-solid fa-map-location-dot text-xl"></i></div>
                <h1 class="text-2xl font-bold text-gray-900">LeadGen Pro</h1>
            </div>
            <button onclick="switchTab('settings')" class="text-gray-500 hover:text-indigo-600 transition"><i class="fa-solid fa-gear text-xl"></i></button>
        </header>

        <!-- Tabs -->
        <div class="flex gap-4 mb-6">
            <button onclick="switchTab('manual')" id="tab-manual" class="flex-1 py-3 font-semibold rounded-lg bg-indigo-600 text-white shadow-md transition">Manual Search</button>
            <button onclick="switchTab('ai')" id="tab-ai" class="flex-1 py-3 font-semibold rounded-lg bg-white text-gray-600 shadow-sm hover:bg-gray-50 transition">AI Agent Search</button>
        </div>

        <!-- Manual Tab -->
        <div id="content-manual" class="bg-white p-6 rounded-xl shadow-sm">
            <h2 class="text-xl font-bold mb-4 border-b pb-2">Manual Search Parameters</h2>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
                <div><label class="block text-sm font-medium mb-1">Location *</label><input id="m-loc" type="text" class="w-full border rounded-lg p-2.5 focus:ring-2 focus:ring-indigo-500 outline-none" placeholder="e.g., New York, NY"></div>
                <div><label class="block text-sm font-medium mb-1">Keyword *</label><input id="m-kw" type="text" class="w-full border rounded-lg p-2.5 focus:ring-2 focus:ring-indigo-500 outline-none" placeholder="e.g., Real Estate Agency"></div>
                <div><label class="block text-sm font-medium mb-1">Number of Leads</label><input id="m-count" type="number" value="50" class="w-full border rounded-lg p-2.5 focus:ring-2 focus:ring-indigo-500 outline-none"></div>
                <div><label class="block text-sm font-medium mb-1">Max Rating (Optional)</label><input id="m-rating" type="number" step="0.1" class="w-full border rounded-lg p-2.5 focus:ring-2 focus:ring-indigo-500 outline-none" placeholder="e.g., 4.5"></div>
            </div>
            <button onclick="startManual()" id="btn-manual" class="w-full bg-indigo-600 hover:bg-indigo-700 text-white font-bold py-3 rounded-lg shadow transition">Start Scraping</button>
        </div>

        <!-- AI Tab -->
        <div id="content-ai" class="hidden bg-white rounded-xl shadow-sm flex flex-col h-[500px]">
            <div class="bg-indigo-600 text-white p-4 rounded-t-xl font-bold flex items-center gap-2">
                <i class="fa-solid fa-robot"></i> AI Lead Generation Agent
            </div>
            <div id="chat-box" class="flex-1 p-4 overflow-y-auto bg-gray-50 space-y-4">
                <div class="flex gap-3">
                    <div class="bg-indigo-100 text-indigo-800 p-3 rounded-lg rounded-tl-none max-w-[80%]">
                        Hello! I am your Google Maps Lead Generation Agent. Please tell me what you are looking for. <br><br>
                        <i>Example: "I need 100 leads for plumbers in Texas with a rating under 4.0"</i>
                    </div>
                </div>
            </div>
            <div class="p-4 bg-white border-t flex gap-2 rounded-b-xl">
                <input id="ai-input" type="text" class="flex-1 border rounded-lg p-3 focus:ring-2 focus:ring-indigo-500 outline-none" placeholder="Type your request here..." onkeypress="if(event.key === 'Enter') sendAI()">
                <button onclick="sendAI()" class="bg-indigo-600 text-white px-6 rounded-lg hover:bg-indigo-700 transition"><i class="fa-solid fa-paper-plane"></i></button>
            </div>
        </div>

        <!-- Settings Tab -->
        <div id="content-settings" class="hidden bg-white p-6 rounded-xl shadow-sm">
            <h2 class="text-xl font-bold mb-4 border-b pb-2">System Settings</h2>
            <label class="block text-sm font-medium mb-1">Apify API Token</label>
            <div class="flex gap-2">
                <input id="api-key" type="password" class="flex-1 border rounded-lg p-2.5 focus:ring-2 focus:ring-indigo-500 outline-none" placeholder="Enter new Apify Token">
                <button onclick="saveSettings()" class="bg-green-600 text-white px-4 rounded-lg hover:bg-green-700">Save</button>
            </div>
            <p class="text-xs text-gray-500 mt-2">Update this if your Apify credits run out.</p>
        </div>

        <!-- Status Area -->
        <div id="status-area" class="hidden mt-6 p-4 rounded-xl border">
            <div class="flex items-center gap-3 mb-2">
                <i id="status-icon" class="fa-solid fa-circle-notch fa-spin text-indigo-600 text-xl"></i>
                <span id="status-text" class="font-semibold text-gray-700">Processing...</span>
            </div>
            <button id="dl-btn" class="hidden w-full mt-3 bg-green-600 hover:bg-green-700 text-white font-bold py-2 rounded-lg transition"><i class="fa-solid fa-download"></i> Download CSV</button>
        </div>
    </div>

    <script>
        let currentJob = null;
        let aiState = {};

        function switchTab(tab) {
            ['manual', 'ai', 'settings'].forEach(t => {
                document.getElementById('content-'+t).classList.add('hidden');
                if(document.getElementById('tab-'+t)) {
                    document.getElementById('tab-'+t).className = 'flex-1 py-3 font-semibold rounded-lg bg-white text-gray-600 shadow-sm hover:bg-gray-50 transition';
                }
            });
            document.getElementById('content-'+tab).classList.remove('hidden');
            if(document.getElementById('tab-'+tab)) {
                document.getElementById('tab-'+tab).className = 'flex-1 py-3 font-semibold rounded-lg bg-indigo-600 text-white shadow-md transition';
            }
        }

        function saveSettings() {
            const key = document.getElementById('api-key').value;
            fetch('/api/settings', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({token: key}) })
            .then(() => alert('Settings Saved!'));
        }

        function showStatus(msg, isSpin=true, isError=false) {
            const area = document.getElementById('status-area');
            area.classList.remove('hidden');
            area.className = `mt-6 p-4 rounded-xl border ${isError ? 'bg-red-50 border-red-200' : 'bg-indigo-50 border-indigo-200'}`;
            document.getElementById('status-text').innerText = msg;
            document.getElementById('status-icon').className = isSpin ? 'fa-solid fa-circle-notch fa-spin text-indigo-600 text-xl' : (isError ? 'fa-solid fa-circle-xmark text-red-600 text-xl' : 'fa-solid fa-circle-check text-green-600 text-xl');
            document.getElementById('dl-btn').classList.add('hidden');
        }

        async function startJob(payload) {
            showStatus('Initializing scraper... This may take a few minutes if deep email extraction is running.');
            const res = await fetch('/api/scrape', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload) });
            const data = await res.json();
            if(data.error) return showStatus(data.error, false, true);
            
            currentJob = data.job_id;
            checkStatus();
        }

        function startManual() {
            const loc = document.getElementById('m-loc').value;
            const kw = document.getElementById('m-kw').value;
            if(!loc || !kw) return alert("Location and Keyword are required!");
            
            startJob({
                location: loc, keyword: kw,
                max_leads: document.getElementById('m-count').value || 50,
                max_rating: document.getElementById('m-rating').value || null
            });
        }

        async function checkStatus() {
            const res = await fetch('/api/status/' + currentJob);
            const data = await res.json();
            if(data.status === 'done') {
                showStatus(`Success! Found ${data.count} leads.`, false, false);
                const btn = document.getElementById('dl-btn');
                btn.classList.remove('hidden');
                btn.onclick = () => window.location = '/api/download/' + currentJob;
            } else if(data.status === 'error') {
                showStatus('Error: ' + data.error, false, true);
            } else {
                setTimeout(checkStatus, 5000);
            }
        }

        // --- AI Chat Logic ---
        function addMsg(text, isBot=false) {
            const box = document.getElementById('chat-box');
            const div = document.createElement('div');
            div.className = `flex gap-3 ${isBot ? '' : 'justify-end'}`;
            div.innerHTML = `<div class="${isBot ? 'bg-indigo-100 text-indigo-800 rounded-tl-none' : 'bg-gray-800 text-white rounded-tr-none'} p-3 rounded-lg max-w-[80%]">${text}</div>`;
            box.appendChild(div);
            box.scrollTop = box.scrollHeight;
        }

        function sendAI() {
            const inp = document.getElementById('ai-input');
            const text = inp.value.trim();
            if(!text) return;
            addMsg(text, false);
            inp.value = '';

            // Simple NLP Parsing Simulation
            setTimeout(() => {
                if(text.toLowerCase().includes('start') || text.toLowerCase() === 'yes') {
                    if(aiState.loc && aiState.kw) {
                        addMsg("Starting the automation now! Please check the status box below.", true);
                        startJob(aiState);
                    } else {
                        addMsg("I still need a location and keyword. What are you looking for?", true);
                    }
                    return;
                }

                // Extract logic
                let locMatch = text.match(/in ([a-zA-Z\s]+)/i);
                let kwMatch = text.match(/for ([a-zA-Z\s]+) in/i) || text.match(/(?:need|want) (?:leads )?(?:for )?([a-zA-Z\s]+) in/i);
                let countMatch = text.match(/(\d+) leads/i);
                let ratingMatch = text.match(/under (\d\.?\d?)/i);

                if(locMatch) aiState.loc = locMatch[1].trim();
                if(kwMatch) aiState.kw = kwMatch[1].trim();
                if(countMatch) aiState.max_leads = countMatch[1];
                if(ratingMatch) aiState.max_rating = ratingMatch[1];

                if(!aiState.loc && !aiState.kw) {
                    aiState.kw = text; // Assume they just typed keyword
                    addMsg(`Got it. Keyword is "${aiState.kw}". Which location?`, true);
                } else if(!aiState.loc) {
                    addMsg("Which location do you want to search in?", true);
                } else if(!aiState.kw) {
                    addMsg("What type of business (keyword) are you looking for?", true);
                } else {
                    let summary = `I understand. You want <b>${aiState.max_leads || 50}</b> leads for <b>${aiState.kw}</b> in <b>${aiState.loc}</b>.`;
                    if(aiState.max_rating) summary += ` (Rating under ${aiState.max_rating})`;
                    summary += `<br><br>Should I start the automation now? (Type 'yes' or 'start')`;
                    addMsg(summary, true);
                }
            }, 600);
        }
    </script>
</body>
</html>
"""

@flask_app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@flask_app.route('/api/settings', methods=['POST'])
def update_settings():
    CONFIG["APIFY_TOKEN"] = request.json.get('token')
    return jsonify({"success": True})

def run_scrape_thread(job_id, data):
    try:
        jobs[job_id] = {'status': 'running'}
        leads = scrape_advanced(
            data.get('location'), 
            data.get('keyword'),
            data.get('max_leads', 50),
            data.get('max_rating'),
            data.get('min_reviews')
        )
        jobs[job_id] = {'status': 'done', 'leads': leads, 'count': len(leads)}
    except Exception as e:
        jobs[job_id] = {'status': 'error', 'error': str(e)}

@flask_app.route('/api/scrape', methods=['POST'])
def start_api_job():
    data = request.json
    job_id = str(uuid.uuid4())[:8]
    t = threading.Thread(target=run_scrape_thread, args=(job_id, data))
    t.daemon = True
    t.start()
    return jsonify({'job_id': job_id})

@flask_app.route('/api/status/<job_id>')
def status(job_id):
    job = jobs.get(job_id, {'status': 'not_found'})
    return jsonify({'status': job['status'], 'count': job.get('count', 0), 'error': job.get('error')})

@flask_app.route('/api/download/<job_id>')
def download(job_id):
    job = jobs.get(job_id)
    if not job or job['status'] != 'done': return "Not ready", 400
    leads = job['leads']
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=leads[0].keys())
    writer.writeheader()
    writer.writerows(leads)
    out.seek(0)
    return send_file(io.BytesIO(out.getvalue().encode('utf-8-sig')), mimetype='text/csv', as_attachment=True, download_name='advanced_leads.csv')

# ══════════════════════════════════════════════
#   TELEGRAM BOT (MANUAL & AI)
# ══════════════════════════════════════════════
def to_csv(leads):
    tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8-sig', newline='')
    writer = csv.DictWriter(tmp, fieldnames=leads[0].keys())
    writer.writeheader()
    writer.writerows(leads)
    tmp.close()
    return tmp.name

# States
M_LOC, M_KW, M_COUNT, M_RATING = range(4)
AI_PROMPT = 10

bot_store = {}

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = [
        [InlineKeyboardButton("🛠️ Manual Search", callback_data="mode_manual")],
        [InlineKeyboardButton("🤖 AI Chat Search", callback_data="mode_ai")]
    ]
    await update.message.reply_text("👋 *Pro Lead Gen Bot*\n\nকীভাবে সার্চ করতে চাও?", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))

async def handle_mode(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    bot_store[uid] = {}
    
    if q.data == "mode_manual":
        await q.edit_message_text("📍 *Manual Mode*\nLocation দাও (e.g. Dhaka):", parse_mode='Markdown')
        return M_LOC
    else:
        await q.edit_message_text("🤖 *AI Agent Mode*\nআমাকে ইংরেজিতে বলো তুমি কী খুঁজছো।\n\n_Example: I need 100 leads for hospitals in Sylhet with rating under 4.0_", parse_mode='Markdown')
        return AI_PROMPT

# --- Manual Flow ---
async def m_loc(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    bot_store[update.message.from_user.id]['loc'] = update.message.text
    await update.message.reply_text("🔍 Keyword দাও (e.g. restaurant):")
    return M_KW

async def m_kw(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    bot_store[update.message.from_user.id]['kw'] = update.message.text
    await update.message.reply_text("🔢 কয়টা লিড লাগবে? (e.g. 50, 100, 200):")
    return M_COUNT

async def m_count(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    bot_store[update.message.from_user.id]['count'] = update.message.text
    await update.message.reply_text("⭐ Max Rating ফিল্টার করবে? (না চাইলে 'skip' লেখো, চাইলে e.g. 4.5 লেখো):")
    return M_RATING

async def m_rating(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.lower()
    uid = update.message.from_user.id
    bot_store[uid]['rating'] = None if txt == 'skip' else txt
    return await ask_confirm(update, uid)

# --- AI Flow ---
async def ai_prompt(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    uid = update.message.from_user.id
    
    loc_m = re.search(r'in ([a-zA-Z\s]+)', text, re.I)
    kw_m = re.search(r'for ([a-zA-Z\s]+) in', text, re.I) or re.search(r'(?:need|want) (?:leads )?(?:for )?([a-zA-Z\s]+) in', text, re.I)
    cnt_m = re.search(r'(\d+) leads', text, re.I)
    rat_m = re.search(r'under (\d\.?\d?)', text, re.I)
    
    if not loc_m or not kw_m:
        await update.message.reply_text("🤖 আমি ঠিক বুঝতে পারিনি। দয়া করে Keyword এবং Location পরিষ্কার করে বলো। (e.g. leads for plumbers in Dhaka)")
        return AI_PROMPT
        
    bot_store[uid] = {
        'loc': loc_m.group(1).strip(),
        'kw': kw_m.group(1).strip(),
        'count': cnt_m.group(1) if cnt_m else 50,
        'rating': rat_m.group(1) if rat_m else None
    }
    return await ask_confirm(update, uid)

# --- Execute ---
async def ask_confirm(update, uid):
    data = bot_store[uid]
    txt = f"📋 *Summary*\n📍 Loc: {data['loc']}\n🔍 Kw: {data['kw']}\n🔢 Leads: {data['count']}\n⭐ Max Rating: {data.get('rating') or 'None'}\n\nশুরু করবো?"
    kb = [[InlineKeyboardButton("✅ Start Automation", callback_data="start_scrape")]]
    await update.message.reply_text(txt, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))
    return ConversationHandler.END

async def execute_scrape(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    data = bot_store.get(uid)
    
    msg = await q.edit_message_text("⏳ *Scraping & Deep Email Extraction চলছে...*\n_একটু সময় লাগতে পারে_", parse_mode='Markdown')
    
    try:
        loop = asyncio.get_event_loop()
        leads = await loop.run_in_executor(None, scrape_advanced, data['loc'], data['kw'], data['count'], data.get('rating'))
        
        if not leads:
            return await ctx.bot.edit_message_text(chat_id=q.message.chat_id, message_id=msg.message_id, text="😔 কোনো result নেই।")

        path = to_csv(leads)
        em = sum(1 for l in leads if str(l.get('Email','')) not in ('N/A','','None'))
        
        await ctx.bot.edit_message_text(chat_id=q.message.chat_id, message_id=msg.message_id, text="✅ হয়ে গেছে! ফাইল পাঠাচ্ছি...")
        with open(path, 'rb') as f:
            await ctx.bot.send_document(
                chat_id=q.message.chat_id, document=f, filename=f"leads.csv",
                caption=f"🎯 *Done!*\n📊 Total: {len(leads)} | 📧 Emails Found: {em}", parse_mode='Markdown'
            )
        os.unlink(path)
    except Exception as e:
        await ctx.bot.edit_message_text(chat_id=q.message.chat_id, message_id=msg.message_id, text=f"❌ Error: `{e}`", parse_mode='Markdown')

def run_telegram_bot():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app = Application.builder().token(CONFIG["TELEGRAM_TOKEN"]).build()
    
    conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(handle_mode, pattern="^mode_")],
        states={
            M_LOC: [MessageHandler(filters.TEXT & ~filters.COMMAND, m_loc)],
            M_KW: [MessageHandler(filters.TEXT & ~filters.COMMAND, m_kw)],
            M_COUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, m_count)],
            M_RATING: [MessageHandler(filters.TEXT & ~filters.COMMAND, m_rating)],
            AI_PROMPT: [MessageHandler(filters.TEXT & ~filters.COMMAND, ai_prompt)]
        },
        fallbacks=[],
        per_message=False,
    )
    app.add_handler(CommandHandler("start", start))
    app.add_handler(conv)
    app.add_handler(CallbackQueryHandler(execute_scrape, pattern="^start_scrape$"))
    
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

# ══════════════════════════════════════════════
#   MAIN RUNNER
# ══════════════════════════════════════════════
if __name__ == "__main__":
    threading.Thread(target=run_telegram_bot, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    flask_app.run(host='0.0.0.0', port=port)
