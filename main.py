import os, csv, asyncio, tempfile, threading, io, uuid, re, time, json, urllib.parse
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from groq import Groq
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
    "GROQ_API_KEY": os.getenv("GROQ_API_KEY", "")
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9"
}

# ══════════════════════════════════════════════
#   GROQ AI BRAIN (NATURAL LANGUAGE PARSER)
# ══════════════════════════════════════════════
def parse_with_ai(user_text):
    if not CONFIG["GROQ_API_KEY"]:
        raise Exception("Groq API Key is missing! Please add it in settings.")
    
    client = Groq(api_key=CONFIG["GROQ_API_KEY"])
    prompt = f"""
    You are an AI assistant for a Google Maps Lead Generation tool.
    Extract the following details from the user's input:
    - loc: The location (e.g., Canada, Dhaka, Texas)
    - kw: The niche or keyword (e.g., car showroom, plumber)
    - count: Number of leads requested (integer, default is 50)
    - rating: Maximum rating requested (float, e.g., 3.0, 4.5)

    User input: "{user_text}"

    Return ONLY a valid JSON object. Do not include any other text.
    Example format: {{"loc": "Canada", "kw": "car showroom", "count": 50, "rating": 3.0}}
    """
    
    try:
        chat_completion = client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="llama3-8b-8192",
            temperature=0,
        )
        response = chat_completion.choices[0].message.content
        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(0))
        return json.loads(response)
    except Exception as e:
        raise Exception("Failed to connect to Groq AI. Please check your API Key in Settings.")

# ══════════════════════════════════════════════
#   DEEP EMAIL EXTRACTOR (PYTHON)
# ══════════════════════════════════════════════
EMAIL_REGEX = r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'

def extract_email_from_website(url):
    if not url or url == "N/A": return "N/A"
    try:
        r = requests.get(url, headers=HEADERS, timeout=8)
        emails = list(set(re.findall(EMAIL_REGEX, r.text)))
        
        # Filter out dummy emails
        valid_emails = [e for e in emails if not any(x in e.lower() for x in ['example', 'domain', 'sentry', '@2x', '.png', '.jpg'])]
        if valid_emails: return valid_emails[0]
        
        soup = BeautifulSoup(r.text, 'html.parser')
        contact_link = None
        for a in soup.find_all('a', href=True):
            if 'contact' in a.get('href', '').lower():
                contact_link = a['href']
                break
        
        if contact_link:
            if not contact_link.startswith('http'):
                contact_link = url.rstrip('/') + '/' + contact_link.lstrip('/')
            r2 = requests.get(contact_link, headers=HEADERS, timeout=8)
            emails2 = list(set(re.findall(EMAIL_REGEX, r2.text)))
            valid_emails2 = [e for e in emails2 if not any(x in e.lower() for x in ['example', 'domain', 'sentry', '@2x'])]
            if valid_emails2: return valid_emails2[0]
    except:
        pass
    return "N/A"

# ══════════════════════════════════════════════
#   100% FREE CUSTOM SCRAPER
# ══════════════════════════════════════════════
def scrape_free(location, keyword, max_leads=50, max_rating=None):
    results = []
    query = f"{keyword} in {location}"
    encoded = urllib.parse.quote(query)
    
    # 1. Scrape Google Search Local Pack (Fallback Method)
    url = f"https://www.google.com/search?q={encoded}&num=30"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find business blocks
        for div in soup.find_all('div', class_=['VkpGBb', 'rllt__details', 'dbg0pd']):
            try:
                name_el = div.find(['h3', 'span', 'a'])
                name = name_el.get_text(strip=True) if name_el else 'N/A'
                
                text = div.get_text(separator=' ', strip=True)
                
                # Extract Phone
                phone_match = re.search(r'(\+?8801[3-9]\d{8}|01[3-9]\d{8}|\+?\d[\d\s\-\(\)]{8,})', text)
                phone = phone_match.group(1).strip() if phone_match else 'N/A'
                
                # Extract Rating
                rating_match = re.search(r'(\d\.\d)\s*\(', text)
                rating = rating_match.group(1) if rating_match else "N/A"
                
                # Filter by Rating
                if max_rating and rating != "N/A" and float(rating) > float(max_rating):
                    continue
                
                # Extract Website Link
                website = "N/A"
                for a in div.find_all('a', href=True):
                    if 'url?q=' in a['href'] and 'google.com' not in a['href']:
                        website = a['href'].split('url?q=')[1].split('&')[0]
                        break
                    elif a['href'].startswith('http') and 'google.com' not in a['href']:
                        website = a['href']
                        break
                
                # Deep Email Extract
                email = "N/A"
                if website != "N/A":
                    email = extract_email_from_website(website)
                    time.sleep(0.5)
                
                if name and name != 'N/A':
                    results.append({
                        "Name": name,
                        "Phone": phone,
                        "Email": email,
                        "Address": "N/A",
                        "Category": keyword,
                        "Rating": rating,
                        "Reviews": "N/A",
                        "Website": website,
                        "Maps_Link": f"https://www.google.com/maps/search/{urllib.parse.quote(name + ' ' + location)}"
                    })
                    
                if len(results) >= int(max_leads):
                    break
            except Exception:
                continue
    except Exception as e:
        print(f"Search API error: {e}")

    # Remove duplicates
    seen = set()
    unique_results = []
    for r in results:
        if r['Name'] not in seen:
            seen.add(r['Name'])
            unique_results.append(r)

    return unique_results[:int(max_leads)]

# ══════════════════════════════════════════════
#   WEB DASHBOARD (FLASK + DARK TAILWIND CSS)
# ══════════════════════════════════════════════
flask_app = Flask(__name__)
jobs = {}

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Pro Lead Gen Agent (Free Edition)</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <script>
        tailwind.config = {
            darkMode: 'class',
            theme: {
                extend: {
                    colors: {
                        darkbg: '#0f172a',
                        darkcard: '#1e293b',
                        darkinput: '#334155',
                    }
                }
            }
        }
    </script>
    <style>
        ::-webkit-scrollbar { width: 8px; }
        ::-webkit-scrollbar-track { background: #1e293b; }
        ::-webkit-scrollbar-thumb { background: #475569; border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: #64748b; }
    </style>
</head>
<body class="bg-darkbg text-gray-200 font-sans antialiased min-h-screen">
    <div class="max-w-5xl mx-auto p-4 sm:p-6 lg:p-8">
        <!-- Header -->
        <header class="flex justify-between items-center bg-darkcard p-5 rounded-2xl shadow-lg mb-8 border border-gray-800">
            <div class="flex items-center gap-4">
                <div class="bg-gradient-to-br from-indigo-500 to-purple-600 text-white p-3 rounded-xl shadow-lg"><i class="fa-solid fa-map-location-dot text-2xl"></i></div>
                <div>
                    <h1 class="text-3xl font-extrabold text-transparent bg-clip-text bg-gradient-to-r from-indigo-400 to-purple-400">LeadGen Pro</h1>
                    <span class="text-xs font-bold bg-green-500 text-white px-2 py-1 rounded-full">100% Free Edition</span>
                </div>
            </div>
            <button onclick="switchTab('settings')" class="text-gray-400 hover:text-white transition bg-gray-800 p-3 rounded-xl border border-gray-700"><i class="fa-solid fa-gear text-xl"></i></button>
        </header>

        <!-- Tabs -->
        <div class="flex gap-4 mb-8">
            <button onclick="switchTab('manual')" id="tab-manual" class="flex-1 py-4 font-bold rounded-xl bg-gradient-to-r from-indigo-600 to-purple-600 text-white shadow-lg transition transform hover:-translate-y-1">Manual Search</button>
            <button onclick="switchTab('ai')" id="tab-ai" class="flex-1 py-4 font-bold rounded-xl bg-darkcard text-gray-400 shadow-md border border-gray-700 hover:bg-gray-800 transition transform hover:-translate-y-1">AI Agent Search</button>
        </div>

        <!-- Manual Tab -->
        <div id="content-manual" class="bg-darkcard p-8 rounded-2xl shadow-xl border border-gray-800">
            <h2 class="text-2xl font-bold mb-6 text-white flex items-center gap-2"><i class="fa-solid fa-sliders text-indigo-400"></i> Manual Parameters</h2>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
                <div><label class="block text-sm font-medium mb-2 text-gray-400">Location *</label><input id="m-loc" type="text" class="w-full bg-darkinput border border-gray-600 rounded-xl p-3 text-white focus:ring-2 focus:ring-indigo-500 outline-none" placeholder="e.g., New York, NY"></div>
                <div><label class="block text-sm font-medium mb-2 text-gray-400">Keyword *</label><input id="m-kw" type="text" class="w-full bg-darkinput border border-gray-600 rounded-xl p-3 text-white focus:ring-2 focus:ring-indigo-500 outline-none" placeholder="e.g., Real Estate Agency"></div>
                <div><label class="block text-sm font-medium mb-2 text-gray-400">Number of Leads (Max 30 for Free)</label><input id="m-count" type="number" value="30" class="w-full bg-darkinput border border-gray-600 rounded-xl p-3 text-white focus:ring-2 focus:ring-indigo-500 outline-none"></div>
                <div><label class="block text-sm font-medium mb-2 text-gray-400">Max Rating (Optional)</label><input id="m-rating" type="number" step="0.1" class="w-full bg-darkinput border border-gray-600 rounded-xl p-3 text-white focus:ring-2 focus:ring-indigo-500 outline-none" placeholder="e.g., 4.5"></div>
            </div>
            <button onclick="startManual()" id="btn-manual" class="w-full bg-gradient-to-r from-green-500 to-emerald-600 hover:from-green-600 hover:to-emerald-700 text-white font-bold py-4 rounded-xl shadow-lg transition text-lg"><i class="fa-solid fa-rocket mr-2"></i> Start Scraping</button>
        </div>

        <!-- AI Tab -->
        <div id="content-ai" class="hidden bg-darkcard rounded-2xl shadow-xl border border-gray-800 flex flex-col h-[600px]">
            <div class="bg-gradient-to-r from-indigo-600 to-purple-600 text-white p-5 rounded-t-2xl font-bold flex items-center gap-3 text-lg">
                <i class="fa-solid fa-robot text-2xl"></i> Groq AI Lead Generation Agent
            </div>
            <div id="chat-box" class="flex-1 p-6 overflow-y-auto bg-[#0f172a] space-y-5">
                <div class="flex gap-4">
                    <div class="bg-darkcard border border-gray-700 text-gray-200 p-4 rounded-2xl rounded-tl-none max-w-[85%] shadow-md">
                        Hello! I am your AI Agent powered by Groq. Tell me exactly what you need in plain English.<br><br>
                        <span class="text-indigo-400 italic">Example: "I need 30 leads for car showrooms in Canada with maximum 3 star rating."</span>
                    </div>
                </div>
            </div>
            <div class="p-4 bg-darkcard border-t border-gray-800 flex gap-3 rounded-b-2xl">
                <input id="ai-input" type="text" class="flex-1 bg-darkinput border border-gray-600 rounded-xl p-4 text-white focus:ring-2 focus:ring-indigo-500 outline-none" placeholder="Type your request here..." onkeypress="if(event.key === 'Enter') sendAI()">
                <button onclick="sendAI()" class="bg-gradient-to-r from-indigo-500 to-purple-600 text-white px-8 rounded-xl hover:shadow-lg transition"><i class="fa-solid fa-paper-plane text-xl"></i></button>
            </div>
        </div>

        <!-- Settings Tab -->
        <div id="content-settings" class="hidden bg-darkcard p-8 rounded-2xl shadow-xl border border-gray-800">
            <h2 class="text-2xl font-bold mb-6 text-white flex items-center gap-2"><i class="fa-solid fa-key text-yellow-500"></i> API Settings</h2>
            <div class="space-y-6">
                <div class="bg-green-900/30 border border-green-500/50 p-4 rounded-xl text-green-400 mb-4">
                    <i class="fa-solid fa-check-circle mr-2"></i> Apify is removed! The scraper is now 100% Free.
                </div>
                <div>
                    <label class="block text-sm font-medium mb-2 text-gray-400">Groq API Key (For AI Brain)</label>
                    <input id="groq-key" type="password" class="w-full bg-darkinput border border-gray-600 rounded-xl p-3 text-white focus:ring-2 focus:ring-indigo-500 outline-none" placeholder="Enter Groq API Key">
                    <p class="text-xs text-gray-400 mt-2">Get your free key from <a href="https://console.groq.com/keys" target="_blank" class="text-indigo-400 underline">console.groq.com</a></p>
                </div>
                <button onclick="saveSettings()" class="w-full bg-gradient-to-r from-blue-500 to-indigo-600 text-white font-bold py-3 rounded-xl shadow-lg hover:shadow-xl transition">Save Settings</button>
            </div>
        </div>

        <!-- Status Area -->
        <div id="status-area" class="hidden mt-8 p-6 rounded-2xl border bg-darkcard border-gray-700 shadow-xl">
            <div class="flex items-center gap-4 mb-4">
                <i id="status-icon" class="fa-solid fa-circle-notch fa-spin text-indigo-500 text-3xl"></i>
                <span id="status-text" class="text-xl font-semibold text-white">Processing...</span>
            </div>
            <button id="dl-btn" class="hidden w-full mt-4 bg-gradient-to-r from-green-500 to-emerald-600 hover:from-green-600 hover:to-emerald-700 text-white font-bold py-4 rounded-xl shadow-lg transition text-lg"><i class="fa-solid fa-download mr-2"></i> Download CSV</button>
        </div>
    </div>

    <script>
        let currentJob = null;
        let aiState = {};

        function switchTab(tab) {
            ['manual', 'ai', 'settings'].forEach(t => {
                document.getElementById('content-'+t).classList.add('hidden');
                let btn = document.getElementById('tab-'+t);
                if(btn) btn.className = 'flex-1 py-4 font-bold rounded-xl bg-darkcard text-gray-400 shadow-md border border-gray-700 hover:bg-gray-800 transition transform hover:-translate-y-1';
            });
            document.getElementById('content-'+tab).classList.remove('hidden');
            let activeBtn = document.getElementById('tab-'+tab);
            if(activeBtn) activeBtn.className = 'flex-1 py-4 font-bold rounded-xl bg-gradient-to-r from-indigo-600 to-purple-600 text-white shadow-lg transition transform hover:-translate-y-1';
        }

        function saveSettings() {
            const groq = document.getElementById('groq-key').value;
            fetch('/api/settings', { 
                method: 'POST', 
                headers: {'Content-Type':'application/json'}, 
                body: JSON.stringify({groq: groq}) 
            }).then(() => alert('Settings Saved Successfully!'));
        }

        function showStatus(msg, isSpin=true, isError=false) {
            const area = document.getElementById('status-area');
            area.classList.remove('hidden');
            document.getElementById('status-text').innerText = msg;
            
            const icon = document.getElementById('status-icon');
            if(isSpin) {
                icon.className = 'fa-solid fa-circle-notch fa-spin text-indigo-500 text-3xl';
            } else if(isError) {
                icon.className = 'fa-solid fa-circle-xmark text-red-500 text-3xl';
            } else {
                icon.className = 'fa-solid fa-circle-check text-green-500 text-3xl';
            }
            document.getElementById('dl-btn').classList.add('hidden');
        }

        async function startJob(payload) {
            showStatus('Scraping data & extracting deep emails (100% Free)...');
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
                max_leads: document.getElementById('m-count').value || 30,
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

        function addMsg(text, isBot=false, isHtml=false) {
            const box = document.getElementById('chat-box');
            const div = document.createElement('div');
            div.className = `flex gap-4 ${isBot ? '' : 'justify-end'}`;
            
            let contentClass = isBot 
                ? 'bg-darkcard border border-gray-700 text-gray-200 p-4 rounded-2xl rounded-tl-none shadow-md' 
                : 'bg-gradient-to-r from-indigo-500 to-purple-600 text-white p-4 rounded-2xl rounded-tr-none shadow-md';
            
            div.innerHTML = `<div class="${contentClass} max-w-[85%]">${isHtml ? text : text.replace(/</g, "&lt;").replace(/>/g, "&gt;")}</div>`;
            box.appendChild(div);
            box.scrollTop = box.scrollHeight;
        }

        async function sendAI() {
            const inp = document.getElementById('ai-input');
            const text = inp.value.trim();
            if(!text) return;
            addMsg(text, false);
            inp.value = '';

            const box = document.getElementById('chat-box');
            const loadDiv = document.createElement('div');
            loadDiv.id = 'typing-indicator';
            loadDiv.className = 'flex gap-4';
            loadDiv.innerHTML = `<div class="bg-darkcard border border-gray-700 text-gray-400 p-4 rounded-2xl rounded-tl-none shadow-md"><i class="fa-solid fa-ellipsis fa-fade text-xl"></i></div>`;
            box.appendChild(loadDiv);
            box.scrollTop = box.scrollHeight;

            try {
                const res = await fetch('/api/chat', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({text: text, state: aiState})
                });
                const data = await res.json();
                
                document.getElementById('typing-indicator').remove();

                if(data.error) {
                    addMsg(data.error, true);
                    return;
                }

                if(data.ready) {
                    aiState = data.state;
                    let summary = `Got it! Here is what I understood:<br><br>
                    📍 <b>Location:</b> ${aiState.loc}<br>
                    🔍 <b>Keyword:</b> ${aiState.kw}<br>
                    🔢 <b>Leads:</b> ${aiState.count}<br>`;
                    if(aiState.rating) summary += `⭐ <b>Max Rating:</b> ${aiState.rating}<br>`;
                    
                    summary += `<br><button onclick='startJob(${JSON.stringify({location: aiState.loc, keyword: aiState.kw, max_leads: aiState.count, max_rating: aiState.rating})})' class='mt-3 bg-gradient-to-r from-green-500 to-emerald-600 text-white px-6 py-2 rounded-lg font-bold shadow hover:shadow-lg transition'>🚀 Start Free Automation</button>`;
                    
                    addMsg(summary, true, true);
                } else {
                    addMsg(data.reply, true);
                }
            } catch (err) {
                document.getElementById('typing-indicator').remove();
                addMsg("Error communicating with server.", true);
            }
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
    if request.json.get('groq'): CONFIG["GROQ_API_KEY"] = request.json.get('groq')
    return jsonify({"success": True})

@flask_app.route('/api/chat', methods=['POST'])
def handle_chat():
    text = request.json.get('text')
    
    if text.lower() in ['yes', 'start', 'do it', 'go']:
        return jsonify({"ready": True, "state": request.json.get('state')})

    try:
        parsed = parse_with_ai(text)
    except Exception as e:
        return jsonify({"error": str(e)})
    
    if not parsed:
        return jsonify({"error": "Failed to parse input. Try again."})

    state = request.json.get('state', {})
    if parsed.get('loc'): state['loc'] = parsed['loc']
    if parsed.get('kw'): state['kw'] = parsed['kw']
    if parsed.get('count'): state['count'] = parsed['count']
    if parsed.get('rating'): state['rating'] = parsed['rating']

    if not state.get('loc') or not state.get('kw'):
        reply = "I still need a bit more info. "
        if not state.get('loc'): reply += "Which **location** are you targeting? "
        if not state.get('kw'): reply += "What **keyword or niche** are you looking for?"
        return jsonify({"ready": False, "reply": reply})
    
    return jsonify({"ready": True, "state": state})

def run_scrape_thread(job_id, data):
    try:
        jobs[job_id] = {'status': 'running'}
        leads = scrape_free(
            data.get('location'), 
            data.get('keyword'),
            data.get('max_leads', 30),
            data.get('max_rating')
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
    if not leads: return "No leads found", 404
    
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=leads[0].keys())
    writer.writeheader()
    writer.writerows(leads)
    out.seek(0)
    return send_file(io.BytesIO(out.getvalue().encode('utf-8-sig')), mimetype='text/csv', as_attachment=True, download_name='free_leads.csv')

# ══════════════════════════════════════════════
#   TELEGRAM BOT
# ══════════════════════════════════════════════
def to_csv(leads):
    tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8-sig', newline='')
    writer = csv.DictWriter(tmp, fieldnames=leads[0].keys())
    writer.writeheader()
    writer.writerows(leads)
    tmp.close()
    return tmp.name

M_LOC, M_KW, M_COUNT, M_RATING, AI_PROMPT = range(5)
bot_store = {}

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = [
        [InlineKeyboardButton("🛠️ Manual Search", callback_data="mode_manual")],
        [InlineKeyboardButton("🤖 Groq AI Search", callback_data="mode_ai")]
    ]
    await update.message.reply_text("👋 *Pro Lead Gen Bot (100% Free)*\n\nকীভাবে সার্চ করতে চাও?", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))

async def handle_mode(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    bot_store[uid] = {}
    
    if q.data == "mode_manual":
        await q.edit_message_text("📍 *Manual Mode*\nLocation দাও (e.g. Dhaka):", parse_mode='Markdown')
        return M_LOC
    else:
        await q.edit_message_text("🤖 *Groq AI Mode*\nআমাকে ইংরেজিতে বলো তুমি কী খুঁজছো।\n\n_Example: I need 30 leads for car showrooms in Canada with maximum 3 star rating_", parse_mode='Markdown')
        return AI_PROMPT

async def m_loc(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    bot_store[update.message.from_user.id]['loc'] = update.message.text
    await update.message.reply_text("🔍 Keyword দাও (e.g. restaurant):")
    return M_KW

async def m_kw(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    bot_store[update.message.from_user.id]['kw'] = update.message.text
    await update.message.reply_text("🔢 কয়টা লিড লাগবে? (Max 30):")
    return M_COUNT

async def m_count(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    bot_store[update.message.from_user.id]['count'] = update.message.text
    await update.message.reply_text("⭐ Max Rating ফিল্টার করবে? (না চাইলে 'skip' লেখো):")
    return M_RATING

async def m_rating(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.lower()
    uid = update.message.from_user.id
    bot_store[uid]['rating'] = None if txt == 'skip' else txt
    return await ask_confirm(update, uid)

async def ai_prompt(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    uid = update.message.from_user.id
    
    msg = await update.message.reply_text("🤖 _Thinking..._", parse_mode='Markdown')
    
    try:
        parsed = parse_with_ai(text)
        if not parsed.get('loc') or not parsed.get('kw'):
            await msg.edit_text("🤖 আমি ঠিক বুঝতে পারিনি। দয়া করে Keyword এবং Location পরিষ্কার করে বলো।")
            return AI_PROMPT
            
        bot_store[uid] = {
            'loc': parsed['loc'],
            'kw': parsed['kw'],
            'count': parsed.get('count', 30),
            'rating': parsed.get('rating')
        }
        await msg.delete()
        return await ask_confirm(update, uid)
    except Exception as e:
        await msg.edit_text(f"❌ {str(e)}")
        return AI_PROMPT

async def ask_confirm(update, uid):
    data = bot_store[uid]
    txt = f"📋 *Summary (Free Scraper)*\n📍 Loc: {data['loc']}\n🔍 Kw: {data['kw']}\n🔢 Leads: {data['count']}\n⭐ Max Rating: {data.get('rating') or 'None'}\n\nশুরু করবো?"
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
        leads = await loop.run_in_executor(None, scrape_free, data['loc'], data['kw'], data['count'], data.get('rating'))
        
        if not leads:
            return await ctx.bot.edit_message_text(chat_id=q.message.chat_id, message_id=msg.message_id, text="😔 কোনো result নেই।")

        path = to_csv(leads)
        em = sum(1 for l in leads if str(l.get('Email','')) not in ('N/A','','None'))
        
        await ctx.bot.edit_message_text(chat_id=q.message.chat_id, message_id=msg.message_id, text="✅ হয়ে গেছে! ফাইল পাঠাচ্ছি...")
        with open(path, 'rb') as f:
            await ctx.bot.send_document(
                chat_id=q.message.chat_id, document=f, filename=f"free_leads.csv",
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

if __name__ == "__main__":
    threading.Thread(target=run_telegram_bot, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    flask_app.run(host='0.0.0.0', port=port)
