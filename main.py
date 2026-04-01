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

CONFIG = {
    "TELEGRAM_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN"),
    "GROQ_API_KEY": os.getenv("GROQ_API_KEY", "")
}

# ══════════════════════════════════════════════
#   1. PURE PYTHON GOOGLE MAPS LIBRARY
# ══════════════════════════════════════════════
class GoogleMapsScraper:
    """
    Fetches raw pages from Google Local Search.
    """
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"
        }

    def get_page(self, keyword, location, start, max_rating=None):
        results = []
        query = urllib.parse.quote(f"{keyword} in {location}")
        url = f"https://www.google.com/search?q={query}&tbm=lcl&start={start}"
        
        try:
            res = requests.get(url, headers=self.headers, timeout=15)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # Find all business blocks
            places = soup.find_all('div', class_=['VkpGBb', 'rllt__details', 'dbg0pd'])
            
            if not places:
                return []
                
            for place in places:
                # Extract Name
                name_tag = place.find(['div', 'h3', 'span'], class_='dbg0pd') or place.find('div', role='heading')
                name = name_tag.get_text(strip=True) if name_tag else "N/A"
                
                if name == "N/A" or len(name) < 3:
                    continue
                    
                text_content = place.get_text(separator=' ', strip=True)
                
                # Extract Rating
                rating_match = re.search(r'(\d\.\d)\s*\(', text_content)
                rating = rating_match.group(1) if rating_match else "N/A"
                
                if max_rating and rating != "N/A" and float(rating) > float(max_rating):
                    continue
                    
                # Extract Phone
                phone_match = re.search(r'(\+?\d{1,2}[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}', text_content)
                phone = phone_match.group(0) if phone_match else "N/A"
                
                # Extract Website
                website = "N/A"
                for a in place.find_all('a', href=True):
                    href = a['href']
                    if '/url?q=' in href and 'google.com' not in href:
                        website = urllib.parse.unquote(href.split('/url?q=')[1].split('&')[0])
                        break
                    elif href.startswith('http') and 'google.com' not in href:
                        website = href
                        break
                        
                results.append({
                    "Name": name,
                    "Phone": phone,
                    "Website": website,
                    "Rating": rating,
                    "Address": location,
                    "Category": keyword,
                    "Maps_Link": f"https://www.google.com/maps/search/{urllib.parse.quote(name + ' ' + location)}"
                })
        except Exception as e:
            pass
            
        return results

# ══════════════════════════════════════════════
#   2. DEEP EMAIL EXTRACTOR LIBRARY
# ══════════════════════════════════════════════
class DeepEmailExtractor:
    def __init__(self):
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        self.email_regex = r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'

    def get_email(self, url):
        if not url or url == "N/A": return "N/A"
        if not url.startswith('http'): url = 'http://' + url
        
        try:
            r = requests.get(url, headers=self.headers, timeout=8, verify=False)
            emails = list(set(re.findall(self.email_regex, r.text)))
            valid_emails = [e for e in emails if not any(x in e.lower() for x in ['example', 'domain', 'sentry', '@2x', '.png', '.jpg'])]
            if valid_emails: return valid_emails[0]
            
            # Try contact page
            soup = BeautifulSoup(r.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                if 'contact' in a.get('href', '').lower():
                    contact_link = urllib.parse.urljoin(url, a['href'])
                    r2 = requests.get(contact_link, headers=self.headers, timeout=8, verify=False)
                    emails2 = list(set(re.findall(self.email_regex, r2.text)))
                    valid_emails2 = [e for e in emails2 if not any(x in e.lower() for x in ['example', 'domain', 'sentry', '@2x'])]
                    if valid_emails2: return valid_emails2[0]
        except:
            pass
        return "N/A"

# ══════════════════════════════════════════════
#   3. MASTER EXECUTION FUNCTION (Target Guarantee Engine)
# ══════════════════════════════════════════════
def run_full_scraper(location, keyword, max_leads=50, max_rating=None):
    maps_lib = GoogleMapsScraper()
    email_lib = DeepEmailExtractor()
    
    final_leads = []
    seen_names = set()
    start = 0
    empty_strikes = 0
    max_leads = int(max_leads)
    
    # এটি লুপ করতে থাকবে যতক্ষণ না লিমিট পূরণ হয় (অথবা ৮০০ রেজাল্ট পর্যন্ত)
    while len(final_leads) < max_leads and start <= 800:
        raw_batch = maps_lib.get_page(keyword, location, start, max_rating)
        
        if not raw_batch:
            empty_strikes += 1
            if empty_strikes >= 3:
                break # গুগলের কাছে আর ডাটা নেই
        else:
            empty_strikes = 0
            
        for lead in raw_batch:
            if len(final_leads) >= max_leads:
                break
                
            if lead['Name'] in seen_names:
                continue
                
            # Enrich Email on the fly
            if lead['Website'] != 'N/A':
                lead['Email'] = email_lib.get_email(lead['Website'])
            else:
                lead['Email'] = "N/A"
                
            # Quality Check: Keep only if it has Phone or Email or Website
            if lead['Phone'] != 'N/A' or lead['Email'] != 'N/A' or lead['Website'] != 'N/A':
                seen_names.add(lead['Name'])
                final_leads.append(lead)
                
        start += 20
        time.sleep(1.5) # Anti-block delay between page requests
        
    return final_leads

# ══════════════════════════════════════════════
#   GROQ AI BRAIN
# ══════════════════════════════════════════════
def parse_with_ai(user_text):
    if not CONFIG["GROQ_API_KEY"]:
        raise Exception("Groq API Key is missing! Please add it in settings.")
    
    client = Groq(api_key=CONFIG["GROQ_API_KEY"])
    prompt = f"""
    You are an AI assistant for a Lead Generation tool.
    Extract the following details from the user's input:
    - loc: The location (e.g., Vancouver, Dhaka, Texas)
    - kw: The niche or keyword (e.g., car showroom, plumber)
    - count: Number of leads requested (integer, default is 50)
    - rating: Maximum rating requested (float, e.g., 3.0, 4.5)

    User input: "{user_text}"

    Return ONLY a valid JSON object. Do not include any other text.
    Example format: {{"loc": "Vancouver", "kw": "car showroom", "count": 50, "rating": 3.0}}
    """
    try:
        chat_completion = client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="llama3-8b-8192",
            temperature=0,
        )
        response = chat_completion.choices[0].message.content
        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        if json_match: return json.loads(json_match.group(0))
        return json.loads(response)
    except Exception as e:
        raise Exception("Failed to connect to Groq AI. Check API Key.")

# ══════════════════════════════════════════════
#   WEB DASHBOARD (FLASK + DARK UI)
# ══════════════════════════════════════════════
flask_app = Flask(__name__)
jobs = {}

# New UI HTML exactly as requested
HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>LeadGen Pro</title>
<script src="https://cdn.tailwindcss.com"></script>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css">
<style>
*{box-sizing:border-box}
body{background:#060b18;color:#cbd5e1;font-family:'Inter',system-ui,sans-serif;min-height:100vh}
.card{background:rgba(15,23,42,0.85);border:1px solid rgba(99,102,241,0.12);border-radius:16px}
.card-hover{transition:all .2s}
.card-hover:hover{border-color:rgba(99,102,241,0.35);transform:translateY(-2px)}
.btn-p{background:linear-gradient(135deg,#4f46e5,#7c3aed);color:#fff;font-weight:600;cursor:pointer;transition:all .2s;border:none}
.btn-p:hover{filter:brightness(1.12);transform:translateY(-1px);box-shadow:0 6px 20px rgba(79,70,229,0.4)}
.btn-p:disabled{opacity:.45;cursor:not-allowed;transform:none;filter:none}
.btn-g{background:linear-gradient(135deg,#059669,#0d9488);color:#fff;font-weight:600;cursor:pointer;transition:all .2s;border:none}
.btn-g:hover{filter:brightness(1.12);transform:translateY(-1px);box-shadow:0 6px 20px rgba(5,150,105,0.4)}
.inp{background:#0f172a;border:1px solid #1e293b;color:#e2e8f0;border-radius:10px;padding:11px 15px;font-size:13px;width:100%;transition:border .2s;outline:none}
.inp:focus{border-color:#4f46e5;box-shadow:0 0 0 3px rgba(79,70,229,0.12)}
.tab{border-radius:9px;padding:9px 18px;font-size:12px;font-weight:600;cursor:pointer;transition:all .2s;border:1px solid transparent;color:#64748b;background:transparent}
.tab.on{background:linear-gradient(135deg,#4f46e5,#7c3aed);color:#fff;box-shadow:0 3px 12px rgba(79,70,229,0.35)}
.tab:not(.on):hover{background:rgba(79,70,229,0.08);color:#a5b4fc}
.prog{height:4px;background:#1e293b;border-radius:99px;overflow:hidden}
.prog-fill{height:100%;border-radius:99px;background:linear-gradient(90deg,#4f46e5,#7c3aed);transition:width .6s ease}
.chat-bot{background:rgba(79,70,229,0.07);border:1px solid rgba(79,70,229,0.18);border-radius:14px 14px 14px 3px;padding:12px 16px;max-width:84%;font-size:13px;line-height:1.6;color:#cbd5e1}
.chat-user{background:linear-gradient(135deg,#4f46e5,#7c3aed);border-radius:14px 14px 3px 14px;padding:12px 16px;max-width:84%;margin-left:auto;font-size:13px;color:#fff}
.pill{padding:1px 7px;border-radius:6px;font-size:11px;font-weight:600;display:inline-block}
.pg{background:rgba(5,150,105,0.12);color:#34d399}
.pr{background:rgba(239,68,68,0.09);color:#f87171}
.pb{background:rgba(59,130,246,0.1);color:#60a5fa}
.spin{animation:spin 1s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.fade{animation:fd .25s ease}
@keyframes fd{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
.blink{animation:bl 1.4s infinite}
@keyframes bl{0%,100%{opacity:1}50%{opacity:.25}}
::-webkit-scrollbar{width:4px}::-webkit-scrollbar-thumb{background:#1e293b;border-radius:2px}
@media(max-width:480px){.tab{padding:8px 10px;font-size:11px}}
</style>
</head>
<body>

<!-- NAV -->
<nav style="background:rgba(6,11,24,.96);border-bottom:1px solid rgba(99,102,241,0.1);backdrop-filter:blur(10px)"
     class="sticky top-0 z-40 px-4 py-3 flex items-center justify-between">
  <div class="flex items-center gap-3">
    <div class="btn-p w-9 h-9 rounded-xl flex items-center justify-center text-sm"
         style="box-shadow:0 0 18px rgba(79,70,229,0.45)">
      <i class="fa-solid fa-location-dot"></i>
    </div>
    <div>
      <div class="font-bold text-white text-sm leading-none">
        LeadGen <span style="color:#818cf8">Pro</span>
      </div>
      <div class="flex items-center gap-1.5 mt-0.5">
        <span class="w-1.5 h-1.5 rounded-full bg-emerald-400 blink"></span>
        <span class="text-xs text-slate-500">Pure Python · Exact Target Guarantee Engine</span>
      </div>
    </div>
  </div>
  <div class="flex items-center gap-2">
    <span class="pill pb text-xs" id="today-badge">
      <i class="fa-solid fa-bolt mr-1 text-xs"></i><span id="tn">0</span> today
    </span>
    <button onclick="showTab('settings')"
      class="w-9 h-9 rounded-xl flex items-center justify-center"
      style="background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.06)">
      <i class="fa-solid fa-gear text-slate-400 text-sm"></i>
    </button>
  </div>
</nav>

<div class="max-w-5xl mx-auto px-4 py-6">

  <!-- STATS -->
  <div class="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
    <div class="card card-hover p-4 fade"><div class="text-2xl font-bold text-white" id="st">0</div><div class="text-xs text-slate-500 mt-1 flex items-center gap-1"><i class="fa-solid fa-users text-indigo-400 text-xs"></i>Total Leads</div></div>
    <div class="card card-hover p-4 fade"><div class="text-2xl font-bold text-emerald-400" id="se">0</div><div class="text-xs text-slate-500 mt-1 flex items-center gap-1"><i class="fa-solid fa-envelope text-emerald-400 text-xs"></i>Emails</div></div>
    <div class="card card-hover p-4 fade"><div class="text-2xl font-bold text-sky-400" id="sp">0</div><div class="text-xs text-slate-500 mt-1 flex items-center gap-1"><i class="fa-solid fa-phone text-sky-400 text-xs"></i>Phones</div></div>
    <div class="card card-hover p-4 fade"><div class="text-2xl font-bold text-violet-400" id="sw">0</div><div class="text-xs text-slate-500 mt-1 flex items-center gap-1"><i class="fa-solid fa-globe text-violet-400 text-xs"></i>Websites</div></div>
  </div>

  <!-- TABS -->
  <div class="flex gap-2 mb-5 overflow-x-auto pb-1">
    <button class="tab on" id="tab-manual" onclick="showTab('manual')"><i class="fa-solid fa-sliders mr-1.5"></i>Manual</button>
    <button class="tab" id="tab-ai" onclick="showTab('ai')"><i class="fa-solid fa-robot mr-1.5"></i>AI Agent</button>
    <button class="tab" id="tab-history" onclick="showTab('history')"><i class="fa-solid fa-clock-rotate-left mr-1.5"></i>History</button>
    <button class="tab" id="tab-settings" onclick="showTab('settings')"><i class="fa-solid fa-gear mr-1.5"></i>Settings</button>
  </div>

  <!-- MANUAL PANE -->
  <div id="pane-manual" class="fade">
    <div class="card p-6 mb-4">
      <h2 class="font-bold text-white text-sm mb-5 flex items-center gap-2">
        <span class="btn-p w-7 h-7 rounded-lg flex items-center justify-center text-xs"><i class="fa-solid fa-crosshairs"></i></span>
        Search Parameters
      </h2>
      <div class="grid grid-cols-1 sm:grid-cols-2 gap-4 mb-5">
        <div>
          <label class="text-xs text-slate-500 mb-1.5 block">📍 Location *</label>
          <input id="m-loc" class="inp" placeholder="e.g. Vancouver, Canada">
        </div>
        <div>
          <label class="text-xs text-slate-500 mb-1.5 block">🔍 Keyword *</label>
          <input id="m-kw" class="inp" placeholder="e.g. car showroom">
        </div>
        <div>
          <label class="text-xs text-slate-500 mb-1.5 block">🔢 Exact Number of Leads</label>
          <input id="m-count" type="number" value="50" class="inp">
        </div>
        <div>
          <label class="text-xs text-slate-500 mb-1.5 block">⭐ Max Rating (Optional)</label>
          <input id="m-rating" type="number" step="0.1" class="inp" placeholder="e.g. 4.0">
        </div>
      </div>
      <button onclick="startManual()" id="btn-run" class="btn-p w-full py-3 rounded-xl text-sm">
        <i class="fa-solid fa-rocket mr-2"></i>Start Scraping
      </button>
    </div>

    <!-- STATUS -->
    <div id="sbox" class="hidden card p-5 mb-4 fade">
      <div class="flex items-center gap-3 mb-3">
        <i id="si" class="fa-solid fa-circle-notch spin text-indigo-400 text-xl"></i>
        <span id="stxt" class="font-semibold text-white text-sm">Processing...</span>
      </div>
      <div class="prog mb-2"><div class="prog-fill" id="sbar" style="width:0%"></div></div>
      <div id="sdet" class="text-xs text-slate-500 mb-3"></div>
      <button id="dlbtn" onclick="doDL()" class="hidden btn-g w-full py-3 rounded-xl text-sm">
        <i class="fa-solid fa-download mr-2"></i>Download CSV
      </button>
    </div>

    <!-- PREVIEW TABLE -->
    <div id="pvbox" class="hidden card p-5 fade">
      <div class="flex items-center justify-between mb-4">
        <h3 class="font-bold text-white text-sm flex items-center gap-2">
          <i class="fa-solid fa-table-cells text-indigo-400 text-xs"></i>
          Preview <span id="pvcnt" class="text-slate-500 font-normal text-xs"></span>
        </h3>
        <button onclick="doDL()" class="btn-g px-4 py-2 rounded-lg text-xs">
          <i class="fa-solid fa-download mr-1"></i>CSV
        </button>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-xs border-collapse">
          <thead><tr id="th" class="text-slate-500"></tr></thead>
          <tbody id="tb"></tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- AI PANE -->
  <div id="pane-ai" class="hidden fade">
    <div class="card overflow-hidden flex flex-col" style="height:550px">
      <div class="px-5 py-4 flex items-center gap-3"
           style="background:rgba(79,70,229,0.07);border-bottom:1px solid rgba(79,70,229,0.13)">
        <div class="btn-p w-9 h-9 rounded-xl flex items-center justify-center text-xs">
          <i class="fa-solid fa-robot"></i>
        </div>
        <div>
          <div class="font-bold text-white text-sm">AI Lead Agent</div>
          <div class="text-xs text-emerald-400 flex items-center gap-1">
            <span class="w-1.5 h-1.5 rounded-full bg-emerald-400 blink"></span>Groq Powered
          </div>
        </div>
      </div>
      <div id="cbox" class="flex-1 overflow-y-auto p-5 space-y-3" style="background:rgba(0,0,0,0.25)">
        <div class="chat-bot fade">
          👋 Hi! Tell me what leads you need.<br><br>
          <span style="color:#818cf8;font-style:italic">"Find 100 car showrooms in Vancouver, max rating 3.5"</span>
        </div>
      </div>
      <div class="p-4 flex gap-2" style="border-top:1px solid rgba(79,70,229,0.1)">
        <input id="ai-inp" class="inp flex-1" placeholder="Type your request..."
               onkeypress="if(event.key==='Enter')sendAI()">
        <button onclick="sendAI()" class="btn-p w-11 h-11 rounded-xl flex items-center justify-center flex-shrink-0 text-sm">
          <i class="fa-solid fa-paper-plane"></i>
        </button>
      </div>
    </div>
  </div>

  <!-- HISTORY PANE -->
  <div id="pane-history" class="hidden fade">
    <div class="card p-6">
      <div class="flex items-center justify-between mb-5">
        <h2 class="font-bold text-white text-sm flex items-center gap-2">
          <i class="fa-solid fa-clock-rotate-left text-violet-400 text-xs"></i>History
        </h2>
        <button onclick="clearH()" class="text-xs text-slate-600 hover:text-red-400 transition">
          <i class="fa-solid fa-trash mr-1"></i>Clear
        </button>
      </div>
      <div id="hlist" class="space-y-2">
        <div class="text-xs text-slate-600 text-center py-8">No history yet</div>
      </div>
    </div>
  </div>

  <!-- SETTINGS PANE -->
  <div id="pane-settings" class="hidden fade">
    <div class="card p-6 space-y-5">
      <h2 class="font-bold text-white text-sm flex items-center gap-2">
        <i class="fa-solid fa-gear text-slate-400 text-xs"></i>Settings
      </h2>
      <div class="p-4 rounded-xl text-xs"
           style="background:rgba(5,150,105,0.07);border:1px solid rgba(5,150,105,0.2);color:#34d399">
        <i class="fa-solid fa-check-circle mr-2"></i>Pure Python engine — no API key needed for scraping
      </div>
      <div>
        <label class="text-xs text-slate-500 mb-1.5 block">Groq API Key (AI Agent এর জন্য)</label>
        <input id="gk" type="password" class="inp" placeholder="gsk_...">
        <p class="text-xs text-slate-600 mt-1.5">
          Free: <a href="https://console.groq.com/keys" target="_blank" style="color:#818cf8">console.groq.com</a>
        </p>
      </div>
      <button onclick="saveS()" class="btn-p w-full py-3 rounded-xl text-sm">
        <i class="fa-solid fa-floppy-disk mr-2"></i>Save
      </button>
      <div id="sok" class="hidden text-center text-xs text-emerald-400">✅ Saved!</div>
    </div>
  </div>

</div>

<script>
let jid=null, aiSt={}, hist=[], today=0;

window.onload=()=>{
  const k=localStorage.getItem('gk'); if(k) document.getElementById('gk').value=k;
  hist=JSON.parse(localStorage.getItem('lh')||'[]'); renderH();
};

function showTab(t){
  ['manual','ai','history','settings'].forEach(x=>{
    document.getElementById('pane-'+x).classList.add('hidden');
    const b=document.getElementById('tab-'+x); if(b) b.classList.remove('on');
  });
  document.getElementById('pane-'+t).classList.remove('hidden');
  const ab=document.getElementById('tab-'+t); if(ab) ab.classList.add('on');
}

function saveS(){
  const k=document.getElementById('gk').value.trim();
  if(!k){alert('Key দাও!');return;}
  localStorage.setItem('gk',k);
  fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({groq:k})});
  const el=document.getElementById('sok'); el.classList.remove('hidden'); setTimeout(()=>el.classList.add('hidden'),2000);
}

function setSt(msg,state='load',pct=null){
  document.getElementById('sbox').classList.remove('hidden');
  document.getElementById('stxt').textContent=msg;
  const ic=document.getElementById('si');
  ic.className=state==='load'?'fa-solid fa-circle-notch spin text-indigo-400 text-xl':
               state==='done'?'fa-solid fa-circle-check text-emerald-400 text-xl':
               'fa-solid fa-circle-xmark text-red-400 text-xl';
  if(pct!=null) document.getElementById('sbar').style.width=pct+'%';
}

function updStats(leads, eCount, pCount, wCount){
  document.getElementById('st').textContent=leads.length;
  document.getElementById('se').textContent=eCount;
  document.getElementById('sp').textContent=pCount;
  document.getElementById('sw').textContent=wCount;
  today+=leads.length; document.getElementById('tn').textContent=today;
}

async function startJob(payload){
  setSt('Scraping শুরু হয়েছে (Target পূরণ না হওয়া পর্যন্ত খুঁজবে)...','load',6);
  document.getElementById('dlbtn').classList.add('hidden');
  document.getElementById('pvbox').classList.add('hidden');
  document.getElementById('sdet').textContent='';
  const btn=document.getElementById('btn-run'); if(btn) btn.disabled=true;

  const r=await fetch('/api/scrape',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
  const d=await r.json();
  if(d.error){setSt(d.error,'err');if(btn)btn.disabled=false;return;}
  jid=d.job_id;

  let pct=6;
  const iv=setInterval(()=>{pct=Math.min(pct+3,87);document.getElementById('sbar').style.width=pct+'%';},8000);

  const poll=async()=>{
    const r2=await fetch('/api/status/'+jid); const d2=await r2.json();
    if(d2.status==='done'){
      clearInterval(iv); if(btn) btn.disabled=false;
      setSt(`✅ সম্পন্ন — ${d2.count} leads found`,'done',100);
      document.getElementById('sdet').textContent=`📧 ${d2.emails} email · 📞 ${d2.phones} phone · 🌐 ${d2.websites} website`;
      document.getElementById('dlbtn').classList.remove('hidden');
      updStats(d2.leads||[], d2.emails, d2.phones, d2.websites);
      showPV(d2.leads||[]);
      addH(payload.location,payload.keyword,d2.count);
    } else if(d2.status==='error'){
      clearInterval(iv); if(btn) btn.disabled=false;
      setSt('Error: '+d2.error,'err');
    } else { setTimeout(poll,6000); }
  };
  setTimeout(poll,4000);
}

function startManual(){
  const loc=document.getElementById('m-loc').value.trim();
  const kw=document.getElementById('m-kw').value.trim();
  if(!loc||!kw){alert('Location আর Keyword দাও!');return;}
  startJob({
      location: loc, 
      keyword: kw, 
      max_leads: parseInt(document.getElementById('m-count').value)||50,
      max_rating: document.getElementById('m-rating').value || null
  });
}

function doDL(){ if(jid) window.location='/api/download/'+jid; }

function showPV(leads){
  if(!leads.length) return;
  document.getElementById('pvbox').classList.remove('hidden');
  document.getElementById('pvcnt').textContent='('+leads.length+' total, top 10)';
  const keys=Object.keys(leads[0]);
  document.getElementById('th').innerHTML=keys.map(k=>`<th class="px-3 py-2 text-left text-slate-500 font-medium whitespace-nowrap" style="border-bottom:1px solid rgba(255,255,255,0.05)">${k}</th>`).join('');
  document.getElementById('tb').innerHTML=leads.slice(0,10).map(l=>
    `<tr style="border-bottom:1px solid rgba(255,255,255,0.04)">${keys.map(k=>{
      const v=(l[k]||'N/A').toString();
      const cls=v==='N/A'?'pr':k==='Email'?'pg':k==='Phone'?'pb':'';
      return `<td class="px-3 py-2.5 text-slate-300 max-w-xs whitespace-nowrap overflow-hidden text-ellipsis">${cls?`<span class="pill ${cls}">${v}</span>`:v}</td>`;
    }).join('')}</tr>`
  ).join('');
}

function addH(loc,kw,count){ hist.unshift({loc,kw,count,t:new Date().toLocaleTimeString()}); hist=hist.slice(0,20); localStorage.setItem('lh',JSON.stringify(hist)); renderH(); }
function clearH(){ hist=[]; localStorage.removeItem('lh'); renderH(); }
function renderH(){
  const el=document.getElementById('hlist');
  if(!hist.length){el.innerHTML='<div class="text-xs text-slate-600 text-center py-8">No history yet</div>';return;}
  el.innerHTML=hist.map((h,i)=>`
    <div class="p-4 rounded-xl flex items-center justify-between fade" style="background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.05)">
      <div>
        <div class="text-sm font-semibold text-white">📍 ${h.loc} · 🔍 ${h.kw}</div>
        <div class="text-xs text-slate-500 mt-1">📊 ${h.count} leads · ${h.t}</div>
      </div>
      <button onclick="rerun(${i})" class="btn-p px-3 py-1.5 rounded-lg text-xs"><i class="fa-solid fa-redo mr-1"></i>Re-run</button>
    </div>`).join('');
}
function rerun(i){ const h=hist[i]; document.getElementById('m-loc').value=h.loc; document.getElementById('m-kw').value=h.kw; showTab('manual'); startJob({location:h.loc,keyword:h.kw,max_leads:h.count}); }

// AI
function addM(html,isU=false){ const box=document.getElementById('cbox'); const d=document.createElement('div'); d.className=(isU?'chat-user':'chat-bot')+' fade'; d.innerHTML=html; box.appendChild(d); box.scrollTop=99999; return d; }

async function sendAI(){
  const inp=document.getElementById('ai-inp'); const text=inp.value.trim(); if(!text) return;
  const key=localStorage.getItem('gk');
  addM(text.replace(/</g,'&lt;'),true); inp.value='';
  const ld=addM('<i class="fa-solid fa-ellipsis blink"></i>');
  const r=await fetch('/api/chat',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text,state:aiSt})});
  const d=await r.json(); ld.remove();
  if(d.error){addM('❌ '+d.error);return;}
  if(d.ready){
    aiSt=d.state;
    let stHTML = `Got it! 🎯<br><br>📍 <b>${d.state.loc}</b> · 🔍 <b>${d.state.kw}</b> · 🔢 <b>${d.state.count} leads</b>`;
    if(d.state.rating) stHTML += ` · ⭐ Max ${d.state.rating}`;
    
    addM(`${stHTML}<br><br>
      <button onclick="startJob({location:'${d.state.loc}',keyword:'${d.state.kw}',max_leads:${d.state.count},max_rating:'${d.state.rating||''}'});showTab('manual')"
        style="margin-top:10px;padding:8px 18px;border-radius:9px;font-size:12px;font-weight:700;cursor:pointer;background:linear-gradient(135deg,#059669,#0d9488);color:white">
        🚀 Start Engine</button>`);
  } else { addM(d.reply); }
}
</script>
</body>
</html>"""

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
        leads = run_full_scraper(
            data.get('location'), 
            data.get('keyword'),
            data.get('max_leads', 50),
            data.get('max_rating')
        )
        jobs[job_id] = {
            'status': 'done', 
            'leads': leads, 
            'count': len(leads),
            'emails': sum(1 for l in leads if l.get('Email', 'N/A') != 'N/A'),
            'phones': sum(1 for l in leads if l.get('Phone', 'N/A') != 'N/A'),
            'websites': sum(1 for l in leads if l.get('Website', 'N/A') != 'N/A')
        }
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
    out = dict(job)
    if out.get('status') == 'done':
        out['leads'] = job.get('leads', [])[:10] # send only top 10 for preview
    return jsonify(out)

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
    return send_file(io.BytesIO(out.getvalue().encode('utf-8-sig')), mimetype='text/csv', as_attachment=True, download_name='Target_Leads.csv')

# ══════════════════════════════════════════════
#   TELEGRAM BOT
# ══════════════════════════════════════════════
def to_csv(leads):
    tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8-sig', newline='')
    if leads:
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
    await update.message.reply_text("👋 *Pro Lead Gen Bot (Target Guarantee Engine)*\n\nকীভাবে সার্চ করতে চাও?", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))

async def handle_mode(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    bot_store[uid] = {}
    
    if q.data == "mode_manual":
        await q.edit_message_text("📍 *Manual Mode*\nLocation দাও (e.g. Vancouver):", parse_mode='Markdown')
        return M_LOC
    else:
        await q.edit_message_text("🤖 *Groq AI Mode*\nআমাকে ইংরেজিতে বলো তুমি কী খুঁজছো।\n\n_Example: I need 100 leads for car showrooms in Vancouver with maximum 3 star rating_", parse_mode='Markdown')
        return AI_PROMPT

async def m_loc(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    bot_store[update.message.from_user.id]['loc'] = update.message.text
    await update.message.reply_text("🔍 Keyword দাও (e.g. restaurant):")
    return M_KW

async def m_kw(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    bot_store[update.message.from_user.id]['kw'] = update.message.text
    await update.message.reply_text("🔢 কয়টা লিড লাগবে? (e.g. 100):")
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
            'count': parsed.get('count', 50),
            'rating': parsed.get('rating')
        }
        await msg.delete()
        return await ask_confirm(update, uid)
    except Exception as e:
        await msg.edit_text(f"❌ {str(e)}")
        return AI_PROMPT

async def ask_confirm(update, uid):
    data = bot_store[uid]
    txt = f"📋 *Target Guarantee Engine*\n📍 Loc: {data['loc']}\n🔍 Kw: {data['kw']}\n🔢 Exact Target: {data['count']}\n⭐ Max Rating: {data.get('rating') or 'None'}\n\nশুরু করবো?"
    kb = [[InlineKeyboardButton("✅ Start Automation", callback_data="start_scrape")]]
    await update.message.reply_text(txt, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))
    return ConversationHandler.END

async def execute_scrape(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    data = bot_store.get(uid)
    
    msg = await q.edit_message_text("⏳ *Scraping & Deep Email Extraction চলছে...*\n_টার্গেট পূরণ না হওয়া পর্যন্ত খুঁজতে থাকবে_", parse_mode='Markdown')
    
    try:
        loop = asyncio.get_event_loop()
        leads = await loop.run_in_executor(None, run_full_scraper, data['loc'], data['kw'], data['count'], data.get('rating'))
        
        if not leads:
            return await ctx.bot.edit_message_text(chat_id=q.message.chat_id, message_id=msg.message_id, text="😔 কোনো result নেই। দয়া করে দেশের নামের বদলে নির্দিষ্ট শহরের নাম দিয়ে সার্চ করো।")

        path = to_csv(leads)
        em = sum(1 for l in leads if str(l.get('Email','')) not in ('N/A','','None'))
        ph = sum(1 for l in leads if str(l.get('Phone','')) not in ('N/A','','None'))
        
        await ctx.bot.edit_message_text(chat_id=q.message.chat_id, message_id=msg.message_id, text="✅ হয়ে গেছে! ফাইল পাঠাচ্ছি...")
        with open(path, 'rb') as f:
            await ctx.bot.send_document(
                chat_id=q.message.chat_id, document=f, filename=f"Target_Leads.csv",
                caption=f"🎯 *Target Reached!*\n📊 Total Leads: {len(leads)} | 📧 Emails: {em} | 📞 Phones: {ph}", parse_mode='Markdown'
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
