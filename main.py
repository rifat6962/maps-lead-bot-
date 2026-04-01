import os, csv, asyncio, tempfile, threading, io, uuid, re, json, time, urllib.parse
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from apify_client import ApifyClient
from groq import Groq
from flask import Flask, render_template_string, request, send_file, jsonify
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler, CallbackQueryHandler
)

load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
APIFY_TOKEN    = os.getenv("APIFY_API_TOKEN")
GROQ_KEY       = os.getenv("GROQ_API_KEY", "")

# Runtime settings (dashboard থেকে update হয়)
runtime = {"groq_key": GROQ_KEY}

# ══════════════════════════════════════════
#  EMAIL EXTRACTOR
# ══════════════════════════════════════════
EMAIL_RE   = r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'
EMAIL_SKIP = ['example','domain','sentry','wixpress','noreply','@2x','.png','.jpg','no-reply','amazonaws']

def extract_email(url):
    if not url or url == 'N/A': return 'N/A'
    if not url.startswith('http'): url = 'http://' + url
    hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    
    def valid(emails):
        return [e.lower() for e in emails if not any(s in e.lower() for s in EMAIL_SKIP) and '.' in e.split('@')[-1]]
    
    for path in ['', '/contact', '/contact-us', '/about']:
        try:
            r = requests.get(urllib.parse.urljoin(url, path), headers=hdrs, timeout=8, verify=False)
            soup = BeautifulSoup(r.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                if a['href'].startswith('mailto:'):
                    e = a['href'].replace('mailto:','').split('?')[0].strip()
                    if valid([e]): return e
            found = valid(re.findall(EMAIL_RE, r.text))
            if found: return found[0]
        except: continue
    return 'N/A'

# ══════════════════════════════════════════
#  APIFY SCRAPER
# ══════════════════════════════════════════
def scrape_leads(location, keyword, max_leads=50):
    client = ApifyClient(APIFY_TOKEN)
    run = client.actor("compass/crawler-google-places").call(run_input={
        "searchStringsArray": [f"{keyword} in {location}"],
        "maxCrawledPlacesPerSearch": int(max_leads),
        "language": "en",
        "includeHistogram": False,
        "includeOpeningHours": False,
        "includePeopleAlsoSearchFor": False,
    })
    leads = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        website = item.get("website", "N/A") or "N/A"
        email   = extract_email(website)
        leads.append({
            "Name":      item.get("title", "N/A"),
            "Phone":     item.get("phone", "N/A") or "N/A",
            "Email":     email,
            "Address":   item.get("address", "N/A") or "N/A",
            "Category":  item.get("categoryName", "N/A") or "N/A",
            "Rating":    item.get("totalScore", "N/A"),
            "Reviews":   item.get("reviewsCount", "N/A"),
            "Website":   website,
            "Maps_Link": item.get("url", "N/A") or "N/A",
        })
    return leads

# ══════════════════════════════════════════
#  GROQ AI PARSER
# ══════════════════════════════════════════
def parse_with_ai(text):
    key = runtime.get("groq_key","")
    if not key: raise Exception("Groq API Key নেই। Settings এ গিয়ে যোগ করো।")
    client = Groq(api_key=key)
    prompt = f'''Extract from user input:
- loc: location
- kw: keyword/niche  
- count: number of leads (default 50)

Input: "{text}"
Return ONLY valid JSON: {{"loc":"...","kw":"...","count":50}}'''
    resp = client.chat.completions.create(
        messages=[{"role":"user","content":prompt}],
        model="llama3-8b-8192", temperature=0
    )
    m = re.search(r'\{.*\}', resp.choices[0].message.content, re.DOTALL)
    return json.loads(m.group(0)) if m else {}

# ══════════════════════════════════════════
#  FLASK DASHBOARD
# ══════════════════════════════════════════
flask_app = Flask(__name__)
jobs = {}

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>LeadGen Pro</title>
<script src="https://cdn.tailwindcss.com"></script>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css">
<style>
  body{background:#0a0f1e;color:#e2e8f0;font-family:'Segoe UI',sans-serif}
  .glass{background:rgba(255,255,255,0.04);backdrop-filter:blur(12px);border:1px solid rgba(255,255,255,0.08)}
  .glow{box-shadow:0 0 20px rgba(99,102,241,0.3)}
  input,select{background:#1e293b!important;border:1px solid #334155!important;color:#e2e8f0!important;outline:none!important}
  input:focus,select:focus{border-color:#6366f1!important;box-shadow:0 0 0 2px rgba(99,102,241,0.2)!important}
  .btn-primary{background:linear-gradient(135deg,#6366f1,#8b5cf6);transition:all .2s}
  .btn-primary:hover{transform:translateY(-1px);box-shadow:0 8px 20px rgba(99,102,241,0.4)}
  .btn-green{background:linear-gradient(135deg,#10b981,#059669);transition:all .2s}
  .btn-green:hover{transform:translateY(-1px);box-shadow:0 8px 20px rgba(16,185,129,0.4)}
  .tab-active{background:linear-gradient(135deg,#6366f1,#8b5cf6)!important;color:white!important}
  .stat-card{background:linear-gradient(135deg,rgba(99,102,241,0.1),rgba(139,92,246,0.05));border:1px solid rgba(99,102,241,0.2)}
  .progress-bar{height:6px;background:#1e293b;border-radius:99px;overflow:hidden}
  .progress-fill{height:100%;background:linear-gradient(90deg,#6366f1,#8b5cf6);transition:width .5s ease;border-radius:99px}
  ::-webkit-scrollbar{width:6px} ::-webkit-scrollbar-track{background:#0f172a} ::-webkit-scrollbar-thumb{background:#334155;border-radius:3px}
  .pulse{animation:pulse 2s infinite} @keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
  .chat-msg-bot{background:rgba(99,102,241,0.1);border:1px solid rgba(99,102,241,0.2);border-radius:16px 16px 16px 4px}
  .chat-msg-user{background:linear-gradient(135deg,#6366f1,#8b5cf6);border-radius:16px 16px 4px 16px}
  .tag{display:inline-block;padding:2px 10px;border-radius:99px;font-size:11px;font-weight:600}
</style>
</head>
<body class="min-h-screen">

<!-- HEADER -->
<div class="glass sticky top-0 z-50 px-6 py-4 flex items-center justify-between" style="border-bottom:1px solid rgba(255,255,255,0.06)">
  <div class="flex items-center gap-3">
    <div class="w-10 h-10 rounded-xl btn-primary flex items-center justify-center glow">
      <i class="fa-solid fa-location-dot text-white"></i>
    </div>
    <div>
      <h1 class="text-xl font-bold text-white">LeadGen <span style="color:#a78bfa">Pro</span></h1>
      <div class="flex items-center gap-2">
        <span class="w-2 h-2 rounded-full bg-green-400 pulse"></span>
        <span class="text-xs text-gray-400">Powered by Apify + Groq AI</span>
      </div>
    </div>
  </div>
  <div class="flex items-center gap-3">
    <span id="total-badge" class="tag" style="background:rgba(99,102,241,0.2);color:#a78bfa">0 Leads Today</span>
    <button onclick="showTab('settings')" class="w-10 h-10 glass rounded-xl flex items-center justify-center hover:border-indigo-500 transition">
      <i class="fa-solid fa-gear text-gray-400"></i>
    </button>
  </div>
</div>

<div class="max-w-6xl mx-auto px-4 py-8">

  <!-- STATS ROW -->
  <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8" id="stats-row">
    <div class="stat-card rounded-2xl p-4">
      <div class="text-2xl font-bold text-white" id="s-total">0</div>
      <div class="text-xs text-gray-400 mt-1">Total Leads</div>
    </div>
    <div class="stat-card rounded-2xl p-4">
      <div class="text-2xl font-bold text-green-400" id="s-email">0</div>
      <div class="text-xs text-gray-400 mt-1">With Email</div>
    </div>
    <div class="stat-card rounded-2xl p-4">
      <div class="text-2xl font-bold text-blue-400" id="s-phone">0</div>
      <div class="text-xs text-gray-400 mt-1">With Phone</div>
    </div>
    <div class="stat-card rounded-2xl p-4">
      <div class="text-2xl font-bold text-purple-400" id="s-web">0</div>
      <div class="text-xs text-gray-400 mt-1">With Website</div>
    </div>
  </div>

  <!-- TABS -->
  <div class="flex gap-3 mb-6">
    <button onclick="showTab('manual')" id="tab-manual" class="tab-active flex-1 py-3 rounded-xl font-semibold text-sm transition glass">
      <i class="fa-solid fa-sliders mr-2"></i>Manual
    </button>
    <button onclick="showTab('ai')" id="tab-ai" class="flex-1 py-3 rounded-xl font-semibold text-sm transition glass text-gray-400">
      <i class="fa-solid fa-robot mr-2"></i>AI Agent
    </button>
    <button onclick="showTab('history')" id="tab-history" class="flex-1 py-3 rounded-xl font-semibold text-sm transition glass text-gray-400">
      <i class="fa-solid fa-clock-rotate-left mr-2"></i>History
    </button>
    <button onclick="showTab('settings')" id="tab-settings" class="flex-1 py-3 rounded-xl font-semibold text-sm transition glass text-gray-400">
      <i class="fa-solid fa-gear mr-2"></i>Settings
    </button>
  </div>

  <!-- MANUAL TAB -->
  <div id="pane-manual">
    <div class="glass rounded-2xl p-6">
      <h2 class="text-lg font-bold text-white mb-5"><i class="fa-solid fa-crosshairs mr-2 text-indigo-400"></i>Search Parameters</h2>
      <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-5">
        <div>
          <label class="text-xs text-gray-400 mb-1 block">📍 Location *</label>
          <input id="m-loc" class="w-full rounded-xl px-4 py-3 text-sm" placeholder="e.g. Gulshan Dhaka">
        </div>
        <div>
          <label class="text-xs text-gray-400 mb-1 block">🔍 Keyword *</label>
          <input id="m-kw" class="w-full rounded-xl px-4 py-3 text-sm" placeholder="e.g. restaurant">
        </div>
        <div>
          <label class="text-xs text-gray-400 mb-1 block">🔢 Number of Leads</label>
          <input id="m-count" type="number" value="50" class="w-full rounded-xl px-4 py-3 text-sm">
        </div>
        <div>
          <label class="text-xs text-gray-400 mb-1 block">🌐 Language</label>
          <select id="m-lang" class="w-full rounded-xl px-4 py-3 text-sm">
            <option value="en">English</option>
            <option value="bn">Bengali</option>
            <option value="ar">Arabic</option>
            <option value="hi">Hindi</option>
          </select>
        </div>
      </div>
      <button onclick="startManual()" class="btn-primary w-full py-3 rounded-xl font-bold text-white text-sm">
        <i class="fa-solid fa-rocket mr-2"></i>Start Scraping
      </button>
    </div>

    <!-- STATUS -->
    <div id="status-box" class="hidden mt-5 glass rounded-2xl p-6">
      <div class="flex items-center gap-3 mb-4">
        <i id="st-icon" class="fa-solid fa-circle-notch fa-spin text-indigo-400 text-2xl"></i>
        <span id="st-text" class="font-semibold text-white">Processing...</span>
      </div>
      <div class="progress-bar mb-4"><div class="progress-fill" id="st-bar" style="width:0%"></div></div>
      <div id="st-details" class="text-xs text-gray-400"></div>
      <button id="dl-btn" onclick="doDownload()" class="hidden btn-green w-full py-3 rounded-xl font-bold text-white text-sm mt-4">
        <i class="fa-solid fa-download mr-2"></i>Download CSV
      </button>
    </div>

    <!-- PREVIEW TABLE -->
    <div id="preview-box" class="hidden mt-5 glass rounded-2xl p-5">
      <div class="flex items-center justify-between mb-4">
        <h3 class="font-bold text-white"><i class="fa-solid fa-table mr-2 text-indigo-400"></i>Preview <span id="preview-count" class="text-gray-400 text-sm"></span></h3>
        <button onclick="doDownload()" class="btn-green px-4 py-2 rounded-xl text-xs font-bold text-white">
          <i class="fa-solid fa-download mr-1"></i>Download
        </button>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-xs" id="preview-table">
          <thead><tr id="tbl-head" class="text-gray-400 border-b border-white/10"></tr></thead>
          <tbody id="tbl-body" class="divide-y divide-white/5"></tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- AI TAB -->
  <div id="pane-ai" class="hidden">
    <div class="glass rounded-2xl overflow-hidden" style="height:580px;display:flex;flex-direction:column">
      <div class="px-5 py-4 flex items-center gap-3" style="background:linear-gradient(135deg,rgba(99,102,241,0.2),rgba(139,92,246,0.1));border-bottom:1px solid rgba(255,255,255,0.06)">
        <div class="w-9 h-9 rounded-xl btn-primary flex items-center justify-center">
          <i class="fa-solid fa-robot text-white text-sm"></i>
        </div>
        <div>
          <div class="font-bold text-white text-sm">Groq AI Lead Agent</div>
          <div class="text-xs text-green-400 flex items-center gap-1"><span class="w-1.5 h-1.5 rounded-full bg-green-400 pulse"></span> Online</div>
        </div>
      </div>
      <div id="chat-box" class="flex-1 overflow-y-auto p-5 space-y-4" style="background:rgba(0,0,0,0.3)">
        <div class="chat-msg-bot p-4 max-w-md text-sm text-gray-200">
          👋 Hi! I'm your AI lead generation agent.<br><br>
          Just tell me what you need in plain English:<br>
          <span class="text-indigo-400 italic">"Find 50 restaurants in Dhaka"</span>
        </div>
      </div>
      <div class="p-4 flex gap-3" style="border-top:1px solid rgba(255,255,255,0.06)">
        <input id="ai-input" class="flex-1 rounded-xl px-4 py-3 text-sm" placeholder="Type your request..."
          onkeypress="if(event.key==='Enter')sendAI()">
        <button onclick="sendAI()" class="btn-primary w-12 h-12 rounded-xl flex items-center justify-center text-white">
          <i class="fa-solid fa-paper-plane text-sm"></i>
        </button>
      </div>
    </div>
  </div>

  <!-- HISTORY TAB -->
  <div id="pane-history" class="hidden">
    <div class="glass rounded-2xl p-6">
      <h2 class="text-lg font-bold text-white mb-5"><i class="fa-solid fa-clock-rotate-left mr-2 text-purple-400"></i>Search History</h2>
      <div id="history-list" class="space-y-3">
        <div class="text-sm text-gray-500 text-center py-8">No searches yet</div>
      </div>
    </div>
  </div>

  <!-- SETTINGS TAB -->
  <div id="pane-settings" class="hidden">
    <div class="glass rounded-2xl p-6 space-y-5">
      <h2 class="text-lg font-bold text-white"><i class="fa-solid fa-key mr-2 text-yellow-400"></i>Settings</h2>
      <div class="p-4 rounded-xl" style="background:rgba(16,185,129,0.1);border:1px solid rgba(16,185,129,0.3)">
        <div class="text-sm text-green-400"><i class="fa-solid fa-check-circle mr-2"></i>Apify scraping active. Highly reliable!</div>
      </div>
      <div>
        <label class="text-xs text-gray-400 mb-2 block">Groq API Key (AI Agent এর জন্য)</label>
        <input id="groq-inp" type="password" class="w-full rounded-xl px-4 py-3 text-sm" placeholder="gsk_...">
        <p class="text-xs text-gray-500 mt-2">Free key: <a href="https://console.groq.com/keys" target="_blank" class="text-indigo-400 underline">console.groq.com</a></p>
      </div>
      <button onclick="saveSettings()" class="btn-primary w-full py-3 rounded-xl font-bold text-white text-sm">
        <i class="fa-solid fa-save mr-2"></i>Save Settings
      </button>
      <div id="save-msg" class="hidden text-center text-sm text-green-400 py-2">✅ Saved successfully!</div>
    </div>
  </div>

</div>

<script>
let currentJob = null, aiState = {}, history = [], totalToday = 0;

window.onload = () => {
  const k = localStorage.getItem('groq_key');
  if(k) document.getElementById('groq-inp').value = k;
  loadHistory();
};

function showTab(t) {
  ['manual','ai','history','settings'].forEach(x => {
    document.getElementById('pane-'+x).classList.add('hidden');
    const btn = document.getElementById('tab-'+x);
    if(btn) btn.className = btn.className.replace('tab-active','').trim() + ' text-gray-400';
  });
  document.getElementById('pane-'+t).classList.remove('hidden');
  const ab = document.getElementById('tab-'+t);
  if(ab) { ab.className = ab.className.replace('text-gray-400','').trim(); ab.classList.add('tab-active'); }
}

function saveSettings() {
  const k = document.getElementById('groq-inp').value.trim();
  if(!k) return alert('API Key দাও!');
  localStorage.setItem('groq_key', k);
  fetch('/api/settings', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({groq:k})})
  .then(() => { document.getElementById('save-msg').classList.remove('hidden'); setTimeout(() => document.getElementById('save-msg').classList.add('hidden'), 3000); });
}

function updateStats(leads) {
  document.getElementById('s-total').textContent = leads.length;
  document.getElementById('s-email').textContent = leads.filter(l=>l.Email&&l.Email!='N/A').length;
  document.getElementById('s-phone').textContent = leads.filter(l=>l.Phone&&l.Phone!='N/A').length;
  document.getElementById('s-web').textContent   = leads.filter(l=>l.Website&&l.Website!='N/A').length;
  totalToday += leads.length;
  document.getElementById('total-badge').textContent = totalToday + ' Leads Today';
}

function setStatus(msg, state='loading', pct=null) {
  const box = document.getElementById('status-box');
  box.classList.remove('hidden');
  document.getElementById('st-text').textContent = msg;
  const icon = document.getElementById('st-icon');
  if(state==='loading') icon.className = 'fa-solid fa-circle-notch fa-spin text-indigo-400 text-2xl';
  else if(state==='done') icon.className = 'fa-solid fa-circle-check text-green-400 text-2xl';
  else icon.className = 'fa-solid fa-circle-xmark text-red-400 text-2xl';
  if(pct !== null) document.getElementById('st-bar').style.width = pct+'%';
}

async function startJob(payload) {
  setStatus('Apify দিয়ে scraping শুরু হয়েছে...', 'loading', 10);
  document.getElementById('dl-btn').classList.add('hidden');
  document.getElementById('preview-box').classList.add('hidden');

  const r = await fetch('/api/scrape', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
  const d = await r.json();
  if(d.error) return setStatus(d.error,'error');
  currentJob = d.job_id;

  let pct = 10;
  const iv = setInterval(() => { pct = Math.min(pct+5,85); document.getElementById('st-bar').style.width=pct+'%'; }, 8000);
  
  const poll = async () => {
    const r2 = await fetch('/api/status/'+currentJob);
    const d2 = await r2.json();
    if(d2.status==='done') {
      clearInterval(iv);
      setStatus(`✅ সম্পন্ন! ${d2.count} টি lead পাওয়া গেছে।`, 'done', 100);
      document.getElementById('dl-btn').classList.remove('hidden');
      document.getElementById('st-details').textContent = `📧 Email: ${d2.emails} | 📞 Phone: ${d2.phones} | 🌐 Website: ${d2.websites}`;
      updateStats(d2.leads||[]);
      showPreview(d2.leads||[]);
      addHistory(payload.location, payload.keyword, d2.count);
    } else if(d2.status==='error') {
      clearInterval(iv);
      setStatus('Error: '+d2.error, 'error');
    } else { setTimeout(poll, 6000); }
  };
  setTimeout(poll, 6000);
}

function startManual() {
  const loc = document.getElementById('m-loc').value.trim();
  const kw  = document.getElementById('m-kw').value.trim();
  if(!loc||!kw) return alert('Location আর Keyword দাও!');
  startJob({location:loc, keyword:kw, max_leads:document.getElementById('m-count').value||50, language:document.getElementById('m-lang').value});
}

function doDownload() { if(currentJob) window.location='/api/download/'+currentJob; }

function showPreview(leads) {
  if(!leads.length) return;
  const box = document.getElementById('preview-box');
  box.classList.remove('hidden');
  document.getElementById('preview-count').textContent = '('+leads.length+' rows)';
  
  const keys = Object.keys(leads[0]);
  document.getElementById('tbl-head').innerHTML = keys.map(k=>`<th class="px-3 py-2 text-left font-medium">${k}</th>`).join('');
  document.getElementById('tbl-body').innerHTML = leads.slice(0,10).map(l=>
    `<tr>${keys.map(k=>`<td class="px-3 py-2 text-gray-300 max-w-xs truncate">${l[k]||'N/A'}</td>`).join('')}</tr>`
  ).join('');
}

function addHistory(loc, kw, count) {
  const item = {loc,kw,count,time:new Date().toLocaleTimeString()};
  history.unshift(item);
  localStorage.setItem('lead_history', JSON.stringify(history.slice(0,20)));
  renderHistory();
}

function loadHistory() {
  const saved = localStorage.getItem('lead_history');
  if(saved) { history = JSON.parse(saved); renderHistory(); }
}

function renderHistory() {
  const el = document.getElementById('history-list');
  if(!history.length) { el.innerHTML='<div class="text-sm text-gray-500 text-center py-8">No searches yet</div>'; return; }
  el.innerHTML = history.map((h,i) => `
    <div class="glass rounded-xl p-4 flex items-center justify-between">
      <div>
        <div class="text-sm font-semibold text-white">📍 ${h.loc} — 🔍 ${h.kw}</div>
        <div class="text-xs text-gray-400 mt-1">📊 ${h.count} leads · ${h.time}</div>
      </div>
      <button onclick="rerun(${i})" class="text-xs btn-primary px-3 py-1 rounded-lg text-white font-semibold">
        <i class="fa-solid fa-redo mr-1"></i>Re-run
      </button>
    </div>`).join('');
}

function rerun(i) {
  const h = history[i];
  document.getElementById('m-loc').value = h.loc;
  document.getElementById('m-kw').value  = h.kw;
  showTab('manual');
  startJob({location:h.loc, keyword:h.kw, max_leads:50});
}

// AI CHAT
function addMsg(text, isUser=false, isHtml=false) {
  const box = document.getElementById('chat-box');
  const d = document.createElement('div');
  d.className = isUser ? 'chat-msg-user p-4 max-w-md text-sm text-white ml-auto' : 'chat-msg-bot p-4 max-w-md text-sm text-gray-200';
  d.innerHTML = isHtml ? text : text.replace(/</g,'&lt;').replace(/>/g,'&gt;');
  box.appendChild(d);
  box.scrollTop = 99999;
}

async function sendAI() {
  const inp = document.getElementById('ai-input');
  const text = inp.value.trim(); if(!text) return;
  const key = localStorage.getItem('groq_key');
  if(!key) { addMsg('⚠️ Settings এ গিয়ে Groq API Key যোগ করো!', false); return; }
  addMsg(text, true); inp.value='';
  addMsg('<i class="fa-solid fa-ellipsis fa-fade"></i>', false, true);

  const r = await fetch('/api/chat', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text,state:aiState,groq_key:key})});
  const d = await r.json();
  document.getElementById('chat-box').lastChild.remove();

  if(d.error) { addMsg('❌ '+d.error, false); return; }
  if(d.ready) {
    aiState = d.state;
    addMsg(`Got it! Starting search...<br>📍 <b>${d.state.loc}</b> · 🔍 <b>${d.state.kw}</b> · 🔢 <b>${d.state.count}</b> leads<br><br>
      <button onclick="startJob({location:'${d.state.loc}',keyword:'${d.state.kw}',max_leads:${d.state.count}})" 
        style="background:linear-gradient(135deg,#10b981,#059669);padding:8px 20px;border-radius:10px;font-weight:700;color:white;margin-top:8px;cursor:pointer">
        🚀 Start Now</button>`, false, true);
    showTab('manual');
  } else { addMsg(d.reply, false); }
}
</script>
</body>
</html>"""

@flask_app.route('/')
def index(): return render_template_string(HTML)

@flask_app.route('/api/settings', methods=['POST'])
def api_settings():
    if request.json.get('groq'): runtime['groq_key'] = request.json['groq']
    return jsonify({"ok": True})

@flask_app.route('/api/chat', methods=['POST'])
def api_chat():
    text     = request.json.get('text','')
    groq_key = request.json.get('groq_key','')
    state    = request.json.get('state',{})
    runtime['groq_key'] = groq_key
    try:
        parsed = parse_with_ai(text)
        if parsed.get('loc'): state['loc']   = parsed['loc']
        if parsed.get('kw'):  state['kw']    = parsed['kw']
        if parsed.get('count'): state['count'] = parsed['count']
        if not state.get('loc') or not state.get('kw'):
            return jsonify({"ready":False,"reply":"Location আর keyword বলো।"})
        return jsonify({"ready":True,"state":state})
    except Exception as e:
        return jsonify({"error":str(e)})

def run_job(job_id, data):
    try:
        jobs[job_id] = {'status':'running'}
        leads = scrape_leads(data['location'], data['keyword'], data.get('max_leads',50))
        jobs[job_id] = {
            'status':'done','leads':leads,'count':len(leads),
            'emails':  sum(1 for l in leads if l.get('Email','N/A') not in ('N/A','')),
            'phones':  sum(1 for l in leads if l.get('Phone','N/A') not in ('N/A','')),
            'websites':sum(1 for l in leads if l.get('Website','N/A') not in ('N/A','')),
        }
    except Exception as e:
        jobs[job_id] = {'status':'error','error':str(e)}

@flask_app.route('/api/scrape', methods=['POST'])
def api_scrape():
    job_id = str(uuid.uuid4())[:8]
    threading.Thread(target=run_job, args=(job_id, request.json), daemon=True).start()
    return jsonify({'job_id': job_id})

@flask_app.route('/api/status/<jid>')
def api_status(jid):
    j = jobs.get(jid, {'status':'not_found'})
    return jsonify({**j, 'leads': j.get('leads',[])[:10] if j.get('status')=='done' else []})

@flask_app.route('/api/download/<jid>')
def api_download(jid):
    j = jobs.get(jid)
    if not j or j['status']!='done': return "Not ready",400
    leads = j['leads']
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=leads[0].keys())
    writer.writeheader(); writer.writerows(leads); out.seek(0)
    return send_file(io.BytesIO(out.getvalue().encode('utf-8-sig')),
        mimetype='text/csv', as_attachment=True, download_name='leads.csv')

# ══════════════════════════════════════════
#  TELEGRAM BOT
# ══════════════════════════════════════════
M_LOC, M_KW, M_COUNT = range(3)
bot_store = {}

def to_csv_file(leads):
    tmp = tempfile.NamedTemporaryFile(mode='w',suffix='.csv',delete=False,encoding='utf-8-sig',newline='')
    writer = csv.DictWriter(tmp, fieldnames=leads[0].keys())
    writer.writeheader(); writer.writerows(leads); tmp.close()
    return tmp.name

async def tg_start(update: Update, ctx):
    kb = [[InlineKeyboardButton("🔍 Search Leads", callback_data="go")]]
    await update.message.reply_text(
        "👋 *LeadGen Pro Bot*\n\nGoogle Maps থেকে leads বের করবো।\nShows: Name, Phone, Email, Address, Website\n\nShুরু করতে নিচের বাটনে ক্লিক করো!",
        parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))

async def tg_go(update: Update, ctx):
    q = update.callback_query; await q.answer()
    bot_store[q.from_user.id] = {}
    await q.edit_message_text("📍 Location দাও:\nউদাহরণ: `Gulshan Dhaka`", parse_mode='Markdown')
    return M_LOC

async def tg_loc(update: Update, ctx):
    bot_store[update.message.from_user.id]['location'] = update.message.text.strip()
    await update.message.reply_text("🔍 Keyword দাও:\nউদাহরণ: `restaurant`", parse_mode='Markdown')
    return M_KW

async def tg_kw(update: Update, ctx):
    bot_store[update.message.from_user.id]['keyword'] = update.message.text.strip()
    await update.message.reply_text("🔢 কয়টা lead লাগবে? (max 50):", parse_mode='Markdown')
    return M_COUNT

async def tg_count(update: Update, ctx):
    uid  = update.message.from_user.id
    bot_store[uid]['max_leads'] = update.message.text.strip()
    d    = bot_store[uid]
    kb   = [[InlineKeyboardButton("✅ শুরু করো", callback_data="scrape"),
             InlineKeyboardButton("❌ বাতিল",    callback_data="cancel")]]
    await update.message.reply_text(
        f"📋 *Summary*\n📍 {d['location']} · 🔍 {d['keyword']} · 🔢 {d['max_leads']} leads\n\nশুরু করবো?",
        parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))
    return ConversationHandler.END

async def tg_scrape(update: Update, ctx):
    q   = update.callback_query; await q.answer()
    uid = q.from_user.id
    d   = bot_store.get(uid, {})
    if not d:
        await q.edit_message_text("❌ Session শেষ। /start দিয়ে আবার শুরু করো।")
        return
    msg = await q.edit_message_text("⏳ *Scraping চলছে...*\n_৩–৫ মিনিট লাগতে পারে_", parse_mode='Markdown')
    try:
        loop  = asyncio.get_event_loop()
        leads = await loop.run_in_executor(None, scrape_leads, d['location'], d['keyword'], d.get('max_leads',50))
        if not leads:
            await ctx.bot.edit_message_text(chat_id=q.message.chat_id, message_id=msg.message_id, text="😔 কোনো result নেই।")
            return
        path = to_csv_file(leads)
        em   = sum(1 for l in leads if l.get('Email','N/A') not in ('N/A',''))
        ph   = sum(1 for l in leads if l.get('Phone','N/A') not in ('N/A',''))
        await ctx.bot.edit_message_text(chat_id=q.message.chat_id, message_id=msg.message_id, text="✅ হয়ে গেছে!")
        with open(path,'rb') as f:
            await ctx.bot.send_document(
                chat_id=q.message.chat_id, document=f,
                filename=f"leads_{d['location']}_{d['keyword']}.csv".replace(' ','_'),
                caption=f"🎯 *{d['location']}* — *{d['keyword']}*\n📊 Total: *{len(leads)}* | 📧 Email: *{em}* | 📞 Phone: *{ph}*\n\n/start দিয়ে নতুন search",
                parse_mode='Markdown')
        os.unlink(path)
    except Exception as e:
        await ctx.bot.edit_message_text(chat_id=q.message.chat_id, message_id=msg.message_id, text=f"❌ `{e}`", parse_mode='Markdown')

async def tg_cancel(update: Update, ctx):
    await update.message.reply_text("❌ বাতিল।")
    return ConversationHandler.END

def run_bot():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(tg_go, pattern="^go$")],
        states={
            M_LOC:   [MessageHandler(filters.TEXT & ~filters.COMMAND, tg_loc)],
            M_KW:    [MessageHandler(filters.TEXT & ~filters.COMMAND, tg_kw)],
            M_COUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, tg_count)],
        },
        fallbacks=[CommandHandler("cancel", tg_cancel)],
        per_message=False,
    )
    app.add_handler(CommandHandler("start", tg_start))
    app.add_handler(conv)
    app.add_handler(CallbackQueryHandler(tg_scrape, pattern="^scrape$"))
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

# ══════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════
if __name__ == "__main__":
    threading.Thread(target=run_bot, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    flask_app.run(host="0.0.0.0", port=port)
