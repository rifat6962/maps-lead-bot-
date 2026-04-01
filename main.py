import os, csv, asyncio, tempfile, threading, io, uuid, re, json, time, urllib.parse
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
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
runtime = {"groq_key": os.getenv("GROQ_API_KEY", "")}

# ══════════════════════════════════════════
#  GOOGLE MAPS SCRAPER — pure Python
# ══════════════════════════════════════════

HEADERS_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118.0.0.0 Safari/537.36",
]

def get_headers():
    import random
    return {
        "User-Agent": random.choice(HEADERS_LIST),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Referer": "https://www.google.com/",
    }

def scrape_maps(location: str, keyword: str, max_leads: int = 50, email_only: bool = False) -> list:
    leads     = []
    seen      = set()
    query     = f"{keyword} in {location}"
    encoded   = urllib.parse.quote_plus(query)

    # ── Phase 1: Google Maps local search (tbm=lcl) ──
    for start in range(0, min(max_leads * 3, 200), 20):
        if len(leads) >= max_leads: break
        url = f"https://www.google.com/search?q={encoded}&tbm=lcl&start={start}&num=20&hl=en"
        try:
            r    = requests.get(url, headers=get_headers(), timeout=12)
            soup = BeautifulSoup(r.text, 'lxml')

            blocks = soup.select('div.VkpGBb, div.rllt__details, div[jscontroller]')
            if not blocks:
                # fallback selector
                blocks = soup.select('div.uMdZh, div.cXedhc')
            if not blocks:
                break

            for block in blocks:
                if len(leads) >= max_leads: break
                text = block.get_text(separator=' ', strip=True)

                # Name
                name_el = (block.select_one('div[role="heading"]') or
                           block.select_one('.dbg0pd') or
                           block.select_one('span.OSrXXb'))
                name = name_el.get_text(strip=True) if name_el else None
                if not name or len(name) < 3 or name in seen:
                    continue

                # Location verify — address এ location এর কোনো word থাকতে হবে
                if not _loc_match(text, location):
                    continue

                seen.add(name)

                # Rating
                rat = re.search(r'(\d\.\d)\s*[\(\d]', text)
                rating = rat.group(1) if rat else 'N/A'

                # Phone
                ph = re.search(
                    r'(\+?1?\s?[\(\-]?\d{3}[\)\-\s]?\s?\d{3}[\-\s]?\d{4}'
                    r'|\+?880[\s\-]?\d{2}[\s\-]?\d{8}'
                    r'|\+?8801[3-9]\d{8}|01[3-9]\d{8})', text
                )
                phone = ph.group(0).strip() if ph else 'N/A'

                # Website
                website = 'N/A'
                for a in block.select('a[href]'):
                    href = a['href']
                    if '/url?q=' in href:
                        clean = urllib.parse.unquote(href.split('/url?q=')[1].split('&')[0])
                        if 'google' not in clean.lower() and clean.startswith('http'):
                            website = clean; break
                    elif href.startswith('http') and 'google' not in href.lower():
                        website = href; break

                # Address
                addr_el = block.select_one('.rllt__details div:nth-child(2), .lqhpac')
                address = addr_el.get_text(strip=True) if addr_el else location

                # Maps link
                maps_link = 'N/A'
                for a in block.select('a[href]'):
                    if 'maps.google' in a.get('href','') or '/maps/' in a.get('href',''):
                        maps_link = a['href']; break

                # Email
                email = extract_email(website)

                if email_only and email == 'N/A':
                    continue

                leads.append({
                    'Name':     name,
                    'Phone':    phone,
                    'Email':    email,
                    'Address':  address,
                    'Rating':   rating,
                    'Website':  website,
                    'Maps_Link':maps_link,
                })

            time.sleep(1.5)

        except Exception as e:
            print(f"Phase1 error: {e}")
            break

    # ── Phase 2: Google Maps direct URL (fallback) ──
    if len(leads) < min(max_leads, 5):
        leads += _phase2_search(keyword, location, max_leads - len(leads),
                                email_only, seen)

    return leads[:max_leads]


def _loc_match(text: str, location: str) -> bool:
    """location এর যেকোনো meaningful word address এ আছে কিনা"""
    text_l = text.lower()
    for word in location.lower().split(','):
        word = word.strip()
        if len(word) > 3 and word in text_l:
            return True
    return False


def _phase2_search(keyword, location, needed, email_only, seen):
    """Google Maps /search URL থেকে JSON data extract"""
    results = []
    query   = urllib.parse.quote_plus(f"{keyword} {location}")
    url     = f"https://www.google.com/maps/search/{query}"
    try:
        r    = requests.get(url, headers=get_headers(), timeout=15)
        text = r.text

        # Maps এর JSON data থেকে business info বের করো
        names    = re.findall(r'"([^"]{3,60})"(?:,null){0,3},"[^"]*","[^"]*"', text)
        phones   = re.findall(
            r'(\+?1?\s?[\(\-]?\d{3}[\)\-\s]?\s?\d{3}[\-\s]?\d{4})', text)
        websites = re.findall(
            r'https?://(?!(?:www\.google|maps\.google|goo\.gl|'
            r'googleapis|facebook|instagram|twitter))[a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,}'
            r'(?:/[^\s"\'<>]*)?', text)
        ratings  = re.findall(r'"(\d\.\d)"', text)

        websites = list(dict.fromkeys(
            w for w in websites if 'google' not in w.lower()
        ))

        for i, name in enumerate(names[:needed]):
            if name in seen: continue
            seen.add(name)
            website = websites[i] if i < len(websites) else 'N/A'
            email   = extract_email(website)
            if email_only and email == 'N/A': continue
            results.append({
                'Name':     name,
                'Phone':    phones[i]  if i < len(phones)  else 'N/A',
                'Email':    email,
                'Address':  location,
                'Rating':   ratings[i] if i < len(ratings) else 'N/A',
                'Website':  website,
                'Maps_Link':'N/A',
            })
    except Exception as e:
        print(f"Phase2 error: {e}")
    return results


# ══════════════════════════════════════════
#  EMAIL EXTRACTOR
# ══════════════════════════════════════════
EMAIL_RE   = r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'
EMAIL_SKIP = ['example','domain','sentry','wixpress','noreply','@2x',
              '.png','.jpg','no-reply','amazonaws','cloudfront','schema']

def extract_email(url: str) -> str:
    if not url or url == 'N/A': return 'N/A'
    if not url.startswith('http'): url = 'http://' + url

    def valid(lst):
        return [e.lower() for e in lst
                if not any(s in e.lower() for s in EMAIL_SKIP)
                and '.' in e.split('@')[-1] and len(e) > 6]

    for path in ['', '/contact', '/contact-us', '/about']:
        try:
            r    = requests.get(
                urllib.parse.urljoin(url, path),
                headers=get_headers(), timeout=8,
                verify=False, allow_redirects=True
            )
            soup = BeautifulSoup(r.text, 'lxml')
            for a in soup.select('a[href^="mailto:"]'):
                e = a['href'].replace('mailto:', '').split('?')[0].strip()
                if valid([e]): return e
            found = valid(re.findall(EMAIL_RE, r.text))
            if found: return found[0]
        except: continue
    return 'N/A'


# ══════════════════════════════════════════
#  GROQ AI
# ══════════════════════════════════════════
def parse_with_ai(text: str, key: str) -> dict:
    if not key: raise Exception("Groq API Key নেই। Settings এ যোগ করো।")
    client = Groq(api_key=key)
    prompt = f'''Extract from user input:
- loc: exact location/city/country (e.g. "Vancouver, Canada")
- kw: business keyword (e.g. "car showroom")
- count: number of leads (integer, default 50)
- email_only: true if user wants only leads that have emails, else false

Input: "{text}"
Return ONLY valid JSON: {{"loc":"Vancouver, Canada","kw":"car showroom","count":50,"email_only":false}}'''

    resp = client.chat.completions.create(
        messages=[{"role": "user", "content": prompt}],
        model="llama-3.3-70b-versatile",
        temperature=0
    )
    m = re.search(r'\{.*\}', resp.choices[0].message.content, re.DOTALL)
    return json.loads(m.group(0)) if m else {}


# ══════════════════════════════════════════
#  FLASK DASHBOARD
# ══════════════════════════════════════════
flask_app = Flask(__name__)
jobs      = {}

HTML = r"""<!DOCTYPE html>
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
        <span class="text-xs text-slate-500">Pure Python · No API</span>
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
    <div class="card card-hover p-4 fade"><div class="text-2xl font-bold text-white" id="st">0</div><div class="text-xs text-slate-500 mt-1 flex items-center gap-1"><i class="fa-solid fa-users text-indigo-400 text-xs"></i>Total</div></div>
    <div class="card card-hover p-4 fade"><div class="text-2xl font-bold text-emerald-400" id="se">0</div><div class="text-xs text-slate-500 mt-1 flex items-center gap-1"><i class="fa-solid fa-envelope text-emerald-400 text-xs"></i>Email</div></div>
    <div class="card card-hover p-4 fade"><div class="text-2xl font-bold text-sky-400" id="sp">0</div><div class="text-xs text-slate-500 mt-1 flex items-center gap-1"><i class="fa-solid fa-phone text-sky-400 text-xs"></i>Phone</div></div>
    <div class="card card-hover p-4 fade"><div class="text-2xl font-bold text-violet-400" id="sw">0</div><div class="text-xs text-slate-500 mt-1 flex items-center gap-1"><i class="fa-solid fa-globe text-violet-400 text-xs"></i>Website</div></div>
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
          <label class="text-xs text-slate-500 mb-1.5 block">🔢 Number of Leads</label>
          <input id="m-count" type="number" value="50" class="inp">
        </div>
        <div>
          <label class="text-xs text-slate-500 mb-1.5 block">📧 Filter</label>
          <select id="m-eo" class="inp">
            <option value="false">All Leads</option>
            <option value="true">Email আছে শুধু</option>
          </select>
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
          <span style="color:#818cf8;font-style:italic">"Find 30 car showrooms in Vancouver, Canada — email only"</span>
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
        <i class="fa-solid fa-check-circle mr-2"></i>Pure Python scraping — 100% free, no API key needed for scraping
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

function updStats(leads){
  document.getElementById('st').textContent=leads.length;
  document.getElementById('se').textContent=leads.filter(l=>l.Email&&l.Email!='N/A').length;
  document.getElementById('sp').textContent=leads.filter(l=>l.Phone&&l.Phone!='N/A').length;
  document.getElementById('sw').textContent=leads.filter(l=>l.Website&&l.Website!='N/A').length;
  today+=leads.length; document.getElementById('tn').textContent=today;
}

async function startJob(payload){
  setSt('Scraping শুরু হয়েছে...','load',6);
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
      setSt(`✅ সম্পন্ন — ${d2.count} leads`,'done',100);
      document.getElementById('sdet').textContent=`📧 ${d2.emails} email · 📞 ${d2.phones} phone · 🌐 ${d2.websites} website`;
      document.getElementById('dlbtn').classList.remove('hidden');
      updStats(d2.leads||[]);
      showPV(d2.leads||[]);
      addH(payload.location,payload.keyword,d2.count,payload.email_only);
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
  startJob({location:loc,keyword:kw,max_leads:parseInt(document.getElementById('m-count').value)||50,email_only:document.getElementById('m-eo').value==='true'});
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

function addH(loc,kw,count,eo){ hist.unshift({loc,kw,count,eo,t:new Date().toLocaleTimeString()}); hist=hist.slice(0,20); localStorage.setItem('lh',JSON.stringify(hist)); renderH(); }
function clearH(){ hist=[]; localStorage.removeItem('lh'); renderH(); }
function renderH(){
  const el=document.getElementById('hlist');
  if(!hist.length){el.innerHTML='<div class="text-xs text-slate-600 text-center py-8">No history yet</div>';return;}
  el.innerHTML=hist.map((h,i)=>`
    <div class="p-4 rounded-xl flex items-center justify-between fade" style="background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.05)">
      <div>
        <div class="text-sm font-semibold text-white">📍 ${h.loc} · 🔍 ${h.kw}</div>
        <div class="text-xs text-slate-500 mt-1">📊 ${h.count} leads · ${h.eo?'Email only · ':''}${h.t}</div>
      </div>
      <button onclick="rerun(${i})" class="btn-p px-3 py-1.5 rounded-lg text-xs"><i class="fa-solid fa-redo mr-1"></i>Re-run</button>
    </div>`).join('');
}
function rerun(i){ const h=hist[i]; document.getElementById('m-loc').value=h.loc; document.getElementById('m-kw').value=h.kw; showTab('manual'); startJob({location:h.loc,keyword:h.kw,max_leads:50,email_only:h.eo||false}); }

// AI
function addM(html,isU=false){ const box=document.getElementById('cbox'); const d=document.createElement('div'); d.className=(isU?'chat-user':'chat-bot')+' fade'; d.innerHTML=html; box.appendChild(d); box.scrollTop=99999; return d; }

async function sendAI(){
  const inp=document.getElementById('ai-inp'); const text=inp.value.trim(); if(!text) return;
  const key=localStorage.getItem('gk');
  if(!key){addM('⚠️ Settings এ Groq API Key দাও।');return;}
  addM(text.replace(/</g,'&lt;'),true); inp.value='';
  const ld=addM('<i class="fa-solid fa-ellipsis blink"></i>');
  const r=await fetch('/api/chat',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text,state:aiSt,groq_key:key})});
  const d=await r.json(); ld.remove();
  if(d.error){addM('❌ '+d.error);return;}
  if(d.ready){
    aiSt=d.state;
    addM(`Got it! 🎯<br><br>📍 <b>${d.state.loc}</b> · 🔍 <b>${d.state.kw}</b> · 🔢 <b>${d.state.count}</b>${d.state.email_only?' · 📧 Email only':''}<br><br>
      <button onclick="startJob({location:'${d.state.loc}',keyword:'${d.state.kw}',max_leads:${d.state.count},email_only:${d.state.email_only||false}});showTab('manual')"
        style="margin-top:10px;padding:8px 18px;border-radius:9px;font-size:12px;font-weight:700;cursor:pointer;background:linear-gradient(135deg,#059669,#0d9488);color:white">
        🚀 Start Now</button>`);
  } else { addM(d.reply); }
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
    text     = request.json.get('text', '')
    groq_key = request.json.get('groq_key', '')
    state    = request.json.get('state', {})
    if groq_key: runtime['groq_key'] = groq_key
    try:
        parsed = parse_with_ai(text, groq_key or runtime['groq_key'])
        if parsed.get('loc'):        state['loc']        = parsed['loc']
        if parsed.get('kw'):         state['kw']         = parsed['kw']
        if parsed.get('count'):      state['count']      = parsed['count']
        if 'email_only' in parsed:   state['email_only'] = parsed['email_only']
        if not state.get('loc') or not state.get('kw'):
            return jsonify({"ready": False, "reply": "Location আর keyword বলো।"})
        return jsonify({"ready": True, "state": state})
    except Exception as e:
        return jsonify({"error": str(e)})

def run_job(job_id, data):
    try:
        jobs[job_id] = {'status': 'running'}
        leads = scrape_maps(
            data['location'], data['keyword'],
            int(data.get('max_leads', 50)),
            bool(data.get('email_only', False))
        )
        jobs[job_id] = {
            'status':   'done',
            'leads':    leads,
            'count':    len(leads),
            'emails':   sum(1 for l in leads if l.get('Email','N/A') not in ('N/A','')),
            'phones':   sum(1 for l in leads if l.get('Phone','N/A') not in ('N/A','')),
            'websites': sum(1 for l in leads if l.get('Website','N/A') not in ('N/A','')),
        }
    except Exception as e:
        jobs[job_id] = {'status': 'error', 'error': str(e)}

@flask_app.route('/api/scrape', methods=['POST'])
def api_scrape():
    job_id = str(uuid.uuid4())[:8]
    threading.Thread(target=run_job, args=(job_id, request.json), daemon=True).start()
    return jsonify({'job_id': job_id})

@flask_app.route('/api/status/<jid>')
def api_status(jid):
    j   = jobs.get(jid, {'status': 'not_found'})
    out = dict(j)
    if out.get('status') == 'done':
        out['leads'] = j.get('leads', [])[:10]
    return jsonify(out)

@flask_app.route('/api/download/<jid>')
def api_download(jid):
    j = jobs.get(jid)
    if not j or j['status'] != 'done': return "Not ready", 400
    leads = j['leads']
    out   = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=leads[0].keys())
    writer.writeheader(); writer.writerows(leads); out.seek(0)
    return send_file(
        io.BytesIO(out.getvalue().encode('utf-8-sig')),
        mimetype='text/csv', as_attachment=True, download_name='leads.csv'
    )

# ══════════════════════════════════════════
#  TELEGRAM BOT
# ══════════════════════════════════════════
M_LOC, M_KW, M_COUNT = range(3)
bot_store = {}

def to_csv_file(leads):
    tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8-sig', newline='')
    writer = csv.DictWriter(tmp, fieldnames=leads[0].keys())
    writer.writeheader(); writer.writerows(leads); tmp.close()
    return tmp.name

async def tg_start(update: Update, ctx):
    kb = [[InlineKeyboardButton("🔍 Search Leads", callback_data="go")]]
    await update.message.reply_text(
        "👋 *LeadGen Pro*\n\n✅ Pure Python scraping\n✅ Location-verified\n✅ Email extraction\n\nShুরু করতে বাটনে ক্লিক করো!",
        parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))

async def tg_go(update: Update, ctx):
    q = update.callback_query; await q.answer()
    bot_store[q.from_user.id] = {}
    await q.edit_message_text("📍 Location দাও:\nউদাহরণ: `Vancouver, Canada`", parse_mode='Markdown')
    return M_LOC

async def tg_loc(update: Update, ctx):
    bot_store[update.message.from_user.id]['location'] = update.message.text.strip()
    await update.message.reply_text("🔍 Keyword দাও:\nউদাহরণ: `car showroom`")
    return M_KW

async def tg_kw(update: Update, ctx):
    bot_store[update.message.from_user.id]['keyword'] = update.message.text.strip()
    await update.message.reply_text("🔢 কয়টা lead লাগবে?")
    return M_COUNT

async def tg_count(update: Update, ctx):
    uid = update.message.from_user.id
    bot_store[uid]['max_leads'] = update.message.text.strip()
    d   = bot_store[uid]
    kb  = [
        [InlineKeyboardButton("✅ All Leads",   callback_data="sc_all"),
         InlineKeyboardButton("📧 Email Only",  callback_data="sc_email")],
        [InlineKeyboardButton("❌ বাতিল",        callback_data="cancel")]
    ]
    await update.message.reply_text(
        f"📋 *{d['location']}* · *{d['keyword']}* · *{d['max_leads']}* leads",
        parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))
    return ConversationHandler.END

async def tg_scrape(update: Update, ctx):
    q          = update.callback_query; await q.answer()
    uid        = q.from_user.id
    email_only = q.data == 'sc_email'
    d          = bot_store.get(uid, {})
    if not d:
        await q.edit_message_text("❌ /start দিয়ে আবার শুরু করো।"); return
    msg = await q.edit_message_text("⏳ *Scraping চলছে...*", parse_mode='Markdown')
    try:
        loop  = asyncio.get_event_loop()
        leads = await loop.run_in_executor(
            None, scrape_maps,
            d['location'], d['keyword'],
            int(d.get('max_leads', 50)), email_only
        )
        if not leads:
            await ctx.bot.edit_message_text(
                chat_id=q.message.chat_id, message_id=msg.message_id,
                text="😔 কোনো result নেই। Location আরো specific করো।")
            return
        path = to_csv_file(leads)
        em   = sum(1 for l in leads if l.get('Email','N/A') not in ('N/A',''))
        ph   = sum(1 for l in leads if l.get('Phone','N/A') not in ('N/A',''))
        await ctx.bot.edit_message_text(
            chat_id=q.message.chat_id, message_id=msg.message_id, text="✅ হয়ে গেছে!")
        with open(path, 'rb') as f:
            await ctx.bot.send_document(
                chat_id=q.message.chat_id, document=f,
                filename=f"leads_{d['location']}_{d['keyword']}.csv".replace(' ','_'),
                caption=f"🎯 *{d['location']}* — *{d['keyword']}*\n📊 {len(leads)} | 📧 {em} | 📞 {ph}\n\n/start",
                parse_mode='Markdown')
        os.unlink(path)
    except Exception as e:
        await ctx.bot.edit_message_text(
            chat_id=q.message.chat_id, message_id=msg.message_id,
            text=f"❌ `{e}`", parse_mode='Markdown')

async def tg_cancel(update: Update, ctx):
    await update.message.reply_text("❌ বাতিল।")
    return ConversationHandler.END

def run_bot():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app  = Application.builder().token(TELEGRAM_TOKEN).build()
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
    app.add_handler(CallbackQueryHandler(tg_scrape, pattern="^sc_(all|email)$"))
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    threading.Thread(target=run_bot, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    flask_app.run(host="0.0.0.0", port=port)
