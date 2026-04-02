import os, csv, asyncio, tempfile, threading, io, uuid, re, time, json, urllib.parse, random
import requests
import concurrent.futures
import urllib3
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from groq import Groq
from flask import Flask, render_template_string, request, send_file, jsonify
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler, CallbackQueryHandler
)
from datetime import datetime, timezone, timedelta
import pytz

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
BD_TZ = pytz.timezone("Asia/Dhaka")

# ════════════════════════════════════════════════════
#   ROTATING HEADERS
# ════════════════════════════════════════════════════
def get_headers():
    HEADERS_LIST = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 Version/16.0 Mobile/15E148 Safari/604.1",
    ]
    return {
        "User-Agent": random.choice(HEADERS_LIST),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Referer": "https://www.google.com/",
        "DNT": "1",
        "Connection": "keep-alive",
    }

# ════════════════════════════════════════════════════
#   GOOGLE SHEETS DB WRAPPER
# ════════════════════════════════════════════════════
class GoogleSheetsDB:
    def __init__(self, webhook_url):
        self.url = webhook_url

    def _post_async(self, payload):
        try:
            requests.post(self.url, json=payload, timeout=15)
        except:
            pass

    def send_action(self, action, data):
        if not self.url: return
        payload = {"action": action, "data": data}
        threading.Thread(target=self._post_async, args=(payload,), daemon=True).start()

    def log(self, action, details):
        self.send_action("log", {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "action": action,
            "details": details
        })

# ════════════════════════════════════════════════════
#   ADVANCED KEYWORD ENGINE — 100-200+ GUARANTEED
# ════════════════════════════════════════════════════
class AdvancedKeywordEngine:
    """Multi-strategy keyword generator: AI + autosuggest + expansion."""

    COMMERCIAL_PREFIXES = [
        "best", "top", "affordable", "cheap", "local", "professional",
        "experienced", "certified", "trusted", "rated", "licensed",
        "expert", "reliable", "fast", "emergency", "24 hour", "same day",
        "family", "luxury", "premium", "budget", "high quality"
    ]
    COMMERCIAL_SUFFIXES = [
        "services", "company", "agency", "near me", "in my area",
        "specialist", "experts", "professionals", "contractor", "provider",
        "consultant", "firm", "studio", "clinic", "center", "shop",
        "office", "team", "solutions", "group"
    ]
    INTENT_MODIFIERS = [
        "hire", "find", "looking for", "need", "want",
        "best rated", "top rated", "highly reviewed", "award winning",
        "recommended", "free quote", "free estimate", "low cost",
        "pricing", "reviews", "complaints", "bad reviews", "poor service",
        "negative reviews", "worst", "avoid", "problems with"
    ]
    NICHE_MODIFIERS = {
        "restaurant": ["takeout", "delivery", "dine in", "catering", "buffet", "food"],
        "dentist": ["dental clinic", "teeth whitening", "orthodontist", "braces", "dental implants"],
        "lawyer": ["attorney", "law firm", "legal services", "counsel", "litigation"],
        "plumber": ["plumbing", "pipe repair", "drain cleaning", "water heater", "leak fix"],
        "realtor": ["real estate agent", "property dealer", "home buyer", "home seller", "property management"],
        "gym": ["fitness center", "workout", "personal trainer", "crossfit", "yoga studio"],
        "salon": ["hair salon", "beauty salon", "spa", "barber shop", "nail salon"],
        "doctor": ["physician", "medical clinic", "urgent care", "specialist", "general practitioner"],
    }

    def __init__(self):
        self.session = requests.Session()

    def google_autosuggest(self, keyword, location):
        """Scrape Google autosuggest for keyword variations."""
        results = set()
        base_terms = [keyword, f"{keyword} {location}", f"best {keyword}", f"{keyword} services"]
        for term in base_terms:
            try:
                url = f"https://suggestqueries.google.com/complete/search?client=firefox&q={urllib.parse.quote_plus(term)}"
                r = self.session.get(url, headers=get_headers(), timeout=5)
                data = r.json()
                if isinstance(data, list) and len(data) > 1:
                    for suggestion in data[1]:
                        results.add(suggestion.strip())
            except:
                pass
            time.sleep(0.3)
        return list(results)

    def expand_with_variations(self, base_kw):
        """Generate prefix/suffix/modifier variations."""
        results = set()
        for prefix in self.COMMERCIAL_PREFIXES:
            results.add(f"{prefix} {base_kw}")
        for suffix in self.COMMERCIAL_SUFFIXES:
            results.add(f"{base_kw} {suffix}")
        for modifier in self.INTENT_MODIFIERS:
            results.add(f"{modifier} {base_kw}")
        # Niche-specific
        base_lower = base_kw.lower()
        for niche_key, mods in self.NICHE_MODIFIERS.items():
            if niche_key in base_lower:
                for mod in mods:
                    results.add(mod)
                    for prefix in self.COMMERCIAL_PREFIXES[:5]:
                        results.add(f"{prefix} {mod}")
        return list(results)

    def ai_generate(self, base_kw, location, used_kws):
        """AI-powered generation with strict 100+ output."""
        fallback = self.expand_with_variations(base_kw)
        if not GROQ_API_KEY:
            return fallback
        try:
            client = Groq(api_key=GROQ_API_KEY)
            prompt = f"""You are a local SEO and lead generation expert.
Seed keyword: "{base_kw}"
Target location: "{location}"
Already used: {list(used_kws)[:20]}

Generate 120 unique search terms a small business owner or customer would type into Google.
Include:
- Service variations (e.g. emergency {base_kw}, mobile {base_kw})
- Problem-based terms (e.g. bad {base_kw}, {base_kw} complaints, {base_kw} problems)  
- Niche subcategories
- Local intent terms
- Competitor/comparison terms
- Review-seeking terms (worst {base_kw}, poor {base_kw} reviews)

Return ONLY a comma-separated list of terms. No numbering, no explanation."""
            res = client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model="llama3-8b-8192",
                temperature=0.8,
                max_tokens=2000,
            )
            text = res.choices[0].message.content
            ai_kws = [k.strip().strip('"').strip("'") for k in text.split(',') if k.strip() and k.strip().lower() not in used_kws]
            combined = list(set(ai_kws + fallback))
            return combined
        except:
            return fallback

    def generate_full_pool(self, base_kw, location, used_kws):
        """Generate 150-200+ keywords using all strategies."""
        all_kws = set()
        # Strategy 1: AI
        ai_kws = self.ai_generate(base_kw, location, used_kws)
        all_kws.update(ai_kws)
        # Strategy 2: Autosuggest
        suggest_kws = self.google_autosuggest(base_kw, location)
        all_kws.update(suggest_kws)
        # Strategy 3: Expansion
        expanded = self.expand_with_variations(base_kw)
        all_kws.update(expanded)
        # Filter out used and clean up
        final = [k for k in all_kws if k.lower() not in used_kws and len(k) > 3]
        # Ensure min 100
        if len(final) < 100:
            extra_prefixes = ["top rated", "best local", "near me", "professional", "affordable", "emergency", "certified", "licensed", "experienced", "trusted"]
            for p in extra_prefixes:
                final.append(f"{p} {base_kw}")
        return list(set(final))


# ════════════════════════════════════════════════════
#   UPGRADED GOOGLE MAPS SCRAPER (NEGATIVE PRIORITY)
# ════════════════════════════════════════════════════
class GoogleMapsScraper:
    def fetch_batch(self, keyword, location):
        query = urllib.parse.quote_plus(f"{keyword} in {location}")
        all_leads = []

        def get_offset(start):
            url = f"https://www.google.com/search?q={query}&tbm=lcl&start={start}&num=20&hl=en"
            try:
                res = requests.get(url, headers=get_headers(), timeout=12)
                soup = BeautifulSoup(res.text, 'html.parser')
                blocks = soup.select('div.VkpGBb, div.rllt__details, div.uMdZh, div.cXedhc')
                batch = []
                for block in blocks:
                    text_content = block.get_text(separator=' ', strip=True)
                    name_el = block.select_one('div[role="heading"], .dbg0pd, span.OSrXXb')
                    name = name_el.get_text(strip=True) if name_el else "N/A"
                    if name == "N/A" or len(name) < 3: continue

                    rating_match = re.search(r'(\d[\.,]\d)\s*[\(\d]', text_content)
                    rating = rating_match.group(1).replace(',', '.') if rating_match else "N/A"

                    review_count_match = re.search(r'\((\d+)\)', text_content)
                    review_count = review_count_match.group(1) if review_count_match else "0"

                    ph = re.search(r'(\+?\d{1,3}[\s\-\(\)]?\d{3,4}[\s\-\(\)]?\d{3,4}[\s\-\(\)]?\d{3,4})', text_content)
                    phone = ph.group(0).strip() if ph else "N/A"

                    website = "N/A"
                    for a in block.select('a[href]'):
                        href = a['href']
                        if '/url?q=' in href:
                            clean = urllib.parse.unquote(href.split('/url?q=')[1].split('&')[0])
                            if 'google' not in clean.lower() and clean.startswith('http'):
                                website = clean; break
                        elif href.startswith('http') and 'google' not in href.lower():
                            website = href; break

                    maps_link = f"https://www.google.com/maps/search/{urllib.parse.quote_plus(name + ' ' + location)}"

                    batch.append({
                        "Name": name,
                        "Phone": phone,
                        "Website": website,
                        "Rating": rating,
                        "ReviewCount": review_count,
                        "Address": location,
                        "Category": keyword,
                        "Maps_Link": maps_link
                    })
                return batch
            except Exception:
                return []

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            results = executor.map(get_offset, [0, 20, 40, 60, 80])
            for res in results:
                all_leads.extend(res)

        # ─── SORT BY RATING ASCENDING (bad reviews first) ───
        def sort_key(lead):
            try:
                return float(lead['Rating'])
            except:
                return 5.0  # no rating goes to bottom
        all_leads.sort(key=sort_key)

        return all_leads

    def find_website_via_search(self, business_name, location):
        """Fallback: search Google for official website."""
        query = urllib.parse.quote_plus(f"{business_name} {location} official website")
        try:
            url = f"https://www.google.com/search?q={query}&num=5&hl=en"
            res = requests.get(url, headers=get_headers(), timeout=8)
            soup = BeautifulSoup(res.text, 'html.parser')
            for a in soup.select('a[href]'):
                href = a.get('href', '')
                if '/url?q=' in href:
                    clean = urllib.parse.unquote(href.split('/url?q=')[1].split('&')[0])
                    if ('google' not in clean.lower() and
                        'facebook' not in clean.lower() and
                        'yelp' not in clean.lower() and
                        clean.startswith('http')):
                        return clean
        except:
            pass
        return "N/A"


# ════════════════════════════════════════════════════
#   AGGRESSIVE DEEP EMAIL EXTRACTOR (90%+ GOAL)
# ════════════════════════════════════════════════════
class DeepEmailExtractor:
    def __init__(self):
        self.email_regex = r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'
        self.bad_keywords = ['example', 'domain', 'sentry', '@2x', '.png', '.jpg',
                             '.jpeg', '.gif', 'wixpress', 'bootstrap', 'rating',
                             'schema', 'jquery', 'cloudflare', 'wordpress', 'email@email']
        self.CONTACT_PATTERNS = ['contact', 'about', 'support', 'help', 'reach', 'connect',
                                  'get-in-touch', 'getintouch', 'info', 'team', 'us']

    def is_valid_email(self, email):
        email = email.lower()
        if len(email) > 80: return False
        return not any(bad in email for bad in self.bad_keywords)

    def extract_from_html(self, html):
        """Extract emails from raw HTML + decoded entities."""
        emails = set()
        # Standard regex
        found = re.findall(self.email_regex, html)
        emails.update(found)
        # Look for obfuscated emails (e.g. "at" replacements)
        obfuscated = re.findall(r'[a-zA-Z0-9._%+\-]+\s*[\[\(]at[\]\)]\s*[a-zA-Z0-9.\-]+\s*[\[\(]dot[\]\)]\s*[a-zA-Z]{2,}', html, re.IGNORECASE)
        for ob in obfuscated:
            cleaned = ob.replace('[at]', '@').replace('(at)', '@').replace('[dot]', '.').replace('(dot)', '.').replace(' ', '')
            if '@' in cleaned:
                emails.add(cleaned.lower())
        # mailto: links
        mailto = re.findall(r'mailto:([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})', html)
        emails.update(mailto)
        return [e for e in emails if self.is_valid_email(e)]

    def crawl_page(self, url, timeout=7):
        try:
            r = requests.get(url, headers=get_headers(), timeout=timeout, verify=False, allow_redirects=True)
            if r.status_code == 200:
                return r.text
        except:
            pass
        return ""

    def get_internal_links(self, html, base_url):
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        for a in soup.select('a[href]'):
            href = a.get('href', '').lower()
            if any(pat in href for pat in self.CONTACT_PATTERNS):
                full_link = urllib.parse.urljoin(base_url, a['href'])
                if full_link.startswith('http'):
                    links.append(full_link)
        return list(set(links))

    def get_email(self, url):
        if not url or url == "N/A": return "N/A"
        if not url.startswith('http'): url = 'http://' + url
        visited = set()
        try:
            # Step 1: Homepage
            html = self.crawl_page(url)
            if html:
                visited.add(url)
                emails = self.extract_from_html(html)
                if emails: return emails[0]
                # Step 2: Internal contact/about pages
                internal_links = self.get_internal_links(html, url)
                for link in internal_links[:4]:
                    if link in visited: continue
                    page_html = self.crawl_page(link)
                    visited.add(link)
                    if page_html:
                        emails2 = self.extract_from_html(page_html)
                        if emails2: return emails2[0]
            # Step 3: Try common paths
            base = url.rstrip('/')
            for path in ['/contact', '/contact-us', '/about', '/about-us', '/info']:
                attempt = base + path
                if attempt in visited: continue
                page_html = self.crawl_page(attempt)
                if page_html:
                    emails3 = self.extract_from_html(page_html)
                    if emails3: return emails3[0]
        except:
            pass
        return "N/A"


# ════════════════════════════════════════════════════
#   SMART EMAIL PERSONALIZER (HUMAN-LIKE)
# ════════════════════════════════════════════════════
class SmartEmailPersonalizer:
    OPENING_VARIANTS = [
        "I came across {name} while searching for {niche} in {location}",
        "I was looking for {niche} services and found {name}",
        "A friend mentioned {name} when I asked about {niche}",
        "I noticed {name} while browsing local {niche} options",
    ]
    RATING_HOOKS_LOW = [
        "I noticed your current rating could use a boost",
        "I saw some recent reviews that might be hurting your business",
        "Looks like some customers had less-than-ideal experiences recently",
        "I spotted a few negative reviews that might be costing you clients",
    ]
    RATING_HOOKS_HIGH = [
        "Your stellar reputation really stands out",
        "The positive reviews you have are impressive",
        "Customers clearly love what you do",
    ]

    def __init__(self):
        self.client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None

    def personalize(self, lead_name, niche, location, rating, template_subject, template_body):
        """Generate a fully human-like personalized email."""
        # Choose opening and hook
        opening = random.choice(self.OPENING_VARIANTS).format(
            name=lead_name, niche=niche, location=location
        )
        try:
            r = float(rating)
            hook = random.choice(self.RATING_HOOKS_LOW if r < 4.0 else self.RATING_HOOKS_HIGH)
            rating_context = f"Rating: {rating}/5 — {'Below average, great opportunity' if r < 4.0 else 'Good standing'}"
        except:
            hook = ""
            rating_context = "Rating: Unknown"

        if not self.client:
            # Fallback: simple template fill
            subject = template_subject.replace("{name}", lead_name).replace("{niche}", niche)
            body = template_body.replace("{name}", lead_name).replace("{niche}", niche).replace("{opening}", opening)
            return subject, body, opening

        try:
            prompt = f"""You are a human professional writing a cold email. Make it feel PERSONALLY written, NOT automated.

Business: {lead_name}
Niche: {niche}
Location: {location}
{rating_context}
Opening line to use: "{opening}"
Hook to use: "{hook}"

Template subject: {template_subject}
Template body: {template_body}

RULES:
- Sound like a real human, not a bot
- NO phrases like "I hope this email finds you", "As a language model", etc.
- Use short sentences. Casual but professional.
- Max 3 short paragraphs
- Include ONE specific mention of their rating or niche situation
- Make each email feel slightly unique — vary word choice

Return ONLY valid JSON:
{{"subject": "...", "body": "...", "personalization_line": "..."}}"""

            res = self.client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model="llama3-8b-8192",
                temperature=0.85,
                max_tokens=800,
            )
            content = res.choices[0].message.content
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group(0))
                return (
                    data.get("subject", template_subject),
                    data.get("body", template_body),
                    data.get("personalization_line", opening)
                )
        except:
            pass

        return template_subject, template_body, opening


# ════════════════════════════════════════════════════
#   SCHEDULING ENGINE (BD TIMEZONE)
# ════════════════════════════════════════════════════
class SchedulerEngine:
    def __init__(self):
        self.scheduled_jobs = {}  # {schedule_id: {time_bd, keyword_sets, triggered}}
        self._running = False

    def add_schedule(self, schedule_id, hour, minute, keyword_sets):
        self.scheduled_jobs[schedule_id] = {
            "hour": hour,
            "minute": minute,
            "keyword_sets": keyword_sets,
            "triggered": False,
            "created_at": datetime.now(BD_TZ).isoformat(),
            "next_run": self._calc_next_run(hour, minute)
        }

    def _calc_next_run(self, hour, minute):
        now_bd = datetime.now(BD_TZ)
        target = now_bd.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= now_bd:
            target += timedelta(days=1)
        return target.isoformat()

    def get_due_jobs(self):
        """Return jobs that are due to run now (BD time)."""
        due = []
        now_bd = datetime.now(BD_TZ)
        for sid, sched in self.scheduled_jobs.items():
            if sched["triggered"]:
                continue
            if now_bd.hour == sched["hour"] and now_bd.minute == sched["minute"]:
                due.append((sid, sched))
        return due

    def mark_triggered(self, schedule_id):
        if schedule_id in self.scheduled_jobs:
            self.scheduled_jobs[schedule_id]["triggered"] = True
            # Reschedule for next day
            s = self.scheduled_jobs[schedule_id]
            s["next_run"] = self._calc_next_run(s["hour"], s["minute"])
            s["triggered"] = False

    def get_all(self):
        return self.scheduled_jobs

    def remove(self, schedule_id):
        self.scheduled_jobs.pop(schedule_id, None)

    def run_forever(self, on_due_callback):
        """Background thread: check every 30 seconds."""
        while True:
            due = self.get_due_jobs()
            for sid, sched in due:
                self.mark_triggered(sid)
                for kset in sched["keyword_sets"]:
                    on_due_callback(kset)
            time.sleep(30)


# Global scheduler
scheduler = SchedulerEngine()


# ════════════════════════════════════════════════════
#   MULTI-KEYWORD JOB QUEUE SYSTEM
# ════════════════════════════════════════════════════
job_queue = []          # list of keyword_set dicts waiting to run
jobs = {}               # active/completed jobs
keyword_engine = AdvancedKeywordEngine()
maps_scraper = GoogleMapsScraper()
email_lib = DeepEmailExtractor()
personalizer = SmartEmailPersonalizer()


def process_keyword_set(job_id, kset, webhook_url, db_webhook_url, templates):
    """Process ONE keyword set fully (scrape → email)."""
    location = kset.get('location')
    base_keyword = kset.get('keyword')
    max_leads = min(int(kset.get('max_leads', 10)), 200)
    max_rating = kset.get('max_rating')
    min_rating = kset.get('min_rating')

    db = GoogleSheetsDB(db_webhook_url)

    jobs[job_id]['status_text'] = f'[{base_keyword}] Generating 150+ keywords...'
    if db_webhook_url:
        db.send_action("init", {})
        db.send_action("update_config", {
            "keyword_seed": base_keyword, "location": location, "target_leads": max_leads,
            "min_rating": min_rating or "", "max_rating": max_rating or "",
            "email_required": "true", "status": "running"
        })
        db.log("Job Start", f"Processing: {base_keyword} in {location}")

    seen_names = set()
    used_keywords = set()
    pending_keywords = [base_keyword]

    # --- PHASE 1: SCRAPE ---
    while len(jobs[job_id]['leads']) < max_leads:
        if not pending_keywords:
            jobs[job_id]['status_text'] = f'[{base_keyword}] Expanding keyword pool (150+ new keywords)...'
            new_kws = keyword_engine.generate_full_pool(base_keyword, location, used_keywords)
            random.shuffle(new_kws)  # Vary order for diversity
            pending_keywords.extend(new_kws)
            for kw in new_kws:
                db.send_action("add_keyword", {"keyword": kw, "source_seed": base_keyword, "status": "pending"})

        current_kw = pending_keywords.pop(0)
        used_keywords.add(current_kw.lower())

        jobs[job_id]['status_text'] = f'[{base_keyword}] Scraping: {current_kw}...'
        raw_leads = maps_scraper.fetch_batch(current_kw, location)

        if not raw_leads:
            continue

        for lead in raw_leads:
            if len(jobs[job_id]['leads']) >= max_leads:
                break
            if lead['Name'] in seen_names:
                continue

            db.send_action("add_scraped", {
                "business_name": lead['Name'], "address": lead['Address'], "phone": lead['Phone'],
                "rating": lead['Rating'], "review_count": lead.get('ReviewCount', 'N/A'),
                "website": lead['Website'], "keyword": current_kw, "status": "scraped"
            })

            # Rating filter
            if max_rating and lead['Rating'] != "N/A":
                try:
                    if float(lead['Rating']) > float(max_rating): continue
                except: pass
            if min_rating and lead['Rating'] != "N/A":
                try:
                    if float(lead['Rating']) < float(min_rating): continue
                except: pass

            # ─── WEBSITE RESOLUTION (3-stage) ───
            website = lead['Website']
            if website == "N/A":
                jobs[job_id]['status_text'] = f'[{base_keyword}] Searching website for: {lead["Name"]}...'
                website = maps_scraper.find_website_via_search(lead['Name'], location)
                lead['Website'] = website

            if website == "N/A":
                continue

            # ─── DEEP EMAIL EXTRACTION ───
            jobs[job_id]['status_text'] = f'[{base_keyword}] Extracting email from: {lead["Name"]}...'
            extracted_email = email_lib.get_email(website)
            if extracted_email == "N/A":
                continue

            db.send_action("add_email_lead", {
                "business_name": lead['Name'], "website": website,
                "email": extracted_email, "source_page": website, "status": "qualified"
            })
            db.send_action("add_qualified", {
                "business_name": lead['Name'], "email": extracted_email, "website": website,
                "rating": lead['Rating'], "keyword": current_kw,
                "personalization_line": "Pending AI...", "email_sent": "no"
            })

            lead['Email'] = extracted_email
            seen_names.add(lead['Name'])
            jobs[job_id]['leads'].append(lead)
            jobs[job_id]['count'] = len(jobs[job_id]['leads'])
            jobs[job_id]['status_text'] = f'🎯 [{base_keyword}] {jobs[job_id]["count"]}/{max_leads} leads found! (Latest: {extracted_email})'

        time.sleep(random.uniform(0.8, 2.0))

    final_leads = jobs[job_id]['leads']
    db.send_action("update_config", {
        "keyword_seed": base_keyword, "location": location, "target_leads": max_leads,
        "min_rating": min_rating or "", "max_rating": max_rating or "",
        "email_required": "true", "status": "stopped"
    })
    db.log("Scraping Done", f"Got {len(final_leads)} qualified leads for {base_keyword}")

    # --- PHASE 2: PERSONALIZED EMAIL SENDING ---
    if webhook_url and templates and final_leads:
        jobs[job_id]['status'] = 'sending_emails'
        jobs[job_id]['total_to_send'] = len(final_leads)
        emails_sent = 0

        for lead in final_leads:
            template = random.choice(templates)
            jobs[job_id]['status_text'] = f'[{base_keyword}] Personalizing email {emails_sent+1}/{len(final_leads)} for {lead["Name"]}...'

            p_subject, p_body, p_line = personalizer.personalize(
                lead['Name'], base_keyword, location,
                lead['Rating'], template['subject'], template['body']
            )

            payload = {"to": lead['Email'], "subject": p_subject, "body": p_body}
            try:
                requests.post(webhook_url, json=payload, timeout=10)
                emails_sent += 1
                jobs[job_id]['emails_sent'] = emails_sent
                db.send_action("update_email_sent", {"email": lead['Email'], "personalization_line": p_line})
                db.log("Email Sent", f"Sent to {lead['Email']} | Personalization: {p_line[:60]}")
            except Exception as e:
                db.log("Email Error", f"Failed: {lead['Email']} — {str(e)}")

            if emails_sent < len(final_leads):
                delay = random.randint(60, 120)
                for i in range(delay, 0, -1):
                    jobs[job_id]['status_text'] = f'Anti-Spam: Waiting {i}s before next email...'
                    time.sleep(1)


def run_multi_keyword_job(job_id, data):
    """Master runner: iterate over ALL keyword sets sequentially."""
    try:
        keyword_sets = data.get('keyword_sets', [])
        single_kw = data.get('keyword')
        if single_kw and not keyword_sets:
            keyword_sets = [{
                'keyword': single_kw,
                'location': data.get('location'),
                'max_leads': data.get('max_leads', 10),
                'max_rating': data.get('max_rating'),
                'min_rating': data.get('min_rating'),
            }]

        webhook_url = data.get('webhook_url')
        db_webhook_url = data.get('db_webhook_url')
        templates = data.get('templates', [])

        jobs[job_id] = {
            'status': 'scraping',
            'count': 0,
            'leads': [],
            'emails_sent': 0,
            'total_to_send': 0,
            'total_keyword_sets': len(keyword_sets),
            'current_set_index': 0,
            'status_text': f'Starting {len(keyword_sets)} keyword set(s)...',
            'logs': []
        }

        for idx, kset in enumerate(keyword_sets):
            jobs[job_id]['current_set_index'] = idx + 1
            jobs[job_id]['status'] = 'scraping'
            jobs[job_id]['leads'] = []  # Reset per keyword set
            jobs[job_id]['count'] = 0
            process_keyword_set(job_id, kset, webhook_url, db_webhook_url, templates)

            # Move leads to history if multi-set
            if len(keyword_sets) > 1:
                jobs[job_id].setdefault('all_leads', []).extend(jobs[job_id]['leads'])
                if idx < len(keyword_sets) - 1:
                    jobs[job_id]['status_text'] = f'Keyword set {idx+1} done. Moving to next...'
                    time.sleep(3)

        # Consolidate leads for multi-set jobs
        if 'all_leads' in jobs[job_id]:
            jobs[job_id]['leads'] = jobs[job_id]['all_leads']

        jobs[job_id]['status'] = 'done'
        jobs[job_id]['status_text'] = f'✅ All {len(keyword_sets)} keyword set(s) completed!'

    except Exception as e:
        jobs[job_id] = {'status': 'error', 'error': str(e)}


# ════════════════════════════════════════════════════
#   FLASK DASHBOARD & API
# ════════════════════════════════════════════════════
flask_app = Flask(__name__)

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>LeadGen Pro Ultra | AI Lead Machine</title>
<script src="https://cdn.tailwindcss.com"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#030712;--surface:#0a0f1e;--surface2:#0f1729;--border:#1a2540;
  --accent:#3b82f6;--accent2:#8b5cf6;--success:#10b981;--warn:#f59e0b;
  --danger:#ef4444;--text:#e2e8f0;--muted:#4b6080;
}
body{background:var(--bg);color:var(--text);font-family:'DM Sans',sans-serif;min-height:100vh;
  background-image:radial-gradient(ellipse at 20% 50%, rgba(59,130,246,0.04) 0%, transparent 60%),
    radial-gradient(ellipse at 80% 20%, rgba(139,92,246,0.04) 0%, transparent 60%)}
.mono{font-family:'Space Mono',monospace}
.card{background:var(--surface);border:1px solid var(--border);border-radius:12px}
.card2{background:var(--surface2);border:1px solid var(--border);border-radius:10px}
.inp{background:#060d1f;border:1px solid var(--border);color:var(--text);border-radius:8px;padding:10px 14px;font-size:13px;width:100%;font-family:'DM Sans',sans-serif;transition:border .2s;outline:none}
.inp:focus{border-color:var(--accent);box-shadow:0 0 0 3px rgba(59,130,246,0.1)}
.btn{cursor:pointer;border:none;border-radius:8px;font-weight:600;font-size:13px;transition:all .2s;padding:10px 20px;font-family:'DM Sans',sans-serif}
.btn-primary{background:linear-gradient(135deg,#2563eb,#7c3aed);color:#fff}
.btn-primary:hover{filter:brightness(1.15);transform:translateY(-1px)}
.btn-primary:disabled{opacity:.4;cursor:not-allowed;transform:none}
.btn-success{background:linear-gradient(135deg,#059669,#0891b2);color:#fff}
.btn-success:hover{filter:brightness(1.15)}
.btn-warn{background:linear-gradient(135deg,#d97706,#dc2626);color:#fff}
.btn-warn:hover{filter:brightness(1.15)}
.btn-ghost{background:transparent;border:1px solid var(--border);color:var(--muted)}
.btn-ghost:hover{border-color:var(--accent);color:var(--accent)}
.tab-btn{background:transparent;border:none;color:var(--muted);cursor:pointer;padding:8px 16px;border-radius:8px;font-size:12px;font-weight:600;transition:all .2s;font-family:'DM Sans',sans-serif;white-space:nowrap}
.tab-btn.active{background:rgba(59,130,246,0.12);color:var(--accent);border:1px solid rgba(59,130,246,0.2)}
.badge{display:inline-flex;align-items:center;padding:2px 8px;border-radius:5px;font-size:11px;font-weight:700}
.badge-ok{background:rgba(16,185,129,0.1);color:#34d399;border:1px solid rgba(16,185,129,0.2)}
.badge-err{background:rgba(239,68,68,0.09);color:#f87171;border:1px solid rgba(239,68,68,0.15)}
.badge-info{background:rgba(59,130,246,0.1);color:#60a5fa;border:1px solid rgba(59,130,246,0.15)}
.badge-warn{background:rgba(245,158,11,0.1);color:#fbbf24;border:1px solid rgba(245,158,11,0.15)}
.prog-track{height:3px;background:var(--border);border-radius:99px;overflow:hidden}
.prog-fill{height:100%;border-radius:99px;background:linear-gradient(90deg,#2563eb,#7c3aed);transition:width .5s ease}
.spin{animation:spin 1s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.pulse{animation:pulse 2s cubic-bezier(0.4,0,0.6,1) infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.fade-in{animation:fadeIn .3s ease}
@keyframes fadeIn{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
.grid-stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px}
.stat-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px}
.stat-card:hover{border-color:rgba(59,130,246,0.3)}
table{width:100%;border-collapse:collapse}
th{padding:8px 12px;text-align:left;color:var(--muted);font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.05em;border-bottom:1px solid var(--border)}
td{padding:10px 12px;font-size:12px;border-bottom:1px solid rgba(255,255,255,0.03)}
tr:hover td{background:rgba(255,255,255,0.02)}
.log-entry{font-family:'Space Mono',monospace;font-size:11px;color:var(--muted);padding:4px 0;border-bottom:1px solid rgba(255,255,255,0.03)}
.log-entry .ts{color:var(--accent);margin-right:8px}
.log-entry .msg{color:#94a3b8}
::-webkit-scrollbar{width:5px}::-webkit-scrollbar-thumb{background:#1a2540;border-radius:3px}
.schedule-item{background:rgba(16,185,129,0.05);border:1px solid rgba(16,185,129,0.15);border-radius:8px;padding:12px}
.kset-item{background:rgba(59,130,246,0.05);border:1px solid rgba(59,130,246,0.15);border-radius:8px;padding:12px}
</style>
</head>
<body>

<!-- NAV -->
<nav style="background:rgba(3,7,18,.95);border-bottom:1px solid var(--border);backdrop-filter:blur(12px)" class="sticky top-0 z-40 px-5 py-3 flex items-center justify-between">
  <div class="flex items-center gap-3">
    <div class="btn-primary btn w-8 h-8 rounded-lg flex items-center justify-center p-0 text-sm">
      <i class="fa-solid fa-bolt"></i>
    </div>
    <div>
      <div class="font-bold text-white mono text-sm">LEADGEN PRO <span style="color:var(--accent)">ULTRA</span></div>
      <div class="text-xs" style="color:var(--muted)">Multi-Keyword · Scheduler · AI Personalization</div>
    </div>
  </div>
  <div id="bd-clock" class="mono text-xs" style="color:var(--muted)"></div>
</nav>

<!-- MAIN -->
<div class="max-w-6xl mx-auto px-4 py-6">

  <!-- STATS ROW -->
  <div class="grid-stats mb-6">
    <div class="stat-card fade-in">
      <div class="text-2xl font-bold text-white" id="st-leads">0</div>
      <div class="text-xs mt-1 flex items-center gap-1.5" style="color:var(--muted)">
        <i class="fa-solid fa-users text-blue-400 text-xs"></i> Valid Leads
      </div>
    </div>
    <div class="stat-card fade-in">
      <div class="text-2xl font-bold text-emerald-400" id="st-emails">0</div>
      <div class="text-xs mt-1 flex items-center gap-1.5" style="color:var(--muted)">
        <i class="fa-solid fa-envelope text-emerald-400 text-xs"></i> Emails Sent
      </div>
    </div>
    <div class="stat-card fade-in">
      <div class="text-2xl font-bold" style="color:var(--accent)" id="st-phones">0</div>
      <div class="text-xs mt-1 flex items-center gap-1.5" style="color:var(--muted)">
        <i class="fa-solid fa-phone text-xs" style="color:var(--accent)"></i> With Phone
      </div>
    </div>
    <div class="stat-card fade-in">
      <div class="text-2xl font-bold text-violet-400" id="st-webs">0</div>
      <div class="text-xs mt-1 flex items-center gap-1.5" style="color:var(--muted)">
        <i class="fa-solid fa-globe text-violet-400 text-xs"></i> With Website
      </div>
    </div>
    <div class="stat-card fade-in">
      <div class="text-2xl font-bold text-amber-400" id="st-sets">0</div>
      <div class="text-xs mt-1 flex items-center gap-1.5" style="color:var(--muted)">
        <i class="fa-solid fa-layer-group text-amber-400 text-xs"></i> Keyword Sets
      </div>
    </div>
  </div>

  <!-- TABS -->
  <div class="flex gap-1 mb-5 overflow-x-auto pb-1" style="border-bottom:1px solid var(--border)">
    <button class="tab-btn active" id="tab-search" onclick="showTab('search')"><i class="fa-solid fa-crosshairs mr-1.5"></i>Run Job</button>
    <button class="tab-btn" id="tab-queue" onclick="showTab('queue')"><i class="fa-solid fa-layer-group mr-1.5"></i>Multi-Keyword Queue</button>
    <button class="tab-btn" id="tab-scheduler" onclick="showTab('scheduler')"><i class="fa-solid fa-clock mr-1.5"></i>BD Scheduler</button>
    <button class="tab-btn" id="tab-database" onclick="showTab('database')"><i class="fa-solid fa-database mr-1.5"></i>Database</button>
    <button class="tab-btn" id="tab-connect" onclick="showTab('connect')"><i class="fa-solid fa-paper-plane mr-1.5"></i>Email Setup</button>
    <button class="tab-btn" id="tab-templates" onclick="showTab('templates')"><i class="fa-solid fa-file-lines mr-1.5"></i>Templates</button>
    <button class="tab-btn" id="tab-history" onclick="showTab('history')"><i class="fa-solid fa-history mr-1.5"></i>History</button>
  </div>

  <!-- ═══ SEARCH TAB ═══ -->
  <div id="pane-search" class="fade-in">
    <div class="card p-6 mb-4">
      <h2 class="font-bold text-white text-sm mb-5 flex items-center gap-2">
        <span class="btn-primary btn rounded-lg w-7 h-7 flex items-center justify-center p-0 text-xs"><i class="fa-solid fa-crosshairs"></i></span>
        Quick Single Job
      </h2>
      <div class="grid grid-cols-1 sm:grid-cols-2 gap-4 mb-5">
        <div><label class="text-xs mb-1.5 block" style="color:var(--muted)">📍 Location *</label><input id="m-loc" class="inp" placeholder="e.g. New York"></div>
        <div><label class="text-xs mb-1.5 block" style="color:var(--muted)">🔍 Keyword *</label><input id="m-kw" class="inp" placeholder="e.g. dentist"></div>
        <div><label class="text-xs mb-1.5 block" style="color:var(--muted)">🎯 Exact Target (Max 200)</label><input id="m-count" type="number" max="200" value="10" class="inp"></div>
        <div><label class="text-xs mb-1.5 block" style="color:var(--muted)">⭐ Max Rating (Targets bad reviews)</label><input id="m-rating" type="number" step="0.1" class="inp" placeholder="e.g. 3.5"></div>
      </div>
      <div class="flex gap-3">
        <button onclick="startJob()" id="btn-run" class="btn btn-primary flex-1 py-3">
          <i class="fa-solid fa-play mr-2"></i>Start Single Job
        </button>
        <button onclick="addToQueue()" class="btn btn-ghost py-3 px-5">
          <i class="fa-solid fa-plus mr-1.5"></i>Add to Queue
        </button>
      </div>
    </div>

    <!-- STATUS CARD -->
    <div id="sbox" class="hidden card p-5 mb-4 fade-in">
      <div class="flex items-center justify-between mb-3">
        <div class="flex items-center gap-3">
          <i id="si" class="fa-solid fa-circle-notch spin text-blue-400 text-lg"></i>
          <span id="stxt" class="font-semibold text-white text-sm">Processing...</span>
        </div>
        <span id="s-set-badge" class="badge badge-info hidden"></span>
      </div>
      <div class="prog-track mb-3"><div class="prog-fill" id="sbar" style="width:0%"></div></div>
      <div id="sdet" class="mono text-xs p-3 rounded-lg mb-3" style="background:#060d1f;color:#64748b;min-height:32px"></div>
      <div id="log-feed" class="max-h-32 overflow-y-auto"></div>
      <button id="dlbtn" onclick="doDL()" class="hidden btn btn-success w-full py-3 mt-3">
        <i class="fa-solid fa-download mr-2"></i>Download Leads CSV
      </button>
    </div>

    <!-- PREVIEW TABLE -->
    <div id="pvbox" class="hidden card p-5 fade-in">
      <div class="flex items-center justify-between mb-4">
        <h3 class="font-bold text-white text-sm">
          <i class="fa-solid fa-table-cells mr-2" style="color:var(--accent)"></i>
          Live Preview <span id="pvcnt" class="font-normal text-xs" style="color:var(--muted)"></span>
        </h3>
        <button onclick="doDL()" class="btn btn-success text-xs py-2 px-4">
          <i class="fa-solid fa-download mr-1"></i>CSV
        </button>
      </div>
      <div class="overflow-x-auto">
        <table>
          <thead><tr id="th"></tr></thead>
          <tbody id="tb"></tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- ═══ QUEUE TAB ═══ -->
  <div id="pane-queue" class="hidden fade-in">
    <div class="card p-6 mb-4">
      <h2 class="font-bold text-white text-sm mb-4 flex items-center gap-2">
        <i class="fa-solid fa-layer-group" style="color:var(--accent)"></i>
        Add Keyword Set to Queue
      </h2>
      <div class="grid grid-cols-1 sm:grid-cols-2 gap-4 mb-4">
        <div><label class="text-xs mb-1.5 block" style="color:var(--muted)">📍 Location</label><input id="q-loc" class="inp" placeholder="e.g. Los Angeles"></div>
        <div><label class="text-xs mb-1.5 block" style="color:var(--muted)">🔍 Keyword</label><input id="q-kw" class="inp" placeholder="e.g. plumber"></div>
        <div><label class="text-xs mb-1.5 block" style="color:var(--muted)">🎯 Target Leads</label><input id="q-count" type="number" value="10" class="inp"></div>
        <div><label class="text-xs mb-1.5 block" style="color:var(--muted)">⭐ Max Rating</label><input id="q-rating" type="number" step="0.1" class="inp" placeholder="e.g. 3.5 (leave blank for all)"></div>
      </div>
      <button onclick="addKsetToQueue()" class="btn btn-primary w-full py-3">
        <i class="fa-solid fa-plus mr-2"></i>Add to Queue
      </button>
    </div>
    <div class="card p-6">
      <div class="flex justify-between items-center mb-4">
        <h2 class="font-bold text-white text-sm"><i class="fa-solid fa-list mr-2" style="color:var(--accent)"></i>Queued Keyword Sets</h2>
        <button onclick="runQueue()" class="btn btn-warn py-2 px-5 text-xs">
          <i class="fa-solid fa-rocket mr-1.5"></i>Run All Queued
        </button>
      </div>
      <div id="q-list" class="space-y-3"></div>
    </div>
  </div>

  <!-- ═══ SCHEDULER TAB ═══ -->
  <div id="pane-scheduler" class="hidden fade-in">
    <div class="card p-6 mb-4">
      <h2 class="font-bold text-white text-sm mb-4 flex items-center gap-2">
        <i class="fa-solid fa-clock" style="color:var(--success)"></i>
        Schedule Automation (Bangladesh Time)
      </h2>
      <div id="bd-now" class="mb-4 p-3 rounded-lg mono text-xs" style="background:#060d1f;color:var(--accent)">
        Current BD Time: Loading...
      </div>
      <div class="grid grid-cols-3 gap-3 mb-4">
        <div><label class="text-xs mb-1.5 block" style="color:var(--muted)">Hour (1-12)</label><input id="sch-hour" type="number" min="1" max="12" value="9" class="inp"></div>
        <div><label class="text-xs mb-1.5 block" style="color:var(--muted)">Minute (0-59)</label><input id="sch-min" type="number" min="0" max="59" value="0" class="inp"></div>
        <div><label class="text-xs mb-1.5 block" style="color:var(--muted)">AM / PM</label>
          <select id="sch-ampm" class="inp">
            <option value="AM">AM</option>
            <option value="PM">PM</option>
          </select>
        </div>
      </div>
      <p class="text-xs mb-4" style="color:var(--muted)">⚡ The scheduler will use your current keyword queue. Add keyword sets in the Queue tab first.</p>
      <button onclick="addSchedule()" class="btn btn-success w-full py-3">
        <i class="fa-solid fa-alarm-clock mr-2"></i>Schedule This Job (BD Time)
      </button>
    </div>
    <div class="card p-6">
      <div class="flex justify-between items-center mb-4">
        <h2 class="font-bold text-white text-sm"><i class="fa-solid fa-list-check mr-2" style="color:var(--success)"></i>Active Schedules</h2>
      </div>
      <div id="sch-list" class="space-y-3"></div>
    </div>
  </div>

  <!-- ═══ DATABASE TAB ═══ -->
  <div id="pane-database" class="hidden fade-in">
    <div class="card p-6">
      <h2 class="font-bold text-white text-sm mb-4"><i class="fa-solid fa-database text-blue-400 mr-2"></i>Connect Google Sheets Database</h2>
      <p class="text-xs mb-4 leading-relaxed" style="color:var(--muted)">
        Auto-saves all keywords, scraped leads, emails, and logs to Google Sheets.<br>
        1. Go to <a href="https://script.google.com" target="_blank" class="text-blue-400 underline">script.google.com</a> → New Project<br>
        2. Paste the script below → Deploy → Web App → Anyone → Copy URL
      </p>
      <div class="relative mb-5">
        <button onclick="copyDBScript()" class="absolute top-2 right-2 btn btn-ghost text-xs py-1 px-3 z-10">Copy</button>
        <textarea id="db-script-code" readonly class="inp mono text-xs h-48" style="color:#93c5fd;resize:none">
function doPost(e) {
  var lock = LockService.getScriptLock();
  lock.tryLock(10000);
  try {
    var payload = JSON.parse(e.postData.contents);
    var action = payload.action;
    var data = payload.data;
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    function getOrCreateSheet(name, headers) {
      var sheet = ss.getSheetByName(name);
      if (!sheet) { sheet = ss.insertSheet(name); sheet.appendRow(headers); sheet.getRange(1,1,1,headers.length).setFontWeight("bold"); }
      return sheet;
    }
    if (action === "init") {
      getOrCreateSheet("Config", ["keyword_seed","location","target_leads","min_rating","max_rating","email_required","status"]);
      getOrCreateSheet("Generated_Keywords", ["keyword","source_seed","status"]);
      getOrCreateSheet("Scraped_Businesses", ["business_name","address","phone","rating","review_count","website","keyword","status"]);
      getOrCreateSheet("Email_Leads", ["business_name","website","email","source_page","status"]);
      getOrCreateSheet("Qualified_Leads", ["business_name","email","website","rating","keyword","personalization_line","email_sent"]);
      getOrCreateSheet("Logs", ["timestamp","action","details"]);
    } else if (action === "log") {
      var s = ss.getSheetByName("Logs"); if(s) s.appendRow([data.timestamp,data.action,data.details]);
    } else if (action === "add_keyword") {
      var s = ss.getSheetByName("Generated_Keywords"); if(s) s.appendRow([data.keyword,data.source_seed,data.status]);
    } else if (action === "add_scraped") {
      var s = ss.getSheetByName("Scraped_Businesses"); if(s) s.appendRow([data.business_name,data.address,data.phone,data.rating,data.review_count,data.website,data.keyword,data.status]);
    } else if (action === "add_email_lead") {
      var s = ss.getSheetByName("Email_Leads"); if(s) s.appendRow([data.business_name,data.website,data.email,data.source_page,data.status]);
    } else if (action === "add_qualified") {
      var s = ss.getSheetByName("Qualified_Leads"); if(s) s.appendRow([data.business_name,data.email,data.website,data.rating,data.keyword,data.personalization_line,data.email_sent]);
    } else if (action === "update_config") {
      var s = ss.getSheetByName("Config"); if(s){s.clearContents();s.appendRow(["keyword_seed","location","target_leads","min_rating","max_rating","email_required","status"]);s.appendRow([data.keyword_seed,data.location,data.target_leads,data.min_rating,data.max_rating,data.email_required,data.status]);}
    } else if (action === "update_email_sent") {
      var s = ss.getSheetByName("Qualified_Leads"); if(s){var v=s.getDataRange().getValues();for(var i=1;i<v.length;i++){if(v[i][1]===data.email){s.getRange(i+1,6).setValue(data.personalization_line);s.getRange(i+1,7).setValue("yes");break;}}}
    }
    return ContentService.createTextOutput(JSON.stringify({status:"success"})).setMimeType(ContentService.MimeType.JSON);
  } catch(e) {
    return ContentService.createTextOutput(JSON.stringify({status:"error",message:e.toString()})).setMimeType(ContentService.MimeType.JSON);
  } finally { lock.releaseLock(); }
}
function doGet(e) { return ContentService.createTextOutput(JSON.stringify({status:"active"})).setMimeType(ContentService.MimeType.JSON); }</textarea>
      </div>
      <label class="text-xs mb-1.5 block" style="color:var(--muted)">🔗 Database Web App URL:</label>
      <input id="db-webhook-url" class="inp mb-4" placeholder="https://script.google.com/macros/s/AKfycb.../exec">
      <button onclick="saveDBWebhook()" class="btn btn-primary w-full py-3">
        <i class="fa-solid fa-link mr-2"></i>Connect Database
      </button>
    </div>
  </div>

  <!-- ═══ CONNECT EMAIL TAB ═══ -->
  <div id="pane-connect" class="hidden fade-in">
    <div class="card p-6">
      <h2 class="font-bold text-white text-sm mb-4"><i class="fa-solid fa-paper-plane text-emerald-400 mr-2"></i>Gmail Sender Setup</h2>
      <p class="text-xs mb-4 leading-relaxed" style="color:var(--muted)">
        1. Go to <a href="https://script.google.com" target="_blank" class="text-emerald-400 underline">script.google.com</a> → New Project<br>
        2. Paste the code below → Deploy → Web App → Anyone → Copy URL
      </p>
      <div class="relative mb-5">
        <textarea readonly class="inp mono text-xs h-28" style="color:#a5b4fc;resize:none">
function doPost(e) {
  try {
    var data = JSON.parse(e.postData.contents);
    MailApp.sendEmail({ to: data.to, subject: data.subject, htmlBody: data.body });
    return ContentService.createTextOutput(JSON.stringify({"status":"success"})).setMimeType(ContentService.MimeType.JSON);
  } catch(err) {
    return ContentService.createTextOutput(JSON.stringify({"status":"error","message":err.toString()})).setMimeType(ContentService.MimeType.JSON);
  }
}</textarea>
      </div>
      <label class="text-xs mb-1.5 block" style="color:var(--muted)">🔗 Email Web App URL:</label>
      <input id="webhook-url" class="inp mb-4" placeholder="https://script.google.com/macros/s/AKfycb.../exec">
      <button onclick="saveWebhook()" class="btn btn-success w-full py-3">
        <i class="fa-solid fa-save mr-2"></i>Save Email Webhook
      </button>
    </div>
  </div>

  <!-- ═══ TEMPLATES TAB ═══ -->
  <div id="pane-templates" class="hidden fade-in">
    <div class="card p-6 mb-4">
      <h2 class="font-bold text-white text-sm mb-4"><i class="fa-solid fa-plus text-indigo-400 mr-2"></i>New Template</h2>
      <input id="t-name" class="inp mb-3" placeholder="Template Name">
      <input id="t-sub" class="inp mb-3" placeholder="Subject (AI will personalize this)">
      <textarea id="t-body" class="inp mb-3 h-24" placeholder="Email body (HTML ok). Use {name}, {niche}, {location} as placeholders." style="resize:vertical"></textarea>
      <button onclick="addTemplate()" class="btn btn-primary w-full py-3">
        <i class="fa-solid fa-plus mr-2"></i>Add Template
      </button>
    </div>
    <div class="card p-6">
      <h2 class="font-bold text-white text-sm mb-4"><i class="fa-solid fa-list text-indigo-400 mr-2"></i>Saved Templates</h2>
      <div id="t-list" class="space-y-3"></div>
    </div>
  </div>

  <!-- ═══ HISTORY TAB ═══ -->
  <div id="pane-history" class="hidden fade-in">
    <div class="card p-6">
      <div class="flex justify-between items-center mb-4">
        <h2 class="font-bold text-white text-sm"><i class="fa-solid fa-history text-indigo-400 mr-2"></i>Run History</h2>
        <button onclick="clearHistory()" class="btn btn-ghost text-xs py-2 px-4" style="color:var(--danger)">
          <i class="fa-solid fa-trash mr-1"></i>Clear
        </button>
      </div>
      <div id="h-list" class="space-y-3"></div>
    </div>
  </div>

</div><!-- /main -->

<script>
// ─── State ───
let jid = null, templates = [], historyData = [], kwQueue = [], schedules = [], tableShown = false;

window.onload = () => {
  document.getElementById('webhook-url').value = localStorage.getItem('webhook_url') || '';
  document.getElementById('db-webhook-url').value = localStorage.getItem('db_webhook_url') || '';
  templates = JSON.parse(localStorage.getItem('templates') || '[]');
  historyData = JSON.parse(localStorage.getItem('history') || '[]');
  kwQueue = JSON.parse(localStorage.getItem('kw_queue') || '[]');
  schedules = JSON.parse(localStorage.getItem('schedules') || '[]');
  updateQueueStat();
  renderTemplates(); renderHistory(); renderQueue(); renderSchedules();
  startBDClock();
};

// ─── BD Clock ───
function startBDClock() {
  function update() {
    const now = new Date();
    const bd = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Dhaka' }));
    const h = bd.getHours(), m = bd.getMinutes(), s = bd.getSeconds();
    const ampm = h >= 12 ? 'PM' : 'AM';
    const hh = h % 12 || 12;
    const ts = `BD: ${hh.toString().padStart(2,'0')}:${m.toString().padStart(2,'0')}:${s.toString().padStart(2,'0')} ${ampm}`;
    document.getElementById('bd-clock').textContent = ts;
    const el = document.getElementById('bd-now');
    if(el) el.textContent = '⏰ ' + ts + ' (Bangladesh Standard Time, GMT+6)';
  }
  update(); setInterval(update, 1000);
}

// ─── Tabs ───
function showTab(t) {
  ['search','queue','scheduler','database','connect','templates','history'].forEach(x => {
    document.getElementById('pane-'+x).classList.add('hidden');
    document.getElementById('tab-'+x).classList.remove('active');
  });
  document.getElementById('pane-'+t).classList.remove('hidden');
  document.getElementById('tab-'+t).classList.add('active');
}

// ─── Webhook ───
function saveWebhook() { localStorage.setItem('webhook_url', document.getElementById('webhook-url').value.trim()); alert("Email Webhook saved!"); }
function saveDBWebhook() { localStorage.setItem('db_webhook_url', document.getElementById('db-webhook-url').value.trim()); alert("Database Webhook saved!"); }
function copyDBScript() { const el = document.getElementById('db-script-code'); el.select(); document.execCommand('copy'); alert("Script copied!"); }

// ─── Templates ───
function addTemplate() {
  const n = document.getElementById('t-name').value.trim();
  const s = document.getElementById('t-sub').value.trim();
  const b = document.getElementById('t-body').value.trim();
  if(!n || !s || !b) return alert('Fill all template fields!');
  templates.push({name:n, subject:s, body:b});
  localStorage.setItem('templates', JSON.stringify(templates));
  document.getElementById('t-name').value=''; document.getElementById('t-sub').value=''; document.getElementById('t-body').value='';
  renderTemplates();
}
function delTemplate(i) { templates.splice(i,1); localStorage.setItem('templates', JSON.stringify(templates)); renderTemplates(); }
function renderTemplates() {
  const el = document.getElementById('t-list');
  if(!templates.length) return el.innerHTML = '<p class="text-xs text-center" style="color:var(--muted)">No templates yet.</p>';
  el.innerHTML = templates.map((t,i) => `
    <div class="card2 p-4 relative">
      <button onclick="delTemplate(${i})" class="absolute top-3 right-3 text-xs" style="color:var(--danger);background:none;border:none;cursor:pointer"><i class="fa-solid fa-trash"></i></button>
      <div class="font-bold text-sm text-white mb-1">${t.name}</div>
      <div class="text-xs mb-1" style="color:var(--accent)">Sub: ${t.subject}</div>
      <div class="text-xs" style="color:var(--muted)">${t.body.replace(/</g,'&lt;').substring(0,120)}...</div>
    </div>`).join('');
}

// ─── History ───
function renderHistory() {
  const el = document.getElementById('h-list');
  if(!historyData.length) return el.innerHTML = '<p class="text-xs text-center" style="color:var(--muted)">No history yet.</p>';
  el.innerHTML = historyData.map(h => `
    <div class="card2 p-3">
      <div class="text-sm font-bold text-white">${h.loc} — ${h.kw}</div>
      <div class="text-xs mt-1" style="color:var(--muted)">Target: ${h.target} | ${h.date}</div>
    </div>`).join('');
}
function clearHistory() { historyData=[]; localStorage.removeItem('history'); renderHistory(); }

// ─── Keyword Queue ───
function addToQueue() {
  const loc = document.getElementById('m-loc').value.trim();
  const kw = document.getElementById('m-kw').value.trim();
  const count = parseInt(document.getElementById('m-count').value)||10;
  const rating = document.getElementById('m-rating').value.trim();
  if(!loc || !kw) return alert('Fill Location & Keyword first!');
  kwQueue.push({keyword:kw, location:loc, max_leads:Math.min(count,200), max_rating:rating||null});
  localStorage.setItem('kw_queue', JSON.stringify(kwQueue));
  updateQueueStat(); renderQueue();
  alert(`Added "${kw}" in "${loc}" to queue!`);
}
function addKsetToQueue() {
  const loc = document.getElementById('q-loc').value.trim();
  const kw = document.getElementById('q-kw').value.trim();
  const count = parseInt(document.getElementById('q-count').value)||10;
  const rating = document.getElementById('q-rating').value.trim();
  if(!loc || !kw) return alert('Fill all fields!');
  kwQueue.push({keyword:kw, location:loc, max_leads:Math.min(count,200), max_rating:rating||null});
  localStorage.setItem('kw_queue', JSON.stringify(kwQueue));
  document.getElementById('q-loc').value=''; document.getElementById('q-kw').value='';
  document.getElementById('q-count').value='10'; document.getElementById('q-rating').value='';
  updateQueueStat(); renderQueue();
}
function removeKset(i) { kwQueue.splice(i,1); localStorage.setItem('kw_queue',JSON.stringify(kwQueue)); updateQueueStat(); renderQueue(); }
function renderQueue() {
  const el = document.getElementById('q-list');
  if(!kwQueue.length) return el.innerHTML = '<p class="text-xs text-center" style="color:var(--muted)">Queue is empty. Add keyword sets above.</p>';
  el.innerHTML = kwQueue.map((k,i) => `
    <div class="kset-item flex items-center justify-between">
      <div>
        <div class="text-sm font-bold text-white">${k.keyword} <span class="font-normal" style="color:var(--muted)">in ${k.location}</span></div>
        <div class="text-xs mt-0.5" style="color:var(--muted)">Target: ${k.max_leads} leads${k.max_rating ? ' | Max Rating: '+k.max_rating : ''}</div>
      </div>
      <button onclick="removeKset(${i})" class="btn btn-ghost text-xs py-1 px-3" style="color:var(--danger)"><i class="fa-solid fa-x"></i></button>
    </div>`).join('');
}
function updateQueueStat() {
  document.getElementById('st-sets').textContent = kwQueue.length;
}
async function runQueue() {
  if(!kwQueue.length) return alert('Queue is empty!');
  const webhook = localStorage.getItem('webhook_url') || '';
  const db_webhook = localStorage.getItem('db_webhook_url') || '';
  if(!db_webhook) alert("Notice: No database webhook. Data won't be saved.");
  showTab('search');
  setSt('Initializing Queue Runner...','load',2);
  document.getElementById('dlbtn').classList.add('hidden');
  document.getElementById('pvbox').classList.add('hidden');
  document.getElementById('btn-run').disabled = true;
  tableShown = false;
  const payload = {
    keyword_sets: kwQueue, webhook_url: webhook, db_webhook_url: db_webhook, templates: templates
  };
  try {
    const r = await fetch('/api/scrape', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
    const d = await r.json();
    if(d.error) { setSt(d.error,'err'); document.getElementById('btn-run').disabled=false; return; }
    jid = d.job_id;
    historyData.unshift({loc: kwQueue.map(k=>k.location).join(', '), kw: kwQueue.map(k=>k.keyword).join(', '), target: kwQueue.reduce((a,k)=>a+k.max_leads,0), date: new Date().toLocaleString()});
    localStorage.setItem('history', JSON.stringify(historyData)); renderHistory();
    startPolling();
  } catch(e) {
    setSt('Server connection failed','err');
    document.getElementById('btn-run').disabled=false;
  }
}

// ─── Scheduler ───
function addSchedule() {
  let hour = parseInt(document.getElementById('sch-hour').value) || 9;
  const min = parseInt(document.getElementById('sch-min').value) || 0;
  const ampm = document.getElementById('sch-ampm').value;
  if(ampm === 'PM' && hour !== 12) hour += 12;
  if(ampm === 'AM' && hour === 12) hour = 0;
  if(!kwQueue.length) return alert('Add keyword sets to queue first!');
  const sid = 'sch_' + Date.now();
  const sched = {
    id: sid,
    hour: hour,
    minute: min,
    display: `${document.getElementById('sch-hour').value}:${min.toString().padStart(2,'0')} ${ampm} BD`,
    keyword_sets: [...kwQueue],
    created_at: new Date().toLocaleString()
  };
  schedules.push(sched);
  localStorage.setItem('schedules', JSON.stringify(schedules));
  // Register with backend
  fetch('/api/schedule', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
    schedule_id: sid, hour: hour, minute: min, keyword_sets: kwQueue,
    webhook_url: localStorage.getItem('webhook_url') || '',
    db_webhook_url: localStorage.getItem('db_webhook_url') || '',
    templates: templates
  })});
  renderSchedules();
  alert(`Scheduled for ${sched.display} BD time!`);
}
function removeSchedule(i) {
  const sid = schedules[i].id;
  fetch('/api/schedule/' + sid, {method:'DELETE'});
  schedules.splice(i,1);
  localStorage.setItem('schedules', JSON.stringify(schedules));
  renderSchedules();
}
function renderSchedules() {
  const el = document.getElementById('sch-list');
  if(!schedules.length) return el.innerHTML = '<p class="text-xs text-center" style="color:var(--muted)">No schedules set.</p>';
  el.innerHTML = schedules.map((s,i) => `
    <div class="schedule-item flex items-center justify-between">
      <div>
        <div class="text-sm font-bold text-white flex items-center gap-2">
          <i class="fa-solid fa-alarm-clock" style="color:var(--success)"></i>
          ${s.display}
        </div>
        <div class="text-xs mt-1" style="color:var(--muted)">${s.keyword_sets.length} keyword set(s) | Added: ${s.created_at}</div>
      </div>
      <button onclick="removeSchedule(${i})" class="btn btn-ghost text-xs py-1 px-3" style="color:var(--danger)"><i class="fa-solid fa-trash"></i></button>
    </div>`).join('');
}

// ─── Run Single Job ───
async function startJob() {
  const loc = document.getElementById('m-loc').value.trim();
  const kw = document.getElementById('m-kw').value.trim();
  let count = parseInt(document.getElementById('m-count').value)||10;
  if(count > 200) count = 200;
  if(!loc || !kw) return alert('Location & Keyword required!');
  const webhook = localStorage.getItem('webhook_url') || '';
  const db_webhook = localStorage.getItem('db_webhook_url') || '';
  setSt('Initializing AI Engine...','load',3);
  document.getElementById('dlbtn').classList.add('hidden');
  document.getElementById('pvbox').classList.add('hidden');
  document.getElementById('btn-run').disabled = true;
  tableShown = false;
  const payload = {
    keyword: kw, location: loc, max_leads: count,
    max_rating: document.getElementById('m-rating').value || null,
    webhook_url: webhook, db_webhook_url: db_webhook, templates: templates
  };
  try {
    const r = await fetch('/api/scrape',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
    const d = await r.json();
    if(d.error) { setSt(d.error,'err'); document.getElementById('btn-run').disabled=false; return; }
    jid = d.job_id;
    historyData.unshift({loc, kw, target: count, date: new Date().toLocaleString()});
    localStorage.setItem('history', JSON.stringify(historyData)); renderHistory();
    startPolling();
  } catch(e) {
    setSt('Server connection failed','err');
    document.getElementById('btn-run').disabled=false;
  }
}

// ─── Polling ───
function startPolling() {
  const poll = async () => {
    try {
      const r2 = await fetch('/api/status/'+jid);
      const d2 = await r2.json();
      const total_sets = d2.total_keyword_sets || 1;
      const cur_set = d2.current_set_index || 1;
      if(total_sets > 1) {
        const badge = document.getElementById('s-set-badge');
        badge.classList.remove('hidden');
        badge.textContent = `Set ${cur_set}/${total_sets}`;
      }
      if(d2.status === 'scraping') {
        const pct = Math.max(3, (d2.count / (d2.target || 10)) * 100);
        setSt(d2.status_text, 'load', pct);
        if(d2.leads?.length > 0) { updStats(d2.leads); if(!tableShown){showPV(d2.leads); tableShown=true;} }
        setTimeout(poll, 2500);
      } else if(d2.status === 'sending_emails') {
        if(!tableShown && d2.leads) { updStats(d2.leads); showPV(d2.leads); tableShown=true; }
        document.getElementById('dlbtn').classList.remove('hidden');
        const pct = d2.total_to_send > 0 ? (d2.emails_sent / d2.total_to_send) * 100 : 50;
        setSt(d2.status_text, 'email', pct);
        setTimeout(poll, 2500);
      } else if(d2.status === 'done') {
        document.getElementById('btn-run').disabled=false;
        if(d2.leads) { updStats(d2.leads); showPV(d2.leads); }
        setSt(d2.status_text, 'done', 100);
        document.getElementById('dlbtn').classList.remove('hidden');
      } else if(d2.status === 'error') {
        document.getElementById('btn-run').disabled=false;
        setSt(d2.error, 'err');
      } else {
        setTimeout(poll, 2500);
      }
    } catch(e) { setTimeout(poll, 2500); }
  };
  setTimeout(poll, 1500);
}

function setSt(msg, state='load', pct=null) {
  document.getElementById('sbox').classList.remove('hidden');
  document.getElementById('sdet').textContent = msg;
  const ic = document.getElementById('si');
  const txt = document.getElementById('stxt');
  if(state==='load') { ic.className='fa-solid fa-circle-notch spin text-blue-400 text-lg'; txt.textContent='Scraping Engine Running...'; }
  else if(state==='email') { ic.className='fa-solid fa-paper-plane pulse text-sky-400 text-lg'; txt.textContent='Sending Personalized Emails...'; }
  else if(state==='done') { ic.className='fa-solid fa-circle-check text-emerald-400 text-lg'; txt.textContent='Task Completed!'; }
  else { ic.className='fa-solid fa-circle-xmark text-red-400 text-lg'; txt.textContent='Error Occurred!'; }
  if(pct != null) document.getElementById('sbar').style.width = Math.min(100, pct) + '%';
}

function updStats(leads) {
  document.getElementById('st-leads').textContent = leads.length;
  document.getElementById('st-emails').textContent = leads.filter(l=>l.Email&&l.Email!='N/A').length;
  document.getElementById('st-phones').textContent = leads.filter(l=>l.Phone&&l.Phone!='N/A').length;
  document.getElementById('st-webs').textContent = leads.filter(l=>l.Website&&l.Website!='N/A').length;
}

function showPV(leads) {
  if(!leads?.length) return;
  document.getElementById('pvbox').classList.remove('hidden');
  document.getElementById('pvcnt').textContent = `(${leads.length} total, showing top 10)`;
  const keys = Object.keys(leads[0]);
  document.getElementById('th').innerHTML = keys.map(k=>`<th>${k}</th>`).join('');
  document.getElementById('tb').innerHTML = leads.slice(0,10).map(l =>
    `<tr>${keys.map(k => {
      const v = (l[k]||'N/A').toString();
      const cls = v==='N/A' ? 'badge-err' : k==='Email' ? 'badge-ok' : k==='Rating' ? 'badge-warn' : '';
      return `<td>${cls ? `<span class="badge ${cls}">${v}</span>` : `<span style="color:#94a3b8">${v.length>40?v.substring(0,40)+'...':v}</span>`}</td>`;
    }).join('')}</tr>`
  ).join('');
}

function doDL() { if(jid) window.location='/api/download/'+jid; }
</script>
</body>
</html>"""

@flask_app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@flask_app.route('/api/scrape', methods=['POST'])
def start_api_job():
    data = request.json
    job_id = str(uuid.uuid4())[:8]
    t = threading.Thread(target=run_multi_keyword_job, args=(job_id, data))
    t.daemon = True
    t.start()
    return jsonify({'job_id': job_id})

@flask_app.route('/api/status/<job_id>')
def status(job_id):
    job = jobs.get(job_id, {'status': 'not_found'})
    out = dict(job)
    if out.get('status') in ['sending_emails', 'done', 'scraping']:
        out['leads'] = job.get('leads', [])
        out['target'] = sum(kset.get('max_leads', 10) for kset in []) or 10
    return jsonify(out)

@flask_app.route('/api/download/<job_id>')
def download(job_id):
    job = jobs.get(job_id)
    if not job or job.get('status') not in ['done', 'sending_emails']:
        return "Not ready", 400
    leads = job.get('leads', [])
    if not leads: return "No leads found", 404
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=leads[0].keys())
    writer.writeheader()
    writer.writerows(leads)
    out.seek(0)
    return send_file(io.BytesIO(out.getvalue().encode('utf-8-sig')), mimetype='text/csv',
                     as_attachment=True, download_name='Target_Leads.csv')

@flask_app.route('/api/schedule', methods=['POST'])
def add_schedule_api():
    data = request.json
    sid = data.get('schedule_id', str(uuid.uuid4())[:8])
    hour = int(data.get('hour', 9))
    minute = int(data.get('minute', 0))
    keyword_sets = data.get('keyword_sets', [])
    webhook_url = data.get('webhook_url', '')
    db_webhook_url = data.get('db_webhook_url', '')
    tmplts = data.get('templates', [])
    scheduler.add_schedule(sid, hour, minute, keyword_sets)
    # Store webhook/templates in schedule for runner
    scheduler.scheduled_jobs[sid]['webhook_url'] = webhook_url
    scheduler.scheduled_jobs[sid]['db_webhook_url'] = db_webhook_url
    scheduler.scheduled_jobs[sid]['templates'] = tmplts
    return jsonify({'status': 'scheduled', 'id': sid, 'next_run': scheduler.scheduled_jobs[sid]['next_run']})

@flask_app.route('/api/schedule/<sid>', methods=['DELETE'])
def del_schedule_api(sid):
    scheduler.remove(sid)
    return jsonify({'status': 'removed'})

@flask_app.route('/api/schedules')
def list_schedules():
    return jsonify(scheduler.get_all())


# ════════════════════════════════════════════════════
#   TELEGRAM BOT (PRESERVED + ENHANCED)
# ════════════════════════════════════════════════════
def to_csv(leads):
    tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8-sig', newline='')
    if leads:
        writer = csv.DictWriter(tmp, fieldnames=leads[0].keys())
        writer.writeheader()
        writer.writerows(leads)
    tmp.close()
    return tmp.name

M_LOC, M_KW, M_COUNT, M_RATING = range(4)
bot_store = {}

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = [[InlineKeyboardButton("🚀 Start Search", callback_data="start_manual")]]
    await update.message.reply_text(
        "👋 *LeadGen Pro Ultra Bot*\n\n✅ 150+ AI Keywords per seed\n✅ Negative Review Prioritization\n✅ 90%+ Website Detection\n✅ Human-like Email Personalization\n✅ Max: 200 leads\n\n_Use the Web Dashboard for scheduling and multi-keyword queue._",
        parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb)
    )

async def handle_mode(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    bot_store[q.from_user.id] = {}
    await q.edit_message_text("📍 Enter Location (e.g. New York):")
    return M_LOC

async def m_loc(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    bot_store[update.message.from_user.id]['loc'] = update.message.text
    await update.message.reply_text("🔍 Enter Keyword (e.g. dentist):")
    return M_KW

async def m_kw(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    bot_store[update.message.from_user.id]['kw'] = update.message.text
    await update.message.reply_text("🔢 Target Valid Emails (Max 200):")
    return M_COUNT

async def m_count(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    count = int(update.message.text) if update.message.text.isdigit() else 10
    if count > 200: count = 200
    uid = update.message.from_user.id
    bot_store[uid]['count'] = count
    await update.message.reply_text("⭐ Max Rating Filter? (e.g. 3.5 | type 'skip'):")
    return M_RATING

async def m_rating(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.lower()
    uid = update.message.from_user.id
    bot_store[uid]['rating'] = None if txt == 'skip' else txt
    data = bot_store[uid]
    summary = f"📋 *Target Config*\n📍 {data['loc']}\n🔍 {data['kw']}\n🔢 {data['count']} valid emails\n⭐ Max Rating: {data.get('rating') or 'None'}\n\nReady?"
    kb = [[InlineKeyboardButton("✅ Start", callback_data="start_scrape")]]
    await update.message.reply_text(summary, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))
    return ConversationHandler.END

def run_bot_scrape(data):
    location = data['loc']
    base_keyword = data['kw']
    max_leads = data['count']
    max_rating = data.get('rating')
    kw_engine = AdvancedKeywordEngine()
    m_scraper = GoogleMapsScraper()
    e_lib = DeepEmailExtractor()
    seen_names = set()
    used_keywords = set()
    pending_keywords = [base_keyword]
    final_leads = []
    while len(final_leads) < max_leads:
        if not pending_keywords:
            new_kws = kw_engine.generate_full_pool(base_keyword, location, used_keywords)
            random.shuffle(new_kws)
            pending_keywords.extend(new_kws)
        current_kw = pending_keywords.pop(0)
        used_keywords.add(current_kw.lower())
        raw_leads = m_scraper.fetch_batch(current_kw, location)
        for lead in raw_leads:
            if len(final_leads) >= max_leads: break
            if lead['Name'] in seen_names: continue
            if max_rating and lead['Rating'] != "N/A":
                try:
                    if float(lead['Rating']) > float(max_rating): continue
                except: pass
            website = lead['Website']
            if website == "N/A":
                website = m_scraper.find_website_via_search(lead['Name'], location)
                lead['Website'] = website
            if website == "N/A": continue
            extracted_email = e_lib.get_email(website)
            if extracted_email == "N/A": continue
            lead['Email'] = extracted_email
            seen_names.add(lead['Name'])
            final_leads.append(lead)
    return final_leads

async def background_bot_task(chat_id, message_id, data, bot):
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id,
            text="⏳ *Scraping with 150+ AI Keywords...*\n_Targeting bad-rated businesses first._",
            parse_mode='Markdown')
        loop = asyncio.get_event_loop()
        final_leads = await loop.run_in_executor(None, run_bot_scrape, data)
        if not final_leads:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="😔 No results found.")
            return
        path = to_csv(final_leads)
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="✅ Done! Sending file...")
        with open(path, 'rb') as f:
            await bot.send_document(chat_id=chat_id, document=f, filename="Target_Leads.csv",
                caption=f"🎯 *Complete!*\n📊 Valid Leads: {len(final_leads)}", parse_mode='Markdown')
        os.unlink(path)
    except Exception as e:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id,
            text=f"❌ Error: `{e}`", parse_mode='Markdown')

async def execute_scrape(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    data = bot_store.get(uid)
    msg = await q.edit_message_text("⏳ *Initializing...*", parse_mode='Markdown')
    asyncio.create_task(background_bot_task(q.message.chat_id, msg.message_id, data, ctx.bot))

def run_telegram_bot():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(handle_mode, pattern="^start_manual$")],
        states={
            M_LOC: [MessageHandler(filters.TEXT & ~filters.COMMAND, m_loc)],
            M_KW: [MessageHandler(filters.TEXT & ~filters.COMMAND, m_kw)],
            M_COUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, m_count)],
            M_RATING: [MessageHandler(filters.TEXT & ~filters.COMMAND, m_rating)]
        },
        fallbacks=[],
        per_message=False,
    )
    app.add_handler(CommandHandler("start", start))
    app.add_handler(conv)
    app.add_handler(CallbackQueryHandler(execute_scrape, pattern="^start_scrape$"))
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


# ════════════════════════════════════════════════════
#   SCHEDULER BACKGROUND THREAD
# ════════════════════════════════════════════════════
def on_scheduled_job(kset_wrapper):
    """Called by scheduler when a job is due."""
    ksets = kset_wrapper if isinstance(kset_wrapper, list) else [kset_wrapper]
    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {'status': 'scraping', 'count': 0, 'leads': [], 'emails_sent': 0,
                    'total_to_send': 0, 'status_text': 'Scheduler triggered job...'}
    webhook = kset_wrapper.get('webhook_url', '') if isinstance(kset_wrapper, dict) else ''
    db_webhook = kset_wrapper.get('db_webhook_url', '') if isinstance(kset_wrapper, dict) else ''
    tmplts = kset_wrapper.get('templates', []) if isinstance(kset_wrapper, dict) else []
    t = threading.Thread(target=run_multi_keyword_job, args=(job_id, {
        'keyword_sets': ksets, 'webhook_url': webhook, 'db_webhook_url': db_webhook, 'templates': tmplts
    }))
    t.daemon = True
    t.start()


if __name__ == "__main__":
    # Start Telegram bot
    if TELEGRAM_TOKEN:
        threading.Thread(target=run_telegram_bot, daemon=True).start()
        print("✅ Telegram bot started")

    # Start scheduler
    threading.Thread(
        target=scheduler.run_forever,
        args=(lambda kset: on_scheduled_job(kset),),
        daemon=True
    ).start()
    print("✅ BD Scheduler started")

    port = int(os.environ.get("PORT", 10000))
    print(f"✅ Flask starting on port {port}")
    flask_app.run(host='0.0.0.0', port=port)
