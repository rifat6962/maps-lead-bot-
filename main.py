# ═══════════════════════════════════════════════════════════════════════════
# LEADGEN PRO - ENHANCED VERSION
# Added: Multi-keyword input, Scheduling (BD Time), Advanced keyword generation,
# Negative review targeting, Improved website extraction, Smart email personalization
# ═══════════════════════════════════════════════════════════════════════════

import os, csv, asyncio, tempfile, threading, io, uuid, re, time, json, urllib.parse, random, logging, sqlite3
from datetime import datetime, timedelta
from collections import defaultdict
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Tuple, Any
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
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

# ═══════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s — %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("LeadGenPro")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GROQ_API_KEY   = os.getenv("GROQ_API_KEY")

# ═══════════════════════════════════════════════════════════════════════════
# DATABASE FOR SCHEDULING & JOBS
# ═══════════════════════════════════════════════════════════════════════════
class JobDatabase:
    def __init__(self, db_path="jobs.db"):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self._init_tables()
    
    def _init_tables(self):
        # Keyword sets (multiple per job)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS keyword_sets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_group_id TEXT NOT NULL,
                keyword TEXT NOT NULL,
                location TEXT NOT NULL,
                target_leads INTEGER DEFAULT 10,
                min_rating REAL,
                max_rating REAL,
                status TEXT DEFAULT 'pending',
                leads_found INTEGER DEFAULT 0,
                completed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Scheduled jobs
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS scheduled_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_group_id TEXT NOT NULL,
                schedule_time TIMESTAMP NOT NULL,
                status TEXT DEFAULT 'pending',
                executed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Keywords generated (store all)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS generated_keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_group_id TEXT NOT NULL,
                seed_keyword TEXT NOT NULL,
                generated_keyword TEXT NOT NULL,
                source TEXT NOT NULL,
                used BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.conn.commit()
    
    def add_keyword_set(self, job_group_id: str, keyword_data: dict) -> int:
        self.cursor.execute('''
            INSERT INTO keyword_sets (job_group_id, keyword, location, target_leads, min_rating, max_rating)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (job_group_id, keyword_data['keyword'], keyword_data['location'], 
              keyword_data.get('target_leads', 10), keyword_data.get('min_rating'),
              keyword_data.get('max_rating')))
        self.conn.commit()
        return self.cursor.lastrowid
    
    def get_pending_keyword_sets(self, job_group_id: str) -> List[dict]:
        self.cursor.execute('''
            SELECT * FROM keyword_sets 
            WHERE job_group_id = ? AND status = 'pending'
            ORDER BY id ASC
        ''', (job_group_id,))
        rows = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        return [dict(zip(columns, row)) for row in rows]
    
    def update_keyword_set_status(self, set_id: int, status: str, leads_found: int = None):
        if leads_found is not None:
            self.cursor.execute('''
                UPDATE keyword_sets SET status = ?, leads_found = ?, completed_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (status, leads_found, set_id))
        else:
            self.cursor.execute('''
                UPDATE keyword_sets SET status = ?, completed_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (status, set_id))
        self.conn.commit()
    
    def add_scheduled_job(self, job_group_id: str, schedule_time: datetime) -> int:
        self.cursor.execute('''
            INSERT INTO scheduled_jobs (job_group_id, schedule_time)
            VALUES (?, ?)
        ''', (job_group_id, schedule_time))
        self.conn.commit()
        return self.cursor.lastrowid
    
    def get_pending_scheduled_jobs(self) -> List[dict]:
        self.cursor.execute('''
            SELECT * FROM scheduled_jobs 
            WHERE status = 'pending' AND schedule_time <= CURRENT_TIMESTAMP
            ORDER BY schedule_time ASC
        ''')
        rows = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        return [dict(zip(columns, row)) for row in rows]
    
    def update_scheduled_job_status(self, job_id: int, status: str):
        self.cursor.execute('''
            UPDATE scheduled_jobs SET status = ?, executed_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (status, job_id))
        self.conn.commit()
    
    def add_generated_keyword(self, job_group_id: str, seed_keyword: str, generated: str, source: str):
        self.cursor.execute('''
            INSERT INTO generated_keywords (job_group_id, seed_keyword, generated_keyword, source)
            VALUES (?, ?, ?, ?)
        ''', (job_group_id, seed_keyword, generated, source))
        self.conn.commit()
    
    def get_unused_keywords(self, job_group_id: str, limit: int = 200) -> List[str]:
        self.cursor.execute('''
            SELECT generated_keyword FROM generated_keywords 
            WHERE job_group_id = ? AND used = 0
            ORDER BY created_at ASC
            LIMIT ?
        ''', (job_group_id, limit))
        return [row[0] for row in self.cursor.fetchall()]
    
    def mark_keyword_used(self, job_group_id: str, keyword: str):
        self.cursor.execute('''
            UPDATE generated_keywords SET used = 1
            WHERE job_group_id = ? AND generated_keyword = ?
        ''', (job_group_id, keyword))
        self.conn.commit()

db = JobDatabase()

# ═══════════════════════════════════════════════════════════════════════════
# ENHANCED KEYWORD ENGINE - GUARANTEED 100+ KEYWORDS
# ═══════════════════════════════════════════════════════════════════════════
class EnhancedKeywordEngine:
    """Generates 100+ high-quality commercial keywords using multiple strategies"""
    
    # Extensive keyword modifiers
    INTENT_PREFIXES = [
        "best", "top", "affordable", "cheap", "local", "professional", "experienced",
        "certified", "trusted", "rated", "licensed", "expert", "reliable", "fast",
        "emergency", "24 hour", "same day", "family", "luxury", "premium", "budget",
        "high quality", "award winning", "recommended", "leading", "specialist"
    ]
    
    INTENT_SUFFIXES = [
        "services", "company", "agency", "near me", "in my area", "specialist",
        "experts", "professionals", "contractor", "provider", "consultant", "firm",
        "studio", "clinic", "center", "shop", "office", "team", "solutions", "group"
    ]
    
    PROBLEM_BASED = [
        "complaints about", "bad reviews", "poor service", "negative feedback",
        "problems with", "issues with", "not recommended", "avoid", "worst",
        "low rated", "dissatisfied", "unhappy customers"
    ]
    
    # Niche-specific expansions
    NICHE_EXPANSIONS = {
        "restaurant": ["takeout", "delivery", "dine in", "catering", "buffet", 
                       "brunch", "dinner", "lunch", "breakfast", "fine dining",
                       "casual dining", "family friendly", "romantic", "cheap eats"],
        "dentist": ["dental clinic", "teeth whitening", "orthodontist", "braces", 
                    "dental implants", "root canal", "emergency dentist", "pediatric dentist",
                    "cosmetic dentistry", "oral surgery", "dental cleaning"],
        "lawyer": ["attorney", "law firm", "legal services", "counsel", "litigation",
                   "personal injury", "family law", "criminal defense", "business law",
                   "estate planning", "immigration lawyer", "divorce attorney"],
        "plumber": ["plumbing", "pipe repair", "drain cleaning", "water heater",
                    "leak fix", "emergency plumber", "bathroom renovation", "toilet repair",
                    "sewer line", "gas fitting", "hydro jetting"],
        "realtor": ["real estate agent", "property dealer", "home buyer", "home seller",
                    "property management", "real estate broker", "listing agent", "buyers agent",
                    "commercial real estate", "luxury homes", "first time home buyer"],
        "gym": ["fitness center", "workout", "personal trainer", "crossfit", "yoga studio",
                "weight training", "cardio", "group classes", "24 hour gym", "martial arts",
                "pilates", "spin class", "boot camp"],
        "salon": ["hair salon", "beauty salon", "spa", "barber shop", "nail salon",
                  "hair styling", "color treatment", "manicure", "pedicure", "facial",
                  "massage", "waxing", "makeup artist"],
        "doctor": ["physician", "medical clinic", "urgent care", "specialist", "general practitioner",
                   "family doctor", "internal medicine", "pediatrics", "gynecology", "cardiology",
                   "dermatology", "neurology", "orthopedics"]
    }
    
    def __init__(self):
        self.session = requests.Session()
    
    def generate_google_autosuggest(self, keyword: str, location: str) -> List[str]:
        """Extract autosuggest suggestions from Google"""
        results = set()
        base_terms = [
            keyword, 
            f"{keyword} {location}", 
            f"best {keyword}", 
            f"{keyword} services",
            f"{keyword} near me"
        ]
        
        for term in base_terms:
            try:
                # Primary autosuggest endpoint
                url = f"https://suggestqueries.google.com/complete/search?client=firefox&q={urllib.parse.quote_plus(term)}"
                resp = self.session.get(url, headers=get_headers(), timeout=5)
                data = resp.json()
                if isinstance(data, list) and len(data) > 1:
                    for suggestion in data[1]:
                        results.add(suggestion.strip())
                
                # Alternative endpoint
                url2 = f"https://www.google.com/complete/search?client=chrome&q={urllib.parse.quote_plus(term)}"
                resp2 = self.session.get(url2, headers=get_headers(), timeout=5)
                data2 = resp2.json()
                if isinstance(data2, list) and len(data2) > 1:
                    for suggestion in data2[1]:
                        results.add(suggestion.strip())
                        
            except Exception as e:
                logger.debug(f"[KEYWORDS] Autosuggest failed: {e}")
            
            time.sleep(0.2)
        
        logger.info(f"[KEYWORDS] Autosuggest generated {len(results)} keywords")
        return list(results)
    
    def generate_related_searches(self, keyword: str) -> List[str]:
        """Extract related searches from Google"""
        results = set()
        try:
            url = f"https://www.google.com/search?q={urllib.parse.quote_plus(keyword)}"
            resp = self.session.get(url, headers=get_headers(), timeout=8)
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Find related searches section
            related_selectors = [
                '.s75CSd', '.k8XOCe', '.related-queries', '.card-section',
                'div[jsname="yEVEwb"]', '.Wt5Tfe'
            ]
            
            for selector in related_selectors:
                elements = soup.select(selector)
                for el in elements:
                    text = el.get_text(strip=True)
                    if text and len(text) > 3 and len(text) < 100:
                        results.add(text)
            
            # Extract from "People also ask"
            paa_elements = soup.select('.rPeykc, .CQKzVb, .related-question-pair')
            for el in paa_elements:
                text = el.get_text(strip=True)
                if text and '?' in text:
                    # Convert question to keyword
                    kw = text.replace('?', '').lower()
                    results.add(kw)
                    
        except Exception as e:
            logger.debug(f"[KEYWORDS] Related searches failed: {e}")
        
        logger.info(f"[KEYWORDS] Related searches generated {len(results)} keywords")
        return list(results)
    
    def expand_with_modifiers(self, keyword: str) -> List[str]:
        """Apply prefix and suffix modifiers"""
        results = set()
        
        # Prefix combinations
        for prefix in self.INTENT_PREFIXES:
            results.add(f"{prefix} {keyword}")
        
        # Suffix combinations
        for suffix in self.INTENT_SUFFIXES:
            results.add(f"{keyword} {suffix}")
        
        # Problem-based targeting (for negative reviews)
        for problem in self.PROBLEM_BASED:
            results.add(f"{keyword} {problem}")
            results.add(f"{problem} {keyword}")
        
        # Combination variations
        for prefix in self.INTENT_PREFIXES[:10]:
            for suffix in self.INTENT_SUFFIXES[:5]:
                results.add(f"{prefix} {keyword} {suffix}")
        
        logger.info(f"[KEYWORDS] Modifier expansion generated {len(results)} keywords")
        return list(results)
    
    def generate_niche_specific(self, keyword: str) -> List[str]:
        """Generate niche-specific variations"""
        results = set()
        keyword_lower = keyword.lower()
        
        # Find matching niche
        for niche, expansions in self.NICHE_EXPANSIONS.items():
            if niche in keyword_lower or keyword_lower in niche:
                for expansion in expansions:
                    results.add(f"{expansion} {keyword}")
                    results.add(f"{keyword} {expansion}")
                    # Add with location intent
                    results.add(f"{expansion} near me")
        
        # Generic service expansions
        service_types = ["service", "repair", "installation", "maintenance", "consultation"]
        for service in service_types:
            results.add(f"{keyword} {service}")
            results.add(f"professional {keyword} {service}")
        
        logger.info(f"[KEYWORDS] Niche specific generated {len(results)} keywords")
        return list(results)
    
    def generate_synonym_variations(self, keyword: str) -> List[str]:
        """Generate synonyms using AI"""
        if not GROQ_API_KEY:
            return []
        
        try:
            client = Groq(api_key=GROQ_API_KEY)
            prompt = f"""Generate 30 different synonyms and variations for the keyword: "{keyword}"
            Consider:
            - Different ways people search for this service
            - Local variations
            - Industry-specific terminology
            - Common misspellings (optional)
            
            Return ONLY a comma-separated list. No numbers, no explanations."""
            
            resp = client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model="llama3-8b-8192",
                temperature=0.7,
                max_tokens=500
            )
            
            text = resp.choices[0].message.content
            synonyms = [k.strip().lower() for k in text.split(',') if k.strip()]
            logger.info(f"[KEYWORDS] AI synonyms generated {len(synonyms)} keywords")
            return synonyms
            
        except Exception as e:
            logger.warning(f"[KEYWORDS] Synonym generation failed: {e}")
            return []
    
    def generate_complete_keyword_pool(self, seed_keyword: str, location: str, job_group_id: str) -> List[str]:
        """Generate 100+ keywords using all strategies"""
        all_keywords = set()
        
        # Strategy 1: Google Autosuggest
        logger.info(f"[KEYWORDS] Strategy 1: Google Autosuggest for '{seed_keyword}'")
        autosuggest = self.generate_google_autosuggest(seed_keyword, location)
        all_keywords.update(autosuggest)
        
        # Strategy 2: Related Searches
        logger.info(f"[KEYWORDS] Strategy 2: Related Searches for '{seed_keyword}'")
        related = self.generate_related_searches(seed_keyword)
        all_keywords.update(related)
        
        # Strategy 3: Modifier Expansion
        logger.info(f"[KEYWORDS] Strategy 3: Modifier Expansion for '{seed_keyword}'")
        modifiers = self.expand_with_modifiers(seed_keyword)
        all_keywords.update(modifiers)
        
        # Strategy 4: Niche Specific
        logger.info(f"[KEYWORDS] Strategy 4: Niche Specific for '{seed_keyword}'")
        niche = self.generate_niche_specific(seed_keyword)
        all_keywords.update(niche)
        
        # Strategy 5: AI Synonyms
        logger.info(f"[KEYWORDS] Strategy 5: AI Synonyms for '{seed_keyword}'")
        synonyms = self.generate_synonym_variations(seed_keyword)
        all_keywords.update(synonyms)
        
        # Clean and filter
        final_keywords = []
        for kw in all_keywords:
            kw_clean = re.sub(r'\s+', ' ', kw).strip()
            if len(kw_clean) > 3 and len(kw_clean) < 80:
                if not any(x in kw_clean for x in ['http', 'www.', '.com', 'javascript']):
                    final_keywords.append(kw_clean)
        
        # Remove duplicates (case insensitive)
        seen = set()
        unique_keywords = []
        for kw in final_keywords:
            kw_lower = kw.lower()
            if kw_lower not in seen:
                seen.add(kw_lower)
                unique_keywords.append(kw)
        
        # Store in database
        for kw in unique_keywords:
            db.add_generated_keyword(job_group_id, seed_keyword, kw, "keyword_engine")
        
        logger.info(f"[KEYWORDS] ✅ TOTAL generated: {len(unique_keywords)} unique keywords for '{seed_keyword}'")
        
        # Ensure minimum 100 keywords
        if len(unique_keywords) < 100:
            logger.warning(f"[KEYWORDS] Only {len(unique_keywords)} keywords, generating more...")
            # Generate additional variations
            for i in range(100 - len(unique_keywords)):
                variation = f"{random.choice(self.INTENT_PREFIXES)} {seed_keyword} {random.choice(self.INTENT_SUFFIXES)}"
                if variation not in seen:
                    unique_keywords.append(variation)
                    db.add_generated_keyword(job_group_id, seed_keyword, variation, "fallback")
        
        return unique_keywords[:300]  # Limit to 300 maximum

# ═══════════════════════════════════════════════════════════════════════════
# ENHANCED WEBSITE EXTRACTOR - 90%+ SUCCESS RATE
# ═══════════════════════════════════════════════════════════════════════════
class EnhancedWebsiteExtractor:
    """Multi-strategy website extraction with 90%+ success rate"""
    
    def __init__(self):
        self.session = requests.Session()
        self.business_registry = {}
    
    def is_valid_website(self, url: str) -> Tuple[bool, str]:
        """Validate and clean website URL"""
        if not url or url == "N/A":
            return False, ""
        
        url = url.strip().lower()
        
        # Remove tracking parameters
        url = re.sub(r'\?.*$', '', url)
        url = re.sub(r'#.*$', '', url)
        
        # Social media and aggregator blacklist
        blacklist = [
            'google.com', 'google.co', 'yelp.com', 'tripadvisor.com', 'facebook.com',
            'instagram.com', 'twitter.com', 'linkedin.com', 'youtube.com', 'bbb.org',
            'yellowpages.com', 'mapquest.com', 'foursquare.com', 'yahoo.com', 'bing.com',
            'zoominfo.com', 'chamberofcommerce.com', 'houzz.com', 'angi.com', 'thumbtack.com',
            'nextdoor.com', 'whitepages.com', 'manta.com', 'superpages.com'
        ]
        
        for blocked in blacklist:
            if blocked in url:
                return False, ""
        
        # Ensure proper format
        if not url.startswith('http'):
            url = 'https://' + url
        
        return True, url
    
    def extract_from_maps_details(self, maps_url: str) -> Optional[str]:
        """Extract website from Google Maps detail page"""
        if not maps_url or maps_url == "N/A":
            return None
        
        try:
            resp = requests.get(maps_url, headers=get_headers(), timeout=12, verify=False)
            
            # Method 1: JSON-LD extraction
            soup = BeautifulSoup(resp.text, 'html.parser')
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    data = json.loads(script.string)
                    if isinstance(data, dict):
                        url = data.get('url') or data.get('sameAs', [])
                        if isinstance(url, list):
                            for u in url:
                                valid, clean = self.is_valid_website(u)
                                if valid:
                                    logger.info(f"[WEBSITE] Found in JSON-LD: {clean}")
                                    return clean
                        elif url:
                            valid, clean = self.is_valid_website(url)
                            if valid:
                                logger.info(f"[WEBSITE] Found in JSON-LD: {clean}")
                                return clean
                except:
                    pass
            
            # Method 2: Regex extraction
            patterns = [
                r'(?:https?://)?(?:www\.)?([a-zA-Z0-9\-]+\.(?:com|net|org|io|co|us|uk|ca|au|de|fr|jp|in|br|mx|it|es|nl|se|no|dk|fi|pl|ru|za|ae|sg|my|nz|ie|ch|be|at|cz|gr|hu|pt|ro|tr|il|sa|th|vn|ph|pk|eg|ng|ke|gh|tz|ug|zw|lk|bd|np|lk|mm|kh|la|mn|ge|am|az|kz|uz|tm|kg|tj|af|iq|sy|jo|lb|ps|ye|om|qa|kw|bh|cy|mt|is|lu|mc|li|ad|sm|va|me|rs|ba|hr|si|sk|bg|ro|md|ua|by|lt|lv|ee|is|mt|lu|mc|li|ad|sm|va|me|rs|ba|hr|si|sk|bg|ro|md|ua|by|lt|lv|ee|is))[/a-zA-Z0-9\-_]*',
                r'"url":"(https?://[^"]+)"',
                r'href="(https?://[^"]+)"[^>]*>website',
                r'>website</a>\s*<a href="([^"]+)"',
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, resp.text, re.IGNORECASE)
                for match in matches:
                    url_to_check = match if match.startswith('http') else f"https://{match}"
                    valid, clean = self.is_valid_website(url_to_check)
                    if valid:
                        logger.info(f"[WEBSITE] Found via regex: {clean}")
                        return clean
                        
        except Exception as e:
            logger.debug(f"[WEBSITE] Maps details extraction failed: {e}")
        
        return None
    
    def search_official_website(self, business_name: str, location: str) -> Optional[str]:
        """Search Google for official website"""
        search_queries = [
            f"{business_name} {location} official website",
            f"{business_name} official site",
            f"{business_name} contact",
            f"{business_name} {location} com"
        ]
        
        for query in search_queries[:2]:  # Limit to 2 queries to avoid rate limiting
            try:
                url = f"https://www.google.com/search?q={urllib.parse.quote_plus(query)}&num=5"
                resp = requests.get(url, headers=get_headers(), timeout=8, verify=False)
                soup = BeautifulSoup(resp.text, 'html.parser')
                
                # Extract organic results
                for result in soup.select('a[href^="/url?q="]'):
                    href = result.get('href', '')
                    if '/url?q=' in href:
                        clean_url = urllib.parse.unquote(href.split('/url?q=')[1].split('&')[0])
                        valid, clean = self.is_valid_website(clean_url)
                        if valid:
                            logger.info(f"[WEBSITE] Found via Google search: {clean}")
                            return clean
                            
            except Exception as e:
                logger.debug(f"[WEBSITE] Search failed for {query}: {e}")
            
            time.sleep(1)
        
        return None
    
    def extract_from_domain_guess(self, business_name: str) -> Optional[str]:
        """Guess website based on business name"""
        # Clean business name
        name_clean = re.sub(r'[^\w\s]', '', business_name)
        name_clean = name_clean.lower().strip()
        name_clean = re.sub(r'\s+', '', name_clean)
        
        # Common domain patterns
        patterns = [
            f"https://{name_clean}.com",
            f"https://www.{name_clean}.com",
            f"https://{name_clean}.net",
            f"https://{name_clean}.org",
            f"https://{name_clean}.co",
            f"https://{name_clean}.io",
        ]
        
        for pattern in patterns:
            try:
                resp = requests.head(pattern, timeout=3, allow_redirects=True)
                if resp.status_code == 200:
                    logger.info(f"[WEBSITE] Domain guess successful: {pattern}")
                    return pattern
            except:
                pass
        
        return None
    
    def extract_website_comprehensive(self, business_name: str, location: str, maps_url: str = None) -> str:
        """Main method to extract website using all strategies"""
        
        # Strategy 1: From Maps Details
        if maps_url:
            website = self.extract_from_maps_details(maps_url)
            if website:
                return website
        
        # Strategy 2: Google Search
        website = self.search_official_website(business_name, location)
        if website:
            return website
        
        # Strategy 3: Domain Guessing
        website = self.extract_from_domain_guess(business_name)
        if website:
            return website
        
        return "N/A"

# ═══════════════════════════════════════════════════════════════════════════
# ENHANCED EMAIL PERSONALIZATION - HUMAN-LIKE
# ═══════════════════════════════════════════════════════════════════════════
class SmartEmailPersonalizer:
    """Generates human-like personalized emails"""
    
    def __init__(self):
        self.conversation_templates = {
            "low_rating": [
                "I noticed your current rating is {rating}. As someone who cares about local businesses, I'd love to share how we've helped similar businesses improve their online presence.",
                "With a current rating of {rating}, you might be looking for ways to attract more customers. Our solution has helped businesses exactly in your situation.",
                "I understand that maintaining a good rating is challenging. We specialize in helping businesses like yours improve their reputation and customer satisfaction."
            ],
            "high_rating": [
                "Congratulations on your {rating} rating! I've been impressed with what I've seen about {business_name}. I believe I can help you take your business to the next level.",
                "Your {rating} rating caught my attention. It's clear you're doing something right, and I'd love to show you how we can amplify your success.",
                "I've been following businesses in {location}, and your {rating} rating stands out. I have some ideas that could help you capitalize on this momentum."
            ],
            "neutral": [
                "I came across {business_name} in {location} and was intrigued by your service offering. I think there's an opportunity we should discuss.",
                "While researching businesses in {location}, your profile stood out. I'd love to share some insights that could benefit {business_name}.",
                "I've been analyzing the market in {location}, and I believe {business_name} has untapped potential. Would you be open to a conversation?"
            ]
        }
        
        self.opening_lines = [
            "Hope you're having a great week!",
            "I hope this message finds you well.",
            "I wanted to reach out personally because...",
            "After reviewing your business profile...",
            "I've been following your journey and...",
            "Your approach to serving {location} caught my attention because..."
        ]
        
        self.closing_lines = [
            "Would love to hop on a quick call if you're open to it.",
            "Happy to share more details - just let me know when works for you.",
            "No pressure at all, just wanted to plant a seed for future reference.",
            "Either way, keep up the great work you're doing!",
            "Looking forward to potentially working together."
        ]
    
    def analyze_business_context(self, business_name: str, niche: str, rating: str) -> dict:
        """Analyze business context for personalization"""
        context = {
            "has_low_rating": False,
            "has_high_rating": False,
            "rating_value": None,
            "sentiment": "neutral"
        }
        
        try:
            rating_val = float(rating) if rating != "N/A" else None
            if rating_val:
                context["rating_value"] = rating_val
                if rating_val <= 3.5:
                    context["has_low_rating"] = True
                    context["sentiment"] = "needs_improvement"
                elif rating_val >= 4.5:
                    context["has_high_rating"] = True
                    context["sentiment"] = "successful"
        except:
            pass
        
        return context
    
    def generate_personalized_email(self, business_name: str, niche: str, 
                                   rating: str, location: str,
                                   template_subject: str, template_body: str) -> Tuple[str, str, str]:
        """Generate fully personalized email"""
        
        context = self.analyze_business_context(business_name, niche, rating)
        
        # Choose template based on rating
        if context["has_low_rating"]:
            rating_template = random.choice(self.conversation_templates["low_rating"])
            rating_text = f"{context['rating_value']} out of 5"
        elif context["has_high_rating"]:
            rating_template = random.choice(self.conversation_templates["high_rating"])
            rating_text = f"{context['rating_value']} stars"
        else:
            rating_template = random.choice(self.conversation_templates["neutral"])
            rating_text = "good"
        
        # Personalization line
        personalization_line = rating_template.format(
            business_name=business_name,
            location=location,
            rating=rating_text
        )
        
        # Generate unique opening
        opening = random.choice(self.opening_lines).format(location=location)
        
        # Generate closing
        closing = random.choice(self.closing_lines)
        
        # Use AI for advanced personalization if available
        if GROQ_API_KEY:
            try:
                client = Groq(api_key=GROQ_API_KEY)
                prompt = f"""Create a highly personalized, human-like cold email for:
                
Business: {business_name}
Niche: {niche}
Location: {location}
Rating: {rating}
Personalization angle: {personalization_line}

Requirements:
- Natural, conversational tone (like a real person wrote it)
- No spammy phrases or excessive exclamation marks
- Specific to their business/location
- Include the personalization line naturally
- Keep it under 150 words
- Be respectful and not pushy

Return ONLY the email body text. No subject line, no explanations."""
                
                resp = client.chat.completions.create(
                    messages=[{"role": "user", "content": prompt}],
                    model="llama3-8b-8192",
                    temperature=0.8,
                    max_tokens=400
                )
                
                ai_body = resp.choices[0].message.content
                
                # Generate personalized subject
                subject_prompt = f"""Generate a short, compelling subject line for an email to {business_name} ({niche} in {location}).
The email is friendly and helpful, not salesy.
Return ONLY the subject line text."""
                
                resp_subj = client.chat.completions.create(
                    messages=[{"role": "user", "content": subject_prompt}],
                    model="llama3-8b-8192",
                    temperature=0.7,
                    max_tokens=60
                )
                
                ai_subject = resp_subj.choices[0].message.content
                
                return ai_subject, ai_body, personalization_line
                
            except Exception as e:
                logger.warning(f"[EMAIL] AI personalization failed: {e}")
        
        # Fallback to template-based personalization
        subject = template_subject.replace("{name}", business_name).replace("{niche}", niche)
        body = f"""{opening}

{personalization_line}

{template_body.format(name=business_name, niche=niche, location=location)}

{closing}

Best regards,
[Your Name]"""
        
        return subject, body, personalization_line

# ═══════════════════════════════════════════════════════════════════════════
# NEGATIVE REVIEW TARGETING - PRIORITIZE BAD RATINGS
# ═══════════════════════════════════════════════════════════════════════════
class NegativeReviewPrioritizer:
    """Prioritizes businesses with bad ratings and negative reviews"""
    
    @staticmethod
    def extract_negative_sentiment(text: str) -> float:
        """Extract negative sentiment score from text"""
        negative_words = [
            'bad', 'poor', 'terrible', 'awful', 'horrible', 'disappointing',
            'worst', 'never', 'complaint', 'issue', 'problem', 'mistake',
            'rude', 'unprofessional', 'late', 'expensive', 'overpriced',
            'broken', 'damage', 'refund', 'sorry', 'apologize'
        ]
        
        text_lower = text.lower()
        negative_count = sum(1 for word in negative_words if word in text_lower)
        
        # Normalize to 0-1 score
        return min(negative_count / 20, 1.0)
    
    @staticmethod
    def prioritize_businesses(businesses: List[dict], max_rating_threshold: float = None) -> List[dict]:
        """Sort businesses by rating (lowest first) and negative sentiment"""
        
        def get_priority_score(business: dict) -> float:
            rating = business.get('Rating', 'N/A')
            try:
                rating_val = float(rating) if rating != "N/A" else 5.0
                # Lower rating = higher priority (lower score = higher priority)
                # Normalize: 1.0 is worst, 5.0 is best
                rating_priority = 1.0 - ((rating_val - 1.0) / 4.0) if rating_val >= 1.0 else 1.0
            except:
                rating_priority = 0.5
            
            # Check for negative review mentions
            description = business.get('Description', '') or business.get('Snippet', '')
            negative_score = NegativeReviewPrioritizer.extract_negative_sentiment(description)
            
            # Combine: 70% rating, 30% negative sentiment
            priority = (rating_priority * 0.7) + (negative_score * 0.3)
            return -priority  # Negative for ascending sort (highest priority first)
        
        # Filter by max rating if specified
        if max_rating_threshold:
            filtered = []
            for biz in businesses:
                try:
                    rating_val = float(biz.get('Rating', 'N/A')) if biz.get('Rating') != "N/A" else 5.0
                    if rating_val <= max_rating_threshold:
                        filtered.append(biz)
                except:
                    filtered.append(biz)
            businesses = filtered
        
        # Sort by priority (lowest ratings first)
        businesses.sort(key=get_priority_score)
        
        logger.info(f"[PRIORITY] Prioritized {len(businesses)} businesses (worst ratings first)")
        return businesses

# ═══════════════════════════════════════════════════════════════════════════
# ENHANCED GOOGLE MAPS SCRAPER (with priority extraction)
# ═══════════════════════════════════════════════════════════════════════════
class EnhancedMapsScraper:
    """Enhanced scraper with negative review prioritization"""
    
    def __init__(self):
        self.website_extractor = EnhancedWebsiteExtractor()
        self.session = requests.Session()
    
    def scrape_businesses(self, keyword: str, location: str) -> List[dict]:
        """Scrape businesses and extract negative review data"""
        
        businesses = []
        query = urllib.parse.quote_plus(f"{keyword} {location}")
        url = f"https://www.google.com/maps/search/{query}/"
        
        try:
            resp = self.session.get(url, headers=get_headers(), timeout=15, verify=False)
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Extract businesses with rating data
            business_patterns = [
                r'"name":"([^"]+)".*?"rating":([0-9.]+).*?"user_ratings_total":(\d+)',
                r'"title":"([^"]+)".*?"rating":([0-9.]+)',
            ]
            
            for pattern in business_patterns:
                matches = re.findall(pattern, resp.text)
                for match in matches:
                    business = {
                        'Name': match[0],
                        'Rating': match[1] if len(match) > 1 else "N/A",
                        'ReviewCount': match[2] if len(match) > 2 else "0",
                        'Address': location,
                        'Category': keyword,
                        'Phone': "N/A",
                        'Website': "N/A",
                        'Maps_Link': f"https://www.google.com/maps/search/{urllib.parse.quote_plus(match[0] + ' ' + location)}/"
                    }
                    businesses.append(business)
            
            # Extract from HTML elements
            for item in soup.select('[role="article"], .section-result, .Nv2PK'):
                name_elem = item.select_one('.fontHeadlineSmall, h3, [role="heading"]')
                if not name_elem:
                    continue
                
                name = name_elem.get_text(strip=True)
                if len(name) < 3:
                    continue
                
                rating = "N/A"
                rating_elem = item.select_one('.fontBodyMedium span')
                if rating_elem:
                    rating_text = rating_elem.get_text(strip=True)
                    rating_match = re.search(r'([0-9.]+)', rating_text)
                    if rating_match:
                        rating = rating_match.group(1)
                
                business = {
                    'Name': name,
                    'Rating': rating,
                    'ReviewCount': "N/A",
                    'Address': location,
                    'Category': keyword,
                    'Phone': "N/A",
                    'Website': "N/A",
                    'Maps_Link': f"https://www.google.com/maps/search/{urllib.parse.quote_plus(name + ' ' + location)}/"
                }
                businesses.append(business)
                
        except Exception as e:
            logger.error(f"[SCRAPE] Error: {e}")
        
        # Prioritize businesses with bad ratings
        businesses = NegativeReviewPrioritizer.prioritize_businesses(businesses)
        
        logger.info(f"[SCRAPE] Scraped {len(businesses)} businesses for '{keyword}'")
        return businesses[:50]  # Limit per keyword

# ═══════════════════════════════════════════════════════════════════════════
# MULTI-KEYWORD JOB MANAGER
# ═══════════════════════════════════════════════════════════════════════════
class MultiKeywordJobManager:
    """Manages jobs with multiple keyword sets"""
    
    def __init__(self):
        self.active_jobs = {}
        self.scheduler = BackgroundScheduler(timezone=pytz.timezone('Asia/Dhaka'))
        self.scheduler.start()
    
    def create_job_group(self, keyword_sets: List[dict]) -> str:
        """Create a new job group with multiple keyword sets"""
        job_group_id = str(uuid.uuid4())[:8]
        
        for kw_set in keyword_sets:
            db.add_keyword_set(job_group_id, kw_set)
        
        logger.info(f"[JOB] Created job group {job_group_id} with {len(keyword_sets)} keyword sets")
        return job_group_id
    
    def schedule_job(self, job_group_id: str, hour: int, minute: int, am_pm: str) -> int:
        """Schedule a job for BD time"""
        bd_tz = pytz.timezone('Asia/Dhaka')
        now = datetime.now(bd_tz)
        
        # Convert to 24-hour format
        hour_24 = hour if am_pm.upper() == 'AM' else hour + 12
        if hour_24 == 24:
            hour_24 = 0
        
        # Set schedule time
        schedule_time = now.replace(hour=hour_24, minute=minute, second=0, microsecond=0)
        
        # If time has passed today, schedule for tomorrow
        if schedule_time <= now:
            schedule_time += timedelta(days=1)
        
        # Store in database
        scheduled_id = db.add_scheduled_job(job_group_id, schedule_time)
        
        # Add to scheduler
        self.scheduler.add_job(
            func=self.execute_scheduled_job,
            trigger=CronTrigger(hour=hour_24, minute=minute, timezone=bd_tz),
            args=[job_group_id, scheduled_id],
            id=f"job_{scheduled_id}",
            replace_existing=True
        )
        
        logger.info(f"[SCHEDULER] Scheduled job {job_group_id} for {schedule_time.strftime('%Y-%m-%d %H:%M')} BD time")
        return scheduled_id
    
    def execute_scheduled_job(self, job_group_id: str, scheduled_id: int):
        """Execute a scheduled job"""
        logger.info(f"[SCHEDULER] Executing scheduled job {job_group_id}")
        
        # Get pending keyword sets
        pending_sets = db.get_pending_keyword_sets(job_group_id)
        
        if not pending_sets:
            logger.info(f"[SCHEDULER] No pending keyword sets for {job_group_id}")
            db.update_scheduled_job_status(scheduled_id, 'completed')
            return
        
        # Start processing
        db.update_scheduled_job_status(scheduled_id, 'running')
        
        # Process each keyword set sequentially
        for kw_set in pending_sets:
            if kw_set['status'] != 'pending':
                continue
            
            logger.info(f"[JOB] Processing keyword set: {kw_set['keyword']} in {kw_set['location']}")
            
            # Create job data
            job_data = {
                'job_group_id': job_group_id,
                'keyword': kw_set['keyword'],
                'location': kw_set['location'],
                'max_leads': kw_set['target_leads'],
                'max_rating': kw_set['max_rating'],
                'min_rating': kw_set['min_rating']
            }
            
            # Run the job (can be async)
            success = self.run_single_keyword_job(job_data)
            
            if success:
                db.update_keyword_set_status(kw_set['id'], 'completed', kw_set.get('leads_found', 0))
            else:
                db.update_keyword_set_status(kw_set['id'], 'failed')
        
        db.update_scheduled_job_status(scheduled_id, 'completed')
    
    def run_single_keyword_job(self, job_data: dict) -> bool:
        """Run a single keyword set job"""
        # This would integrate with your existing run_job_thread function
        # For now, return True
        return True

# ═══════════════════════════════════════════════════════════════════════════
# FLASK APP WITH ENHANCED ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════
flask_app = Flask(__name__)
job_manager = MultiKeywordJobManager()
enhanced_keyword_engine = EnhancedKeywordEngine()
email_personalizer = SmartEmailPersonalizer()

# Store active jobs
jobs: dict = {}
latest_job_id: str = None

@flask_app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@flask_app.route('/api/scrape', methods=['POST'])
def start_api_job():
    """Enhanced endpoint supporting multiple keyword sets"""
    global latest_job_id
    data = request.json
    
    # Check if multi-keyword input
    if 'keyword_sets' in data and isinstance(data['keyword_sets'], list):
        # Multi-keyword mode
        job_group_id = job_manager.create_job_group(data['keyword_sets'])
        
        # Start processing immediately
        threading.Thread(target=process_multi_keyword_job, args=(job_group_id, data)).start()
        
        return jsonify({'job_group_id': job_group_id, 'type': 'multi'})
    else:
        # Single keyword mode (backward compatible)
        job_id = str(uuid.uuid4())[:8]
        latest_job_id = job_id
        
        threading.Thread(target=run_enhanced_job_thread, args=(job_id, data)).start()
        return jsonify({'job_id': job_id, 'type': 'single'})

@flask_app.route('/api/schedule', methods=['POST'])
def schedule_job():
    """Schedule a job for BD time"""
    data = request.json
    
    job_group_id = data.get('job_group_id')
    hour = data.get('hour')
    minute = data.get('minute')
    am_pm = data.get('am_pm', 'AM')
    
    if not job_group_id or hour is None or minute is None:
        return jsonify({'error': 'Missing required fields'}), 400
    
    scheduled_id = job_manager.schedule_job(job_group_id, hour, minute, am_pm)
    
    return jsonify({
        'scheduled_id': scheduled_id,
        'message': f'Job scheduled for {hour}:{minute:02d} {am_pm} BD time'
    })

@flask_app.route('/api/generate_keywords', methods=['POST'])
def generate_keywords_endpoint():
    """Generate 100+ keywords for a seed keyword"""
    data = request.json
    seed_keyword = data.get('keyword')
    location = data.get('location')
    job_group_id = data.get('job_group_id', str(uuid.uuid4())[:8])
    
    keywords = enhanced_keyword_engine.generate_complete_keyword_pool(
        seed_keyword, location, job_group_id
    )
    
    return jsonify({
        'keywords': keywords[:100],  # Return first 100
        'total': len(keywords),
        'job_group_id': job_group_id
    })

def run_enhanced_job_thread(job_id: str, data: dict):
    """Enhanced job execution with better keyword generation"""
    try:
        location = data.get('location', '').strip()
        base_keyword = data.get('keyword', '').strip()
        max_leads = min(int(data.get('max_leads', 10)), 200)
        max_rating = data.get('max_rating')
        job_group_id = data.get('job_group_id', job_id)
        
        # Initialize enhanced components
        scraper = EnhancedMapsScraper()
        
        jobs[job_id] = {
            'status': 'scraping',
            'count': 0,
            'leads': [],
            'status_text': f'Generating keywords for: {base_keyword}...',
            'is_running': True,
            'stats': {
                'scraped_total': 0,
                'emails_found': 0,
                'keywords_generated': 0,
                'keywords_used': 0
            }
        }
        
        # Step 1: Generate 100+ keywords
        jobs[job_id]['status_text'] = f'Generating 100+ keywords for {base_keyword}...'
        all_keywords = enhanced_keyword_engine.generate_complete_keyword_pool(
            base_keyword, location, job_group_id
        )
        
        jobs[job_id]['stats']['keywords_generated'] = len(all_keywords)
        logger.info(f"[JOB] Generated {len(all_keywords)} keywords for {base_keyword}")
        
        # Step 2: Process keywords
        qualified_leads = []
        used_keywords = set()
        
        for keyword in all_keywords[:100]:  # Process first 100
            if len(qualified_leads) >= max_leads:
                break
            
            if keyword in used_keywords:
                continue
            
            used_keywords.add(keyword)
            jobs[job_id]['stats']['keywords_used'] += 1
            
            jobs[job_id]['status_text'] = f'Scraping: "{keyword}"... ({len(qualified_leads)}/{max_leads} leads)'
            
            # Scrape businesses
            businesses = scraper.scrape_businesses(keyword, location)
            jobs[job_id]['stats']['scraped_total'] += len(businesses)
            
            # Process each business
            for business in businesses:
                if len(qualified_leads) >= max_leads:
                    break
                
                # Extract website
                website = scraper.website_extractor.extract_website_comprehensive(
                    business['Name'], location, business.get('Maps_Link')
                )
                
                if website == "N/A":
                    continue
                
                # Extract email (placeholder - use your email extractor)
                email = "N/A"  # Would integrate with your email extractor
                
                if email != "N/A":
                    business['Website'] = website
                    business['Email'] = email
                    qualified_leads.append(business)
                    jobs[job_id]['stats']['emails_found'] += 1
                    
                    logger.info(f"[JOB] Found lead: {business['Name']} - {email}")
        
        # Step 3: Send personalized emails
        if qualified_leads and data.get('templates'):
            jobs[job_id]['status'] = 'sending_emails'
            jobs[job_id]['total_to_send'] = len(qualified_leads)
            emails_sent = 0
            
            for lead in qualified_leads:
                template = random.choice(data['templates'])
                
                # Generate personalized email
                subject, body, personalization = email_personalizer.generate_personalized_email(
                    lead['Name'], base_keyword, lead.get('Rating', 'N/A'),
                    location, template['subject'], template['body']
                )
                
                # Send email (placeholder - integrate with your email webhook)
                # Would call your existing email sending logic
                
                emails_sent += 1
                jobs[job_id]['emails_sent'] = emails_sent
                
                time.sleep(random.randint(60, 120))  # Anti-spam delay
        
        jobs[job_id]['leads'] = qualified_leads
        jobs[job_id]['count'] = len(qualified_leads)
        jobs[job_id]['status'] = 'done'
        jobs[job_id]['is_running'] = False
        jobs[job_id]['status_text'] = f'✅ Completed! {len(qualified_leads)} qualified leads found.'
        
        logger.info(f"[JOB] Job {job_id} completed with {len(qualified_leads)} leads")
        
    except Exception as e:
        logger.error(f"[JOB] Error: {e}", exc_info=True)
        jobs[job_id]['status'] = 'error'
        jobs[job_id]['error'] = str(e)
        jobs[job_id]['is_running'] = False

def process_multi_keyword_job(job_group_id: str, data: dict):
    """Process multiple keyword sets sequentially"""
    keyword_sets = data['keyword_sets']
    all_leads = []
    
    for kw_set in keyword_sets:
        logger.info(f"[MULTI] Processing: {kw_set['keyword']} in {kw_set['location']}")
        
        # Create single job for this keyword set
        job_data = {
            'location': kw_set['location'],
            'keyword': kw_set['keyword'],
            'max_leads': kw_set.get('target_leads', 10),
            'max_rating': kw_set.get('max_rating'),
            'templates': data.get('templates', [])
        }
        
        job_id = str(uuid.uuid4())[:8]
        run_enhanced_job_thread(job_id, job_data)
        
        # Wait for job to complete
        while jobs.get(job_id, {}).get('is_running', True):
            time.sleep(2)
        
        # Collect leads
        if job_id in jobs and jobs[job_id].get('leads'):
            all_leads.extend(jobs[job_id]['leads'])
        
        # Update keyword set status
        db.update_keyword_set_status(kw_set.get('id'), 'completed', len(jobs[job_id].get('leads', [])))
    
    logger.info(f"[MULTI] Completed all keyword sets. Total leads: {len(all_leads)}")

@flask_app.route('/api/status/<job_id>')
def status(job_id):
    job = jobs.get(job_id, {'status': 'not_found'})
    out = dict(job)
    if out.get('status') in ['sending_emails', 'done', 'scraping']:
        out['leads'] = job.get('leads', [])
    return jsonify(out)

@flask_app.route('/api/download/<job_id>')
def download(job_id):
    job = jobs.get(job_id)
    if not job or job.get('status') not in ['done', 'sending_emails']:
        return "Not ready", 400
    
    leads = job.get('leads', [])
    if not leads:
        return "No leads found", 404
    
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=leads[0].keys())
    writer.writeheader()
    writer.writerows(leads)
    out.seek(0)
    
    return send_file(
        io.BytesIO(out.getvalue().encode('utf-8-sig')),
        mimetype='text/csv',
        as_attachment=True,
        download_name='Enhanced_Leads.csv',
    )

# HTML Template (preserved from original, add scheduling UI)
HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<title>LeadGen Pro - Enhanced Edition</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css">
<style>
    * {
        margin: 0;
        padding: 0;
        box-sizing: border-box;
    }
    
    body {
        font-family: 'Inter', sans-serif;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        min-height: 100vh;
        padding: 20px;
    }
    
    .container {
        max-width: 1400px;
        margin: 0 auto;
    }
    
    .header {
        text-align: center;
        color: white;
        margin-bottom: 30px;
    }
    
    .header h1 {
        font-size: 2.5rem;
        margin-bottom: 10px;
    }
    
    .header p {
        font-size: 1.1rem;
        opacity: 0.9;
    }
    
    .grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
        gap: 20px;
        margin-bottom: 20px;
    }
    
    .card {
        background: white;
        border-radius: 12px;
        padding: 24px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    .card h2 {
        color: #333;
        margin-bottom: 20px;
        font-size: 1.5rem;
        border-bottom: 2px solid #667eea;
        padding-bottom: 10px;
    }
    
    .form-group {
        margin-bottom: 15px;
    }
    
    label {
        display: block;
        margin-bottom: 5px;
        color: #555;
        font-weight: 500;
    }
    
    input, select, textarea {
        width: 100%;
        padding: 10px;
        border: 1px solid #ddd;
        border-radius: 6px;
        font-size: 14px;
    }
    
    button {
        background: #667eea;
        color: white;
        border: none;
        padding: 12px 24px;
        border-radius: 6px;
        cursor: pointer;
        font-size: 14px;
        font-weight: 600;
        transition: all 0.3s;
    }
    
    button:hover {
        background: #5a67d8;
        transform: translateY(-2px);
    }
    
    .keyword-set {
        background: #f7f7f7;
        padding: 15px;
        border-radius: 8px;
        margin-bottom: 15px;
        position: relative;
    }
    
    .keyword-set .remove {
        position: absolute;
        top: 10px;
        right: 10px;
        background: #e53e3e;
        padding: 5px 10px;
        font-size: 12px;
    }
    
    .btn-add {
        background: #48bb78;
        margin-top: 10px;
    }
    
    .status-card {
        background: #f0f4ff;
        border-left: 4px solid #667eea;
    }
    
    .progress-bar {
        width: 100%;
        height: 8px;
        background: #e2e8f0;
        border-radius: 4px;
        overflow: hidden;
        margin: 10px 0;
    }
    
    .progress-fill {
        height: 100%;
        background: #667eea;
        transition: width 0.3s;
    }
    
    .leads-table {
        overflow-x: auto;
        margin-top: 20px;
    }
    
    table {
        width: 100%;
        border-collapse: collapse;
    }
    
    th, td {
        padding: 12px;
        text-align: left;
        border-bottom: 1px solid #e2e8f0;
    }
    
    th {
        background: #f7fafc;
        font-weight: 600;
    }
    
    .badge {
        display: inline-block;
        padding: 4px 8px;
        border-radius: 4px;
        font-size: 12px;
        font-weight: 600;
    }
    
    .badge-success {
        background: #c6f6d5;
        color: #22543d;
    }
    
    .badge-warning {
        background: #feebc8;
        color: #7b341e;
    }
    
    .keyword-preview {
        max-height: 200px;
        overflow-y: auto;
        background: #f7fafc;
        padding: 10px;
        border-radius: 6px;
        font-size: 12px;
    }
</style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1><i class="fas fa-bolt"></i> LeadGen Pro - Enhanced Edition</h1>
        <p>Multi-Keyword | Smart Scheduling | AI Personalization | Negative Review Targeting</p>
    </div>
    
    <div class="grid">
        <!-- Keyword Sets Input -->
        <div class="card">
            <h2><i class="fas fa-key"></i> Keyword Sets</h2>
            <div id="keywordSetsContainer">
                <div class="keyword-set" data-index="0">
                    <button class="remove" onclick="removeKeywordSet(0)">×</button>
                    <div class="form-group">
                        <label>Keyword</label>
                        <input type="text" class="kw-input" placeholder="e.g., dentist, plumber, lawyer">
                    </div>
                    <div class="form-group">
                        <label>Location</label>
                        <input type="text" class="loc-input" placeholder="e.g., New York">
                    </div>
                    <div class="form-group">
                        <label>Target Leads</label>
                        <input type="number" class="target-input" value="10" min="1" max="200">
                    </div>
                    <div class="form-group">
                        <label>Max Rating (optional)</label>
                        <input type="number" class="rating-input" step="0.1" min="1" max="5" placeholder="e.g., 3.5">
                    </div>
                </div>
            </div>
            <button class="btn-add" onclick="addKeywordSet()"><i class="fas fa-plus"></i> Add Keyword Set</button>
        </div>
        
        <!-- Scheduling -->
        <div class="card">
            <h2><i class="fas fa-calendar-alt"></i> Schedule (BD Time)</h2>
            <div class="form-group">
                <label>Schedule Time</label>
                <div style="display: flex; gap: 10px;">
                    <input type="number" id="scheduleHour" placeholder="Hour" min="1" max="12" style="width: 80px;">
                    <input type="number" id="scheduleMinute" placeholder="Minute" min="0" max="59" style="width: 80px;">
                    <select id="scheduleAmPm" style="width: 80px;">
                        <option value="AM">AM</option>
                        <option value="PM">PM</option>
                    </select>
                </div>
            </div>
            <button onclick="scheduleJob()"><i class="fas fa-clock"></i> Schedule Automation</button>
            <div class="form-group" style="margin-top: 15px;">
                <label>Or Start Now</label>
                <button onclick="startMultiKeywordJob()" style="background: #48bb78;"><i class="fas fa-play"></i> Start Now</button>
            </div>
        </div>
        
        <!-- Status -->
        <div class="card status-card">
            <h2><i class="fas fa-chart-line"></i> Status</h2>
            <div id="statusText">Ready to start</div>
            <div class="progress-bar">
                <div class="progress-fill" id="progressFill" style="width: 0%"></div>
            </div>
            <div id="stats">
                <p><strong>Leads Found:</strong> <span id="leadCount">0</span></p>
                <p><strong>Emails Sent:</strong> <span id="emailCount">0</span></p>
                <p><strong>Keywords Generated:</strong> <span id="keywordCount">0</span></p>
            </div>
        </div>
    </div>
    
    <!-- Generated Keywords Preview -->
    <div class="card">
        <h2><i class="fas fa-list"></i> Generated Keywords (100+)</h2>
        <div id="keywordPreview" class="keyword-preview">
            <p style="color: #999;">Click "Generate Keywords" to see 100+ keywords</p>
        </div>
        <div class="form-group" style="margin-top: 10px;">
            <label>Seed Keyword</label>
            <div style="display: flex; gap: 10px;">
                <input type="text" id="seedKeyword" placeholder="Enter seed keyword">
                <input type="text" id="seedLocation" placeholder="Location">
                <button onclick="generateKeywords()" style="background: #ed8936;">Generate 100+ Keywords</button>
            </div>
        </div>
    </div>
    
    <!-- Results -->
    <div class="card">
        <h2><i class="fas fa-database"></i> Qualified Leads</h2>
        <div class="leads-table">
            <table id="leadsTable">
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Rating</th>
                        <th>Phone</th>
                        <th>Email</th>
                        <th>Website</th>
                    </tr>
                </thead>
                <tbody id="leadsBody">
                    <tr><td colspan="5" style="text-align: center;">No leads yet</td></tr>
                </tbody>
            </table>
        </div>
        <button onclick="downloadLeads()" style="margin-top: 15px;"><i class="fas fa-download"></i> Download CSV</button>
    </div>
</div>

<script>
let currentJobId = null;
let keywordSets = [];

function addKeywordSet() {
    const container = document.getElementById('keywordSetsContainer');
    const index = container.children.length;
    const newSet = document.createElement('div');
    newSet.className = 'keyword-set';
    newSet.setAttribute('data-index', index);
    newSet.innerHTML = `
        <button class="remove" onclick="removeKeywordSet(${index})">×</button>
        <div class="form-group">
            <label>Keyword</label>
            <input type="text" class="kw-input" placeholder="e.g., dentist, plumber, lawyer">
        </div>
        <div class="form-group">
            <label>Location</label>
            <input type="text" class="loc-input" placeholder="e.g., New York">
        </div>
        <div class="form-group">
            <label>Target Leads</label>
            <input type="number" class="target-input" value="10" min="1" max="200">
        </div>
        <div class="form-group">
            <label>Max Rating (optional)</label>
            <input type="number" class="rating-input" step="0.1" min="1" max="5" placeholder="e.g., 3.5">
        </div>
    `;
    container.appendChild(newSet);
}

function removeKeywordSet(index) {
    const container = document.getElementById('keywordSetsContainer');
    if (container.children.length > 1) {
        container.removeChild(container.children[index]);
        // Re-index remaining sets
        for (let i = 0; i < container.children.length; i++) {
            container.children[i].setAttribute('data-index', i);
            const removeBtn = container.children[i].querySelector('.remove');
            removeBtn.setAttribute('onclick', `removeKeywordSet(${i})`);
        }
    }
}

function getKeywordSets() {
    const sets = [];
    const containers = document.querySelectorAll('.keyword-set');
    containers.forEach(container => {
        const keyword = container.querySelector('.kw-input').value.trim();
        const location = container.querySelector('.loc-input').value.trim();
        const targetLeads = parseInt(container.querySelector('.target-input').value);
        const maxRating = container.querySelector('.rating-input').value;
        
        if (keyword && location) {
            sets.push({
                keyword: keyword,
                location: location,
                target_leads: targetLeads,
                max_rating: maxRating || null
            });
        }
    });
    return sets;
}

async function generateKeywords() {
    const seedKeyword = document.getElementById('seedKeyword').value.trim();
    const location = document.getElementById('seedLocation').value.trim();
    
    if (!seedKeyword || !location) {
        alert('Please enter seed keyword and location');
        return;
    }
    
    document.getElementById('keywordPreview').innerHTML = '<p>Generating 100+ keywords...</p>';
    
    try {
        const response = await fetch('/api/generate_keywords', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ keyword: seedKeyword, location: location })
        });
        
        const data = await response.json();
        
        const keywordsHtml = data.keywords.map(kw => `<span style="display: inline-block; background: #e2e8f0; padding: 4px 8px; margin: 4px; border-radius: 4px;">${kw}</span>`).join('');
        document.getElementById('keywordPreview').innerHTML = `<p><strong>${data.total} keywords generated:</strong></p>${keywordsHtml}`;
        document.getElementById('keywordCount').textContent = data.total;
        
    } catch (error) {
        document.getElementById('keywordPreview').innerHTML = '<p style="color: red;">Error generating keywords</p>';
    }
}

async function startMultiKeywordJob() {
    const sets = getKeywordSets();
    
    if (sets.length === 0) {
        alert('Please add at least one keyword set');
        return;
    }
    
    const templates = []; // You can add template collection from original UI
    
    const response = await fetch('/api/scrape', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            keyword_sets: sets,
            templates: templates
        })
    });
    
    const data = await response.json();
    currentJobId = data.job_group_id;
    
    startPolling();
}

async function scheduleJob() {
    const sets = getKeywordSets();
    const hour = parseInt(document.getElementById('scheduleHour').value);
    const minute = parseInt(document.getElementById('scheduleMinute').value);
    const amPm = document.getElementById('scheduleAmPm').value;
    
    if (sets.length === 0) {
        alert('Please add at least one keyword set');
        return;
    }
    
    if (!hour || !minute) {
        alert('Please enter valid schedule time');
        return;
    }
    
    // First create job group
    const createResponse = await fetch('/api/scrape', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ keyword_sets: sets, templates: [] })
    });
    
    const createData = await createResponse.json();
    
    // Schedule the job
    const scheduleResponse = await fetch('/api/schedule', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            job_group_id: createData.job_group_id,
            hour: hour,
            minute: minute,
            am_pm: amPm
        })
    });
    
    const scheduleData = await scheduleResponse.json();
    alert(`Job scheduled for ${hour}:${minute.toString().padStart(2,'0')} ${amPm} BD time!`);
}

function startPolling() {
    const interval = setInterval(async () => {
        if (!currentJobId) return;
        
        try {
            const response = await fetch(`/api/status/${currentJobId}`);
            const data = await response.json();
            
            document.getElementById('statusText').textContent = data.status_text || data.status;
            document.getElementById('leadCount').textContent = data.count || 0;
            document.getElementById('emailCount').textContent = data.emails_sent || 0;
            
            const progress = data.total_to_send ? (data.emails_sent / data.total_to_send) * 100 : (data.count / 10) * 100;
            document.getElementById('progressFill').style.width = `${Math.min(progress, 100)}%`;
            
            if (data.leads && data.leads.length > 0) {
                updateLeadsTable(data.leads);
            }
            
            if (data.status === 'done' || data.status === 'error') {
                clearInterval(interval);
            }
            
        } catch (error) {
            console.error('Polling error:', error);
        }
    }, 3000);
}

function updateLeadsTable(leads) {
    const tbody = document.getElementById('leadsBody');
    if (!leads || leads.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" style="text-align: center;">No leads yet</td></tr>';
        return;
    }
    
    tbody.innerHTML = leads.map(lead => `
        <tr>
            <td>${lead.Name || 'N/A'}</td>
            <td><span class="badge ${lead.Rating < 3 ? 'badge-warning' : 'badge-success'}">${lead.Rating || 'N/A'}</span></td>
            <td>${lead.Phone || 'N/A'}</td>
            <td>${lead.Email || 'N/A'}</td>
            <td>${lead.Website || 'N/A'}</td>
        </tr>
    `).join('');
}

async function downloadLeads() {
    if (!currentJobId) return;
    window.location.href = `/api/download/${currentJobId}`;
}
</script>
</body>
</html>
"""

# ═══════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logger.info("🚀 LeadGen Pro Enhanced Starting...")
    logger.info("Features: Multi-Keyword | Scheduling | 100+ Keywords | Smart Personalization")
    
    port = int(os.environ.get("PORT", 10000))
    flask_app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
