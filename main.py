import os
import csv
import asyncio
import re
import time
import tempfile
import urllib.parse
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler, CallbackQueryHandler
)

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

LOCATION, KEYWORD, CONFIRM = range(3)
user_data_store = {}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ══════════════════════════════════════════════
#   EMAIL EXTRACTOR
# ══════════════════════════════════════════════

EMAIL_REGEX = r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'

SKIP_PATTERNS = [
    '@2x', '@3x', 'example.com', 'sentry.io', 'yourdomain',
    'domain.com', 'wixpress', 'squarespace', 'amazonaws',
    'cloudfront', '@schema', 'noreply', 'no-reply',
    'placeholder', 'test.com', 'email.com'
]

def clean_emails(raw_list):
    cleaned = []
    for e in raw_list:
        e = e.lower().strip()
        if any(skip in e for skip in SKIP_PATTERNS):
            continue
        parts = e.split('@')
        if len(parts) == 2 and '.' in parts[1] and len(e) > 6:
            cleaned.append(e)
    return list(dict.fromkeys(cleaned))


def extract_email_from_url(url: str) -> str:
    if not url or url == "N/A":
        return "N/A"

    try:
        parsed = urllib.parse.urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        # Layer 1: Homepage
        r = requests.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
        soup = BeautifulSoup(r.text, 'html.parser')

        # mailto: links সবার আগে চেক
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.startswith('mailto:'):
                email = href.replace('mailto:', '').split('?')[0].strip()
                cleaned = clean_emails([email])
                if cleaned:
                    return cleaned[0]

        # HTML থেকে regex
        found = clean_emails(re.findall(EMAIL_REGEX, r.text))
        if found:
            return found[0]

        # Layer 2: Contact / About page
        contact_paths = [
            '/contact', '/contact-us', '/about', '/about-us',
            '/reach-us', '/get-in-touch', '/contactus'
        ]

        for path in contact_paths:
            try:
                contact_url = base_url + path
                r2 = requests.get(
                    contact_url, headers=HEADERS, timeout=8, allow_redirects=True
                )
                if r2.status_code == 200:
                    soup2 = BeautifulSoup(r2.text, 'html.parser')

                    for a in soup2.find_all('a', href=True):
                        href = a['href']
                        if href.startswith('mailto:'):
                            email = href.replace('mailto:', '').split('?')[0].strip()
                            cleaned = clean_emails([email])
                            if cleaned:
                                return cleaned[0]

                    found2 = clean_emails(re.findall(EMAIL_REGEX, r2.text))
                    if found2:
                        return found2[0]
            except Exception:
                continue

    except Exception:
        pass

    return "N/A"


# ══════════════════════════════════════════════
#   GOOGLE MAPS SCRAPER
# ══════════════════════════════════════════════

def scrape_google_maps(location: str, keyword: str, max_results: int = 50) -> list:
    results = []
    query = f"{keyword} {location}"
    encoded = urllib.parse.quote(query)
    url = f"https://www.google.com/maps/search/{encoded}"

    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        response = session.get(url, timeout=20)
        html = response.text

        # Business name pattern
        names = re.findall(r'"([^"]+)",\d+\.\d+,\d+\.\d+,\d+', html)

        # Phone numbers
        phones = re.findall(
            r'(\+?880[\s-]?\d{2}[\s-]?\d{8}|\+?8801[3-9]\d{8}|01[3-9]\d{8}|'
            r'\+?\d{1,3}[\s-]?\(?\d{3}\)?[\s-]?\d{3}[\s-]?\d{4})',
            html
        )

        # Addresses (after common address keywords)
        addresses = re.findall(
            r'"([^"]*(?:Road|Street|Avenue|Lane|Dhaka|Chittagong|Sylhet|'
            r'Rajshahi|Floor|Building|House|Block)[^"]*)"',
            html, re.IGNORECASE
        )

        # Ratings
        ratings = re.findall(r'"(\d\.\d)"', html)

        # Websites
        websites = re.findall(
            r'https?://(?!(?:www\.google|maps\.google|goo\.gl|'
            r'googleapis|facebook|instagram))[a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,}'
            r'(?:/[^\s"\'<>]*)?',
            html
        )
        websites = [w for w in websites if 'google' not in w.lower()]

        # Google Maps place links
        place_links = re.findall(r'(https://www\.google\.com/maps/place/[^\s"\'<>]+)', html)

        # Zip করে lead বানাও
        max_len = min(max_results, max(len(names), 5))

        for i in range(max_len):
            lead = {
                'name':      names[i]     if i < len(names)     else 'N/A',
                'phone':     phones[i]    if i < len(phones)     else 'N/A',
                'address':   addresses[i] if i < len(addresses)  else 'N/A',
                'rating':    ratings[i]   if i < len(ratings)    else 'N/A',
                'reviews':   'N/A',
                'website':   websites[i]  if i < len(websites)   else 'N/A',
                'email':     'N/A',
                'maps_link': place_links[i] if i < len(place_links) else 'N/A',
                'category':  'N/A',
            }

            # Email extract
            if lead['website'] != 'N/A':
                lead['email'] = extract_email_from_url(lead['website'])
                time.sleep(0.5)

            if lead['name'] != 'N/A':
                results.append(lead)

    except Exception as e:
        print(f"Scrape error: {e}")

    # Google Maps JavaScript data থেকেও চেষ্টা করো
    if not results:
        results = scrape_via_search_api(location, keyword, max_results)

    return results


def scrape_via_search_api(location: str, keyword: str, max_results: int) -> list:
    """Fallback: Google Search থেকে business info নাও"""
    results = []
    query = f"{keyword} {location} phone email"
    encoded = urllib.parse.quote(query)
    url = f"https://www.google.com/search?q={encoded}&num=20"

    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        response = session.get(url, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Business cards from search
        for div in soup.find_all('div', class_=['VkpGBb', 'rllt__details', 'dbg0pd']):
            try:
                name_el = div.find(['h3', 'span', 'a'])
                name = name_el.get_text(strip=True) if name_el else 'N/A'

                text = div.get_text(separator=' ', strip=True)

                phone_match = re.search(
                    r'(\+?8801[3-9]\d{8}|01[3-9]\d{8}|\+?\d[\d\s\-\(\)]{8,})',
                    text
                )
                phone = phone_match.group(1).strip() if phone_match else 'N/A'

                emails = clean_emails(re.findall(EMAIL_REGEX, text))
                email = emails[0] if emails else 'N/A'

                if name and name != 'N/A':
                    results.append({
                        'name': name, 'phone': phone,
                        'email': email, 'address': 'N/A',
                        'category': 'N/A', 'rating': 'N/A',
                        'reviews': 'N/A', 'website': 'N/A',
                        'maps_link': 'N/A'
                    })
            except Exception:
                continue

    except Exception as e:
        print(f"Search API error: {e}")

    return results[:max_results]


# ══════════════════════════════════════════════
#   CSV SAVER
# ══════════════════════════════════════════════

def save_to_csv(leads: list) -> str:
    tmp = tempfile.NamedTemporaryFile(
        mode='w', suffix='.csv', delete=False,
        encoding='utf-8-sig', newline=''
    )
    fieldnames = [
        'name', 'phone', 'email', 'address',
        'category', 'rating', 'reviews', 'website', 'maps_link'
    ]
    writer = csv.DictWriter(tmp, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(leads)
    tmp.close()
    return tmp.name


# ══════════════════════════════════════════════
#   TELEGRAM BOT
# ══════════════════════════════════════════════

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 *Google Maps Lead Generator*\n\n"
        "আমি Google Maps থেকে business leads বের করি।\n"
        "প্রতিটা lead এ থাকবে:\n"
        "📌 নাম, ফোন, ইমেইল, ঠিকানা, রেটিং, ওয়েবসাইট\n\n"
        "শুরু করতে /generate লেখো।",
        parse_mode='Markdown'
    )

async def generate_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📍 *Step 1 — Location*\n\n"
        "কোন এলাকার leads চাও?\n"
        "উদাহরণ: `Gulshan Dhaka`, `Chittagong`, `Sylhet`",
        parse_mode='Markdown'
    )
    return LOCATION

async def get_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.message.from_user.id
    user_data_store[uid] = {'location': update.message.text.strip()}
    await update.message.reply_text(
        f"✅ Location: *{user_data_store[uid]['location']}*\n\n"
        "🔍 *Step 2 — Keyword*\n\n"
        "কোন ধরনের business খুঁজবো?\n"
        "উদাহরণ: `restaurant`, `hospital`, `clothing shop`",
        parse_mode='Markdown'
    )
    return KEYWORD

async def get_keyword(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.message.from_user.id
    user_data_store[uid]['keyword'] = update.message.text.strip()
    loc = user_data_store[uid]['location']
    kw  = user_data_store[uid]['keyword']

    keyboard = [[
        InlineKeyboardButton("✅ শুরু করো", callback_data="go"),
        InlineKeyboardButton("❌ বাতিল",    callback_data="cancel")
    ]]
    await update.message.reply_text(
        f"📋 *Summary*\n\n"
        f"📍 Location : `{loc}`\n"
        f"🔍 Keyword  : `{kw}`\n\n"
        f"⏱ সময় লাগবে ২–৪ মিনিট\n"
        f"শুরু করবো?",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return CONFIRM

async def confirm_scraping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id

    if query.data == "cancel":
        await query.edit_message_text("❌ বাতিল করা হয়েছে।")
        return ConversationHandler.END

    loc = user_data_store[uid]['location']
    kw  = user_data_store[uid]['keyword']

    msg = await query.edit_message_text(
        f"⏳ *Scraping শুরু হয়েছে...*\n\n"
        f"📍 {loc} → 🔍 {kw}\n\n"
        f"🔄 Google Maps থেকে data নিচ্ছি...",
        parse_mode='Markdown'
    )

    try:
        # Async এর ভেতরে sync function চালাও
        loop = asyncio.get_event_loop()
        leads = await loop.run_in_executor(
            None, scrape_google_maps, loc, kw
        )

        if not leads:
            await context.bot.edit_message_text(
                chat_id=query.message.chat_id,
                message_id=msg.message_id,
                text=(
                    "😔 কোনো result পাওয়া যায়নি।\n"
                    "অন্য keyword বা location দিয়ে চেষ্টা করো।"
                )
            )
            return ConversationHandler.END

        with_email   = sum(1 for l in leads if l['email']   != 'N/A')
        with_phone   = sum(1 for l in leads if l['phone']   != 'N/A')
        with_website = sum(1 for l in leads if l['website'] != 'N/A')

        csv_path = save_to_csv(leads)
        filename = f"leads_{loc}_{kw}.csv".replace(' ', '_')

        await context.bot.edit_message_text(
            chat_id=query.message.chat_id,
            message_id=msg.message_id,
            text="✅ *সম্পন্ন! CSV পাঠাচ্ছি...*",
            parse_mode='Markdown'
        )

        with open(csv_path, 'rb') as f:
            await context.bot.send_document(
                chat_id=query.message.chat_id,
                document=f,
                filename=filename,
                caption=(
                    f"🎯 *{loc}* — *{kw}*\n\n"
                    f"📊 মোট leads   : *{len(leads)}*\n"
                    f"📧 Email পাওয়া : *{with_email}*\n"
                    f"📞 Phone পাওয়া : *{with_phone}*\n"
                    f"🌐 Website আছে : *{with_website}*\n\n"
                    f"নতুন search → /generate"
                ),
                parse_mode='Markdown'
            )

        os.unlink(csv_path)

    except Exception as e:
        await context.bot.edit_message_text(
            chat_id=query.message.chat_id,
            message_id=msg.message_id,
            text=f"❌ Error: `{str(e)}`\n\nআবার চেষ্টা করো।",
            parse_mode='Markdown'
        )

    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ বাতিল।")
    return ConversationHandler.END

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 *Commands*\n\n"
        "/start — শুরু\n"
        "/generate — নতুন lead search\n"
        "/cancel — বাতিল\n"
        "/help — সাহায্য",
        parse_mode='Markdown'
    )

# ══════════════════════════════════════════════
#   RUN
# ══════════════════════════════════════════════

def main():
    app = Application.builder().token(TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("generate", generate_start)],
        states={
            LOCATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_location)],
            KEYWORD:  [MessageHandler(filters.TEXT & ~filters.COMMAND, get_keyword)],
            CONFIRM:  [CallbackQueryHandler(confirm_scraping)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    app.add_handler(CommandHandler("start",  start))
    app.add_handler(CommandHandler("help",   help_cmd))
    app.add_handler(conv)

    print("✅ Bot চালু!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
