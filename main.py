import os
import csv
import asyncio
import re
import tempfile
import urllib.parse
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler, CallbackQueryHandler
)
from playwright.async_api import async_playwright

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

LOCATION, KEYWORD, CONFIRM = range(3)
user_data_store = {}

# ══════════════════════════════════════════════
#   EMAIL EXTRACTOR
# ══════════════════════════════════════════════

EMAIL_REGEX = r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'

SKIP_PATTERNS = [
    '@2x', '@3x', 'example.com', 'sentry.io', 'yourdomain',
    'domain.com', 'wixpress', 'squarespace', 'amazonaws',
    'cloudfront', '@schema', 'noreply', 'no-reply'
]

def clean_emails(raw_list):
    cleaned = []
    for e in raw_list:
        e = e.lower().strip()
        if any(skip in e for skip in SKIP_PATTERNS):
            continue
        if len(e) > 6 and '.' in e.split('@')[-1]:
            cleaned.append(e)
    return list(dict.fromkeys(cleaned))


async def extract_email_from_website(page, website_url: str) -> str:
    if not website_url or website_url == "N/A":
        return "N/A"

    try:
        # ── Layer 1: Homepage HTML থেকে email খোঁজো ──
        await page.goto(website_url, wait_until="domcontentloaded", timeout=15000)
        await asyncio.sleep(1.5)

        html = await page.content()
        found = clean_emails(re.findall(EMAIL_REGEX, html))
        if found:
            return found[0]

        # ── Layer 2: mailto: link থেকে খোঁজো ──
        mailto_links = await page.locator('a[href^="mailto:"]').all()
        for link in mailto_links[:5]:
            href = await link.get_attribute('href') or ''
            email = href.replace('mailto:', '').split('?')[0].strip()
            cleaned = clean_emails([email])
            if cleaned:
                return cleaned[0]

        # ── Layer 3: Contact / About page এ গিয়ে খোঁজো ──
        parsed = urllib.parse.urlparse(website_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        contact_paths = [
            '/contact', '/contact-us', '/about', '/about-us',
            '/reach-us', '/get-in-touch', '/contactus'
        ]

        for path in contact_paths:
            try:
                contact_url = base_url + path
                resp = await page.goto(
                    contact_url, wait_until="domcontentloaded", timeout=10000
                )
                if resp and resp.status == 200:
                    await asyncio.sleep(1)

                    # mailto: links আগে চেক করো
                    mailto_links = await page.locator('a[href^="mailto:"]').all()
                    for link in mailto_links[:5]:
                        href = await link.get_attribute('href') or ''
                        email = href.replace('mailto:', '').split('?')[0].strip()
                        cleaned = clean_emails([email])
                        if cleaned:
                            return cleaned[0]

                    # তারপর HTML থেকে
                    html = await page.content()
                    found = clean_emails(re.findall(EMAIL_REGEX, html))
                    if found:
                        return found[0]
            except Exception:
                continue

    except Exception:
        pass

    return "N/A"


# ══════════════════════════════════════════════
#   GOOGLE MAPS SCRAPER
# ══════════════════════════════════════════════

async def scrape_google_maps(location: str, keyword: str, max_results: int = 50) -> list:
    results = []
    search_query = f"{keyword} in {location}"

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )

        # ── Main tab: Google Maps ──
        maps_page = await context.new_page()
        url = f"https://www.google.com/maps/search/{search_query.replace(' ', '+')}"
        await maps_page.goto(url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(3)

        # Scroll করে সব result load করো
        for _ in range(12):
            try:
                feed = maps_page.locator('[role="feed"]')
                await feed.evaluate("el => el.scrollTop += 1500")
                await asyncio.sleep(1.5)
            except Exception:
                break

        listings = await maps_page.locator(
            '[role="feed"] > div > div[jsaction]'
        ).all()

        # ── Email এর জন্য আলাদা tab ──
        email_page = await context.new_page()

        for listing in listings[:max_results]:
            try:
                data = {
                    'name': 'N/A', 'phone': 'N/A', 'address': 'N/A',
                    'category': 'N/A', 'rating': 'N/A', 'reviews': 'N/A',
                    'website': 'N/A', 'email': 'N/A', 'maps_link': 'N/A'
                }

                # Business Name
                name_el = listing.locator(
                    '.qBF1Pd, .fontHeadlineSmall'
                ).first
                if await name_el.count() > 0:
                    data['name'] = (await name_el.inner_text()).strip()

                # Rating
                rating_el = listing.locator('.MW4etd').first
                if await rating_el.count() > 0:
                    data['rating'] = (await rating_el.inner_text()).strip()

                # Reviews
                review_el = listing.locator('.UY7F9').first
                if await review_el.count() > 0:
                    txt = (await review_el.inner_text()).strip()
                    data['reviews'] = re.sub(r'[()،,]', '', txt).strip()

                # Category + Address
                info_els = await listing.locator(
                    '.W4Efsd span:not([aria-hidden])'
                ).all()
                info_texts = []
                for el in info_els:
                    txt = (await el.inner_text()).strip()
                    if txt and txt not in ['·', '']:
                        info_texts.append(txt)
                if info_texts:
                    data['category'] = info_texts[0]
                if len(info_texts) > 1:
                    data['address'] = info_texts[1]

                # Detail page: phone + website + maps link
                try:
                    await listing.click()
                    await asyncio.sleep(2.5)

                    # Phone
                    phone_el = maps_page.locator(
                        'button[aria-label*="phone"], a[href^="tel:"]'
                    ).first
                    if await phone_el.count() > 0:
                        href = await phone_el.get_attribute('href') or ''
                        if href.startswith('tel:'):
                            data['phone'] = href.replace('tel:', '').strip()
                        else:
                            aria = await phone_el.get_attribute('aria-label') or ''
                            data['phone'] = aria.replace('Phone:', '').strip()

                    # Website
                    web_el = maps_page.locator(
                        'a[data-item-id="authority"], '
                        'a[aria-label*="website"], '
                        '[data-item-id*="website"] a'
                    ).first
                    if await web_el.count() > 0:
                        data['website'] = await web_el.get_attribute('href') or 'N/A'

                    # Maps link
                    current_url = maps_page.url
                    if 'maps' in current_url:
                        data['maps_link'] = current_url

                    # Back
                    back_btn = maps_page.locator('button[aria-label="Back"]').first
                    if await back_btn.count() > 0:
                        await back_btn.click()
                        await asyncio.sleep(1.5)

                except Exception:
                    pass

                # ── Email extraction ──
                if data['website'] != 'N/A':
                    data['email'] = await extract_email_from_website(
                        email_page, data['website']
                    )

                if data['name'] != 'N/A':
                    results.append(data)

            except Exception:
                continue

        await email_page.close()
        await maps_page.close()
        await browser.close()

    return results


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
        "📌 নাম, ফোন, *ইমেইল*, ঠিকানা, রেটিং, ওয়েবসাইট\n\n"
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
        f"⏱ সময় লাগবে ৩–৮ মিনিট (email extraction এর জন্য)\n"
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
        f"🔄 Google Maps লোড হচ্ছে...\n"
        f"_(email extraction এ একটু বেশি সময় লাগবে)_",
        parse_mode='Markdown'
    )

    try:
        leads = await scrape_google_maps(loc, kw)

        if not leads:
            await context.bot.edit_message_text(
                chat_id=query.message.chat_id,
                message_id=msg.message_id,
                text="😔 কোনো result পাওয়া যায়নি।\nঅন্য keyword বা location দিয়ে চেষ্টা করো।"
            )
            return ConversationHandler.END

        # Stats বের করো
        with_email   = sum(1 for l in leads if l['email'] != 'N/A')
        with_phone   = sum(1 for l in leads if l['phone'] != 'N/A')
        with_website = sum(1 for l in leads if l['website'] != 'N/A')

        csv_path = save_to_csv(leads)
        filename = f"leads_{loc}_{kw}.csv".replace(' ', '_')

        await context.bot.edit_message_text(
            chat_id=query.message.chat_id,
            message_id=msg.message_id,
            text=f"✅ *সম্পন্ন! CSV পাঠাচ্ছি...*",
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

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help",  help_cmd))
    app.add_handler(conv)

    print("✅ Bot চালু!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
