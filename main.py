import os, csv, asyncio, tempfile
from dotenv import load_dotenv
from apify_client import ApifyClient
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler, CallbackQueryHandler
)

load_dotenv()
TOKEN       = os.getenv("TELEGRAM_BOT_TOKEN")
APIFY_TOKEN = os.getenv("APIFY_API_TOKEN")
RENDER_URL  = os.getenv("RENDER_URL")          # তোমার Render URL

LOCATION, KEYWORD, CONFIRM = range(3)
store = {}

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

def to_csv(leads):
    tmp = tempfile.NamedTemporaryFile(
        mode='w', suffix='.csv', delete=False,
        encoding='utf-8-sig', newline=''
    )
    writer = csv.DictWriter(tmp, fieldnames=leads[0].keys())
    writer.writeheader()
    writer.writerows(leads)
    tmp.close()
    return tmp.name

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 *Google Maps Lead Bot*\n\nশুরু করতে /generate লেখো।",
        parse_mode='Markdown'
    )

async def gen_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📍 Location দাও:\nউদাহরণ: `Gulshan Dhaka`",
        parse_mode='Markdown'
    )
    return LOCATION

async def get_loc(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    store[update.message.from_user.id] = {'location': update.message.text.strip()}
    await update.message.reply_text(
        "🔍 Keyword দাও:\nউদাহরণ: `restaurant`",
        parse_mode='Markdown'
    )
    return KEYWORD

async def get_kw(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.message.from_user.id
    store[uid]['keyword'] = update.message.text.strip()
    loc, kw = store[uid]['location'], store[uid]['keyword']
    kb = [[InlineKeyboardButton("✅ শুরু", callback_data="go"),
           InlineKeyboardButton("❌ বাতিল", callback_data="no")]]
    await update.message.reply_text(
        f"📍 `{loc}` → 🔍 `{kw}`\n\nশুরু করবো?",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(kb)
    )
    return CONFIRM

async def confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    if q.data == "no":
        await q.edit_message_text("❌ বাতিল।")
        return ConversationHandler.END

    loc, kw = store[uid]['location'], store[uid]['keyword']
    msg = await q.edit_message_text(
        f"⏳ *Scraping চলছে...*\n📍 {loc} → 🔍 {kw}\n\n_৩–৫ মিনিট লাগতে পারে_",
        parse_mode='Markdown'
    )
    try:
        loop = asyncio.get_event_loop()
        leads = await loop.run_in_executor(None, scrape, loc, kw)
        if not leads:
            await ctx.bot.edit_message_text(
                chat_id=q.message.chat_id, message_id=msg.message_id,
                text="😔 কোনো result নেই।"
            )
            return ConversationHandler.END

        path = to_csv(leads)
        em = sum(1 for l in leads if str(l.get('email','')) not in ('N/A','','None'))
        ph = sum(1 for l in leads if str(l.get('phone','')) not in ('N/A','','None'))

        await ctx.bot.edit_message_text(
            chat_id=q.message.chat_id, message_id=msg.message_id,
            text="✅ হয়ে গেছে! পাঠাচ্ছি..."
        )
        with open(path, 'rb') as f:
            await ctx.bot.send_document(
                chat_id=q.message.chat_id, document=f,
                filename=f"leads_{loc}_{kw}.csv".replace(' ', '_'),
                caption=(
                    f"🎯 *{loc}* — *{kw}*\n"
                    f"📊 Total: *{len(leads)}* | 📧 Email: *{em}* | 📞 Phone: *{ph}*\n\n"
                    f"নতুন search → /generate"
                ),
                parse_mode='Markdown'
            )
        os.unlink(path)
    except Exception as e:
        await ctx.bot.edit_message_text(
            chat_id=q.message.chat_id, message_id=msg.message_id,
            text=f"❌ Error: `{e}`", parse_mode='Markdown'
        )
    return ConversationHandler.END

async def cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ বাতিল।")
    return ConversationHandler.END

def main():
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
    print("✅ Bot চালু!")

    # Webhook mode — polling এর বদলে
    app.run_webhook(
        listen="0.0.0.0",
        port=int(os.environ.get("PORT", 10000)),
        webhook_url=f"{RENDER_URL}/{TOKEN}",
        secret_token="mysecret123",
        url_path=TOKEN,
        drop_pending_updates=True,
    )

if __name__ == "__main__":
    main()
