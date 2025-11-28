# -*- coding: utf-8 -*-
import os
import logging
import asyncio
import telegram
from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    CallbackQueryHandler
)
from fuzzywuzzy import process, fuzz
import db_utils  # ✅ DATA SOURCE: Ye wahi file hai jo DB se connect karti hai

# ==================== CONFIGURATION ====================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.environ.get('DATABASE_URL')

# Force Subscribe Config (Optional)
FORCE_SUB_CHANNEL_ID = os.environ.get('FORCE_SUB_CHANNEL_ID') 
FORCE_SUB_GROUP_ID = os.environ.get('FORCE_SUB_GROUP_ID')

# Links
CHANNEL_LINK = "https://t.me/filmfybox"
GROUP_LINK = "https://t.me/Filmfybox002"

# Start Image
START_IMG_URL = os.environ.get('START_IMG_URL', 'https://blogger.googleusercontent.com/img/b/R29vZ2xl/AVvXsEhYD6_-uyyYg_YxJMkk06sbRQ5N-IH7HFjr3P1AYZLiQ6qSp3Ap_FgRWGjCKk6okFRh0bRTH5-TtrizBxsQpjxR6bdnNidTjiT-ICWhqaC0xcEJs89bSOTwrzBAMFYtWAv48llz96Ye9E3Q3vEHrtk1id8aceQbp_uxAJ4ASqZIEsK5FcaMYcrhj45i70c/s320/logo-design-for-flimfybox-a-cinematic-mo_OhkRefmbTCK6_RylGkOrAw_CtxTQGw_Tu6dY2kc64sagw.jpeg') 

# Auto Delete Timer
DELETE_TIMEOUT = 60 

# ==================== LOGGING ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== DATABASE CHECK ====================
def check_database_connection():
    """Startup par check karega ki Supabase DB connect ho raha hai ya nahi"""
    try:
        conn = db_utils.get_db_connection()
        if conn:
            logger.info("✅ SUCCESS: Connected to Data Source (Database)!")
            conn.close()
        else:
            logger.error("❌ ERROR: Could not connect to Data Source. Check DATABASE_URL.")
    except Exception as e:
        logger.error(f"❌ DATABASE ERROR: {e}")

# ==================== HELPER FUNCTIONS ====================

async def delete_after_delay(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay: int = DELETE_TIMEOUT):
    """Message ko 60 second baad delete kar dega"""
    try:
        await asyncio.sleep(delay)
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info(f"🗑️ Auto-deleted message {message_id} in {chat_id}")
    except Exception as e:
        pass

async def check_membership(user_id, context):
    """Force Subscribe Check"""
    if not FORCE_SUB_CHANNEL_ID and not FORCE_SUB_GROUP_ID:
        return True
    try:
        if FORCE_SUB_CHANNEL_ID:
            chat_member = await context.bot.get_chat_member(chat_id=FORCE_SUB_CHANNEL_ID, user_id=user_id)
            if chat_member.status in ['left', 'kicked', 'restricted']: return False
        if FORCE_SUB_GROUP_ID:
            group_member = await context.bot.get_chat_member(chat_id=FORCE_SUB_GROUP_ID, user_id=user_id)
            if group_member.status in ['left', 'kicked', 'restricted']: return False
        return True
    except:
        return True

def get_fsub_keyboard():
    keyboard = [
        [InlineKeyboardButton("📢 Join Channel", url=CHANNEL_LINK),
         InlineKeyboardButton("👥 Join Group", url=GROUP_LINK)],
        [InlineKeyboardButton("🔄 Try Again", callback_data="check_fsub")]
    ]
    return InlineKeyboardMarkup(keyboard)

async def send_file_to_user(update, context, movie_id):
    """Private chat me file bhejne ka function"""
    conn = db_utils.get_db_connection()
    if not conn:
        msg = await update.message.reply_text("❌ Database Connection Error.")
        asyncio.create_task(delete_after_delay(context, update.effective_chat.id, msg.message_id, 10))
        return

    try:
        movie = db_utils.get_movie_by_id(conn, movie_id)
        if not movie:
            msg = await update.message.reply_text("❌ Movie not found.")
            asyncio.create_task(delete_after_delay(context, update.effective_chat.id, msg.message_id, 10))
            return

        title = movie['title']
        # Data Source se File ID ya URL nikalo
        file_id = movie.get('file_id') or movie.get('url')

        caption_text = (
            f"🎬 <b>{title}</b>\n\n"
            f"🔗 <b>JOIN »</b> <a href='{CHANNEL_LINK}'>FilmfyBox</a>\n\n"
            "🔹 <b>Please drop the movie name, and I’ll find it for you as soon as possible. 🎬✨👇</b>\n"
            f"🔹 <b><a href='{GROUP_LINK}'>FlimfyBox Chat</a></b>"
        )
        join_btn = InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Join Channel", url=CHANNEL_LINK)]])

        # Warning Message
        warning_msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⚠️ ❌👉This file automatically❗️delete after 1 minute❗️so please forward in another chat👈❌",
            parse_mode='Markdown'
        )

        sent_msg = None
        # Try sending as Document -> Video -> Text Link
        if file_id:
            try:
                sent_msg = await context.bot.send_document(chat_id=update.effective_chat.id, document=file_id, caption=caption_text, parse_mode='HTML', reply_markup=join_btn)
            except:
                try:
                    sent_msg = await context.bot.send_video(chat_id=update.effective_chat.id, video=file_id, caption=caption_text, parse_mode='HTML', reply_markup=join_btn)
                except:
                    sent_msg = await context.bot.send_message(chat_id=update.effective_chat.id, text=f"🎬 <b>{title}</b>\n\n🔗 Link: {file_id}\n\n{caption_text}", parse_mode='HTML', reply_markup=join_btn)
        else:
            msg = await update.message.reply_text("❌ File ID missing in database.")
            asyncio.create_task(delete_after_delay(context, update.effective_chat.id, msg.message_id))

        # Auto Delete Logic
        if sent_msg:
            asyncio.create_task(delete_after_delay(context, update.effective_chat.id, sent_msg.message_id))
            asyncio.create_task(delete_after_delay(context, update.effective_chat.id, warning_msg.message_id))

    except Exception as e:
        logger.error(f"Send File Error: {e}")
    finally:
        conn.close()

# ==================== HANDLERS ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command / Deep Link handler"""
    user = update.effective_user
    args = context.args

    # --- DEEP LINK (File Delivery) ---
    if args and args[0].startswith('movie_'):
        movie_id = int(args[0].split('_')[1])
        if not await check_membership(user.id, context):
            msg = await update.message.reply_text("⚠️ **Access Denied!** Join Channel first.", reply_markup=get_fsub_keyboard(), parse_mode='Markdown')
            asyncio.create_task(delete_after_delay(context, update.effective_chat.id, msg.message_id))
            return
        await send_file_to_user(update, context, movie_id)
        return

    # --- NORMAL START UI ---
    bot_username = context.bot.username
    add_group_url = f"https://t.me/{bot_username}?startgroup=true"
    text = f"HEY {user.mention_markdown()}..👋\n\nIM ⚡ **POWERFUL AUTO-FILTER BOT...**\n😎 YOU CAN USE ME AS A AUTO-FILTER IN YOUR GROUP ....\n\n©️ MAINTAINED BY: FILMFYBOX"
    
    keyboard = [
        [InlineKeyboardButton("➕ Add Me To Your Groups ➕", url=add_group_url)],
        [InlineKeyboardButton("↗️ CHANNEL", url=CHANNEL_LINK), InlineKeyboardButton("👥 GROUP", url=GROUP_LINK)],
        [InlineKeyboardButton("ℹ️ HELP", callback_data="help"), InlineKeyboardButton("😊 ABOUT", callback_data="about")]
    ]

    try:
        sent_msg = await update.message.reply_photo(photo=START_IMG_URL, caption=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        asyncio.create_task(delete_after_delay(context, update.effective_chat.id, sent_msg.message_id))
    except:
        sent_msg = await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        asyncio.create_task(delete_after_delay(context, update.effective_chat.id, sent_msg.message_id))

async def group_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    CORE LOGIC:
    1. Har message par DB check karo.
    2. Agar movie hai -> Button do.
    3. Agar movie nahi hai -> Chup raho (Return).
    """
    # Basic Checks: Text hona chahiye, command nahi honi chahiye
    if not update.message or not update.message.text: return
    if update.message.text.startswith('/'): return

    query = update.message.text.strip()
    if len(query) < 3: return

    try:
        # ✅ DATA SOURCE CHECK
        conn = db_utils.get_db_connection()
        if not conn: return # DB nahi connect hua to chup raho
        
        cur = conn.cursor()
        
        # 1. Exact Match Check
        cur.execute("SELECT id, title FROM movies WHERE LOWER(title) = LOWER(%s) LIMIT 1", (query,))
        exact_match = cur.fetchone()
        
        movie_data = None
        if exact_match:
            movie_data = exact_match
        else:
            # 2. Fuzzy Match Check
            cur.execute("SELECT id, title FROM movies")
            all_movies = cur.fetchall()
            movie_dict = {m[1]: m[0] for m in all_movies}
            titles = list(movie_dict.keys())
            match = process.extractOne(query, titles, scorer=fuzz.token_sort_ratio)
            # 85% Match hone par hi result dena, nahi to galat movie mat dena
            if match and match[1] >= 85: 
                movie_data = (movie_dict[match[0]], match[0])

        cur.close()
        conn.close()

        # ✅ LOGIC: Result hai to do, nahi to chup raho
        if movie_data:
            movie_id, movie_title = movie_data
            bot_username = context.bot.username
            deep_link = f"https://t.me/{bot_username}?start=movie_{movie_id}"

            keyboard = [[InlineKeyboardButton("📂 Get File Here", url=deep_link)]]
            
            # Group me reply karo
            sent_msg = await update.message.reply_text(
                f"✅ **Found:** {movie_title}\n\nClick below to get the file in private 👇",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown',
                reply_to_message_id=update.message.message_id
            )
            # Button wala message bhi delete kar do clean rakhne ke liye
            asyncio.create_task(delete_after_delay(context, update.effective_chat.id, sent_msg.message_id, 120))
        
        else:
            # Agar movie nahi mili -> RETURN (Chup raho)
            return

    except Exception as e:
        logger.error(f"Group Handler Error: {e}")

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "help":
        await query.message.reply_text("Just add me to your group and send movie name!", parse_mode='Markdown')
    elif query.data == "about":
        await query.message.reply_text(f"Bot by <a href='{CHANNEL_LINK}'>FilmfyBox</a>", parse_mode='HTML', disable_web_page_preview=True)
    elif query.data == "check_fsub":
        await query.message.reply_text("Try clicking the link again!")

# ==================== MAIN ====================
app = Flask('')
@app.route('/')
def home(): return "Bot Running"

def run_flask():
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))

def main():
    # 1. Start hote hi confirm karo ki Data Source connect ho gaya
    check_database_connection()

    if not TELEGRAM_BOT_TOKEN:
        print("Error: TELEGRAM_BOT_TOKEN missing.")
        return

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(callback_handler))
    
    # ✅ GROUP HANDLER: Har text message par chalega
    application.add_handler(MessageHandler(filters.TEXT & filters.ChatType.GROUPS, group_message_handler))

    from threading import Thread
    Thread(target=run_flask).start()

    print("Bot Started...")
    application.run_polling()

if __name__ == '__main__':
    main()
