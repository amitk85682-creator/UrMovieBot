# -*- coding: utf-8 -*-
import os
import asyncio
import logging
import re
import threading
import psycopg2
from flask import Flask
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatMember
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    CallbackQueryHandler
)
from telegram.constants import ParseMode
from fuzzywuzzy import fuzz

# ==================== CONFIGURATION ====================
# Environment Variables (Fill these in your deployment settings)
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.environ.get('DATABASE_URL')
ADMIN_USER_ID = int(os.environ.get('ADMIN_USER_ID', 0))

# Channel & Group Usernames/Links
CHANNEL_USERNAME = "@filmfybox"  # For checking subscription
GROUP_USERNAME = "@Filmfybox002" # For checking subscription
CHANNEL_LINK = "https://t.me/filmfybox"
GROUP_LINK = "https://t.me/Filmfybox002"
START_IMG_URL = "https://blogger.googleusercontent.com/img/b/R29vZ2xl/AVvXsEhYD6_-uyyYg_YxJMkk06sbRQ5N-IH7HFjr3P1AYZLiQ6qSp3Ap_FgRWGjCKk6okFRh0bRTH5-TtrizBxsQpjxR6bdnNidTjiT-ICWhqaC0xcEJs89bSOTwrzBAMFYtWAv48llz96Ye9E3Q3vEHrtk1id8aceQbp_uxAJ4ASqZIEsK5FcaMYcrhj45i70c/s320/logo-design-for-flimfybox-a-cinematic-mo_OhkRefmbTCK6_RylGkOrAw_CtxTQGw_Tu6dY2kc64sagw.jpeg"

# Auto Delete Time (in seconds)
AUTO_DELETE_TIME = 60 

# Logging Setup
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== DATABASE CONNECTION ====================
def get_db_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        logger.error(f"DB Connection Error: {e}")
        return None

# ==================== HELPER FUNCTIONS ====================

async def delete_message_later(context, chat_id, message_id, delay=AUTO_DELETE_TIME):
    """Deletes a message after X seconds."""
    await asyncio.sleep(delay)
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass

async def is_user_subscribed(bot, user_id):
    """Checks if user has joined both Channel and Group."""
    try:
        # Check Channel
        chat_member_ch = await bot.get_chat_member(chat_id=CHANNEL_USERNAME, user_id=user_id)
        if chat_member_ch.status not in [ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER]:
            return False
        
        # Check Group (Optional: Remove if you only want Channel check)
        chat_member_gr = await bot.get_chat_member(chat_id=GROUP_USERNAME, user_id=user_id)
        if chat_member_gr.status not in [ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER]:
            return False
            
        return True
    except Exception as e:
        logger.error(f"Subscription check error (Make sure bot is admin in channel/group): {e}")
        # If bot fails to check (e.g. not admin), allow access to avoid blocking users
        return True 

def get_movie_details(movie_id):
    """Fetch movie details and qualities from DB."""
    conn = get_db_connection()
    if not conn: return None, []
    
    try:
        cur = conn.cursor()
        # Get Basic Info
        cur.execute("SELECT title, file_id, url FROM movies WHERE id = %s", (movie_id,))
        movie = cur.fetchone()
        
        if not movie: return None, []
        
        title, main_file_id, main_url = movie
        
        # Get Qualities (Assuming you have a movie_files table or similar logic)
        # If you don't have a separate table, we just return the main file.
        qualities = []
        
        # Check for 'movie_files' table existence or structure
        try:
            cur.execute("SELECT quality, file_id, url FROM movie_files WHERE movie_id = %s", (movie_id,))
            rows = cur.fetchall()
            for q, fid, url in rows:
                qualities.append({'quality': q, 'file_id': fid, 'url': url})
        except:
            pass # Table might not exist
            
        # Add main file as "Default" if no qualities found, or list is empty
        if not qualities:
            if main_file_id:
                qualities.append({'quality': '🎬 Watch/Download', 'file_id': main_file_id, 'url': None})
            elif main_url:
                qualities.append({'quality': '🔗 Stream Link', 'file_id': None, 'url': main_url})
                
        return title, qualities
    finally:
        if conn: conn.close()

# ==================== HANDLERS ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handles /start and Deep Linking (e.g., /start getfile_123).
    """
    user = update.effective_user
    chat_id = update.effective_chat.id
    args = context.args

    # --- 1. Handle Deep Link (File Delivery) ---
    if args and args[0].startswith("getfile_"):
        movie_id = args[0].split("_")[1]
        
        # CHECK SUBSCRIPTION FIRST
        if not await is_user_subscribed(context.bot, user.id):
            join_markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("📢 Join Channel", url=CHANNEL_LINK),
                 InlineKeyboardButton("💬 Join Group", url=GROUP_LINK)],
                [InlineKeyboardButton("🔄 Try Again", url=f"https://t.me/{context.bot.username}?start=getfile_{movie_id}")]
            ])
            msg = await update.message.reply_photo(
                photo=START_IMG_URL,
                caption=f"👋 Hello {user.first_name},\n\n❌ **You must join our Channel and Group to get this movie.**\n\n👇 Join below and click 'Try Again'.",
                reply_markup=join_markup
            )
            asyncio.create_task(delete_message_later(context, chat_id, msg.message_id, 120))
            return

        # Fetch and Send Movie
        title, qualities = get_movie_details(movie_id)
        
        if not title or not qualities:
            await update.message.reply_text("❌ Movie data not found or deleted.")
            return

        # If multiple qualities, show buttons
        if len(qualities) > 1:
            buttons = []
            for q in qualities:
                # Callback data: q_movieID_qualityIndex
                idx = qualities.index(q)
                btn_text = f"📂 {q['quality']}"
                buttons.append([InlineKeyboardButton(btn_text, callback_data=f"qual_{movie_id}_{idx}")])
            
            markup = InlineKeyboardMarkup(buttons)
            msg = await update.message.reply_photo(
                photo=START_IMG_URL,
                caption=f"🎬 **{title}**\n\nSelect quality to download:",
                reply_markup=markup
            )
            # Note: Not auto-deleting the menu immediately, user needs time to pick
        
        # If single file, send directly
        else:
            q = qualities[0]
            await send_file_to_user(context, chat_id, q['file_id'], q['url'], title)

        return

    # --- 2. Normal Start Message ---
    # Only reply if it's a private chat
    if update.effective_chat.type == "private":
        buttons = [
            [InlineKeyboardButton("📢 Channel", url=CHANNEL_LINK),
             InlineKeyboardButton("💬 Group", url=GROUP_LINK)],
            [InlineKeyboardButton("❓ Help", callback_data="help"),
             InlineKeyboardButton("ℹ️ About", callback_data="about")]
        ]
        
        txt = (
            f"👋 **Hello {user.first_name}!**\n\n"
            "I am **Ur Movie Bot** 🤖.\n"
            "I can provide movies directly to your PM from our group.\n\n"
            "👉 **How to use:**\n"
            "1. Join our Group.\n"
            "2. Type movie name there.\n"
            "3. Click '📂 Get File Here'.\n\n"
            "**Maintained by FlimfyBox**"
        )
        
        await update.message.reply_photo(
            photo=START_IMG_URL,
            caption=txt,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.MARKDOWN
        )

async def send_file_to_user(context, chat_id, file_id, url, caption_title):
    """Sends the actual file/link and schedules auto-delete."""
    caption = (
        f"🎬 <b>{caption_title}</b>\n\n"
        f"⚠️ <i>This message will be deleted in {AUTO_DELETE_TIME} seconds. Forward it to save!</i>\n\n"
        f"Join: {CHANNEL_USERNAME}"
    )
    
    sent_msg = None
    try:
        if file_id:
            sent_msg = await context.bot.send_document(
                chat_id=chat_id,
                document=file_id,
                caption=caption,
                parse_mode=ParseMode.HTML
            )
        elif url:
            sent_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"{caption}\n\n🔗 <b>Link:</b> {url}",
                parse_mode=ParseMode.HTML
            )
            
        if sent_msg:
            asyncio.create_task(delete_message_later(context, chat_id, sent_msg.message_id))
            
    except Exception as e:
        logger.error(f"Error sending file: {e}")
        await context.bot.send_message(chat_id, "❌ Error sending file. Contact Admin.")

async def group_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Listens to group messages.
    If movie found -> Reply with 'Get File Here' button.
    If not found -> DO NOTHING (Silent).
    """
    msg_text = update.message.text
    if not msg_text or msg_text.startswith("/"): return
    
    # Clean query
    query = re.sub(r'[^\w\s]', '', msg_text).strip()
    if len(query) < 2: return

    conn = get_db_connection()
    if not conn: return

    try:
        cur = conn.cursor()
        # Find movie (Fuzzy matching via SQL ILIKE logic first for speed)
        # We check exact match or contains match
        cur.execute("SELECT id, title FROM movies WHERE title ILIKE %s LIMIT 1", (f"%{query}%",))
        result = cur.fetchone()
        
        # If standard SQL match fails, you can add Python Fuzzy logic here if DB is small
        # But for "i Papcorn" speed, SQL LIKE is better. 
        # If you want strict fuzzy:
        if not result:
             cur.execute("SELECT id, title FROM movies")
             all_movies = cur.fetchall()
             best_match = None
             highest_score = 0
             
             for mid, mtitle in all_movies:
                 score = fuzz.token_sort_ratio(query.lower(), mtitle.lower())
                 if score > 85: # Threshold
                     if score > highest_score:
                         highest_score = score
                         best_match = (mid, mtitle)
             
             if best_match:
                 result = best_match

        # LOGIC: If found, send button. If not, RETURN (Stay Silent)
        if result:
            movie_id, movie_title = result
            bot_username = context.bot.username
            
            # Deep link to start the bot
            deep_link = f"https://t.me/{bot_username}?start=getfile_{movie_id}"
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📂 Get File Here", url=deep_link)]
            ])
            
            reply_msg = await update.message.reply_text(
                f"✅ **Found:** {movie_title}\n\n👇 Click below to get the file in PM.",
                reply_markup=keyboard,
                parse_mode=ParseMode.MARKDOWN,
                quote=True
            )
            
            # Auto delete the group prompt to keep group clean
            asyncio.create_task(delete_message_later(context, update.effective_chat.id, reply_msg.message_id, 300)) # 5 mins

        # ELSE: Pass (Do absolutely nothing)
            
    except Exception as e:
        logger.error(f"Group Handler Error: {e}")
    finally:
        if conn: conn.close()

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    
    if data == "help":
        await query.answer()
        await query.message.reply_text("🆘 **Help**\n\nJust type the movie name in our group, and if I have it, I will give you a button to download it!")
        
    elif data == "about":
        await query.answer()
        await query.message.reply_text("🤖 **About**\n\nName: Ur Movie Bot\nOwner: FlimfyBox Team")

    elif data.startswith("qual_"):
        # User selected a quality from the list
        _, movie_id, qual_idx = data.split("_")
        qual_idx = int(qual_idx)
        
        title, qualities = get_movie_details(movie_id)
        if qualities and qual_idx < len(qualities):
            q = qualities[qual_idx]
            await query.answer(f"Sending {q['quality']}...")
            await send_file_to_user(context, query.message.chat_id, q['file_id'], q['url'], title)
            # Delete the menu message
            await query.message.delete()
        else:
            await query.answer("❌ Expired or invalid.", show_alert=True)

# ==================== FLASK (Keep alive) ====================
app = Flask('')
@app.route('/')
def home(): return "Ur Movie Bot Is Running!"

def run_flask():
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))

# ==================== MAIN ====================
def main():
    if not TELEGRAM_BOT_TOKEN:
        print("Error: TELEGRAM_BOT_TOKEN not found.")
        return

    # Start Flask
    threading.Thread(target=run_flask, daemon=True).start()

    # Bot Setup
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # 1. Group Message Handler (Most Important)
    # Filters: Text, Not Command, Is Group
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS, group_message_handler))

    # 2. Start & Deep Link Handler
    application.add_handler(CommandHandler("start", start))

    # 3. Callback Handler
    application.add_handler(CallbackQueryHandler(callback_handler))

    # Keep Admin commands if you need them to add movies, 
    # otherwise remove them to keep code light. 
    # (Included a dummy check here just to show where they go)
    
    print("🤖 Ur Movie Bot Started...")
    application.run_polling()

if __name__ == '__main__':
    main()
