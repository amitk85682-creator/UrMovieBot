# -*- coding: utf-8 -*-
import os
import threading
import asyncio
import logging
import re
import psycopg2
from typing import Optional
from flask import Flask
from fuzzywuzzy import process, fuzz
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatMember
from telegram.constants import ParseMode, ChatAction
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    CallbackQueryHandler
)

# ==================== SETTINGS ====================
# अपनी ID और टोकन यहाँ सही से डालें
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.environ.get('DATABASE_URL')
# Force Sub ke liye Channel/Group ki numeric ID (-100...) zaroori hai
FSUB_CHANNEL_ID = os.environ.get('FSUB_CHANNEL_ID') # e.g., -10012345678
FSUB_GROUP_ID = os.environ.get('FSUB_GROUP_ID')     # e.g., -10087654321

# Links & Branding
CHANNEL_LINK = "https://t.me/filmfybox"
GROUP_LINK = "https://t.me/Filmfybox002"
START_IMG = "https://blogger.googleusercontent.com/img/b/R29vZ2xl/AVvXsEhYD6_-uyyYg_YxJMkk06sbRQ5N-IH7HFjr3P1AYZLiQ6qSp3Ap_FgRWGjCKk6okFRh0bRTH5-TtrizBxsQpjxR6bdnNidTjiT-ICWhqaC0xcEJs89bSOTwrzBAMFYtWAv48llz96Ye9E3Q3vEHrtk1id8aceQbp_uxAJ4ASqZIEsK5FcaMYcrhj45i70c/s320/logo-design-for-flimfybox-a-cinematic-mo_OhkRefmbTCK6_RylGkOrAw_CtxTQGw_Tu6dY2kc64sagw.jpeg"

# Database Utility Import (Jaisa tumne kaha tha)
try:
    import db_utils
    FIXED_DATABASE_URL = getattr(db_utils, "FIXED_DATABASE_URL", None)
except Exception:
    FIXED_DATABASE_URL = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== DATABASE FUNCTIONS ====================
def get_db_connection():
    try:
        conn_str = FIXED_DATABASE_URL or DATABASE_URL
        return psycopg2.connect(conn_str)
    except Exception as e:
        logger.error(f"DB Error: {e}")
        return None

# --- OLD LOGIC RESTORED: Fetch Qualities from movie_files table ---
def get_all_movie_qualities(movie_id):
    """Fetch all available qualities (480p, 720p, etc) for a movie."""
    conn = get_db_connection()
    if not conn: return []
    try:
        cur = conn.cursor()
        
        # 1. Check movie_files table (Tumhara purana logic)
        cur.execute("""
            SELECT quality, url, file_id 
            FROM movie_files 
            WHERE movie_id = %s 
            ORDER BY quality DESC
        """, (movie_id,))
        files = cur.fetchall() # Returns list of (quality, url, file_id)
        
        # 2. Check main movies table (Fallback)
        cur.execute("SELECT url, file_id FROM movies WHERE id = %s", (movie_id,))
        main_movie = cur.fetchone()
        
        final_list = []
        # Add main file if exists
        if main_movie and (main_movie[0] or main_movie[1]):
            final_list.append(("Watch/Download", main_movie[0], main_movie[1]))
            
        # Add extra qualities
        for f in files:
            final_list.append(f)
            
        return final_list
    except Exception as e:
        logger.error(f"Error fetching qualities: {e}")
        return []
    finally:
        if conn: conn.close()

def search_movie_in_db(query):
    """Search movie and return best match ID and Title"""
    conn = get_db_connection()
    if not conn: return None
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, title FROM movies")
        all_movies = cur.fetchall()
        
        # Fuzzy Matching
        movie_dict = {m[1]: m[0] for m in all_movies} # title: id
        match = process.extractOne(query, list(movie_dict.keys()), scorer=fuzz.token_sort_ratio)
        
        if match and match[1] >= 75: # 75% match threshold
            return {"id": movie_dict[match[0]], "title": match[0]}
        return None
    except Exception as e:
        logger.error(f"Search Error: {e}")
        return None
    finally:
        if conn: conn.close()

# ==================== HELPER FUNCTIONS ====================
async def delete_after_delay(context, chat_id, message_ids, delay=60):
    """Auto delete messages"""
    await asyncio.sleep(delay)
    for msg_id in message_ids:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except: pass

async def is_user_joined(user_id, context):
    """Check Force Subscribe logic"""
    if not FSUB_CHANNEL_ID or not FSUB_GROUP_ID:
        return True # Agar ID set nahi hai to ignore karega
    try:
        # Check Channel
        member_ch = await context.bot.get_chat_member(FSUB_CHANNEL_ID, user_id)
        if member_ch.status in ['left', 'kicked']: return False
        # Check Group
        member_gr = await context.bot.get_chat_member(FSUB_GROUP_ID, user_id)
        if member_gr.status in ['left', 'kicked']: return False
        return True
    except Exception as e:
        logger.error(f"Join Check Error: {e}")
        return True # Error aye to user ko rokna nahi chahiye

# ==================== HANDLERS ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles /start and Deep Linking (Redirect from Group)"""
    user = update.effective_user
    chat_id = update.effective_chat.id
    args = context.args

    # 1. Deep Link Handler (Jab user group se button daba ke aaye)
    if args and args[0].startswith("get_"):
        movie_id = args[0].split("_")[1]
        
        # Force Sub Check
        joined = await is_user_joined(user.id, context)
        if not joined:
            buttons = [
                [InlineKeyboardButton("📢 Join FlimfyBox", url=CHANNEL_LINK)],
                [InlineKeyboardButton("💬 Join FlimfyBox Chat", url=GROUP_LINK)],
                [InlineKeyboardButton("🔄 Try Again", url=f"https://t.me/{context.bot.username}?start=get_{movie_id}")]
            ]
            await update.message.reply_photo(
                photo=START_IMG,
                caption="⚠️ **Access Denied!**\n\nMovie pane ke liye Channels join karna zaroori hai.",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
            return

        # Fetch Qualities (Wahi purana logic)
        qualities = get_all_movie_qualities(movie_id)
        
        if not qualities:
            await update.message.reply_text("❌ Sorry, File database se delete ho gayi hai.")
            return

        # Quality Buttons Banao
        keyboard = []
        for q_name, url, file_id in qualities:
            # Data format: qual_<movie_id>_<index>
            idx = qualities.index((q_name, url, file_id))
            btn_text = f"🎬 {q_name}"
            keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"qual_{movie_id}_{idx}")])
        
        await update.message.reply_photo(
            photo=START_IMG,
            caption=f"✅ **File Found!**\n\nPlease select quality to download:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    # 2. Normal Start Message
    buttons = [
        [InlineKeyboardButton("📢 FlimfyBox", url=CHANNEL_LINK), InlineKeyboardButton("💬 FlimfyBox Chat", url=GROUP_LINK)],
        [InlineKeyboardButton("🆘 Help", callback_data="help"), InlineKeyboardButton("ℹ️ About", callback_data="about")]
    ]
    await update.message.reply_photo(
        photo=START_IMG,
        caption="👋 **Hi, I am Ur Movie Bot!**\n\nAdd me to your group, I will provide movies there securely.\n\nJust type movie name in group!",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def group_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    GROUP MEIN:
    - User msg karega.
    - Bot search karega.
    - Agar mila -> Button dega 'Get File Here' (Redirect to PM).
    - Agar nahi mila -> CHUP rahega (Silent).
    """
    msg = update.message.text
    if not msg or msg.startswith("/"): return
    
    # Search DB
    movie = search_movie_in_db(msg)
    
    if movie:
        bot_username = context.bot.username
        # Deep link banaya
        deep_link = f"https://t.me/{bot_username}?start=get_{movie['id']}"
        
        btn = [[InlineKeyboardButton("📂 Get File Here", url=deep_link)]]
        
        sent_msg = await update.message.reply_text(
            f"🎬 **{movie['title']}** found!\n\n👇 Click below to get file in PM (Safe & Fast).",
            reply_markup=InlineKeyboardMarkup(btn)
        )
        
        # Auto Delete Group Msg
        asyncio.create_task(delete_after_delay(context, update.effective_chat.id, [sent_msg.message_id]))

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles Quality Selection & Other Buttons"""
    query = update.callback_query
    data = query.data
    
    # Handle Quality Selection
    if data.startswith("qual_"):
        # Format: qual_<movie_id>_<index>
        try:
            _, movie_id, idx = data.split("_")
            idx = int(idx)
            
            qualities = get_all_movie_qualities(movie_id)
            if idx >= len(qualities):
                await query.answer("Link expired/invalid", show_alert=True)
                return
                
            q_name, url, file_id = qualities[idx]
            
            caption = f"🎬 **Movie:** Found\n💿 **Quality:** {q_name}\n\n⚠️ *Auto-delete in 60s*"
            
            sent_msg = None
            if file_id:
                sent_msg = await context.bot.send_document(
                    chat_id=query.message.chat_id,
                    document=file_id,
                    caption=caption,
                    parse_mode=ParseMode.MARKDOWN
                )
            elif url:
                sent_msg = await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=f"{caption}\n\n🔗 **Link:** {url}",
                    parse_mode=ParseMode.MARKDOWN
                )
            
            await query.answer("Sending file...")
            
            # Auto Delete File
            if sent_msg:
                asyncio.create_task(delete_after_delay(context, query.message.chat_id, [sent_msg.message_id], 60))
                
        except Exception as e:
            logger.error(f"Callback Error: {e}")
            await query.answer("Error fetching file", show_alert=True)

    # Help/About
    elif data == "help":
        await query.message.edit_caption("Just add me to your group and type movie name!", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="back")]]))
    elif data == "about":
        await query.message.edit_caption("Ur Movie Bot\nOwner: FlimfyBox", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="back")]]))
    elif data == "back":
        # Wapis Start wale buttons
        buttons = [
            [InlineKeyboardButton("📢 FlimfyBox", url=CHANNEL_LINK), InlineKeyboardButton("💬 FlimfyBox Chat", url=GROUP_LINK)],
            [InlineKeyboardButton("🆘 Help", callback_data="help"), InlineKeyboardButton("ℹ️ About", callback_data="about")]
        ]
        await query.message.edit_caption(caption="👋 **Hi, I am Ur Movie Bot!**", reply_markup=InlineKeyboardMarkup(buttons))

# ==================== FLASK SERVER ====================
app = Flask('')
@app.route('/')
def home(): return "Ur Movie Bot Running"

def run_flask():
    app.run(host='0.0.0.0', port=8080)

# ==================== MAIN ====================
def main():
    threading.Thread(target=run_flask, daemon=True).start()
    
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(callback_handler))
    
    # Group Handler - Sabse neeche taaki commands block na kare
    app.add_handler(MessageHandler(filters.TEXT & filters.ChatType.GROUPS, group_message_handler))
    
    print("Bot Started...")
    app.run_polling()

if __name__ == "__main__":
    main()
