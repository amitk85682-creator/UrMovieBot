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
import db_utils  # Purani file same rahegi

# ==================== CONFIGURATION ====================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.environ.get('DATABASE_URL')

# Force Sub Channels/Groups (IDs must start with -100)
FORCE_SUB_CHANNEL_ID = os.environ.get('FORCE_SUB_CHANNEL_ID') 
FORCE_SUB_GROUP_ID = os.environ.get('FORCE_SUB_GROUP_ID')

# Links
CHANNEL_LINK = "https://t.me/filmfybox"
GROUP_LINK = "https://t.me/Filmfybox002"

# Start Image
START_IMG_URL = os.environ.get('START_IMG_URL', 'https://i.ibb.co/vzCD215/image-1b251c.png') 

# Auto Delete Time (in seconds)
DELETE_TIMEOUT = 60 

# ==================== LOGGING ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== HELPER FUNCTIONS ====================

async def delete_after_delay(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay: int = DELETE_TIMEOUT):
    """Message ko delay ke baad delete karne ka function"""
    try:
        await asyncio.sleep(delay)
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info(f"Deleted message {message_id} in chat {chat_id}")
    except Exception as e:
        logger.debug(f"Deletion failed (maybe already deleted): {e}")

async def check_membership(user_id, context):
    """Check if user has joined Channel and Group"""
    if not FORCE_SUB_CHANNEL_ID and not FORCE_SUB_GROUP_ID:
        return True

    try:
        # Check Channel
        if FORCE_SUB_CHANNEL_ID:
            chat_member = await context.bot.get_chat_member(chat_id=FORCE_SUB_CHANNEL_ID, user_id=user_id)
            if chat_member.status in ['left', 'kicked', 'restricted']:
                return False
        
        # Check Group
        if FORCE_SUB_GROUP_ID:
            group_member = await context.bot.get_chat_member(chat_id=FORCE_SUB_GROUP_ID, user_id=user_id)
            if group_member.status in ['left', 'kicked', 'restricted']:
                return False

        return True
    except Exception as e:
        logger.error(f"Membership check error: {e}")
        return True # Error aane par allow karein taki users fase nahi

def get_fsub_keyboard():
    """Join karne ke liye buttons"""
    keyboard = [
        [InlineKeyboardButton("📢 Join Channel", url=CHANNEL_LINK),
         InlineKeyboardButton("👥 Join Group", url=GROUP_LINK)],
        [InlineKeyboardButton("🔄 Try Again", callback_data="check_fsub")]
    ]
    return InlineKeyboardMarkup(keyboard)

async def send_file_to_user(update, context, movie_id):
    """File send karega with Original Caption & Auto Delete"""
    conn = db_utils.get_db_connection()
    if not conn:
        msg = await update.message.reply_text("❌ Database Error.")
        asyncio.create_task(delete_after_delay(context, update.effective_chat.id, msg.message_id, 10))
        return

    try:
        movie = db_utils.get_movie_by_id(conn, movie_id)
        if not movie:
            msg = await update.message.reply_text("❌ Movie not found or removed.")
            asyncio.create_task(delete_after_delay(context, update.effective_chat.id, msg.message_id, 10))
            return

        title = movie['title']
        file_id = movie.get('file_id') or movie.get('url')

        # --- ORIGINAL CAPTION STYLE ---
        caption_text = (
            f"🎬 <b>{title}</b>\n\n"
            f"🔗 <b>JOIN »</b> <a href='{CHANNEL_LINK}'>FilmfyBox</a>\n\n"
            "🔹 <b>Please drop the movie name, and I’ll find it for you as soon as possible. 🎬✨👇</b>\n"
            f"🔹 <b><a href='{GROUP_LINK}'>FlimfyBox Chat</a></b>"
        )
        
        # Join Channel Button for Media
        join_btn = InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Join Channel", url=CHANNEL_LINK)]])

        chat_id = update.effective_chat.id
        
        # Warning Message (Delete Alert)
        warning_msg = await context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ ❌👉This file automatically❗️delete after 1 minute❗️so please forward in another chat👈❌",
            parse_mode='Markdown'
        )

        sent_msg = None

        # Send File Logic
        if file_id:
            try:
                # 1. Try sending as Document (Best for Files)
                sent_msg = await context.bot.send_document(
                    chat_id=chat_id,
                    document=file_id,
                    caption=caption_text,
                    parse_mode='HTML',
                    reply_markup=join_btn
                )
            except:
                try:
                    # 2. Try sending as Video
                    sent_msg = await context.bot.send_video(
                        chat_id=chat_id,
                        video=file_id,
                        caption=caption_text,
                        parse_mode='HTML',
                        reply_markup=join_btn
                    )
                except:
                    # 3. Fallback: Text Message with Link
                    sent_msg = await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"🎬 <b>{title}</b>\n\n🔗 Link: {file_id}\n\n{caption_text}",
                        parse_mode='HTML',
                        reply_markup=join_btn
                    )
        else:
            msg = await update.message.reply_text("❌ File id missing in database.")
            asyncio.create_task(delete_after_delay(context, chat_id, msg.message_id))

        # --- AUTO DELETE BOTH MESSAGES ---
        if sent_msg:
            # Delete File
            asyncio.create_task(delete_after_delay(context, chat_id, sent_msg.message_id))
            # Delete Warning
            asyncio.create_task(delete_after_delay(context, chat_id, warning_msg.message_id))

    except Exception as e:
        logger.error(f"Error sending file: {e}")
        msg = await update.message.reply_text("❌ Error sending file.")
        asyncio.create_task(delete_after_delay(context, update.effective_chat.id, msg.message_id))
    finally:
        conn.close()

# ==================== HANDLERS ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles /start command with Auto-Delete"""
    user = update.effective_user
    chat_id = update.effective_chat.id
    args = context.args

    # --- DEEP LINK LOGIC ---
    if args and args[0].startswith('movie_'):
        movie_id = int(args[0].split('_')[1])
        
        # Force Sub Check
        is_member = await check_membership(user.id, context)
        if not is_member:
            msg = await update.message.reply_text(
                "⚠️ **Access Denied!**\n\nPlease join our Channel and Group first to get the movie.",
                reply_markup=get_fsub_keyboard(),
                parse_mode='Markdown'
            )
            # Delete Warning Msg
            asyncio.create_task(delete_after_delay(context, chat_id, msg.message_id))
            return

        # Send File (File function handles deletion internally)
        await send_file_to_user(update, context, movie_id)
        return

    # --- NORMAL START UI ---
    bot_username = context.bot.username
    add_group_url = f"https://t.me/{bot_username}?startgroup=true"

    text = f"""
HEY {user.mention_markdown()}..👋

IM ⚡ **POWERFUL AUTO-FILTER BOT...**

😎 YOU CAN USE ME AS A AUTO-FILTER IN YOUR GROUP ....
ITS EASY TO USE ME: JUST ADD ME TO YOUR GROUP AS ADMIN, THATS ALL, I WILL PROVIDE MOVIES THERE...😎

⚠️ **MORE HELP CHECK HELP BUTTON..**

©️ MAINTAINED BY: FILMFYBOX
    """

    keyboard = [
        [InlineKeyboardButton("➕ Add Me To Your Groups ➕", url=add_group_url)],
        [InlineKeyboardButton("↗️ CHANNEL", url=CHANNEL_LINK),
         InlineKeyboardButton("👥 GROUP", url=GROUP_LINK)],
        [InlineKeyboardButton("ℹ️ HELP", callback_data="help"),
         InlineKeyboardButton("😊 ABOUT", callback_data="about")]
    ]

    try:
        sent_msg = await update.message.reply_photo(
            photo=START_IMG_URL,
            caption=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        # AUTO DELETE START MESSAGE
        asyncio.create_task(delete_after_delay(context, chat_id, sent_msg.message_id))
        
    except Exception as e:
        sent_msg = await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        asyncio.create_task(delete_after_delay(context, chat_id, sent_msg.message_id))

async def group_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Silent Group Handler with Auto-Delete on Success"""
    if not update.message or not update.message.text:
        return
    
    if update.message.text.startswith('/'):
        return

    query = update.message.text.strip()
    if len(query) < 3:
        return

    try:
        conn = db_utils.get_db_connection()
        if not conn: return
        
        cur = conn.cursor()
        # Search logic (Exact + Fuzzy)
        cur.execute("SELECT id, title FROM movies WHERE LOWER(title) = LOWER(%s) LIMIT 1", (query,))
        exact_match = cur.fetchone()
        
        movie_data = None
        if exact_match:
            movie_data = exact_match
        else:
            cur.execute("SELECT id, title FROM movies")
            all_movies = cur.fetchall()
            movie_dict = {m[1]: m[0] for m in all_movies}
            titles = list(movie_dict.keys())
            match = process.extractOne(query, titles, scorer=fuzz.token_sort_ratio)
            if match and match[1] >= 85: 
                movie_data = (movie_dict[match[0]], match[0])

        cur.close()
        conn.close()

        if movie_data:
            movie_id, movie_title = movie_data
            bot_username = context.bot.username
            deep_link = f"https://t.me/{bot_username}?start=movie_{movie_id}"

            keyboard = [[InlineKeyboardButton("📂 Get File Here", url=deep_link)]]
            
            sent_msg = await update.message.reply_text(
                f"✅ **Found:** {movie_title}\n\nClick below to get the file in private 👇",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown',
                reply_to_message_id=update.message.message_id
            )
            
            # AUTO DELETE GROUP BUTTON MESSAGE
            asyncio.create_task(delete_after_delay(context, update.effective_chat.id, sent_msg.message_id))

    except Exception as e:
        logger.error(f"Group handler error: {e}")

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    # In messages ko bhi auto delete kar sakte hain agar chahein, 
    # filhal ye user ke action par aate hain to turant delete nahi kar rahe
    
    if query.data == "help":
        await query.message.reply_text(
            "**ℹ️ Help**\n\n1. Add me to your group.\n2. Make me Admin.\n3. Send Movie Name.\n4. I will provide the link!",
            parse_mode='Markdown'
        )
    
    elif query.data == "about":
        await query.message.reply_text(
            f"**😊 About**\n\nBot Name: FilmfyBox Auto Filter\nOwner: <a href='{CHANNEL_LINK}'>FilmfyBox</a>",
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    elif query.data == "check_fsub":
        await query.message.reply_text("Please click the movie link again after joining!")

# ==================== FLASK ====================
app = Flask('')

@app.route('/')
def home():
    return "Bot is running!"

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)

# ==================== MAIN ====================
def main():
    if not TELEGRAM_BOT_TOKEN:
        print("Error: TELEGRAM_BOT_TOKEN missing.")
        return

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(callback_handler))
    application.add_handler(MessageHandler(filters.TEXT & filters.ChatType.GROUPS, group_message_handler))

    from threading import Thread
    Thread(target=run_flask).start()

    print("Bot Started...")
    application.run_polling()

if __name__ == '__main__':
    main()
