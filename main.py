# -*- coding: utf-8 -*-
import os
import threading
import asyncio
import logging
import re
import psycopg2
from typing import Optional
from flask import Flask
import telegram
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    CallbackQueryHandler
)
from datetime import datetime, timedelta
from fuzzywuzzy import process, fuzz
from urllib.parse import urlparse

# ==================== LOGGING SETUP ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== ENVIRONMENT VARIABLES ====================
# ये सेक्शन आपके example code से लिया है
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.environ.get('DATABASE_URL')
BLOGGER_API_KEY = os.environ.get('BLOGGER_API_KEY')
BLOG_ID = os.environ.get('BLOG_ID')
UPDATE_SECRET_CODE = os.environ.get('UPDATE_SECRET_CODE', 'default_secret_123')
ADMIN_USER_ID = int(os.environ.get('ADMIN_USER_ID', 0))
FILMFYBOX_CHANNEL_URL = os.environ.get('FILMFYBOX_CHANNEL_URL', 'http://t.me/filmfybox')
BOT_USERNAME = os.environ.get('BOT_USERNAME', 'urmoviebot')
GROUP_LINK = os.environ.get('GROUP_LINK', 'https://t.me/Filmfybox002')

# Configuration
AUTO_DELETE_DELAY = 60
SIMILARITY_THRESHOLD = 85

# Validate required variables
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN missing")
    raise ValueError("TELEGRAM_BOT_TOKEN is required")

if not DATABASE_URL:
    logger.error("DATABASE_URL missing")
    raise ValueError("DATABASE_URL is required")

# ==================== UTILITY FUNCTIONS (आपके example से लिया है) ====================
def preprocess_query(query):
    query = re.sub(r'[^\w\s-]', '', query)
    query = ' '.join(query.split())
    stop_words = ['movie', 'film', 'full', 'download', 'watch', 'online', 'free', 'फिल्म', 'मूवी', 'सीरीज']
    words = query.lower().split()
    words = [w for w in words if w not in stop_words]
    return ' '.join(words).strip()

def _normalize_title_for_match(title: str) -> str:
    if not title:
        return ""
    t = re.sub(r'[^\w\s]', ' ', title)
    t = re.sub(r'\s+', ' ', t).strip()
    return t.lower()

async def delete_messages_after_delay(context, chat_id, message_ids, delay=AUTO_DELETE_DELAY):
    try:
        await asyncio.sleep(delay)
        for msg_id in message_ids:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
                logger.info(f"Deleted message {msg_id}")
            except Exception as e:
                logger.error(f"Failed to delete {msg_id}: {e}")
    except Exception as e:
        logger.error(f"Auto-delete error: {e}")

# ==================== DATABASE FUNCTIONS (आपके example से लिया है) ====================
def get_db_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        logger.error(f"DB Connection Error: {e}")
        return None

def setup_database():
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        
        # Enable pg_trgm
        cur.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm;')
        
        # Movies table (आपके example के exact schema से)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS movies (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL UNIQUE,
                url TEXT NOT NULL,
                file_id TEXT
            )
        ''')
        
        # Sync table
        cur.execute('CREATE TABLE IF NOT EXISTS sync_info (id SERIAL PRIMARY KEY, last_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP);')
        
        # Indexes
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movies_title ON movies (title);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movies_title_trgm ON movies USING gin (title gin_trgm_ops);')
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("DB Setup Complete")
    except Exception as e:
        logger.error(f"DB Setup Error: {e}")

def get_movies_from_db(user_query, limit=10):
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return []
        cur = conn.cursor()
        
        # Exact match
        cur.execute(
            "SELECT id, title, url, file_id FROM movies WHERE LOWER(title) LIKE LOWER(%s) ORDER BY title LIMIT %s",
            (f'%{user_query}%', limit)
        )
        exact_matches = cur.fetchall()
        if exact_matches:
            return exact_matches
        
        # Fuzzy match
        cur.execute("SELECT id, title, url, file_id FROM movies")
        all_movies = cur.fetchall()
        if not all_movies:
            return []
        
        movie_titles = [m[1] for m in all_movies]
        movie_dict = {m[1]: m for m in all_movies}
        matches = process.extract(user_query, movie_titles, scorer=fuzz.token_sort_ratio, limit=limit)
        
        filtered = []
        for match in matches:
            if len(match)>=2:
                title, score = match[0], match[1]
                if score >= SIMILARITY_THRESHOLD and title in movie_dict:
                    filtered.append(movie_dict[title])
        
        return filtered[:limit]
    except Exception as e:
        logger.error(f"DB Query Error: {e}")
        return []
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

# ==================== QUALITY HANDLING (आपके example से लिया है) ====================
def get_all_movie_qualities(movie_id):
    conn = get_db_connection()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute("SELECT title, url, file_id FROM movies WHERE id = %s", (movie_id,))
        main_movie = cur.fetchone()
        if not main_movie:
            return []
        
        # All similar movies for same title
        cur.execute("SELECT id, title, url, file_id FROM movies WHERE LOWER(title) LIKE LOWER(%s)", (f'%{_normalize_title_for_match(main_movie[1])}%',))
        all_versions = cur.fetchall()
        cur.close()
        conn.close()
        
        # Group by quality/language
        qualities = []
        seen = set()
        for m in all_versions:
            mid, title, url, file_id = m
            # Parse quality from title
            title_lower = title.lower()
            quality = "HD"
            if "480p" in title_lower: quality = "480p"
            elif "720p" in title_lower: quality = "720p"
            elif "1080p" in title_lower: quality = "1080p"
            elif "4k" in title_lower: quality = "4K"
            
            language = "Hindi"
            if "english" in title_lower: language = "English"
            elif "dual" in title_lower: language = "Dual Audio"
            
            key = f"{quality}_{language}"
            if key not in seen:
                seen.add(key)
                qualities.append( (quality, language, mid, url, file_id) )
        return qualities
    except Exception as e:
        logger.error(f"Quality Fetch Error: {e}")
        return []

# ==================== KEYBOARD MARKUPS (Netflix Style + आपके example से) ====================
def create_quality_selection_keyboard(movie_id, title, qualities):
    keyboard = []
    for quality, language, mid, _, _ in qualities:
        btn_text = f"🎬 {quality} {language}"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"quality_{mid}")])
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
    return InlineKeyboardMarkup(keyboard)

def create_season_selection_keyboard(seasons, anchor_id):
    keyboard = []
    row = []
    for season in sorted(seasons):
        row.append(InlineKeyboardButton(f"📺 Season {season}", callback_data=f"season_{season}_{anchor_id}"))
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
    return InlineKeyboardMarkup(keyboard)

def create_episode_selection_keyboard(episodes, season_num, anchor_id):
    keyboard = []
    row = []
    for ep in sorted(episodes):
        row.append(InlineKeyboardButton(f"Ep {ep}", callback_data=f"episode_{season_num}_{ep}_{anchor_id}"))
        if len(row) == 4:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🔙 Back to Seasons", callback_data=f"back_season_{anchor_id}")])
    return InlineKeyboardMarkup(keyboard)

def get_file_options_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✨ Join Movie Channel", url=FILMFYBOX_CHANNEL_URL)],
        [InlineKeyboardButton("💬 Join Group", url=GROUP_LINK),
         InlineKeyboardButton("♻️ Share Bot", url=f"https://t.me/share/url?url=https://t.me/{BOT_USERNAME}")]
    ])

# ==================== FILE DELIVERY (आपके example का exact code) ====================
async def send_movie_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE, movie_id: int, title: str, url: Optional[str] = None, file_id: Optional[str] = None):
    """यह फ़ंक्शन आपके example code से बिल्कुल कॉपी की गई है - ये file deliver करेगा"""
    chat_id = update.effective_chat.id if update else context._chat_id

    try:
        # Loading message
        loading_msg = await context.bot.send_message(
            chat_id=chat_id,
            text="⏳ <b>Processing Request...</b>\n<i>Fetching file from database...</i>",
            parse_mode='HTML'
        )
        await asyncio.sleep(0.5)

        # Premium caption (आपके example से)
        caption_text = (
            f"🎬 <b>{title}</b>\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
            f"💿 <b>Quality:</b> <i>High Definition</i>\n"
            f"🔊 <b>Language:</b> <i>Hindi / English</i>\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
            f"🚀 <b>Join Our Channels:</b>\n"
            f"📢 <a href='{FILMFYBOX_CHANNEL_URL}'>Main Channel</a> | 💬 <a href='{GROUP_LINK}'>Support Group</a>\n\n"
            f"⚠️ <i>Auto-delete in 60s. Forward explicitly!</i>"
        )

        sent_msg = None
        join_keyboard = get_file_options_keyboard()

        # 1. Send via File ID (आपके example से)
        if file_id:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=loading_msg.message_id,
                text="📤 <b>Uploading File...</b>",
                parse_mode='HTML'
            )
            sent_msg = await context.bot.send_document(
                chat_id=chat_id,
                document=file_id,
                caption=caption_text,
                parse_mode='HTML',
                reply_markup=join_keyboard
            )

        # 2. Copy from Private Channel (t.me/c/...) (आपके example से)
        elif url and "t.me/c/" in url:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=loading_msg.message_id,
                text="🔄 <b>Retrieving from Archive...</b>",
                parse_mode='HTML'
            )
            parts = url.rstrip('/').split('/')
            ch_id_str = parts[-2]
            from_chat_id = int("-100" + ch_id_str) if not ch_id_str.startswith("-100") else int(ch_id_str)
            message_id = int(parts[-1])

            sent_msg = await context.bot.copy_message(
                chat_id=chat_id,
                from_chat_id=from_chat_id,
                message_id=message_id,
                caption=caption_text,
                parse_mode='HTML',
                reply_markup=join_keyboard
            )

        # 3. Public Link or Direct URL (आपके example से)
        elif url and url.startswith("http"):
            sent_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"🎬 <b>{title}</b>\n\n🔗 <b>Download Link:</b> {url}\n\n{caption_text}",
                parse_mode='HTML',
                reply_markup=join_keyboard
            )

        # 4. Fallback
        else:
            sent_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ <b>Error:</b> No valid file or link found for {title}",
                parse_mode='HTML'
            )

        # Cleanup loading message
        await context.bot.delete_message(chat_id=chat_id, message_id=loading_msg.message_id)

        # Auto-delete
        if sent_msg:
            timer_msg = await context.bot.send_message(
                chat_id=chat_id,
                text="⏳ <i>This message will self-destruct in 60 seconds.</i>",
                parse_mode='HTML'
            )
            asyncio.create_task(
                delete_messages_after_delay(
                    context,
                    chat_id,
                    [sent_msg.message_id, timer_msg.message_id],
                    AUTO_DELETE_DELAY
                )
            )

    except Exception as e:
        logger.error(f"File Delivery Error: {e}")
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=loading_msg.message_id)
            await context.bot.send_message(
                chat_id=chat_id,
                text="❌ <b>Error:</b> Failed to send file. Please try again.",
                parse_mode='HTML'
            )
        except Exception as e2:
            logger.error(f"Secondary Error: {e2}")

# ==================== NAVIGATION HANDLERS (Netflix Style) ====================
async def show_season_menu(context, chat_id, anchor_movie):
    """Season list दिखाता है - Netflix जैसा"""
    anchor_title = anchor_movie[1]
    norm_anchor = _normalize_title_for_match(anchor_title)
    
    # All similar movies
    all_movies = get_movies_from_db(norm_anchor, limit=50)
    
    # Extract unique seasons
    seasons = set()
    for m in all_movies:
        title = m[1]
        season_match = re.search(r'(?:s|season)\s?(\d{1,2})', title.lower())
        if season_match:
            seasons.add(int(season_match.group(1)))
    
    if not seasons:
        # Agar season nahi hai - direct quality menu
        qualities = get_all_movie_qualities(anchor_movie[0])
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"🎬 <b>{anchor_title.split('(')[0].strip()}</b>\n\n👇 Select Quality:",
            reply_markup=create_quality_selection_keyboard(anchor_movie[0], anchor_title, qualities),
            parse_mode='HTML'
        )
        return
    
    # Season menu send
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"📺 <b>{anchor_title.split('(')[0].strip()}</b>\n\n📌 Select Season:",
        reply_markup=create_season_selection_keyboard(seasons, anchor_movie[0]),
        parse_mode='HTML'
    )

async def show_episode_menu(context, chat_id, season_num, anchor_id):
    """Episode list दिखाता है"""
    conn = get_db_connection()
    if not conn:
        return
    cur = conn.cursor()
    cur.execute("SELECT title FROM movies WHERE id = %s", (anchor_id,))
    anchor_title = cur.fetchone()[0]
    cur.close()
    conn.close()
    
    norm_anchor = _normalize_title_for_match(anchor_title)
    all_movies = get_movies_from_db(norm_anchor, limit=50)
    
    # Extract episodes for this season
    episodes = set()
    for m in all_movies:
        title = m[1]
        season_match = re.search(r'(?:s|season)\s?(\d{1,2})', title.lower())
        episode_match = re.search(r'(?:e|ep|episode)\s?(\d{1,3})', title.lower())
        if season_match and episode_match:
            if int(season_match.group(1)) == season_num:
                episodes.add(int(episode_match.group(1)))
    
    if not episodes:
        # Agar episode nahi hai - complete season pack
        qualities = get_all_movie_qualities(anchor_id)
        filtered_qualities = [q for q in qualities if f"s{season_num}" in q[2].lower()]
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"📺 <b>Season {season_num}</b> (Complete Pack)\n\n👇 Select Quality:",
            reply_markup=create_quality_selection_keyboard(anchor_id, f"Season {season_num}", filtered_qualities),
            parse_mode='HTML'
        )
        return
    
    # Episode menu send
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"📺 <b>Season {season_num}</b>\n\n👇 Select Episode:",
        reply_markup=create_episode_selection_keyboard(episodes, season_num, anchor_id),
        parse_mode='HTML'
    )

# ==================== TELEGRAM HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat_id = update.effective_chat.id

    # Deep link handling (आपके example से)
    if context.args and context.args[0].startswith("movie_"):
        try:
            movie_id = int(context.args[0].split('_')[1])
            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                cur.execute("SELECT id, title, url, file_id FROM movies WHERE id = %s", (movie_id,))
                movie_data = cur.fetchone()
                cur.close()
                conn.close()
                if movie_data:
                    await send_movie_to_user(update, context, movie_data[0], movie_data[1], movie_data[2], movie_data[3])
                else:
                    msg = await update.message.reply_text("❌ Movie not found")
                    asyncio.create_task(delete_messages_after_delay(context, chat_id, [msg.message_id]))
        except Exception as e:
            logger.error(f"Deep Link Error: {e}")
            msg = await update.message.reply_text("❌ Invalid link")
            asyncio.create_task(delete_messages_after_delay(context, chat_id, [msg.message_id]))
        return

    # Start message (Netflix Style)
    start_text = f"""
👋 Hey {user.first_name}!,

🤖 I'm **Netflix Style Movie Bot**
✅ POWERFUL AUTO-FILTER BOT LIKE NETFLIX

💡 Just type any movie/series name and I'll show you:
- Seasons (if it's a series)
- Episodes
- Multiple Qualities

⚠️ All messages auto-delete after 60 seconds.

© MAINTAINED BY: Your Team 🚀
    """
    await update.message.reply_text(
        start_text,
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add Me To Group", url=f"https://t.me/{BOT_USERNAME}?startgroup=true")],
            [InlineKeyboardButton("📢 Channel", url=FILMFYBOX_CHANNEL_URL),
             InlineKeyboardButton("👥 Group", url=GROUP_LINK)]
        ])
    )

async def search_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """यह फ़ंक्शन user के query को handle करता है"""
    query = update.message.text.strip()
    if len(query) < 3:
        return
    
    processed_query = preprocess_query(query)
    movies_found = get_movies_from_db(processed_query, limit=1)
    
    if not movies_found:
        msg = await update.message.reply_text("❌ Movie/Series not found. Try with correct name.")
        asyncio.create_task(delete_messages_after_delay(context, update.effective_chat.id, [msg.message_id]))
        return
    
    # Best match
    best_match = movies_found[0]
    await show_season_menu(context, update.effective_chat.id, best_match)

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Inline button handler (आपके example से)"""
    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = query.message.chat_id

    try:
        if data == "cancel":
            await query.message.delete()
            return
        
        # Quality selection (आपके example से)
        if data.startswith("quality_"):
            movie_id = int(data.split("_")[1])
            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                cur.execute("SELECT id, title, url, file_id FROM movies WHERE id = %s", (movie_id,))
                movie_data = cur.fetchone()
                cur.close()
                conn.close()
                if movie_data:
                    await query.message.delete()
                    await send_movie_to_user(update, context, movie_data[0], movie_data[1], movie_data[2], movie_data[3])
            return
        
        # Season selection
        if data.startswith("season_"):
            parts = data.split("_")
            season_num = int(parts[1])
            anchor_id = int(parts[2])
            await query.message.delete()
            await show_episode_menu(context, chat_id, season_num, anchor_id)
            return
        
        # Episode selection
        if data.startswith("episode_"):
            parts = data.split("_")
            season_num = int(parts[1])
            episode_num = int(parts[2])
            anchor_id = int(parts[3])
            
            # Get movie for this episode
            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                cur.execute("SELECT title FROM movies WHERE id = %s", (anchor_id,))
                anchor_title = cur.fetchone()[0]
                cur.close()
                conn.close()
                
                norm_anchor = _normalize_title_for_match(anchor_title)
                all_movies = get_movies_from_db(norm_anchor, limit=50)
                
                # Find exact episode movie
                target_movie = None
                for m in all_movies:
                    title = m[1]
                    s_match = re.search(r'(?:s|season)\s?(\d{1,2})', title.lower())
                    e_match = re.search(r'(?:e|ep|episode)\s?(\d{1,3})', title.lower())
                    if s_match and e_match:
                        if int(s_match.group(1)) == season_num and int(e_match.group(1)) == episode_num:
                            target_movie = m
                            break
                
                if target_movie:
                    qualities = get_all_movie_qualities(target_movie[0])
                    await query.message.delete()
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"🎬 <b>Season {season_num} Episode {episode_num}</b>\n\n👇 Select Quality:",
                        reply_markup=create_quality_selection_keyboard(target_movie[0], target_movie[1], qualities),
                        parse_mode='HTML'
                    )
            return
        
        # Back to seasons
        if data.startswith("back_season_"):
            anchor_id = int(data.split("_")[2])
            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                cur.execute("SELECT id, title, url, file_id FROM movies WHERE id = %s", (anchor_id,))
                anchor_movie = cur.fetchone()
                cur.close()
                conn.close()
                if anchor_movie:
                    await query.message.delete()
                    await show_season_menu(context, chat_id, anchor_movie)
            return

    except Exception as e:
        logger.error(f"Button Callback Error: {e}")
        try:
            await query.edit_message_text("❌ Something went wrong. Please try again.")
        except:
            pass

# ==================== GROUP HANDLER (आपके example से) ====================
async def group_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text or update.message.from_user.is_bot:
        return
    text = update.message.text.strip()
    if len(text) < 4 or text.startswith('/'):
        return
    
    processed_query = preprocess_query(text)
    movies_found = get_movies_from_db(processed_query, limit=1)
    if not movies_found:
        return
    
    best_match = movies_found[0]
    movie_id = best_match[0]
    title = best_match[1]
    
    # Send group button (आपके example से)
    reply_text = f"@{update.effective_user.username}, 🎬 **{title}** के लिए नीचे का बटन क्लिक करें:"
    msg = await update.message.reply_text(
        reply_text,
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📂 Get File Here", url=f"https://t.me/{BOT_USERNAME}?start=movie_{movie_id}")]
        ])
    )
    asyncio.create_task(delete_messages_after_delay(context, update.effective_chat.id, [msg.message_id]))

# ==================== FLASK APP (आपके example से) ====================
flask_app = Flask('')

@flask_app.route('/')
def home():
    return "Bot is running!"

@flask_app.route('/health')
def health():
    return "OK", 200

@flask_app.route(f'/{UPDATE_SECRET_CODE}')
def trigger_update():
    # Add your Blogger sync logic here from your example
    return "Update initiated"

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    flask_app.run(host='0.0.0.0', port=port)

# ==================== MAIN FUNCTION ====================
def main():
    logger.info("Starting Netflix Style Movie Bot...")
    setup_database()

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).read_timeout(30).write_timeout(30).build()

    # Register handlers
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(CommandHandler('start', start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, search_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS, group_message_handler))

    # Start Flask
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()

    # Run bot
    application.run_polling()

if __name__ == '__main__':
    main()
