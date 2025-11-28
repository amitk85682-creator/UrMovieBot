# Add this snippet near the top of main.py, after your imports:
try:
    # prefer db_utils' fixed URL if it exists
    import db_utils
    FIXED_DATABASE_URL = getattr(db_utils, "FIXED_DATABASE_URL", None)
except Exception:
    FIXED_DATABASE_URL = None

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
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
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
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.environ.get('DATABASE_URL')
BLOGGER_API_KEY = os.environ.get('BLOGGER_API_KEY')
BLOG_ID = os.environ.get('BLOG_ID')
UPDATE_SECRET_CODE = os.environ.get('UPDATE_SECRET_CODE', 'default_secret_123')
ADMIN_USER_ID = int(os.environ.get('ADMIN_USER_ID', 0))

# Fixed Links as per your requirement
BOT_NAME = "Ur Movie Bot"
CHANNEL_USERNAME = "filmfybox"
CHANNEL_LINK = "https://t.me/filmfybox"
GROUP_LINK = "https://t.me/Filmfybox002"
GROUP_USERNAME = "Filmfybox002"
START_IMAGE_URL = "https://blogger.googleusercontent.com/img/b/R29vZ2xl/AVvXsEhYD6_-uyyYg_YxJMkk06sbRQ5N-IH7HFjr3P1AYZLiQ6qSp3Ap_FgRWGjCKk6okFRh0bRTH5-TtrizBxsQpjxR6bdnNidTjiT-ICWhqaC0xcEJs89bSOTwrzBAMFYtWAv48llz96Ye9E3Q3vEHrtk1id8aceQbp_uxAJ4ASqZIEsK5FcaMYcrhj45i70c/s320/logo-design-for-flimfybox-a-cinematic-mo_OhkRefmbTCK6_RylGkOrAw_CtxTQGw_Tu6dY2kc64sagw.jpeg"

# Configuration
SIMILARITY_THRESHOLD = 85
AUTO_DELETE_DELAY = 60  # Seconds for auto delete all bot messages
MOVIES_PER_PAGE = 5

# Validate required environment variables
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN environment variable is not set")
    raise ValueError("TELEGRAM_BOT_TOKEN is not set.")

if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable is not set")
    raise ValueError("DATABASE_URL is not set.")

# ==================== UTILITY FUNCTIONS ====================
def preprocess_query(query):
    """Clean and normalize user query"""
    query = re.sub(r'[^\w\s-]', '', query)
    query = ' '.join(query.split())
    stop_words = ['movie', 'film', 'full', 'download', 'watch', 'online', 'free', 'फिल्म', 'मूवी', 'सीरीज']
    words = query.lower().split()
    words = [w for w in words if w not in stop_words]
    return ' '.join(words).strip()

def _normalize_title_for_match(title: str) -> str:
    """Normalize title for fuzzy matching"""
    if not title:
        return ""
    t = re.sub(r'[^\w\s]', ' ', title)
    t = re.sub(r'\s+', ' ', t).strip()
    return t.lower()

async def delete_message_after_delay(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay: int = AUTO_DELETE_DELAY):
    """Auto delete message after specified delay"""
    try:
        await asyncio.sleep(delay)
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info(f"Deleted message {message_id} from chat {chat_id}")
    except Exception as e:
        logger.error(f"Failed to delete message {message_id}: {e}")

# ==================== DATABASE FUNCTIONS ====================
def setup_database():
    """Setup minimal database tables"""
    try:
        conn_str = FIXED_DATABASE_URL or DATABASE_URL
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()

        # Enable pg_trgm extension
        cur.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm;')

        # Create only required movies table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS movies (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL UNIQUE,
                url TEXT NOT NULL,
                file_id TEXT
            )
        ''')

        # Create sync info table
        cur.execute('CREATE TABLE IF NOT EXISTS sync_info (id SERIAL PRIMARY KEY, last_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP);')

        # Indexes
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movies_title ON movies (title);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movies_title_trgm ON movies USING gin (title gin_trgm_ops);')

        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database setup completed successfully")
    except Exception as e:
        logger.error(f"Error setting up database: {e}")

def get_db_connection():
    """Get database connection with error handling"""
    try:
        conn_str = FIXED_DATABASE_URL or DATABASE_URL
        if not conn_str:
            logger.error("No database URL configured.")
            return None
        return psycopg2.connect(conn_str)
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

def update_movies_in_db():
    """Update movies from Blogger API (only read, no request handling)"""
    logger.info("Starting movie update process...")
    setup_database()

    conn = None
    cur = None
    new_movies_added = 0

    try:
        conn = get_db_connection()
        if not conn:
            return "Database connection failed"

        cur = conn.cursor()

        # Get last sync time
        cur.execute("SELECT last_sync FROM sync_info ORDER BY id DESC LIMIT 1;")
        last_sync_result = cur.fetchone()
        last_sync_time = last_sync_result[0] if last_sync_result else None

        cur.execute("SELECT title FROM movies;")
        existing_movies = {row[0] for row in cur.fetchall()}

        if not BLOGGER_API_KEY or not BLOG_ID:
            return "Blogger API keys not configured"

        from googleapiclient.discovery import build
        service = build('blogger', 'v3', developerKey=BLOGGER_API_KEY)
        all_items = []

        # Fetch all posts
        posts_request = service.posts().list(blogId=BLOG_ID, maxResults=500)
        while posts_request is not None:
            posts_response = posts_request.execute()
            all_items.extend(posts_response.get('items', []))
            posts_request = service.posts().list_next(posts_request, posts_response)

        # Fetch all pages
        pages_request = service.pages().list(blogId=BLOG_ID)
        pages_response = pages_request.execute()
        all_items.extend(pages_response.get('items', []))

        unique_titles = set()
        for item in all_items:
            title = item.get('title')
            url = item.get('url')

            if last_sync_time and 'published' in item:
                try:
                    published_time = datetime.strptime(item['published'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    if published_time < last_sync_time:
                        continue
                except:
                    pass

            if title and url and title.strip() not in existing_movies and title.strip() not in unique_titles:
                try:
                    cur.execute("INSERT INTO movies (title, url) VALUES (%s, %s);", (title.strip(), url.strip()))
                    new_movies_added += 1
                    unique_titles.add(title.strip())
                except psycopg2.Error as e:
                    logger.error(f"Error inserting movie {title}: {e}")
                    conn.rollback()
                    continue

        # Update sync time
        cur.execute("INSERT INTO sync_info (last_sync) VALUES (CURRENT_TIMESTAMP);")

        conn.commit()
        return f"Update complete. Added {new_movies_added} new items."

    except Exception as e:
        logger.error(f"Error during movie update: {e}")
        if conn:
            conn.rollback()
        return f"An error occurred during update: {e}"

    finally:
        if cur: cur.close()
        if conn: conn.close()

def get_movie_from_db(user_query):
    """Search for exact/fuzzy match movie in database"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return None

        cur = conn.cursor()
        processed_query = preprocess_query(user_query)
        logger.info(f"Searching for: '{processed_query}'")

        # Exact match first
        cur.execute(
            "SELECT id, title, url, file_id FROM movies WHERE LOWER(title) LIKE LOWER(%s) LIMIT 1",
            (f'%{processed_query}%',)
        )
        exact_match = cur.fetchone()

        if exact_match:
            cur.close()
            conn.close()
            return exact_match

        # Fuzzy match with high threshold
        cur.execute("SELECT id, title, url, file_id FROM movies")
        all_movies = cur.fetchall()

        if not all_movies:
            cur.close()
            conn.close()
            return None

        movie_titles = [movie[1] for movie in all_movies]
        movie_dict = {movie[1]: movie for movie in all_movies}

        matches = process.extract(processed_query, movie_titles, scorer=fuzz.token_sort_ratio, limit=1)
        if matches and len(matches) > 0:
            title, score = matches[0][0], matches[0][1]
            if score >= SIMILARITY_THRESHOLD and title in movie_dict:
                cur.close()
                conn.close()
                return movie_dict[title]

        cur.close()
        conn.close()
        return None

    except Exception as e:
        logger.error(f"Database query error: {e}")
        return None
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

# ==================== KEYBOARD MARKUPS ====================
def get_start_keyboard():
    """Start menu keyboard exactly as per your image"""
    keyboard = [
        [
            InlineKeyboardButton("➕ Add Me To Your Groups ➕", url=f"https://t.me/{(os.environ.get('BOT_USERNAME') or 'urmoviebot')}?startgroup=true")
        ],
        [
            InlineKeyboardButton("📢 CHANNEL", url=CHANNEL_LINK),
            InlineKeyboardButton("👥 GROUP", url=GROUP_LINK)
        ],
        [
            InlineKeyboardButton("❓ HELP", callback_data="help"),
            InlineKeyboardButton("ℹ️ ABOUT", callback_data="about")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_join_prompt_keyboard():
    """Keyboard for join channel/group prompt"""
    keyboard = [
        [
            InlineKeyboardButton("📢 Join Channel", url=CHANNEL_LINK),
            InlineKeyboardButton("👥 Join Group", url=GROUP_LINK)
        ],
        [
            InlineKeyboardButton("🔄 Check Membership", callback_data="check_membership")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_group_movie_button(movie_id: int):
    """Inline button for group: 📂 Get File Here"""
    bot_username = os.environ.get('BOT_USERNAME') or 'urmoviebot'
    deep_link = f"https://t.me/{bot_username}?start=movie_{movie_id}"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📂 Get File Here", url=deep_link)]
    ])
    return keyboard

def get_file_options_keyboard():
    """Keyboard with Join Channel button for every file"""
    keyboard = [
        [
            InlineKeyboardButton("🔗 Join Channel", url=CHANNEL_LINK),
            InlineKeyboardButton("👥 Join Group", url=GROUP_LINK)
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

# ==================== MOVIE DELIVERY FUNCTIONS ====================
async def send_movie_to_user(context: ContextTypes.DEFAULT_TYPE, user_id: int, movie_data: tuple):
    """Send movie file/link to user's PM with caption and buttons"""
    try:
        movie_id, title, url, file_id = movie_data
        chat_id = user_id

        # Warning message (auto-delete)
        warning_msg = await context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ ❌👉This file automatically❗️delete after 1 minute❗️so please forward in another chat👈❌",
            parse_mode='Markdown'
        )
        asyncio.create_task(delete_message_after_delay(context, chat_id, warning_msg.message_id))

        sent_msg = None
        # Caption as per your requirement
        caption_text = (
            f"🎬 <b>{title}</b>\n\n"
            "🔗 <b>JOIN »</b> <a href='http://t.me/filmfybox'>FilmfyBox</a>\n\n"
            "🔹 <b>Please drop the movie name, and I’ll find it for you as soon as possible. 🎬✨👇</b>\n"
            "🔹 <b><a href='https://t.me/Filmfybox002'>FlimfyBox Chat</a></b>"
        )

        # Send file if file_id exists
        if file_id:
            sent_msg = await context.bot.send_document(
                chat_id=chat_id,
                document=file_id,
                caption=caption_text,
                parse_mode='HTML',
                reply_markup=get_file_options_keyboard()
            )
        
        # Send via URL if it's a Telegram channel link
        elif url and url.startswith("https://t.me/c/"):
            try:
                parts = url.rstrip('/').split('/')
                from_chat_id = int("-100" + parts[-2])
                message_id = int(parts[-1])
                sent_msg = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=from_chat_id,
                    message_id=message_id,
                    caption=caption_text,
                    parse_mode='HTML',
                    reply_markup=get_file_options_keyboard()
                )
            except Exception as e:
                logger.error(f"Failed to copy channel message: {e}")
                sent_msg = await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🎬 {title}\n\n{caption_text}",
                    parse_mode='HTML',
                    reply_markup=get_file_options_keyboard()
                )
        
        # Send normal URL
        else:
            sent_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"🎬 {title}\n\n{caption_text}",
                parse_mode='HTML',
                reply_markup=get_file_options_keyboard()
            )

        # Auto delete the movie message
        if sent_msg:
            asyncio.create_task(delete_message_after_delay(context, chat_id, sent_msg.message_id))

    except telegram.error.Forbidden:
        logger.error(f"User {user_id} blocked the bot")
    except Exception as e:
        logger.error(f"Error sending movie to user: {e}")

# ==================== TELEGRAM BOT HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler - with deep link support for movie delivery"""
    user = update.effective_user
    chat_id = update.effective_chat.id

    # Handle deep link for movie delivery
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
                    await send_movie_to_user(context, chat_id, movie_data)
                else:
                    msg = await update.message.reply_text("❌ मूवी डेटाबेस में नहीं मिली।")
                    asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id))
        except Exception as e:
            logger.error(f"Error processing deep link: {e}")
            msg = await update.message.reply_text("❌ कुछ गलत हुआ। फिर से कोशिश करें।")
            asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id))
        return

    # Send start message with image and buttons
    start_caption = f"""
👋 Hey {user.first_name}!,

🤖 I'm **{BOT_NAME}**
✅ POWERFUL AUTO-FILTER BOT...

💡 YOU CAN USE ME AS AUTO-FILTER IN YOUR GROUP....
ITS EASY TO USE: JUST ADD ME TO YOUR GROUP AS ADMIN,
THATS ALL, I WILL PROVIDE MOVIES THERE.... 😊

⚠️ MORE HELP CHECK HELP BUTTON....

© MAINTAINED BY: FlimfyBox Team 🚀
    """
    
    # Send start image with caption and buttons
    try:
        msg = await update.message.reply_photo(
            photo=START_IMAGE_URL,
            caption=start_caption,
            parse_mode='Markdown',
            reply_markup=get_start_keyboard()
        )
        asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id))
    except Exception as e:
        logger.error(f"Error sending start message: {e}")
        # Fallback text message
        msg = await update.message.reply_text(
            start_caption,
            parse_mode='Markdown',
            reply_markup=get_start_keyboard()
        )
        asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id))

async def group_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all group messages - only reply if movie exists"""
    if not update.message or not update.message.text or update.message.from_user.is_bot:
        return

    message_text = update.message.text.strip()
    user = update.effective_user
    chat_id = update.effective_chat.id

    # Ignore short messages and commands
    if len(message_text) < 4 or message_text.startswith('/'):
        return

    # Search for movie
    movie_data = get_movie_from_db(message_text)
    if not movie_data:
        # No data? Do NOT reply anything - just read
        return

    # If movie found: Send "📂 Get File Here" button
    movie_id, title, _, _ = movie_data
    reply_text = f"@{user.username}, 🎬 **{title}** के लिए नीचे का बटन क्लिक करें:"
    
    msg = await update.message.reply_text(
        reply_text,
        parse_mode='Markdown',
        reply_markup=get_group_movie_button(movie_id)
    )
    # Auto delete this reply after delay
    asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id))

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline button callbacks"""
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id

    try:
        if query.data == "help":
            help_text = """
❓ **Help - कैसे उपयोग करें?**

1. ग्रुप में बस मूवी का नाम टाइप करें
2. बॉट आपको "📂 Get File Here" बटन देगा
3. बटन पर क्लिक करें - आप बॉट के प्राइवेट चैट में जाएंगे
4. वहां आपको मूवी की फ़ाइल मिल जाएगी

⚠️ नोट:
- मूवी का नाम सही लिखें
- सभी संदेश 1 मिनट के बाद ऑटो डिलीट हो जाते हैं
            """
            msg = await query.edit_message_caption(
                caption=help_text,
                parse_mode='Markdown',
                reply_markup=get_start_keyboard()
            )
            asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id))

        elif query.data == "about":
            about_text = """
ℹ️ **About {BOT_NAME}**

यह बॉट आपको ग्रुप में बस मूवी का नाम टाइप करने पर मूवी की फ़ाइल प्रदान करता है।

✅ फीचर्स:
- ग्रुप से प्राइवेट चैट में फ़ाइल भेजना
- सभी संदेश ऑटो डिलीट
- उच्च सटीकता वाला सearch

📢 चैनल: {CHANNEL_LINK}
👥 ग्रुप: {GROUP_LINK}
© MAINTAINED BY: FlimfyBox Team
            """.format(BOT_NAME=BOT_NAME, CHANNEL_LINK=CHANNEL_LINK, GROUP_LINK=GROUP_LINK)
            msg = await query.edit_message_caption(
                caption=about_text,
                parse_mode='Markdown',
                reply_markup=get_start_keyboard()
            )
            asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id))

        elif query.data == "check_membership":
            # Always return True — skip membership check (no admin required)
            msg = await query.edit_message_text(
                text="✅ आपको चैनल और ग्रुप में जॉइन होने का स्टेटस कन्फर्म हुआ!",
                reply_markup=None
            )
            asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id))

    except Exception as e:
        logger.error(f"Error in button callback: {e}")
        try:
            await query.edit_message_text("❌ कुछ गलत हुआ। फिर से कोशिश करें।")
        except:
            pass

# ==================== ERROR HANDLER ====================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Log errors gracefully"""
    logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)

# ==================== FLASK APP ====================
flask_app = Flask('')

@flask_app.route('/')
def home():
    return f"{BOT_NAME} is running!"

@flask_app.route('/health')
def health():
    return "OK", 200

@flask_app.route(f'/{UPDATE_SECRET_CODE}')
def trigger_update():
    result = update_movies_in_db()
    return result

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    flask_app.run(host='0.0.0.0', port=port)

# ==================== MAIN BOT FUNCTION ====================
def main():
    """Run the Telegram bot"""
    logger.info(f"{BOT_NAME} is starting...")

    try:
        setup_database()
    except Exception as e:
        logger.error(f"Database setup failed but continuing: {e}")

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).read_timeout(30).write_timeout(30).build()

    # Register handlers
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(CommandHandler('start', start))
    
    # Group message handler - priority over other handlers
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS,
        group_message_handler
    ))

    # Private chat handler: Search movies when user sends text
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,
        lambda update, context: asyncio.create_task(
            send_movie_to_user(context, update.effective_chat.id, get_movie_from_db(update.message.text))
            if get_movie_from_db(update.message.text) else None
        )
    ))

    application.add_error_handler(error_handler)

    # Start Flask in background thread
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    logger.info("Flask server started in background.")

    # Run the bot
    logger.info("Starting bot polling...")
    application.run_polling()

if __name__ == '__main__':
    main()
