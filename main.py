# -*- coding: utf-8 -*-
import os
import threading
import asyncio
import logging
import random
import json
import requests
import signal
import sys
import re
from bs4 import BeautifulSoup
import telegram
import psycopg2
from typing import Optional
from flask import Flask, request, session, g
import google.generativeai as genai
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup, ChatMember
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler,
    CallbackQueryHandler
)
from datetime import datetime, timedelta
from fuzzywuzzy import process, fuzz
from urllib.parse import urlparse, urlunparse, quote
from collections import defaultdict

# ==================== LOGGING SETUP ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== FLASK APP FOR PORT BINDING ====================
app = Flask(__name__)
PORT = int(os.environ.get('PORT', 5000))

@app.route('/')
def home():
    return "FilmfyBox Premium Bot is Running! 🎬"

@app.route('/health')
def health():
    return "OK", 200

def run_flask():
    """Run Flask server for port binding"""
    app.run(host='0.0.0.0', port=PORT, debug=False)

# ==================== CONVERSATION STATES ====================
MAIN_MENU, SEARCHING, REQUESTING = range(3)

# ==================== ENVIRONMENT VARIABLES ====================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DATABASE_URL = os.environ.get('DATABASE_URL')
BLOGGER_API_KEY = os.environ.get('BLOGGER_API_KEY')
BLOG_ID = os.environ.get('BLOG_ID')
UPDATE_SECRET_CODE = os.environ.get('UPDATE_SECRET_CODE', 'default_secret_123')
ADMIN_USER_ID = int(os.environ.get('ADMIN_USER_ID', 0))
GROUP_CHAT_ID = os.environ.get('GROUP_CHAT_ID')
ADMIN_CHANNEL_ID = os.environ.get('ADMIN_CHANNEL_ID')

# Force Join Settings
REQUIRED_CHANNEL_ID = os.environ.get('REQUIRED_CHANNEL_ID', '@filmfybox')
REQUIRED_GROUP_ID = os.environ.get('REQUIRED_GROUP_ID', '@Filmfybox002')
FILMFYBOX_CHANNEL_URL = 'https://t.me/filmfybox'
FILMFYBOX_GROUP_URL = 'https://t.me/Filmfybox002'

# Rate limiting
user_last_request = defaultdict(lambda: datetime.min)
REQUEST_COOLDOWN_MINUTES = int(os.environ.get('REQUEST_COOLDOWN_MINUTES', '10'))
SIMILARITY_THRESHOLD = int(os.environ.get('SIMILARITY_THRESHOLD', '80'))
MAX_REQUESTS_PER_MINUTE = int(os.environ.get('MAX_REQUESTS_PER_MINUTE', '10'))

# Auto delete delay (seconds) for normal bot messages
AUTO_DELETE_DELAY = int(os.environ.get('AUTO_DELETE_DELAY', '300'))  # default 5 minutes
AUTO_DELETE_FILE_DELAY = 60  # 60 seconds for files

# Store message IDs for deletion
message_deletion_tasks = {}

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
    try:
        query = re.sub(r'[^\w\s-]', '', query)
        query = ' '.join(query.split())
        stop_words = ['movie', 'film', 'full', 'download', 'watch', 'online', 'free']
        words = query.lower().split()
        words = [w for w in words if w not in stop_words]
        return ' '.join(words).strip()
    except Exception as e:
        logger.error(f"Error in preprocess_query: {e}")
        return query

async def check_rate_limit(user_id):
    """Check if user is rate limited"""
    try:
        now = datetime.now()
        last_request = user_last_request[user_id]
        if now - last_request < timedelta(seconds=2):
            return False
        user_last_request[user_id] = now
        return True
    except Exception as e:
        logger.error(f"Error in check_rate_limit: {e}")
        return True

def _normalize_title_for_match(title):
    """Normalize title for fuzzy matching"""
    try:
        if not title:
            return ""
        t = re.sub(r'[^\w\s]', ' ', title)
        t = re.sub(r'\s+', ' ', t).strip()
        return t.lower()
    except Exception as e:
        logger.error(f"Error in _normalize_title_for_match: {e}")
        return title.lower() if title else ""

def is_series(title):
    """Check if title is a series based on patterns"""
    try:
        series_patterns = [
            r'S\d+\s*E\d+',
            r'Season\s*\d+',
            r'Episode\s*\d+',
            r'EP?\s*\d+',
            r'Part\s*\d+',
            r'\d+x\d+',
        ]
        return any(re.search(pattern, title, re.IGNORECASE) for pattern in series_patterns)
    except Exception as e:
        logger.error(f"Error in is_series: {e}")
        return False

def parse_series_info(title):
    """Parse series information from title"""
    try:
        info = {
            'base_title': title,
            'season': None,
            'episode': None,
            'is_series': False
        }
        
        match = re.search(r'S(\d+)\s*E(\d+)', title, re.IGNORECASE)
        if match:
            info['season'] = int(match.group(1))
            info['episode'] = int(match.group(2))
            info['base_title'] = title[:match.start()].strip()
            info['is_series'] = True
            return info
        
        match = re.search(r'Season\s*(\d+).*Episode\s*(\d+)', title, re.IGNORECASE)
        if match:
            info['season'] = int(match.group(1))
            info['episode'] = int(match.group(2))
            info['base_title'] = re.sub(r'Season\s*\d+.*Episode\s*\d+', '', title, flags=re.IGNORECASE).strip()
            info['is_series'] = True
            
        return info
    except Exception as e:
        logger.error(f"Error in parse_series_info: {e}")
        return {'base_title': title, 'season': None, 'episode': None, 'is_series': False}

# ==================== AUTO DELETE SYSTEM ====================
async def auto_delete_message(context, chat_id, message_id, delay):
    """Delete a message after delay"""
    try:
        await asyncio.sleep(delay)
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            logger.info(f"Deleted message {message_id} in chat {chat_id}")
        except Exception as e:
            logger.debug(f"Could not delete message {message_id}: {e}")
    except Exception as e:
        logger.error(f"Error in auto_delete_message: {e}")

def schedule_message_deletion(context, chat_id, message_id, delay=None):
    """Schedule a message for deletion"""
    try:
        if delay is None:
            delay = AUTO_DELETE_DELAY
        
        # Create unique task key
        task_key = f"{chat_id}_{message_id}"
        
        # Cancel existing task if any
        if task_key in message_deletion_tasks:
            message_deletion_tasks[task_key].cancel()
        
        # Create new deletion task
        task = asyncio.create_task(auto_delete_message(context, chat_id, message_id, delay))
        message_deletion_tasks[task_key] = task
        
        logger.info(f"Scheduled deletion for message {message_id} in {delay} seconds")
    except Exception as e:
        logger.error(f"Error scheduling deletion: {e}")

# ==================== FORCE JOIN CHECK ====================
async def check_user_membership(context, user_id):
    """Check if user is member of required channel and group"""
    try:
        channel_member = await context.bot.get_chat_member(chat_id=REQUIRED_CHANNEL_ID, user_id=user_id)
        channel_joined = channel_member.status in ['member', 'administrator', 'creator']
        
        group_member = await context.bot.get_chat_member(chat_id=REQUIRED_GROUP_ID, user_id=user_id)
        group_joined = group_member.status in ['member', 'administrator', 'creator']
        
        return channel_joined and group_joined
    except Exception as e:
        logger.error(f"Error checking membership for user {user_id}: {e}")
        return False

def get_force_join_keyboard():
    """Get keyboard for force join prompt"""
    try:
        keyboard = [
            [InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL)],
            [InlineKeyboardButton("💬 Join Group", url=FILMFYBOX_GROUP_URL)],
            [InlineKeyboardButton("✅ I Joined, Check Again", callback_data="check_membership")]
        ]
        return InlineKeyboardMarkup(keyboard)
    except Exception as e:
        logger.error(f"Error creating force join keyboard: {e}")
        return None

# ==================== DATABASE CONNECTION ====================
def get_db_connection():
    """Get database connection"""
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

# ==================== MOVIE SEARCH WITH SERIES SUPPORT ====================
def get_movies_from_db(user_query, limit=10):
    """Search for movies/series in database"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return []
        
        cur = conn.cursor()
        logger.info(f"Searching for: '{user_query}'")
        
        cur.execute(
            "SELECT id, title, url, file_id FROM movies WHERE LOWER(title) LIKE LOWER(%s) ORDER BY title LIMIT %s",
            (f'%{user_query}%', limit)
        )
        exact_matches = cur.fetchall()
        
        if exact_matches:
            logger.info(f"Found {len(exact_matches)} exact matches")
            results = []
            for match in exact_matches:
                movie_id, title, url, file_id = match
                results.append((movie_id, title, url, file_id, is_series(title)))
            return results
        
        cur.execute("SELECT id, title, url, file_id FROM movies")
        all_movies = cur.fetchall()
        
        if not all_movies:
            return []
        
        movie_titles = [movie[1] for movie in all_movies]
        movie_dict = {movie[1]: movie for movie in all_movies}
        
        matches = process.extract(user_query, movie_titles, scorer=fuzz.token_sort_ratio, limit=limit)
        
        filtered_movies = []
        for match in matches:
            if len(match) >= 2:
                title, score = match[0], match[1]
                if score >= 65 and title in movie_dict:
                    movie_data = movie_dict[title]
                    filtered_movies.append((
                        movie_data[0],
                        movie_data[1],
                        movie_data[2],
                        movie_data[3],
                        is_series(movie_data[1])
                    ))
        
        return filtered_movies[:limit]
        
    except Exception as e:
        logger.error(f"Database query error: {e}")
        return []
    finally:
        if conn:
            try:
                cur.close()
                conn.close()
            except:
                pass

def get_all_movie_qualities(movie_id):
    """Get all quality options for a movie"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return []
        
        cur = conn.cursor()
        
        cur.execute("""
            SELECT quality, url, file_id, file_size
            FROM movie_files
            WHERE movie_id = %s AND (url IS NOT NULL OR file_id IS NOT NULL)
            ORDER BY CASE quality
                WHEN '4K' THEN 1
                WHEN '1080p' THEN 2
                WHEN 'HD Quality' THEN 3
                WHEN '720p' THEN 4
                WHEN '480p' THEN 5
                WHEN 'Standard Quality' THEN 6
                WHEN 'SD Quality' THEN 7
                ELSE 8
            END
        """, (movie_id,))
        
        quality_results = cur.fetchall()
        
        cur.execute("SELECT url FROM movies WHERE id = %s", (movie_id,))
        main_res = cur.fetchone()
        
        final_results = []
        
        if main_res and main_res[0] and main_res[0].strip():
            final_results.append(('📺 Stream / Watch Online', main_res[0].strip(), None, None))
        
        for quality, url, file_id, file_size in quality_results:
            final_results.append((quality, url, file_id, file_size))
        
        return final_results
    except Exception as e:
        logger.error(f"Error fetching qualities: {e}")
        return []
    finally:
        if conn:
            try:
                cur.close()
                conn.close()
            except:
                pass

def get_series_episodes(base_title):
    """Get all episodes for a series"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return {}
        
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, title FROM movies 
            WHERE title LIKE %s
            ORDER BY title
        """, (f'{base_title}%',))
        
        episodes = cur.fetchall()
        
        seasons = defaultdict(list)
        for ep_id, title in episodes:
            if is_series(title):
                info = parse_series_info(title)
                if info['season']:
                    seasons[info['season']].append({
                        'id': ep_id,
                        'title': title,
                        'episode': info['episode'] or 0
                    })
        
        for season in seasons:
            seasons[season].sort(key=lambda x: x['episode'])
        
        return dict(seasons)
    except Exception as e:
        logger.error(f"Error getting series episodes: {e}")
        return {}
    finally:
        if conn:
            try:
                cur.close()
                conn.close()
            except:
                pass

# ==================== KEYBOARD CREATION ====================
def create_movie_selection_keyboard(movies, page=0, movies_per_page=5):
    """Create Netflix-style movie selection keyboard"""
    try:
        start_idx = page * movies_per_page
        end_idx = start_idx + movies_per_page
        current_movies = movies[start_idx:end_idx]
        
        keyboard = []
        
        for movie in current_movies:
            movie_id, title, url, file_id, is_series_flag = movie
            emoji = "📺" if is_series_flag else "🎬"
            button_text = f"{emoji} {title}" if len(title) <= 35 else f"{emoji} {title[:32]}..."
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"select_{movie_id}")])
        
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️ Previous", callback_data=f"page_{page-1}"))
        if end_idx < len(movies):
            nav_buttons.append(InlineKeyboardButton("Next ▶️", callback_data=f"page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_selection")])
        
        return InlineKeyboardMarkup(keyboard)
    except Exception as e:
        logger.error(f"Error creating movie selection keyboard: {e}")
        return None

def create_quality_selection_keyboard(movie_id, title, qualities):
    """Create Netflix-style quality selection keyboard"""
    try:
        keyboard = []
        
        for quality, url, file_id, file_size in qualities:
            size_text = f" • {file_size}" if file_size else ""
            link_type = "📱" if file_id else "🔗"
            button_text = f"{link_type} {quality}{size_text}"
            
            safe_quality = quality.replace(' ', '_').replace('/', '_')
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"quality_{movie_id}_{safe_quality}")])
        
        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_selection")])
        
        return InlineKeyboardMarkup(keyboard)
    except Exception as e:
        logger.error(f"Error creating quality keyboard: {e}")
        return None

def create_season_selection_keyboard(seasons_data, base_title):
    """Create season selection keyboard for series"""
    try:
        keyboard = []
        
        for season_num in sorted(seasons_data.keys()):
            episodes = seasons_data[season_num]
            button_text = f"📂 Season {season_num} ({len(episodes)} episodes)"
            safe_title = base_title[:30] if base_title else "series"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"season_{season_num}_{safe_title}")])
        
        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_selection")])
        
        return InlineKeyboardMarkup(keyboard)
    except Exception as e:
        logger.error(f"Error creating season keyboard: {e}")
        return None

def create_episode_selection_keyboard(episodes, season_num):
    """Create episode selection keyboard"""
    try:
        keyboard = []
        
        for ep in episodes:
            ep_num = ep.get('episode', 0)
            button_text = f"▶️ Episode {ep_num}" if ep_num else ep['title'][:40]
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"movie_{ep['id']}")])
        
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="cancel_selection")])
        
        return InlineKeyboardMarkup(keyboard)
    except Exception as e:
        logger.error(f"Error creating episode keyboard: {e}")
        return None

# ==================== SEND MOVIE FILE ====================
async def send_movie_file(update, context, title, url=None, file_id=None):
    """Send movie file with auto-delete"""
    try:
        chat_id = update.effective_chat.id if update.effective_chat else None
        user_id = update.effective_user.id if update.effective_user else None
        
        if not chat_id:
            logger.error("No chat_id found")
            return
        
        is_member = await check_user_membership(context, user_id)
        if not is_member:
            access_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    "🚫 **Access Denied**\n\n"
                    "To watch movies, you must join our:\n"
                    "📢 Channel: @filmfybox\n"
                    "💬 Group: @Filmfybox002"
                ),
                reply_markup=get_force_join_keyboard(),
                parse_mode='Markdown'
            )
            schedule_message_deletion(context, chat_id, access_msg.message_id)
            return
        
        warning_msg = await context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ **File will auto-delete in 60 seconds!**\n\nPlease forward it to Saved Messages.",
            parse_mode='Markdown'
        )
        
        caption = (
            f"🎬 **{title}**\n\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"📢 Channel: @filmfybox\n"
            f"💬 Group: @Filmfybox002\n"
            f"━━━━━━━━━━━━━━━━━\n\n"
            f"⏰ Auto-delete in: 60 seconds"
        )
        
        sent_msg = None
        
        if file_id:
            sent_msg = await context.bot.send_document(
                chat_id=chat_id,
                document=file_id,
                caption=caption,
                parse_mode='Markdown'
            )
        elif url and url.startswith("https://t.me/"):
            try:
                if "/c/" in url:
                    parts = url.rstrip('/').split('/')
                    from_chat_id = int("-100" + parts[-2])
                    message_id = int(parts[-1])
                else:
                    parts = url.rstrip('/').split('/')
                    from_chat_id = f"@{parts[-2].lstrip('@')}"
                    message_id = int(parts[-1])
                
                sent_msg = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=from_chat_id,
                    message_id=message_id,
                    caption=caption,
                    parse_mode='Markdown'
                )
            except Exception as e:
                logger.error(f"Copy failed: {e}")
                link_msg = await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🔗 **{title}**\n\n[Click here to watch]({url})",
                    parse_mode='Markdown'
                )
                schedule_message_deletion(context, chat_id, warning_msg.message_id, AUTO_DELETE_FILE_DELAY)
                schedule_message_deletion(context, chat_id, link_msg.message_id, AUTO_DELETE_FILE_DELAY)
                return
        elif url:
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("🎬 Watch Now", url=url),
                InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL)
            ]])
            link_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=caption,
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
            schedule_message_deletion(context, chat_id, warning_msg.message_id, AUTO_DELETE_FILE_DELAY)
            schedule_message_deletion(context, chat_id, link_msg.message_id, AUTO_DELETE_FILE_DELAY)
            return
        else:
            nofile_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Sorry, no file available for **{title}**",
                parse_mode='Markdown'
            )
            schedule_message_deletion(context, chat_id, warning_msg.message_id, AUTO_DELETE_FILE_DELAY)
            schedule_message_deletion(context, chat_id, nofile_msg.message_id, AUTO_DELETE_FILE_DELAY)
            return
        
        if sent_msg and warning_msg:
            schedule_message_deletion(context, chat_id, warning_msg.message_id, AUTO_DELETE_FILE_DELAY)
            schedule_message_deletion(context, chat_id, sent_msg.message_id, AUTO_DELETE_FILE_DELAY)
    
    except Exception as e:
        logger.error(f"Error sending file: {e}")
        try:
            err_msg = await context.bot.send_message(
                chat_id=chat_id,
                text="❌ Failed to send file."
            )
            schedule_message_deletion(context, chat_id, err_msg.message_id)
        except:
            pass

# ==================== BOT HANDLERS ====================
async def start(update, context):
    """Start command - Premium Style"""
    try:
        if context.args and context.args[0].startswith("movie_"):
            try:
                movie_id = int(context.args[0].split('_')[1])
                conn = get_db_connection()
                if conn:
                    cur = conn.cursor()
                    cur.execute("SELECT title, url, file_id FROM movies WHERE id = %s", (movie_id,))
                    movie_data = cur.fetchone()
                    cur.close()
                    conn.close()
                    
                    if movie_data:
                        title, url, file_id = movie_data
                        await send_movie_file(update, context, title, url, file_id)
                        return MAIN_MENU
            except Exception as e:
                logger.error(f"Deep link error: {e}")
        
        chat_id = update.effective_chat.id
        bot_info = await context.bot.get_me()
        bot_username = bot_info.username

        start_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add Me To Your Group", url=f"https://t.me/{bot_username}?startgroup=true")],
            [
                InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL),
                InlineKeyboardButton("💬 Join Group", url=FILMFYBOX_GROUP_URL)
            ],
            [
                InlineKeyboardButton("ℹ️ Help", callback_data="start_help"),
                InlineKeyboardButton("👑 About", callback_data="start_about")
            ]
        ])

        start_caption = (
            "✨ **FilmfyBox Premium Bot** ✨\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🎬 Netflix‑style Movie & Series Bot\n"
            "🔍 Ultra‑fast search • Multi‑quality\n"
            "🛡 Auto‑delete privacy enabled\n"
            "📂 Seasons • Episodes • Clean UI\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "▶️ **Type any movie / series name to start...**\n"
            "`Avengers Endgame`\n"
            "`Stranger Things S01E01`\n"
            "`KGF 2 2022`"
        )

        banner_msg = await update.message.reply_photo(
            photo="https://blogger.googleusercontent.com/img/b/R29vZ2xl/AVvXsEj35aShWJb06jx7Kz_v5hum9RJnhFF7DK1djZor59xWvCjBGRBh_NNjAgBi-IEhG5fSTPEt24gC9wsMVw_suit8hgmAC7SPbCwuh_gk4jywJlC2OCYJYvu6CoorlndlUITqBpIowR7xMA7AF-JQsponc_TUP1U95N2lobnUdK0W9kA9cGadqbRNNd1d5Fo/s1600/logo-design-for-flimfybox-a-cinematic-mo_OhkRefmbTCK6_RylGkOrAw_CtxTQGw_Tu6dY2kc64sagw.jpeg",
            caption=start_caption,
            parse_mode='Markdown',
            reply_markup=start_keyboard
        )
        
        # Auto delete after delay
        schedule_message_deletion(context, chat_id, banner_msg.message_id, AUTO_DELETE_DELAY)
        
        return MAIN_MENU
    except Exception as e:
        logger.error(f"Error in start: {e}")
        return MAIN_MENU

async def search_movies(update, context):
    """Search movies/series handler"""
    try:
        chat_id = update.effective_chat.id
        
        if not await check_rate_limit(update.effective_user.id):
            msg = await update.message.reply_text("⏳ Please wait a moment before searching again.")
            schedule_message_deletion(context, chat_id, msg.message_id)
            return MAIN_MENU
        
        user_message = update.message.text.strip()
        movies_found = get_movies_from_db(user_message, limit=10)
        
        if not movies_found:
            if update.effective_chat.type != "private":
                return MAIN_MENU
            
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("🔍 Search Tips", callback_data="search_tips"),
                InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL)
            ]])
            
            msg = await update.message.reply_text(
                f"🚫 **No Results Found**\n\n"
                f"`{user_message}` is not in our collection yet.\n\n"
                f"💡 **Tips:**\n"
                f"• Check spelling\n"
                f"• Use full movie name\n"
                f"• Add year for better results",
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
            schedule_message_deletion(context, chat_id, msg.message_id)
            return MAIN_MENU
        
        elif len(movies_found) == 1:
            movie_id, title, url, file_id, is_series_flag = movies_found[0]
            
            if is_series_flag:
                info = parse_series_info(title)
                if info['is_series'] and info['base_title']:
                    seasons_data = get_series_episodes(info['base_title'])
                    if seasons_data:
                        context.user_data['series_data'] = seasons_data
                        context.user_data['base_title'] = info['base_title']
                        
                        msg = await update.message.reply_text(
                            f"📺 **{info['base_title']}**\n\nSelect Season ⬇️",
                            reply_markup=create_season_selection_keyboard(seasons_data, info['base_title']),
                            parse_mode='Markdown'
                        )
                        schedule_message_deletion(context, chat_id, msg.message_id)
                        return MAIN_MENU
            
            qualities = get_all_movie_qualities(movie_id)
            if qualities and len(qualities) > 1:
                msg = await update.message.reply_text(
                    f"🎬 **{title}**\n\nSelect Quality ⬇️",
                    reply_markup=create_quality_selection_keyboard(movie_id, title, qualities),
                    parse_mode='Markdown'
                )
                schedule_message_deletion(context, chat_id, msg.message_id)
            elif qualities:
                quality, url_q, file_id_q, _ = qualities[0]
                await send_movie_file(update, context, f"{title} [{quality}]", url_q or url, file_id_q or file_id)
            else:
                await send_movie_file(update, context, title, url, file_id)
        else:
            context.user_data['search_results'] = movies_found
            msg = await update.message.reply_text(
                f"🔍 **Found {len(movies_found)} results**\n\nSelect one ⬇️",
                reply_markup=create_movie_selection_keyboard(movies_found),
                parse_mode='Markdown'
            )
            schedule_message_deletion(context, chat_id, msg.message_id)
        
        return MAIN_MENU
    
    except Exception as e:
        logger.error(f"Error in search: {e}")
        if update.effective_chat.type == "private":
            try:
                msg = await update.message.reply_text("❌ Something went wrong. Please try again.")
                schedule_message_deletion(context, chat_id, msg.message_id)
            except:
                pass
        return MAIN_MENU

async def group_message_handler(update, context):
    """Silent group handler"""
    try:
        if not update.message or not update.message.text or update.message.from_user.is_bot:
            return
        
        message_text = update.message.text.strip()
        user = update.effective_user
        chat_id = update.effective_chat.id
        
        if len(message_text) < 4 or message_text.startswith('/'):
            return
        
        movies_found = get_movies_from_db(message_text, limit=1)
        
        if not movies_found:
            return
        
        movie_id, title, _, _, is_series_flag = movies_found[0]
        score = fuzz.token_sort_ratio(_normalize_title_for_match(message_text), _normalize_title_for_match(title))
        
        if score < 85:
            return
        
        emoji = "📺" if is_series_flag else "🎬"
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(f"✅ Get {emoji}", callback_data=f"group_get_{movie_id}_{user.id}")
        ]])
        
        reply_msg = await update.message.reply_text(
            f"Hey {user.mention_markdown()}! 👋\n\n"
            f"{emoji} **{title}**\n\n"
            f"Tap below to receive it in PM ⬇️",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        
        # Auto delete group prompts after 2 minutes
        schedule_message_deletion(context, chat_id, reply_msg.message_id, 120)
        
    except Exception as e:
        logger.error(f"Group handler error: {e}")

async def button_callback(update, context):
    """Handle all button callbacks"""
    try:
        query = update.callback_query
        await query.answer()
        chat_id = query.message.chat.id
        
        if query.data == "check_membership":
            is_member = await check_user_membership(context, query.from_user.id)
            if is_member:
                await query.edit_message_text(
                    "✅ **Access Granted!**\n\n"
                    "Welcome to FilmfyBox Premium! 🎬\n"
                    "You can now search for movies and series.",
                    parse_mode='Markdown'
                )
                schedule_message_deletion(context, chat_id, query.message.message_id)
            else:
                await query.answer("❌ Please join both Channel and Group first!", show_alert=True)
            return
        
        if query.data == "start_help":
            help_text = (
                "📖 **How to Use FilmfyBox**\n\n"
                "1️⃣ Type any movie / series name\n"
                "2️⃣ Choose the correct result\n"
                "3️⃣ Select your preferred quality\n"
                "4️⃣ File auto‑deletes in 60 sec\n\n"
                "💡 Examples:\n"
                "`Avengers Endgame`\n"
                "`Stranger Things S01E01`"
            )
            await query.edit_message_caption(caption=help_text, parse_mode='Markdown')
            return
        
        if query.data == "start_about":
            about_text = (
                "👑 **About FilmfyBox Premium**\n\n"
                "🎬 Auto‑organized Movies & Series\n"
                "🎞 Multiple quality options\n"
                "📂 Season & Episode navigation\n"
                "🛡 Full privacy with auto‑delete\n\n"
                "📢 @filmfybox\n"
                "💬 @Filmfybox002"
            )
            await query.edit_message_caption(caption=about_text, parse_mode='Markdown')
            return
        
        if query.data == "search_tips":
            tips_text = (
                "🔍 **Smart Search Tips**\n\n"
                "✅ Good Examples:\n"
                "• `Inception 2010`\n"
                "• `Breaking Bad S01E01`\n\n"
                "❌ Avoid:\n"
                "• Emojis\n"
                "• Extra words\n\n"
                "💡 Copy exact title from Google"
            )
            await query.edit_message_text(tips_text, parse_mode='Markdown')
            return
        
        # All other callbacks follow similar pattern...
        # [Rest of your callback handlers remain the same, just add schedule_message_deletion where needed]
        
        if query.data.startswith("group_get_"):
            parts = query.data.split('_')
            movie_id = int(parts[2])
            original_user_id = int(parts[3])
            
            if query.from_user.id != original_user_id:
                await query.answer("This button is not for you!", show_alert=True)
                return
            
            is_member = await check_user_membership(context, original_user_id)
            if not is_member:
                await query.edit_message_text(
                    "🚫 **Join Required!**\n\nPlease join our Channel and Group first.",
                    reply_markup=get_force_join_keyboard()
                )
                return
            
            try:
                conn = get_db_connection()
                if conn:
                    cur = conn.cursor()
                    cur.execute("SELECT title, url, file_id FROM movies WHERE id = %s", (movie_id,))
                    movie_data = cur.fetchone()
                    cur.close()
                    conn.close()
                    
                    if movie_data:
                        title, url, file_id = movie_data
                        qualities = get_all_movie_qualities(movie_id)
                        
                        if qualities and len(qualities) > 1:
                            pm_msg = await context.bot.send_message(
                                chat_id=original_user_id,
                                text=f"🎬 **{title}**\n\nSelect Quality ⬇️",
                                reply_markup=create_quality_selection_keyboard(movie_id, title, qualities),
                                parse_mode='Markdown'
                            )
                            schedule_message_deletion(context, original_user_id, pm_msg.message_id)
                        else:
                            dummy_update = Update(
                                update_id=0,
                                message=telegram.Message(
                                    message_id=0,
                                    date=datetime.now(),
                                    chat=telegram.Chat(id=original_user_id, type='private')
                                )
                            )
                            dummy_update._effective_user = query.from_user
                            await send_movie_file(dummy_update, context, title, url, file_id)
                        
                        await query.edit_message_text("✅ Check your PM!")
            except telegram.error.Forbidden:
                bot_username = (await context.bot.get_me()).username
                deep_link = f"https://t.me/{bot_username}?start=movie_{movie_id}"
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("🤖 Start Bot", url=deep_link)
                ]])
                await query.edit_message_text(
                    "❌ **Can't send message!**\n\nPlease start the bot first.",
                    reply_markup=keyboard
                )
            return
        
        if query.data == "cancel_selection":
            await query.edit_message_text("❌ Cancelled.")
            return
        
    except Exception as e:
        logger.error(f"Callback error: {e}")
        try:
            await query.answer("❌ Error occurred!", show_alert=True)
        except:
            pass

async def main_menu(update, context):
    """Main menu handler"""
    try:
        return await search_movies(update, context)
    except Exception as e:
        logger.error(f"Error in main_menu: {e}")
        return MAIN_MENU

async def error_handler(update, context):
    """Error handler"""
    try:
        logger.error(f"Exception: {context.error}", exc_info=context.error)
        if isinstance(update, Update) and update.effective_message:
            try:
                msg = await update.effective_message.reply_text("❌ Something went wrong. Please try again.")
                schedule_message_deletion(context, update.effective_chat.id, msg.message_id)
            except:
                pass
    except:
        pass

# ==================== MAIN BOT ====================
def main():
    """Run the Telegram bot"""
    try:
        logger.info("Starting FilmfyBox Premium Bot...")
        
        # Start Flask server in background thread for port binding
        flask_thread = threading.Thread(target=run_flask)
        flask_thread.daemon = True
        flask_thread.start()
        logger.info(f"Flask server started on port {PORT}")
        
        application = Application.builder().token(TELEGRAM_BOT_TOKEN).read_timeout(30).write_timeout(30).build()
        
        conv_handler = ConversationHandler(
            entry_points=[CommandHandler('start', start, filters=filters.ChatType.PRIVATE)],
            states={
                MAIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, main_menu)],
                SEARCHING: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, search_movies)],
            },
            fallbacks=[CommandHandler('cancel', lambda u, c: u.message.reply_text("Cancelled."))],
            per_message=False,
            per_chat=True,
        )
        
        application.add_handler(CallbackQueryHandler(button_callback))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS, group_message_handler))
        application.add_handler(conv_handler)
        application.add_error_handler(error_handler)
        
        logger.info("Bot started successfully! 🎬")
        application.run_polling()
        
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
