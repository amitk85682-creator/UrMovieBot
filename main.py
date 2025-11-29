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

# Bot Logo Image
BOT_LOGO_URL = "https://blogger.googleusercontent.com/img/b/R29vZ2xl/AVvXsEj35aShWJb06jx7Kz_v5hum9RJnhFF7DK1djZor59xWvCjBGRBh_NNjAgBi-IEhG5fSTPEt24gC9wsMVw_suit8hgmAC7SPbCwuh_gk4jywJlC2OCYJYvu6CoorlndlUITqBpIowR7xMA7AF-JQsponc_TUP1U95N2lobnUdK0W9kA9cGadqbRNNd1d5Fo/s1600/logo-design-for-flimfybox-a-cinematic-mo_OhkRefmbTCK6_RylGkOrAw_CtxTQGw_Tu6dY2kc64sagw.jpeg"

# Rate limiting
user_last_request = defaultdict(lambda: datetime.min)
REQUEST_COOLDOWN_MINUTES = int(os.environ.get('REQUEST_COOLDOWN_MINUTES', '10'))
SIMILARITY_THRESHOLD = int(os.environ.get('SIMILARITY_THRESHOLD', '80'))
MAX_REQUESTS_PER_MINUTE = int(os.environ.get('MAX_REQUESTS_PER_MINUTE', '10'))

# Message auto-delete time (seconds)
MESSAGE_DELETE_TIME = 300  # 5 minutes for normal messages
FILE_DELETE_TIME = 60  # 60 seconds for files

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
    stop_words = ['movie', 'film', 'full', 'download', 'watch', 'online', 'free']
    words = query.lower().split()
    words = [w for w in words if w not in stop_words]
    return ' '.join(words).strip()

async def check_rate_limit(user_id):
    """Check if user is rate limited"""
    now = datetime.now()
    last_request = user_last_request[user_id]
    if now - last_request < timedelta(seconds=2):
        return False
    user_last_request[user_id] = now
    return True

def _normalize_title_for_match(title: str) -> str:
    """Normalize title for fuzzy matching"""
    if not title:
        return ""
    t = re.sub(r'[^\w\s]', ' ', title)
    t = re.sub(r'\s+', ' ', t).strip()
    return t.lower()

def is_series(title):
    """Check if title is a series based on patterns"""
    series_patterns = [
        r'S\d+\s*E\d+',
        r'Season\s*\d+',
        r'Episode\s*\d+',
        r'EP?\s*\d+',
        r'Part\s*\d+',
        r'\d+x\d+',
    ]
    return any(re.search(pattern, title, re.IGNORECASE) for pattern in series_patterns)

def parse_series_info(title):
    """Parse series information from title"""
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

# ==================== AUTO-DELETE HELPER ====================
async def auto_delete_message(context, chat_id, message_id, delay=MESSAGE_DELETE_TIME):
    """Auto-delete a single message after delay"""
    try:
        await asyncio.sleep(delay)
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.error(f"Failed to auto-delete message {message_id}: {e}")

async def delete_messages_after_delay(context, chat_id, message_ids, delay=FILE_DELETE_TIME):
    """Delete multiple messages after delay"""
    try:
        await asyncio.sleep(delay)
        for msg_id in message_ids:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except Exception as e:
                logger.error(f"Failed to delete message {msg_id}: {e}")
    except Exception as e:
        logger.error(f"Error in delete_messages_after_delay: {e}")

# ==================== FORCE JOIN CHECK ====================
async def check_user_membership(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Check if user is member of required channel and group"""
    try:
        # Check channel membership
        channel_member = await context.bot.get_chat_member(chat_id=REQUIRED_CHANNEL_ID, user_id=user_id)
        channel_joined = channel_member.status in ['member', 'administrator', 'creator']
        
        # Check group membership
        group_member = await context.bot.get_chat_member(chat_id=REQUIRED_GROUP_ID, user_id=user_id)
        group_joined = group_member.status in ['member', 'administrator', 'creator']
        
        return channel_joined and group_joined
    except Exception as e:
        logger.error(f"Error checking membership for user {user_id}: {e}")
        return False

def get_force_join_keyboard():
    """Get keyboard for force join prompt"""
    keyboard = [
        [
            InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL),
            InlineKeyboardButton("💬 Join Group", url=FILMFYBOX_GROUP_URL)
        ],
        [InlineKeyboardButton("✅ I Joined, Check Again", callback_data="check_membership")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_main_menu_keyboard():
    """Get main menu keyboard for /start"""
    keyboard = [
        [
            InlineKeyboardButton("➕ Add to Group", url=f"https://t.me/{os.environ.get('BOT_USERNAME', 'your_bot')}?startgroup=true"),
            InlineKeyboardButton("📢 Channel", url=FILMFYBOX_CHANNEL_URL)
        ],
        [
            InlineKeyboardButton("💬 Discussion Group", url=FILMFYBOX_GROUP_URL),
            InlineKeyboardButton("ℹ️ About", callback_data="about_bot")
        ],
        [
            InlineKeyboardButton("❓ Help", callback_data="help_bot"),
            InlineKeyboardButton("🔍 Search Tips", callback_data="search_tips")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

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
        
        # Exact match (using ILIKE for case-insensitive LIKE)
        cur.execute(
            "SELECT id, title, url, file_id FROM movies WHERE LOWER(title) LIKE LOWER(%s) ORDER BY title LIMIT %s",
            (f'%{user_query}%', limit)
        )
        exact_matches = cur.fetchall()
        
        results = []

        if exact_matches:
            logger.info(f"Found {len(exact_matches)} ILIKE matches")
            for match in exact_matches:
                movie_id, title, url, file_id = match
                results.append((movie_id, title, url, file_id, is_series(title)))
            
            # If enough results are found, return them directly
            if len(results) >= limit:
                return results

        # Fuzzy matching (only if not enough results from ILIKE)
        if len(results) < limit:
            cur.execute("SELECT id, title, url, file_id FROM movies")
            all_movies = cur.fetchall()
            
            if not all_movies:
                return results

            # --- FIX IS HERE: Changed [movie] to [movie[1]] ---
            movie_titles = [movie[1] for movie in all_movies]
            movie_dict = {movie[1]: movie for movie in all_movies}

            # Search only for remaining slots
            fuzzy_limit = limit - len(results)
            matches = process.extract(user_query, movie_titles, scorer=fuzz.token_sort_ratio, limit=fuzzy_limit)

            # Keep track of IDs already added to avoid duplicates
            existing_ids = {r[0] for r in results} 

            for match in matches:
                # process.extract returns a tuple (matched_string, score)
                if len(match) >= 2:
                    title = match[0]
                    score = match[1]
                    
                    movie_data = movie_dict.get(title)
                    if score >= SIMILARITY_THRESHOLD and movie_data and movie_data[0] not in existing_ids:
                        # movie_data indices are: [0]=id, [1]=title, [2]=url, [3]=file_id
                        results.append((
                            movie_data[0],      # ID
                            movie_data[1],      # Title
                            movie_data[2],      # URL
                            movie_data[3],      # File ID
                            is_series(movie_data[1])
                        ))
                        existing_ids.add(movie_data[0])
                        if len(results) >= limit:
                            break
        
        return results
            
    except Exception as e:
        logger.error(f"Database query error: {e}")
        return []
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

def get_all_movie_qualities(movie_id):
    """Get all quality options for a movie"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
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
        
        if main_res and main_res and main_res.strip():
            final_results.append(('🎬 Watch Online', main_res.strip(), None, None))
        
        for quality, url, file_id, file_size in quality_results:
            final_results.append((quality, url, file_id, file_size))
        
        cur.close()
        conn.close()
        return final_results
    except Exception as e:
        logger.error(f"Error fetching qualities: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_series_episodes(base_title):
    """Get all episodes for a series"""
    conn = get_db_connection()
    if not conn:
        return {}
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, title FROM movies 
            WHERE title LIKE %s AND (
                title ~* 'S\\d+\\s*E\\d+' OR 
                title ~* 'Season\\s*\\d+' OR 
                title ~* 'Episode\\s*\\d+'
            )
            ORDER BY title
        """, (f'{base_title}%',))
        
        episodes = cur.fetchall()
        
        seasons = defaultdict(list)
        for ep_id, title in episodes:
            info = parse_series_info(title)
            if info['season']:
                seasons[info['season']].append({
                    'id': ep_id,
                    'title': title,
                    'episode': info['episode'] or 0
                })
        
        for season in seasons:
            seasons[season].sort(key=lambda x: x['episode'])
        
        cur.close()
        conn.close()
        return dict(seasons)
    except Exception as e:
        logger.error(f"Error getting series episodes: {e}")
        return {}
    finally:
        if conn:
            conn.close()

# ==================== PREMIUM KEYBOARDS ====================
def create_movie_selection_keyboard(movies, page=0, movies_per_page=5):
    """Create premium movie selection keyboard"""
    start_idx = page * movies_per_page
    end_idx = start_idx + movies_per_page
    current_movies = movies[start_idx:end_idx]
    
    keyboard = []
    
    for movie in current_movies:
        movie_id, title, url, file_id, is_series_flag = movie
        emoji = "🎭" if is_series_flag else "🎬"
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

def create_quality_selection_keyboard(movie_id, title, qualities):
    """Create premium quality selection keyboard"""
    keyboard = []
    
    for quality, url, file_id, file_size in qualities:
        size_text = f" • {file_size}" if file_size else ""
        link_type = "📱" if file_id else "🌐"
        button_text = f"{link_type} {quality}{size_text}"
        
        safe_quality = quality.replace(' ', '_').replace('/', '_')
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"quality_{movie_id}_{safe_quality}")])
    
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_selection")])
    
    return InlineKeyboardMarkup(keyboard)

def create_season_selection_keyboard(seasons_data, base_title):
    """Create season selection keyboard"""
    keyboard = []
    
    for season_num in sorted(seasons_data.keys()):
        episodes = seasons_data[season_num]
        button_text = f"📂 Season {season_num} • {len(episodes)} Episodes"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"season_{season_num}_{base_title[:30]}")])
    
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_selection")])
    
    return InlineKeyboardMarkup(keyboard)

def create_episode_selection_keyboard(episodes, season_num):
    """Create episode selection keyboard"""
    keyboard = []
    
    for ep in episodes:
        ep_num = ep.get('episode', 0)
        button_text = f"▶️ Episode {ep_num}" if ep_num else ep['title'][:40]
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"movie_{ep['id']}")])
    
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="cancel_selection")])
    
    return InlineKeyboardMarkup(keyboard)

# ==================== SEND MOVIE WITH AUTO-DELETE ====================
async def send_movie_file(update: Update, context: ContextTypes.DEFAULT_TYPE, title: str, url: Optional[str] = None, file_id: Optional[str] = None):
    """Send movie file with premium styling and auto-delete"""
    chat_id = update.effective_chat.id
    
    # Check membership first
    is_member = await check_user_membership(context, update.effective_user.id)
    if not is_member:
        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "🚫 **Access Denied**\n\n"
                "⚡ To unlock premium content, join:\n\n"
                "📢 **Channel:** [FilmfyBox](https://t.me/filmfybox)\n"
                "💬 **Group:** [FilmfyBox Chat](https://t.me/Filmfybox002)\n\n"
                "🎬 Unlimited movies & series await!"
            ),
            reply_markup=get_force_join_keyboard(),
            parse_mode='Markdown'
        )
        asyncio.create_task(auto_delete_message(context, chat_id, msg.message_id, MESSAGE_DELETE_TIME))
        return
    
    try:
        # Premium warning message
        warning_msg = await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "⚡ **Premium File Delivery**\n\n"
                "⏰ Auto-deletes in **60 seconds**\n"
                "💾 Forward to Saved Messages now!\n\n"
                "🎬 Enjoy your movie! ✨"
            ),
            parse_mode='Markdown'
        )
        
        # Premium caption with animated emojis
        caption = (
            f"🎬 **{title}**\n\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"⚡ **Premium Content**\n"
            f"📺 Quality: HD/Full HD\n"
            f"━━━━━━━━━━━━━━━━━\n\n"
            f"📢 [Join Channel]({FILMFYBOX_CHANNEL_URL})\n"
            f"💬 [Join Group]({FILMFYBOX_GROUP_URL})\n\n"
            f"⏰ Auto-delete: **60s** ⚡"
        )
        
        sent_msg = None
        
        if file_id:
            sent_msg = await context.bot.send_document(
                chat_id=chat_id,
                document=file_id,
                caption=caption,
                parse_mode='Markdown'
            )
        elif url and url.startswith("https://t.me/c/"):
            try:
                parts = url.rstrip('/').split('/')
                from_chat_id = int("-100" + parts[-2])
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
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("🎬 Watch Now", url=url)
                ]])
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🎬 **{title}**\n\n[Click to watch]({url})",
                    reply_markup=keyboard,
                    parse_mode='Markdown'
                )
                return
        elif url and url.startswith("https://t.me/") and "/c/" not in url:
            try:
                parts = url.rstrip('/').split('/')
                username = parts[-2].lstrip("@")
                message_id = int(parts[-1])
                sent_msg = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=f"@{username}",
                    message_id=message_id,
                    caption=caption,
                    parse_mode='Markdown'
                )
            except Exception as e:
                logger.error(f"Public copy failed: {e}")
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("🎬 Watch Now", url=url)
                ]])
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🎬 **{title}**\n\n[Click to watch]({url})",
                    reply_markup=keyboard,
                    parse_mode='Markdown'
                )
                return
        elif url:
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("🎬 Watch Now", url=url),
                InlineKeyboardButton("📢 Channel", url=FILMFYBOX_CHANNEL_URL)
            ]])
            msg = await context.bot.send_message(
                chat_id=chat_id,
                text=caption,
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
            asyncio.create_task(auto_delete_message(context, chat_id, msg.message_id, MESSAGE_DELETE_TIME))
            return
        else:
            msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Sorry, **{title}** is not available right now.",
                parse_mode='Markdown'
            )
            asyncio.create_task(auto_delete_message(context, chat_id, msg.message_id, MESSAGE_DELETE_TIME))
            return
        
        # Auto-delete after 60 seconds
        if sent_msg and warning_msg:
            asyncio.create_task(
                delete_messages_after_delay(
                    context,
                    chat_id,
                    [warning_msg.message_id, sent_msg.message_id],
                    FILE_DELETE_TIME
                )
            )
    
    except Exception as e:
        logger.error(f"Error sending file: {e}")
        msg = await context.bot.send_message(chat_id=chat_id, text="❌ Failed to send file.")
        asyncio.create_task(auto_delete_message(context, chat_id, msg.message_id, MESSAGE_DELETE_TIME))

# ==================== BOT HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Premium /start command with image and buttons"""
    user = update.effective_user
    
    # Check membership
    is_member = await check_user_membership(context, user.id)
    
    if not is_member:
        # Force join prompt
        msg = await context.bot.send_photo(
            chat_id=update.effective_chat.id,
            photo=BOT_LOGO_URL,
            caption=(
                f"👋 **Hey {user.first_name}!**\n\n"
                f"🎬 Welcome to **FilmfyBox Premium**\n\n"
                f"⚡ Your Netflix-style bot for:\n"
                f"🎭 **10,000+ Movies**\n"
                f"📺 **5,000+ Series**\n"
                f"🎥 **HD/Full HD Quality**\n\n"
                f"━━━━━━━━━━━━━━━━━\n"
                f"🚫 **Access Required**\n\n"
                f"To unlock premium content:\n"
                f"📢 Join our Channel\n"
                f"💬 Join our Group\n"
                f"━━━━━━━━━━━━━━━━━\n\n"
                f"⚡ Join now and start watching!"
            ),
            reply_markup=get_force_join_keyboard(),
            parse_mode='Markdown'
        )
        asyncio.create_task(auto_delete_message(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_TIME))
        return MAIN_MENU
    
    # Handle deep link
    if context.args and context.args.startswith("movie_"):
        try:
            movie_id = int(context.args.split('_')<!--citation:1-->)
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
    
    # Welcome message with premium image
    msg = await context.bot.send_photo(
        chat_id=update.effective_chat.id,
        photo=BOT_LOGO_URL,
        caption=(
            f"👋 **Hey {user.first_name}!**\n\n"
            f"⚡ Welcome to **FilmfyBox Premium** ⚡\n\n"
            f"🎬 Your personal Netflix! 🍿\n\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"📚 **10,000+ Movies**\n"
            f"📺 **5,000+ Series**\n"
            f"🎥 **HD Quality**\n"
            f"⚡ **Fast Delivery**\n"
            f"━━━━━━━━━━━━━━━━━\n\n"
            f"💡 **How to use:**\n"
            f"• Type movie/series name\n"
            f"• Select from results\n"
            f"• Choose quality\n"
            f"• Enjoy! ✨\n\n"
            f"🔍 **Examples:**\n"
            f"`Avengers Endgame`\n"
            f"`Stranger Things S01 E01`\n"
            f"`The Dark Knight`\n\n"
            f"⚡ Start searching now!"
        ),
        reply_markup=get_main_menu_keyboard(),
        parse_mode='Markdown'
    )
    asyncio.create_task(auto_delete_message(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_TIME))
    return MAIN_MENU

async def search_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Search movies/series handler with auto-delete"""
    try:
        # Rate limit check... (same as before)
        if not await check_rate_limit(update.effective_user.id):
            msg = await update.message.reply_text("⏰ Please wait before searching again.")
            await auto_delete_message(context, update.effective_chat.id, msg.message_id, 30)
            return MAIN_MENU
        
        user_message = update.message.text.strip()
        
        # Delete user's message
        try:
            await update.message.delete()
        except Exception:
            pass
        
        # Search in database
        movies_found = get_movies_from_db(user_message, limit=10)
        
        if not movies_found:
            # ... (Not found logic same as before) ...
            if update.effective_chat.type != telegram.constants.ChatType.PRIVATE:
                return MAIN_MENU
            
            msg = await context.bot.send_message(chat_id=update.effective_chat.id, text="😔 **Not Found!**", parse_mode='Markdown')
            await auto_delete_message(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_TIME)
            return MAIN_MENU
        
        # Single exact match case
        elif len(movies_found) == 1:
            movie_id, title, url, file_id, is_series_flag = movies_found[0]
            
            if is_series_flag:
                info = parse_series_info(title)
                base_title_to_use = info['base_title'] if info['base_title'] else title
                seasons_data = get_series_episodes(base_title_to_use)
                
                if seasons_data:
                    context.user_data['series_data'] = seasons_data
                    context.user_data['base_title'] = base_title_to_use
                    
                    # LOGIC FIX: Agar sirf 1 season hai, to direct episodes dikhao
                    if len(seasons_data) == 1:
                        season_num = list(seasons_data.keys())[0]
                        episodes = seasons_data[season_num]
                        msg = await context.bot.send_message(
                            chat_id=update.effective_chat.id,
                            text=f"📺 **{base_title_to_use}**\n\n⬇️ Select Episode:", # Direct Episode
                            reply_markup=create_episode_selection_keyboard(episodes, season_num),
                            parse_mode='Markdown'
                        )
                    else:
                        # Agar 1 se zyada seasons hain, tabhi season selection dikhao
                        msg = await context.bot.send_message(
                            chat_id=update.effective_chat.id,
                            text=f"📺 **{base_title_to_use}**\n\n⬇️ Select Season:",
                            reply_markup=create_season_selection_keyboard(seasons_data, base_title_to_use),
                            parse_mode='Markdown'
                        )
                    await auto_delete_message(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_TIME)
                    return MAIN_MENU
            
            # ... (Rest of the single movie logic remains same) ...
            qualities = get_all_movie_qualities(movie_id)
            if qualities and len(qualities) > 1:
                 # ... Quality selection ...
                 pass # (Code same as before)
            else:
                 # ... Send file ...
                 pass # (Code same as before)
        
        # Multiple matches case
        else:
            context.user_data['search_results'] = movies_found
            msg = await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"🔍 **Found {len(movies_found)} results**\n\n⬇️ Select one:",
                reply_markup=create_movie_selection_keyboard(movies_found),
                parse_mode='Markdown'
            )
            await auto_delete_message(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_TIME)
        
        return MAIN_MENU
        
    except Exception as e:
        logger.error(f"Error in search: {e}")
        return MAIN_MENU
async def group_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Silent group handler"""
    if not update.message or not update.message.text or update.message.from_user.is_bot:
        return
    
    message_text = update.message.text.strip()
    user = update.effective_user
    
    if len(message_text) < 4 or message_text.startswith('/'):
        return
    
    movies_found = get_movies_from_db(message_text, limit=1)
    
    if not movies_found:
        return
    
    movie_id, title, _, _, is_series_flag = movies_found
    score = fuzz.token_sort_ratio(_normalize_title_for_match(message_text), _normalize_title_for_match(title))
    
    if score < 85:
        return
    
    emoji = "📺" if is_series_flag else "🎬"
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton(f"✅ Get {emoji}", callback_data=f"group_get_{movie_id}_{user.id}")
    ]])
    
    try:
        reply_msg = await update.message.reply_text(
            f"Hey {user.mention_markdown()}! 👋\n\n"
            f"{emoji} **{title}**\n\n"
            f"⚡ Click to get in PM ⬇️",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        
        asyncio.create_task(
            delete_messages_after_delay(context, update.effective_chat.id, [reply_msg.message_id], 120)
        )
    except Exception as e:
        logger.error(f"Group prompt error: {e}")

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all button callbacks"""
    try:
        query = update.callback_query
        await query.answer()
        
        # ... (Previous code in button_callback) ...

        # Movie selection (from multiple search results)
        if query.data.startswith("select_"):
            movie_id = int(query.data.replace("select_", ""))
            
            conn = get_db_connection()
            if not conn:
                await query.edit_message_text("❌ Database error.")
                return
            
            try:
                cur = conn.cursor()
                cur.execute("SELECT title, url, file_id FROM movies WHERE id = %s", (movie_id,))
                result = cur.fetchone()
                cur.close()
                
                if not result:
                    await query.edit_message_text("❌ Movie not found!")
                    return
                
                title, url, file_id = result
                
                # Check for series structure
                if is_series(title):
                    info = parse_series_info(title)
                    base_title_to_use = info['base_title'] if info['base_title'] else title
                    seasons_data = get_series_episodes(base_title_to_use)
                    
                    if seasons_data:
                        context.user_data['series_data'] = seasons_data
                        context.user_data['base_title'] = base_title_to_use
                        
                        # LOGIC FIX: Check season count
                        if len(seasons_data) == 1:
                            # Skip Season Selection -> Show Episodes Directly
                            season_num = list(seasons_data.keys())[0]
                            episodes = seasons_data[season_num]
                            
                            await query.edit_message_text(
                                f"📺 **{base_title_to_use}**\n\n⬇️ Select Episode:",
                                reply_markup=create_episode_selection_keyboard(episodes, season_num),
                                parse_mode='Markdown'
                            )
                        else:
                            # Show Season Selection (Normal behavior)
                            await query.edit_message_text(
                                f"📺 **{base_title_to_use}**\n\n⬇️ Select Season:",
                                reply_markup=create_season_selection_keyboard(seasons_data, base_title_to_use),
                                parse_mode='Markdown'
                            )
                        return
                
                # ... (Rest of logic for Single Movie/Quality selection remains same) ...
                
            finally:
                if conn:
                    conn.close()
            return
        
        # About bot
        if query.data == "about_bot":
            await query.edit_message_caption(
                caption=(
                    "ℹ️ **About FilmfyBox**\n\n"
                    "⚡ Premium Telegram Bot for Movies & Series\n\n"
                    "📊 **Stats:**\n"
                    "🎬 10,000+ Movies\n"
                    "📺 5,000+ Series\n"
                    "🎥 HD/Full HD Quality\n"
                    "⚡ Fast Delivery\n\n"
                    "👨‍💻 **Developer:** @YourUsername\n"
                    "📢 **Channel:** @filmfybox\n"
                    "💬 **Group:** @Filmfybox002\n\n"
                    "⚡ Powered by AI & Premium Servers"
                ),
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Back", callback_data="back_to_start")
                ]]),
                parse_mode='Markdown'
            )
            return
        
        # Help bot
        if query.data == "help_bot":
            await query.edit_message_caption(
                caption=(
                    "❓ **How to Use**\n\n"
                    "**Step 1:** Join Channel & Group ✅\n"
                    "**Step 2:** Type movie/series name 🔍\n"
                    "**Step 3:** Select from results 📋\n"
                    "**Step 4:** Choose quality 🎥\n"
                    "**Step 5:** Enjoy! 🍿\n\n"
                    "━━━━━━━━━━━━━━━━━\n\n"
                    "**💡 Examples:**\n"
                    "`Avengers Endgame`\n"
                    "`Breaking Bad S01 E01`\n"
                    "`Inception 2010`\n\n"
                    "**📝 Tips:**\n"
                    "• Use correct spelling\n"
                    "• Add year for accuracy\n"
                    "• For series: Include S and E numbers\n\n"
                    "⚡ Need more help? Contact admin!"
                ),
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Back", callback_data="back_to_start")
                ]]),
                parse_mode='Markdown'
            )
            return
        
        # Search tips
        if query.data == "search_tips":
            await query.edit_message_text(
                text=(
                    "🔍 **Search Tips**\n\n"
                    "✅ **Good Examples:**\n"
                    "• `Inception 2010`\n"
                    "• `Breaking Bad S01 E01`\n"
                    "• `The Dark Knight`\n\n"
                    "❌ **Avoid:**\n"
                    "• Emojis (🎬, ❤️)\n"
                    "• Words like 'download', 'watch'\n"
                    "• Wrong spelling\n\n"
                    "💡 **Pro Tips:**\n"
                    "• Copy name from Google/IMDB\n"
                    "• Add release year\n"
                    "• For series: Use S01 E01 format\n\n"
                    "⚡ Happy searching!"
                ),
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Back", callback_data="cancel_selection")
                ]]),
                parse_mode='Markdown'
            )
            return
        
        # Back to start
        if query.data == "back_to_start":
            user = query.from_user
            await query.edit_message_caption(
                caption=(
                    f"👋 **Hey {user.first_name}!**\n\n"
                    f"⚡ Welcome to **FilmfyBox Premium** ⚡\n\n"
                    f"🎬 Your personal Netflix! 🍿\n\n"
                    f"━━━━━━━━━━━━━━━━━\n"
                    f"📚 **10,000+ Movies**\n"
                    f"📺 **5,000+ Series**\n"
                    f"🎥 **HD Quality**\n"
                    f"⚡ **Fast Delivery**\n"
                    f"━━━━━━━━━━━━━━━━━\n\n"
                    f"💡 **How to use:**\n"
                    f"• Type movie/series name\n"
                    f"• Select from results\n"
                    f"• Choose quality\n"
                    f"• Enjoy! ✨\n\n"
                    f"⚡ Start searching now!"
                ),
                reply_markup=get_main_menu_keyboard(),
                parse_mode='Markdown'
            )
            return
        
        # Group get
        if query.data.startswith("group_get_"):
            parts = query.data.split('_')
            movie_id = int(parts<!--citation:2-->)
            original_user_id = int(parts<!--citation:3-->)
            
            if query.from_user.id != original_user_id:
                await query.answer("This button is not for you!", show_alert=True)
                return
            
            is_member = await check_user_membership(context, original_user_id)
            if not is_member:
                await query.edit_message_text(
                    "🚫 **Join Required!**\n\n"
                    "Please join our Channel and Group first.",
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
                        
                        if is_series(title):
                            info = parse_series_info(title)
                            seasons_data = get_series_episodes(info['base_title'])
                            if seasons_data:
                                context.user_data['series_data'] = seasons_data
                                context.user_data['base_title'] = info['base_title']
                                
                                msg = await context.bot.send_message(
                                    chat_id=original_user_id,
                                    text=f"📺 **{info['base_title']}**\n\n⬇️ Select Season:",
                                    reply_markup=create_season_selection_keyboard(seasons_data, info['base_title']),
                                    parse_mode='Markdown'
                                )
                                asyncio.create_task(auto_delete_message(context, original_user_id, msg.message_id, MESSAGE_DELETE_TIME))
                                await query.edit_message_text("✅ Check your PM!")
                                return
                        
                        qualities = get_all_movie_qualities(movie_id)
                        if qualities and len(qualities) > 1:
                            msg = await context.bot.send_message(
                                chat_id=original_user_id,
                                text=f"🎬 **{title}**\n\n⬇️ Select Quality:",
                                reply_markup=create_quality_selection_keyboard(movie_id, title, qualities),
                                parse_mode='Markdown'
                            )
                            asyncio.create_task(auto_delete_message(context, original_user_id, msg.message_id, MESSAGE_DELETE_TIME))
                        else:
                            dummy_update = Update(
                                update_id=0,
                                message=telegram.Message(
                                    message_id=0,
                                    date=datetime.now(),
                                    chat=telegram.Chat(id=original_user_id, type='private')
                                )
                            )
                            await send_movie_file(dummy_update, context, title, url, file_id)
                        
                        await query.edit_message_text("✅ Check your PM!")
                        
            except telegram.error.Forbidden:
                bot_username = (await context.bot.get_me()).username
                deep_link = f"https://t.me/{bot_username}?start=movie_{movie_id}"
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("🤖 Start Bot", url=deep_link),
                    InlineKeyboardButton("🔄 Try Again", callback_data=query.data)
                ]])
                await query.edit_message_text(
                    "❌ **Can't send message!**\n\n"
                    "Please start the bot first, then try again.",
                    reply_markup=keyboard
                )
            return
        
        # Movie selection
        if query.data.startswith("select_"):
            movie_id = int(query.data.replace("select_", ""))
            
            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                cur.execute("SELECT title, url, file_id FROM movies WHERE id = %s", (movie_id,))
                result = cur.fetchone()
                cur.close()
                conn.close()
                
                if result:
                    title, url, file_id = result
                    
                    if is_series(title):
                        info = parse_series_info(title)
                        seasons_data = get_series_episodes(info['base_title'])
                        if seasons_data:
                            context.user_data['series_data'] = seasons_data
                            context.user_data['base_title'] = info['base_title']
                            
                            await query.edit_message_text(
                                f"📺 **{info['base_title']}**\n\n⬇️ Select Season:",
                                reply_markup=create_season_selection_keyboard(seasons_data, info['base_title']),
                                parse_mode='Markdown'
                            )
                            return
                    
                    qualities = get_all_movie_qualities(movie_id)
                    if qualities and len(qualities) > 1:
                        await query.edit_message_text(
                            f"🎬 **{title}**\n\n⬇️ Select Quality:",
                            reply_markup=create_quality_selection_keyboard(movie_id, title, qualities),
                            parse_mode='Markdown'
                        )
                    else:
                        await send_movie_file(update, context, title, url, file_id)
                        await query.edit_message_text("✅ Sent!")
            return
        
        # Season selection
        if query.data.startswith("season_"):
            parts = query.data.split('_', 2)
            season_num = int(parts<!--citation:1-->)
            
            seasons_data = context.user_data.get('series_data', {})
            episodes = seasons_data.get(season_num, [])
            
            if episodes:
                await query.edit_message_text(
                    f"📺 **Season {season_num}**\n\n⬇️ Select Episode:",
                    reply_markup=create_episode_selection_keyboard(episodes, season_num),
                    parse_mode='Markdown'
                )
            return
        
        # Episode/movie selection
        if query.data.startswith("movie_") and not query.data.startswith("movie_{"):
            movie_id = int(query.data.replace("movie_", ""))
            
            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                cur.execute("SELECT title, url, file_id FROM movies WHERE id = %s", (movie_id,))
                result = cur.fetchone()
                cur.close()
                conn.close()
                
                if result:
                    title, url, file_id = result
                    qualities = get_all_movie_qualities(movie_id)
                    
                    if qualities and len(qualities) > 1:
                        await query.edit_message_text(
                            f"🎬 **{title}**\n\n⬇️ Select Quality:",
                            reply_markup=create_quality_selection_keyboard(movie_id, title, qualities),
                            parse_mode='Markdown'
                        )
                    else:
                        await send_movie_file(update, context, title, url, file_id)
                        await query.edit_message_text("✅ Sent!")
            return
        
        # Quality selection
        if query.data.startswith("quality_"):
            parts = query.data.split('_', 2)
            movie_id = int(parts<!--citation:1-->)
            selected_quality = parts<!--citation:2-->.replace('_', ' ')
            
            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                
                cur.execute("SELECT title FROM movies WHERE id = %s", (movie_id,))
                title_res = cur.fetchone()
                title = title_res if title_res else "Movie"
                
                cur.execute("""
                    SELECT url, file_id FROM movie_files
                    WHERE movie_id = %s AND quality = %s
                """, (movie_id, selected_quality))
                file_data = cur.fetchone()
                
                if not file_data:
                    cur.execute("SELECT url, file_id FROM movies WHERE id = %s", (movie_id,))
                    file_data = cur.fetchone()
                
                cur.close()
                conn.close()
                
                if file_data:
                    url, file_id = file_data
                    await query.edit_message_text(f"⚡ Sending **{title}** [{selected_quality}]...", parse_mode='Markdown')
                    await send_movie_file(update, context, f"{title} [{selected_quality}]", url, file_id)
                else:
                    await query.edit_message_text("❌ File not found!")
            return
        
        # Page navigation
        if query.data.startswith("page_"):
            page = int(query.data.replace("page_", ""))
            movies = context.user_data.get('search_results', [])
            if movies:
                await query.edit_message_text(
                    f"🔍 **Found {len(movies)} results**\n\n⬇️ Select one:",
                    reply_markup=create_movie_selection_keyboard(movies, page),
                    parse_mode='Markdown'
                )
            return
        
        # Cancel
        if query.data == "cancel_selection":
            await query.edit_message_text("❌ Cancelled.")
            return
    
    except Exception as e:
        logger.error(f"Callback error: {e}")
        try:
            await query.answer("❌ Error occurred!", show_alert=True)
        except:
            pass

async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main menu handler"""
    return await search_movies(update, context)

# ==================== ERROR HANDLER ====================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Error handler"""
    logger.error(f"Exception: {context.error}", exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        try:
            msg = await update.effective_message.reply_text("❌ Something went wrong. Please try again.")
            asyncio.create_task(auto_delete_message(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_TIME))
        except:
            pass

# ==================== MAIN BOT ====================
def main():
    """Run the Telegram bot"""
    logger.info("Starting FilmfyBox Premium Bot...")
    
    # Setup database (use your existing function)
    try:
        from setup_database import setup_database
        setup_database()
    except:
        pass
    
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).read_timeout(30).write_timeout(30).build()
    
    # Conversation handler
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
    
    # Handlers
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS, group_message_handler))
    application.add_handler(conv_handler)
    
    # Add your existing admin command handlers here
    # application.add_handler(CommandHandler("addmovie", add_movie))
    # etc...
    
    application.add_error_handler(error_handler)
    
    # Keep your existing Flask setup
    # flask_thread = threading.Thread(target=run_flask)
    # flask_thread.daemon = True
    # flask_thread.start()
    
    logger.info("Bot started successfully! 🎬⚡")
    application.run_polling()

if __name__ == '__main__':
    main()
