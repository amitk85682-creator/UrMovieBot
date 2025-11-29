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
from googleapiclient.discovery import build
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
REQUIRED_CHANNEL_ID = os.environ.get('REQUIRED_CHANNEL_ID', '@filmfybox')  # @filmfybox
REQUIRED_GROUP_ID = os.environ.get('REQUIRED_GROUP_ID', '@Filmfybox002')  # @Filmfybox002
FILMFYBOX_CHANNEL_URL = 'https://t.me/filmfybox'
FILMFYBOX_GROUP_URL = 'https://t.me/Filmfybox002'

# Rate limiting
user_last_request = defaultdict(lambda: datetime.min)
REQUEST_COOLDOWN_MINUTES = int(os.environ.get('REQUEST_COOLDOWN_MINUTES', '10'))
SIMILARITY_THRESHOLD = int(os.environ.get('SIMILARITY_THRESHOLD', '80'))
MAX_REQUESTS_PER_MINUTE = int(os.environ.get('MAX_REQUESTS_PER_MINUTE', '10'))

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

def normalize_url(url):
    """Normalize and clean URLs"""
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        if '#' in url:
            base, anchor = url.split('#', 1)
            parsed = urlparse(base)
            normalized_base = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, parsed.query, ''))
            url = f"{normalized_base}#{anchor}"
        else:
            parsed = urlparse(url)
            url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
        return url
    except:
        return url

def _normalize_title_for_match(title: str) -> str:
    """Normalize title for fuzzy matching"""
    if not title:
        return ""
    t = re.sub(r'[^\w\s]', ' ', title)
    t = re.sub(r'\s+', ' ', t).strip()
    return t.lower()

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
        [InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL)],
        [InlineKeyboardButton("💬 Join Group", url=FILMFYBOX_GROUP_URL)],
        [InlineKeyboardButton("✅ I Joined, Check Again", callback_data="check_membership")]
    ]
    return InlineKeyboardMarkup(keyboard)

# ==================== DATABASE FUNCTIONS ====================
def setup_database():
    """Setup database tables and indexes"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        cur.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm;')
        
        # Movies table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS movies (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL UNIQUE,
                url TEXT,
                file_id TEXT,
                is_series BOOLEAN DEFAULT FALSE,
                total_seasons INTEGER DEFAULT 0
            )
        ''')
        
        # Series Seasons table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS series_seasons (
                id SERIAL PRIMARY KEY,
                movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE,
                season_number INTEGER NOT NULL,
                season_name TEXT,
                has_complete_pack BOOLEAN DEFAULT FALSE,
                complete_pack_file_id TEXT,
                complete_pack_url TEXT,
                UNIQUE(movie_id, season_number)
            )
        ''')
        
        # Series Episodes table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS series_episodes (
                id SERIAL PRIMARY KEY,
                season_id INTEGER REFERENCES series_seasons(id) ON DELETE CASCADE,
                episode_number INTEGER NOT NULL,
                episode_name TEXT,
                UNIQUE(season_id, episode_number)
            )
        ''')
        
        # Movie/Episode Quality Files
        cur.execute('''
            CREATE TABLE IF NOT EXISTS movie_files (
                id SERIAL PRIMARY KEY,
                movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE,
                episode_id INTEGER REFERENCES series_episodes(id) ON DELETE CASCADE,
                quality TEXT NOT NULL,
                url TEXT,
                file_id TEXT,
                file_size TEXT,
                CHECK (movie_id IS NOT NULL OR episode_id IS NOT NULL)
            )
        ''')
        
        # User requests table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS user_requests (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                username TEXT,
                first_name TEXT,
                movie_title TEXT NOT NULL,
                requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notified BOOLEAN DEFAULT FALSE,
                group_id BIGINT,
                message_id BIGINT
            )
        ''')
        
        # Sync info table
        cur.execute('CREATE TABLE IF NOT EXISTS sync_info (id SERIAL PRIMARY KEY, last_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP);')
        
        # Movie aliases table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS movie_aliases (
                id SERIAL PRIMARY KEY,
                movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE,
                alias TEXT NOT NULL,
                UNIQUE(movie_id, alias)
            )
        ''')
        
        # Create indexes
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movies_title ON movies (title);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movies_title_trgm ON movies USING gin (title gin_trgm_ops);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_user_requests_movie_title ON user_requests (movie_title);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_user_requests_user_id ON user_requests (user_id);')
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database setup completed successfully")
    except Exception as e:
        logger.error(f"Error setting up database: {e}")

def get_db_connection():
    """Get database connection"""
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

def get_movies_from_db(user_query, limit=10):
    """Search for movies in database with fuzzy matching"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return []
        
        cur = conn.cursor()
        logger.info(f"Searching for: '{user_query}'")
        
        # Exact match
        cur.execute(
            "SELECT id, title, url, file_id, is_series FROM movies WHERE LOWER(title) LIKE LOWER(%s) ORDER BY title LIMIT %s",
            (f'%{user_query}%', limit)
        )
        exact_matches = cur.fetchall()
        
        if exact_matches:
            logger.info(f"Found {len(exact_matches)} exact matches")
            cur.close()
            conn.close()
            return exact_matches
        
        # Fuzzy matching
        cur.execute("SELECT id, title, url, file_id, is_series FROM movies")
        all_movies = cur.fetchall()
        
        if not all_movies:
            cur.close()
            conn.close()
            return []
        
        movie_titles = [movie for movie in all_movies]
        movie_dict = {movie: movie for movie in all_movies}
        
        matches = process.extract(user_query, movie_titles, scorer=fuzz.token_sort_ratio, limit=limit)
        
        filtered_movies = []
        for match in matches:
            if len(match) >= 2:
                title, score = match, match
                if score >= 65 and title in movie_dict:
                    filtered_movies.append(movie_dict[title])
        
        logger.info(f"Found {len(filtered_movies)} fuzzy matches")
        cur.close()
        conn.close()
        return filtered_movies[:limit]
        
    except Exception as e:
        logger.error(f"Database query error: {e}")
        return []
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

def get_series_data(movie_id):
    """Get complete series data with seasons and episodes"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cur = conn.cursor()
        
        # Get series info
        cur.execute("SELECT title, total_seasons FROM movies WHERE id = %s AND is_series = TRUE", (movie_id,))
        series_info = cur.fetchone()
        
        if not series_info:
            cur.close()
            conn.close()
            return None
        
        title, total_seasons = series_info
        
        # Get all seasons
        cur.execute("""
            SELECT id, season_number, season_name, has_complete_pack, complete_pack_file_id, complete_pack_url
            FROM series_seasons
            WHERE movie_id = %s
            ORDER BY season_number
        """, (movie_id,))
        seasons = cur.fetchall()
        
        series_data = {
            'title': title,
            'total_seasons': total_seasons,
            'seasons': []
        }
        
        for season in seasons:
            season_id, season_num, season_name, has_pack, pack_file_id, pack_url = season
            
            # Get episodes for this season
            cur.execute("""
                SELECT id, episode_number, episode_name
                FROM series_episodes
                WHERE season_id = %s
                ORDER BY episode_number
            """, (season_id,))
            episodes = cur.fetchall()
            
            season_data = {
                'season_id': season_id,
                'season_number': season_num,
                'season_name': season_name or f"Season {season_num}",
                'has_complete_pack': has_pack,
                'complete_pack_file_id': pack_file_id,
                'complete_pack_url': pack_url,
                'episodes': []
            }
            
            for episode in episodes:
                episode_id, ep_num, ep_name = episode
                season_data['episodes'].append({
                    'episode_id': episode_id,
                    'episode_number': ep_num,
                    'episode_name': ep_name or f"Episode {ep_num}"
                })
            
            series_data['seasons'].append(season_data)
        
        cur.close()
        conn.close()
        return series_data
        
    except Exception as e:
        logger.error(f"Error getting series data: {e}")
        return None
    finally:
        if conn:
            conn.close()

def get_quality_options(movie_id=None, episode_id=None):
    """Get available quality options for a movie or episode"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor()
        
        if episode_id:
            cur.execute("""
                SELECT quality, url, file_id, file_size
                FROM movie_files
                WHERE episode_id = %s
                ORDER BY CASE quality
                    WHEN '4K' THEN 1
                    WHEN '1080p' THEN 2
                    WHEN '720p' THEN 3
                    WHEN '480p' THEN 4
                    ELSE 5
                END
            """, (episode_id,))
        elif movie_id:
            cur.execute("""
                SELECT quality, url, file_id, file_size
                FROM movie_files
                WHERE movie_id = %s AND episode_id IS NULL
                ORDER BY CASE quality
                    WHEN '4K' THEN 1
                    WHEN '1080p' THEN 2
                    WHEN '720p' THEN 3
                    WHEN '480p' THEN 4
                    ELSE 5
                END
            """, (movie_id,))
        else:
            cur.close()
            conn.close()
            return []
        
        qualities = cur.fetchall()
        cur.close()
        conn.close()
        return qualities
        
    except Exception as e:
        logger.error(f"Error getting quality options: {e}")
        return []
    finally:
        if conn:
            conn.close()

# ==================== NETFLIX-LIKE UI KEYBOARDS ====================
def create_movie_selection_keyboard(movies, page=0, movies_per_page=5):
    """Create Netflix-style movie selection keyboard"""
    start_idx = page * movies_per_page
    end_idx = start_idx + movies_per_page
    current_movies = movies[start_idx:end_idx]
    
    keyboard = []
    
    for movie in current_movies:
        movie_id, title, url, file_id, is_series = movie
        emoji = "📺" if is_series else "🎬"
        button_text = f"{emoji} {title}" if len(title) <= 35 else f"{emoji} {title[:32]}..."
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"select_{movie_id}")])
    
    # Navigation
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("◀️ Previous", callback_data=f"page_{page-1}"))
    if end_idx < len(movies):
        nav_buttons.append(InlineKeyboardButton("Next ▶️", callback_data=f"page_{page+1}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_selection")])
    
    return InlineKeyboardMarkup(keyboard)

def create_season_selection_keyboard(series_data):
    """Create Netflix-style season selection keyboard"""
    keyboard = []
    
    for season in series_data['seasons']:
        season_num = season['season_number']
        season_name = season['season_name']
        button_text = f"📂 {season_name}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"season_{season['season_id']}_{season_num}")])
    
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="cancel_selection")])
    
    return InlineKeyboardMarkup(keyboard)

def create_episode_selection_keyboard(season_data, season_id):
    """Create Netflix-style episode selection with Complete Pack option"""
    keyboard = []
    
    # Complete Season Pack (if available)
    if season_data.get('has_complete_pack'):
        keyboard.append([InlineKeyboardButton(
            f"📦 Complete {season_data['season_name']} Pack",
            callback_data=f"complete_pack_{season_id}"
        )])
        keyboard.append([InlineKeyboardButton("━━━━ OR SELECT EPISODE ━━━━", callback_data="dummy")])
    
    # Individual Episodes
    for episode in season_data['episodes']:
        ep_num = episode['episode_number']
        ep_name = episode['episode_name']
        button_text = f"▶️ E{ep_num:02d} - {ep_name}" if len(ep_name) <= 30 else f"▶️ E{ep_num:02d} - {ep_name[:27]}..."
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"episode_{episode['episode_id']}")])
    
    keyboard.append([InlineKeyboardButton("🔙 Back to Seasons", callback_data="back_to_seasons")])
    
    return InlineKeyboardMarkup(keyboard)

def create_quality_selection_keyboard(movie_id=None, episode_id=None, quality_type="movie"):
    """Create Netflix-style quality selection keyboard"""
    keyboard = []
    
    qualities = get_quality_options(movie_id=movie_id, episode_id=episode_id)
    
    if not qualities:
        return InlineKeyboardMarkup([[InlineKeyboardButton("❌ No files available", callback_data="no_files")]])
    
    for quality, url, file_id, file_size in qualities:
        size_text = f" • {file_size}" if file_size else ""
        link_type = "📱 File" if file_id else "🔗 Link"
        button_text = f"{link_type} {quality}{size_text}"
        
        if episode_id:
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"ep_quality_{episode_id}_{quality}")])
        else:
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"mv_quality_{movie_id}_{quality}")])
    
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_episodes" if episode_id else "cancel_selection")])
    
    return InlineKeyboardMarkup(keyboard)

# ==================== FILE DELIVERY WITH AUTO-DELETE ====================
async def send_movie_file(update: Update, context: ContextTypes.DEFAULT_TYPE, title: str, url: Optional[str] = None, file_id: Optional[str] = None):
    """Send movie file with Netflix-style caption and auto-delete"""
    chat_id = update.effective_chat.id
    
    # Check membership
    is_member = await check_user_membership(context, update.effective_user.id)
    if not is_member:
        await context.bot.send_message(
            chat_id=chat_id,
            text="🚫 **Access Denied**\n\nTo watch movies, you must join our channel and group first!",
            reply_markup=get_force_join_keyboard(),
            parse_mode='Markdown'
        )
        return
    
    try:
        # Netflix-style caption
        caption = (
            f"🎬 **{title}**\n\n"
            f"┏━━━━━━━━━━━━━━━━┓\n"
            f"┣ 📢 **Channel:** [FilmfyBox]({FILMFYBOX_CHANNEL_URL})\n"
            f"┣ 💬 **Group:** [FilmfyBox Chat]({FILMFYBOX_GROUP_URL})\n"
            f"┗━━━━━━━━━━━━━━━━┛\n\n"
            f"⚠️ File will auto-delete in 60 seconds"
        )
        
        sent_msg = None
        
        # File ID delivery
        if file_id:
            sent_msg = await context.bot.send_document(
                chat_id=chat_id,
                document=file_id,
                caption=caption,
                parse_mode='Markdown'
            )
        
        # Private channel link
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
                await context.bot.send_message(chat_id=chat_id, text=f"🔗 **{title}**\n\n{url}")
                return
        
        # Public channel link
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
                await context.bot.send_message(chat_id=chat_id, text=f"🔗 **{title}**\n\n{url}")
                return
        
        # External link
        elif url:
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("🎬 Watch Now", url=url),
                InlineKeyboardButton("📢 Channel", url=FILMFYBOX_CHANNEL_URL)
            ]])
            await context.bot.send_message(
                chat_id=chat_id,
                text=caption,
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
            return
        
        # Auto-delete after 60 seconds
        if sent_msg:
            asyncio.create_task(delete_messages_after_delay(context, chat_id, [sent_msg.message_id], 60))
    
    except Exception as e:
        logger.error(f"Error sending file: {e}")
        await context.bot.send_message(chat_id=chat_id, text="❌ Failed to send file. Please contact admin.")

async def delete_messages_after_delay(context, chat_id, message_ids, delay=60):
    """Delete messages after delay"""
    try:
        await asyncio.sleep(delay)
        for msg_id in message_ids:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
                logger.info(f"Deleted message {msg_id}")
            except Exception as e:
                logger.error(f"Failed to delete message {msg_id}: {e}")
    except Exception as e:
        logger.error(f"Error in delete_messages_after_delay: {e}")

# ==================== GROUP MESSAGE HANDLER (SILENT MODE) ====================
async def group_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle group messages silently - only respond if exact match found"""
    if not update.message or not update.message.text or update.message.from_user.is_bot:
        return
    
    message_text = update.message.text.strip()
    user = update.effective_user
    
    # Ignore short messages and commands
    if len(message_text) < 4 or message_text.startswith('/'):
        return
    
    # Search in database
    movies_found = get_movies_from_db(message_text, limit=1)
    
    if not movies_found:
        # SILENT - No response
        return
    
    # Check match quality
    match_title = movies_found
    score = fuzz.token_sort_ratio(_normalize_title_for_match(message_text), _normalize_title_for_match(match_title))
    
    if score < 85:
        # Not confident enough - stay silent
        return
    
    movie_id, title, _, _, is_series = movies_found
    
    # Found good match - send prompt
    emoji = "📺" if is_series else "🎬"
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton(f"✅ Yes, Get {emoji}", callback_data=f"group_get_{movie_id}_{user.id}")
    ]])
    
    try:
        reply_msg = await update.message.reply_text(
            text=f"Hey {user.mention_markdown()},\n\n{emoji} **{title}**\n\nClick below to get it in PM ⬇️",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        
        # Auto-delete prompt after 2 minutes
        asyncio.create_task(delete_messages_after_delay(context, update.effective_chat.id, [reply_msg.message_id], 120))
    except Exception as e:
        logger.error(f"Failed to send group prompt: {e}")

# ==================== TELEGRAM BOT HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command"""
    welcome_text = """
🎬 **Welcome to FilmfyBox**

Your Netflix-style Movie & Series Bot!

📌 **How to use:**
• Send movie/series name
• Get instant downloads
• Enjoy premium quality

⚡ **Quick Start:**
Just type the name of any movie or series!

Example: `Stranger Things` or `Avengers`
"""
    await update.message.reply_text(welcome_text, parse_mode='Markdown')
    return MAIN_MENU

async def search_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Search movies handler"""
    try:
        if not await check_rate_limit(update.effective_user.id):
            await update.message.reply_text("⚠️ Please wait before searching again.")
            return MAIN_MENU
        
        user_message = update.message.text.strip()
        processed_query = preprocess_query(user_message)
        search_query = processed_query if processed_query else user_message
        
        movies_found = get_movies_from_db(search_query, limit=10)
        
        if not movies_found:
            if update.effective_chat.type != "private":
                return MAIN_MENU
            
            await update.message.reply_text(
                f"😔 Sorry, **{user_message}** not found in our collection.\n\n"
                f"💡 **Tip:** Try with full movie name + year",
                parse_mode='Markdown'
            )
            return MAIN_MENU
        
        elif len(movies_found) == 1:
            movie_id, title, url, file_id, is_series = movies_found
            
            if is_series:
                # Handle series
                series_data = get_series_data(movie_id)
                if series_data:
                    context.user_data['series_data'] = series_data
                    context.user_data['movie_id'] = movie_id
                    
                    await update.message.reply_text(
                        f"📺 **{title}**\n\nSelect Season ⬇️",
                        reply_markup=create_season_selection_keyboard(series_data),
                        parse_mode='Markdown'
                    )
                else:
                    await update.message.reply_text("❌ Series data not available.")
            else:
                # Handle movie - show quality options
                qualities = get_quality_options(movie_id=movie_id)
                if qualities:
                    await update.message.reply_text(
                        f"🎬 **{title}**\n\nSelect Quality ⬇️",
                        reply_markup=create_quality_selection_keyboard(movie_id=movie_id),
                        parse_mode='Markdown'
                    )
                else:
                    await update.message.reply_text("❌ No files available for this movie.")
        
        else:
            # Multiple results
            context.user_data['search_results'] = movies_found
            await update.message.reply_text(
                f"🔍 **Found {len(movies_found)} results**\n\nSelect one ⬇️",
                reply_markup=create_movie_selection_keyboard(movies_found),
                parse_mode='Markdown'
            )
        
        return MAIN_MENU
    
    except Exception as e:
        logger.error(f"Error in search: {e}")
        await update.message.reply_text("❌ Something went wrong.")
        return MAIN_MENU

# ==================== CALLBACK HANDLER ====================
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all button callbacks"""
    try:
        query = update.callback_query
        await query.answer()
        
        # Check membership button
        if query.data == "check_membership":
            is_member = await check_user_membership(context, query.from_user.id)
            if is_member:
                await query.edit_message_text("✅ **Access Granted!**\n\nYou can now use the bot. Search for movies/series!", parse_mode='Markdown')
            else:
                await query.answer("❌ Please join both Channel and Group first!", show_alert=True)
            return
        
        # Group get movie
        if query.data.startswith("group_get_"):
            parts = query.data.split('_')
            movie_id = int(parts)
            original_user_id = int(parts)
            
            if query.from_user.id != original_user_id:
                await query.answer("This button is not for you!", show_alert=True)
                return
            
            # Check membership
            is_member = await check_user_membership(context, original_user_id)
            if not is_member:
                await query.edit_message_text(
                    "🚫 **Access Denied**\n\nPlease join our Channel and Group first!",
                    reply_markup=get_force_join_keyboard(),
                    parse_mode='Markdown'
                )
                return
            
            # Check if series or movie
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT title, is_series FROM movies WHERE id = %s", (movie_id,))
            result = cur.fetchone()
            cur.close()
            conn.close()
            
            if not result:
                await query.answer("Movie not found!", show_alert=True)
                return
            
            title, is_series = result
            
            if is_series:
                series_data = get_series_data(movie_id)
                if series_data:
                    context.user_data['series_data'] = series_data
                    context.user_data['movie_id'] = movie_id
                    await context.bot.send_message(
                        chat_id=original_user_id,
                        text=f"📺 **{title}**\n\nSelect Season ⬇️",
                        reply_markup=create_season_selection_keyboard(series_data),
                        parse_mode='Markdown'
                    )
                    await query.edit_message_text(f"✅ Check your PM for **{title}**!", parse_mode='Markdown')
            else:
                qualities = get_quality_options(movie_id=movie_id)
                if qualities:
                    await context.bot.send_message(
                        chat_id=original_user_id,
                        text=f"🎬 **{title}**\n\nSelect Quality ⬇️",
                        reply_markup=create_quality_selection_keyboard(movie_id=movie_id),
                        parse_mode='Markdown'
                    )
                    await query.edit_message_text(f"✅ Check your PM for **{title}**!", parse_mode='Markdown')
            return
        
        # Movie selection
        if query.data.startswith("select_"):
            movie_id = int(query.data.replace("select_", ""))
            
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT title, is_series FROM movies WHERE id = %s", (movie_id,))
            result = cur.fetchone()
            cur.close()
            conn.close()
            
            if not result:
                await query.edit_message_text("❌ Movie not found.")
                return
            
            title, is_series = result
            
            if is_series:
                series_data = get_series_data(movie_id)
                if series_data:
                    context.user_data['series_data'] = series_data
                    context.user_data['movie_id'] = movie_id
                    await query.edit_message_text(
                        f"📺 **{title}**\n\nSelect Season ⬇️",
                        reply_markup=create_season_selection_keyboard(series_data),
                        parse_mode='Markdown'
                    )
            else:
                qualities = get_quality_options(movie_id=movie_id)
                if qualities:
                    await query.edit_message_text(
                        f"🎬 **{title}**\n\nSelect Quality ⬇️",
                        reply_markup=create_quality_selection_keyboard(movie_id=movie_id),
                        parse_mode='Markdown'
                    )
            return
        
        # Season selection
        if query.data.startswith("season_"):
            parts = query.data.split('_')
            season_id = int(parts)
            season_num = int(parts)
            
            series_data = context.user_data.get('series_data')
            if not series_data:
                await query.answer("Session expired. Please search again.", show_alert=True)
                return
            
            season_data = next((s for s in series_data['seasons'] if s['season_id'] == season_id), None)
            if season_data:
                context.user_data['current_season'] = season_data
                await query.edit_message_text(
                    f"📺 **{series_data['title']}**\n📂 **{season_data['season_name']}**\n\nSelect Episode ⬇️",
                    reply_markup=create_episode_selection_keyboard(season_data, season_id),
                    parse_mode='Markdown'
                )
            return
        
        # Complete pack delivery
        if query.data.startswith("complete_pack_"):
            season_id = int(query.data.replace("complete_pack_", ""))
            season_data = context.user_data.get('current_season')
            
            if not season_data:
                await query.answer("Session expired!", show_alert=True)
                return
            
            series_data = context.user_data.get('series_data')
            title = f"{series_data['title']} - {season_data['season_name']} Complete"
            
            await send_movie_file(
                update,
                context,
                title,
                url=season_data.get('complete_pack_url'),
                file_id=season_data.get('complete_pack_file_id')
            )
            return
        
        # Episode selection
        if query.data.startswith("episode_"):
            episode_id = int(query.data.replace("episode_", ""))
            await query.edit_message_text(
                "📺 Select Quality ⬇️",
                reply_markup=create_quality_selection_keyboard(episode_id=episode_id),
                parse_mode='Markdown'
            )
            return
        
        # Movie quality selection
        if query.data.startswith("mv_quality_"):
            parts = query.data.split('_')
            movie_id = int(parts)
            quality = parts
            
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT title FROM movies WHERE id = %s", (movie_id,))
            title = cur.fetchone()
            cur.execute("""
                SELECT url, file_id FROM movie_files
                WHERE movie_id = %s AND quality = %s
            """, (movie_id, quality))
            file_data = cur.fetchone()
            cur.close()
            conn.close()
            
            if file_data:
                url, file_id = file_data
                await send_movie_file(update, context, f"{title} [{quality}]", url, file_id)
            return
        
        # Episode quality selection
        if query.data.startswith("ep_quality_"):
            parts = query.data.split('_')
            episode_id = int(parts)
            quality = parts
            
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT se.episode_name, se.episode_number, ss.season_name, m.title
                FROM series_episodes se
                JOIN series_seasons ss ON se.season_id = ss.id
                JOIN movies m ON ss.movie_id = m.id
                WHERE se.id = %s
            """, (episode_id,))
            ep_data = cur.fetchone()
            
            cur.execute("""
                SELECT url, file_id FROM movie_files
                WHERE episode_id = %s AND quality = %s
            """, (episode_id, quality))
            file_data = cur.fetchone()
            cur.close()
            conn.close()
            
            if ep_data and file_data:
                ep_name, ep_num, season_name, series_title = ep_data
                url, file_id = file_data
                title = f"{series_title} - {season_name} E{ep_num:02d} [{quality}]"
                await send_movie_file(update, context, title, url, file_id)
            return
        
        # Back buttons
        if query.data == "back_to_seasons":
            series_data = context.user_data.get('series_data')
            if series_data:
                await query.edit_message_text(
                    f"📺 **{series_data['title']}**\n\nSelect Season ⬇️",
                    reply_markup=create_season_selection_keyboard(series_data),
                    parse_mode='Markdown'
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

# ==================== MAIN MENU HANDLER ====================
async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main menu handler"""
    return await search_movies(update, context)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel handler"""
    await update.message.reply_text("Cancelled.")
    return MAIN_MENU

# ==================== ADMIN COMMANDS (Keep your existing ones) ====================
# [Keep all your existing admin command functions here]

# ==================== ERROR HANDLER ====================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Error handler"""
    logger.error(f"Exception: {context.error}", exc_info=context.error)

# ==================== FLASK APP ====================
flask_app = Flask('')

@flask_app.route('/')
def home():
    return "FilmfyBox Bot Running!"

@flask_app.route('/health')
def health():
    return "OK", 200

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    flask_app.secret_key = os.urandom(24)
    flask_app.run(host='0.0.0.0', port=port)

# ==================== MAIN BOT ====================
def main():
    """Main bot function"""
    logger.info("Starting FilmfyBox Bot...")
    
    setup_database()
    
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).read_timeout(30).write_timeout(30).build()
    
    # Conversation handler
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start, filters=filters.ChatType.PRIVATE)],
        states={
            MAIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, main_menu)],
            SEARCHING: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, search_movies)],
        },
        fallbacks=[CommandHandler('cancel', cancel, filters=filters.ChatType.PRIVATE)],
        per_message=False,
        per_chat=True,
    )
    
    # Handlers
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS, group_message_handler))
    application.add_handler(conv_handler)
    
    # [Add your admin command handlers here]
    
    application.add_error_handler(error_handler)
    
    # Start Flask
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    
    logger.info("Bot started successfully!")
    application.run_polling()

if __name__ == '__main__':
    main()
