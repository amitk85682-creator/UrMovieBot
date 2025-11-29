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
# Removed unused import of Flask, request, session, g as the Flask setup is commented out in main()
# from flask import Flask, request, session, g 
import google.generativeai as genai # Only for dependency clarity, not used in the provided code
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
# Fix: Ensure BOT_USERNAME is set from env or default for /startgroup link
BOT_USERNAME = os.environ.get('BOT_USERNAME', 'your_bot')

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
    
    # Try SXXEXX format first
    match = re.search(r'S(\d+)\s*E(\d+)', title, re.IGNORECASE)
    if match:
        info['season'] = int(match.group(1))
        info['episode'] = int(match.group(2))
        info['base_title'] = re.sub(r'\s*(S\d+\s*E\d+)', '', title, flags=re.IGNORECASE).strip()
        info['is_series'] = True
        return info
    
    # Try Season X and Episode X format (less aggressive title stripping)
    match_s = re.search(r'Season\s*(\d+)', title, re.IGNORECASE)
    match_e = re.search(r'Episode\s*(\d+)', title, re.IGNORECASE)

    if match_s or match_e:
        info['season'] = int(match_s.group(1)) if match_s else None
        info['episode'] = int(match_e.group(1)) if match_e else None
        
        # Strip series info to get a cleaner base title
        base_title = title
        if match_s:
            base_title = re.sub(r'Season\s*\d+', '', base_title, flags=re.IGNORECASE).strip()
        if match_e:
            base_title = re.sub(r'Episode\s*\d+', '', base_title, flags=re.IGNORECASE).strip()
        
        # Clean up remaining delimiters and redundant spaces
        info['base_title'] = re.sub(r'[\s-]*\W+[\s-]*$', '', base_title).strip()
        info['is_series'] = True
        
    return info

# ==================== AUTO-DELETE HELPER ====================
async def auto_delete_message(context, chat_id, message_id, delay=MESSAGE_DELETE_TIME):
    """Auto-delete a single message after delay"""
    try:
        # Use context.application.job_queue to schedule the task robustly
        context.application.job_queue.run_once(
            lambda job_context: asyncio.create_task(
                context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            ),
            delay,
            name=f"del_{message_id}_{chat_id}"
        )
    except Exception as e:
        logger.error(f"Failed to schedule auto-delete for message {message_id}: {e}")

async def delete_messages_after_delay(context, chat_id, message_ids, delay=FILE_DELETE_TIME):
    """Delete multiple messages after delay"""
    # Use context.application.job_queue for robust scheduling
    context.application.job_queue.run_once(
        lambda job_context: asyncio.create_task(
            _execute_delete_messages(context, chat_id, message_ids)
        ),
        delay,
        name=f"del_multi_{chat_id}"
    )

async def _execute_delete_messages(context, chat_id, message_ids):
    """Helper function to execute the actual message deletion"""
    for msg_id in message_ids:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception as e:
            # Suppress common errors like "Message not found" if user deleted it first
            if "message to delete not found" not in str(e):
                logger.error(f"Failed to delete message {msg_id}: {e}")

# ==================== FORCE JOIN CHECK ====================
async def check_user_membership(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Check if user is member of required channel and group"""
    try:
        # Check channel membership
        channel_member: ChatMember = await context.bot.get_chat_member(chat_id=REQUIRED_CHANNEL_ID, user_id=user_id)
        channel_joined = channel_member.status in [ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.CREATOR]
        
        # Check group membership
        group_member: ChatMember = await context.bot.get_chat_member(chat_id=REQUIRED_GROUP_ID, user_id=user_id)
        group_joined = group_member.status in [ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.CREATOR]
        
        return channel_joined and group_joined
    except telegram.error.BadRequest as e:
        # Handle cases where the bot is not an admin/member in the channel/group
        logger.warning(f"Bot failed to get membership info for user {user_id}: {e}")
        return True # Default to True to avoid locking out users if the bot setup is incomplete/buggy
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
            # Use BOT_USERNAME for the startgroup link
            InlineKeyboardButton("➕ Add to Group", url=f"https://t.me/{BOT_USERNAME}?startgroup=true"),
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
        # Set connect_timeout for robustness
        return psycopg2.connect(DATABASE_URL, connect_timeout=5) 
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
        
        # Fetch the main URL from the movies table, which is usually the 'watch online' link
        cur.execute("SELECT url FROM movies WHERE id = %s", (movie_id,))
        main_res = cur.fetchone()
        
        final_results = []
        
        # Check if the main URL is present and not just empty space
        if main_res and main_res[0] and main_res[0].strip():
            final_results.append(('🎬 Watch Online', main_res[0].strip(), None, None))
        
        for quality, url, file_id, file_size in quality_results:
            final_results.append((quality, url, file_id, file_size))
        
        cur.close()
        return final_results
    except Exception as e:
        logger.error(f"Error fetching qualities: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_series_episodes(base_title):
    """Get all episodes for a series based on a fuzzy base title match"""
    conn = get_db_connection()
    if not conn:
        return {}
    
    try:
        cur = conn.cursor()
        
        # Find all series titles matching the base title
        # Use LIKE and ensure the title contains some series pattern (for safety)
        cur.execute("""
            SELECT id, title FROM movies 
            WHERE title ILIKE %s AND (
                title ~* 'S\\d+\\s*E\\d+' OR 
                title ~* 'Season\\s*\\d+' OR 
                title ~* 'Episode\\s*\\d+'
            )
            ORDER BY title
        """, (f'{base_title}%',))
        
        episodes = cur.fetchall()
        
        seasons = defaultdict(list)
        for ep_id, title in episodes:
            # Re-parse to get the season/episode info
            info = parse_series_info(title) 
            
            # Double check the base title matches (fuzzy matching can be tricky)
            # If the title is much longer than the base title, it might be a different show
            if info['base_title'] and fuzz.token_sort_ratio(base_title, info['base_title']) >= 80:
                if info['season']:
                    seasons[info['season']].append({
                        'id': ep_id,
                        'title': title,
                        'episode': info['episode'] or 0
                    })
        
        for season in seasons:
            seasons[season].sort(key=lambda x: x['episode'])
        
        cur.close()
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
        # Truncate title gracefully
        button_text = f"{emoji} {title}" if len(title) <= 35 else f"{emoji} {title[:32].strip()}..."
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
        # Format file size for display
        size_text = f" • {file_size}" if file_size else ""
        link_type = "📱" if file_id else ("🌐" if url and not url.startswith("https://t.me/") else "🔗")
        button_text = f"{link_type} {quality}{size_text}"
        
        # Quality can be '🎬 Watch Online' which needs to be preserved for callback
        safe_quality = quote(quality) # URL encode the quality string for safe transport
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"quality_{movie_id}_{safe_quality}")])
    
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_selection")])
    
    return InlineKeyboardMarkup(keyboard)

def create_season_selection_keyboard(seasons_data, base_title):
    """Create season selection keyboard"""
    keyboard = []
    
    # Sort seasons numerically
    for season_num in sorted(seasons_data.keys()):
        episodes = seasons_data[season_num]
        button_text = f"📂 Season {season_num} • {len(episodes)} Episodes"
        # The base_title might be long, so only include the season number and rely on context.user_data
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"season_{season_num}")])
    
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_selection")])
    
    return InlineKeyboardMarkup(keyboard)

def create_episode_selection_keyboard(episodes, season_num):
    """Create episode selection keyboard"""
    keyboard = []
    
    # Sort episodes by their number (already done in get_series_episodes, but useful to ensure)
    sorted_episodes = sorted(episodes, key=lambda x: x.get('episode', 0))

    # Arrange episodes in two columns (or more if needed, but two is generally good)
    row = []
    for ep in sorted_episodes:
        ep_num = ep.get('episode', 0)
        # Use episode number if available, otherwise truncate the full title
        button_text = f"▶️ E{ep_num}" if ep_num else ep['title'][:15]
        # The movie ID is what we need to fetch the content
        row.append(InlineKeyboardButton(button_text, callback_data=f"movie_{ep['id']}"))
        
        if len(row) == 3: # 3 episodes per row
            keyboard.append(row)
            row = []
            
    if row:
        keyboard.append(row)
        
    # The 'Back' button should take them to the Season selection
    keyboard.append([InlineKeyboardButton("🔙 Back to Seasons", callback_data="back_to_seasons")])
    
    return InlineKeyboardMarkup(keyboard)

# ==================== SEND MOVIE WITH AUTO-DELETE ====================
async def send_movie_file(update: Update, context: ContextTypes.DEFAULT_TYPE, title: str, url: Optional[str] = None, file_id: Optional[str] = None):
    """Send movie file with premium styling and auto-delete"""
    # Fix: Determine chat_id correctly whether from message or callback
    if update.callback_query:
        chat_id = update.callback_query.message.chat_id
    elif update.message:
        chat_id = update.message.chat_id
    else:
        # Fallback for dummy_update in group_get_ logic
        chat_id = update.effective_chat.id 

    user_id = update.effective_user.id
    
    # Check membership first
    is_member = await check_user_membership(context, user_id)
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
        await auto_delete_message(context, chat_id, msg.message_id, MESSAGE_DELETE_TIME)
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
            # Assume file_id is for a Document/Video/Media type
            sent_msg = await context.bot.send_document(
                chat_id=chat_id,
                document=file_id,
                caption=caption,
                parse_mode='Markdown'
            )
        elif url and url.startswith("https://t.me/"):
            # Attempt to copy a Telegram message link (private or public)
            try:
                parts = url.rstrip('/').split('/')
                message_id = int(parts[-1])
                
                # Check for private channel link
                if '/c/' in url:
                    # Private channel: t.me/c/{chat_id}/{msg_id} -> from_chat_id = -100{chat_id}
                    from_chat_id_str = parts[-2]
                    from_chat_id = int("-100" + from_chat_id_str)
                else:
                    # Public channel/group: t.me/{username}/{msg_id} -> from_chat_id = @{username}
                    from_chat_id = f"@{parts[-2]}"

                sent_msg = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=from_chat_id,
                    message_id=message_id,
                    caption=caption,
                    parse_mode='Markdown'
                )
            except Exception as e:
                logger.error(f"Telegram Copy failed for URL {url}: {e}")
                
                # Fallback to external URL button if copy fails
                if not sent_msg:
                    keyboard = InlineKeyboardMarkup([[
                        InlineKeyboardButton("🎬 Watch Now", url=url),
                        InlineKeyboardButton("📢 Channel", url=FILMFYBOX_CHANNEL_URL)
                    ]])
                    sent_msg = await context.bot.send_message(
                        chat_id=chat_id,
                        text=caption,
                        reply_markup=keyboard,
                        parse_mode='Markdown'
                    )
        elif url:
            # External URL link button
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("🎬 Watch Now", url=url),
                InlineKeyboardButton("📢 Channel", url=FILMFYBOX_CHANNEL_URL)
            ]])
            sent_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=caption,
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
        else:
            # No file_id or URL
            msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Sorry, **{title}** is not available right now.",
                parse_mode='Markdown'
            )
            await auto_delete_message(context, chat_id, msg.message_id, MESSAGE_DELETE_TIME)
            return
        
        # Auto-delete after 60 seconds
        if sent_msg and warning_msg:
            # Check if sent_msg is a list of messages (e.g., in case of media group) or a single message
            msg_ids_to_delete = [warning_msg.message_id]
            if isinstance(sent_msg, list):
                 msg_ids_to_delete.extend([m.message_id for m in sent_msg])
            else:
                 msg_ids_to_delete.append(sent_msg.message_id)

            await delete_messages_after_delay(
                context,
                chat_id,
                msg_ids_to_delete,
                FILE_DELETE_TIME
            )
            
    except Exception as e:
        logger.error(f"Error sending file: {e}")
        msg = await context.bot.send_message(chat_id=chat_id, text="❌ Failed to send file. Contact admin for help.")
        await auto_delete_message(context, chat_id, msg.message_id, MESSAGE_DELETE_TIME)

# ==================== BOT HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Premium /start command with image and buttons"""
    user = update.effective_user
    chat_id = update.effective_chat.id
    
    # Check membership
    is_member = await check_user_membership(context, user.id)
    
    if not is_member and update.effective_chat.type == telegram.constants.ChatType.PRIVATE:
        # Force join prompt (only in private chat)
        msg = await context.bot.send_photo(
            chat_id=chat_id,
            photo=BOT_LOGO_URL,
            caption=(
                f"👋 **Hey {user.first_name}!**\n\n"
                f"🎬 Welcome to **Ur Movie Bot**\n\n"
                f"🎭 **10,000+ Movies**\n"
                f"📺 **5,000+ Series**\n"
                f"🎥 **HD/Full HD Quality**\n\n"
                f"━━━━━━━━━━━━━━━━━\n"
                f"📢 Join our Channel\n"
                f"💬 Join our Group\n"
                f"━━━━━━━━━━━━━━━━━\n\n"
                f"⚡ Join now and start watching!"
            ),
            reply_markup=get_force_join_keyboard(),
            parse_mode='Markdown'
        )
        await auto_delete_message(context, chat_id, msg.message_id, MESSAGE_DELETE_TIME)
        return MAIN_MENU
    
    # Handle deep link
    if context.args:
        arg_str = " ".join(context.args)
        if arg_str.startswith("movie_"):
            try:
                movie_id = int(arg_str.split('_')[1])
                conn = get_db_connection()
                if conn:
                    cur = conn.cursor()
                    cur.execute("SELECT title, url, file_id FROM movies WHERE id = %s", (movie_id,))
                    movie_data = cur.fetchone()
                    cur.close()
                    conn.close()
                    
                    if movie_data:
                        title, url, file_id = movie_data
                        # Send main movie file (or prompt for quality if multiple)
                        qualities = get_all_movie_qualities(movie_id)
                        if qualities and len(qualities) > 1:
                            msg = await context.bot.send_message(
                                chat_id=chat_id,
                                text=f"🎬 **{title}**\n\n⬇️ Select Quality:",
                                reply_markup=create_quality_selection_keyboard(movie_id, title, qualities),
                                parse_mode='Markdown'
                            )
                            await auto_delete_message(context, chat_id, msg.message_id, MESSAGE_DELETE_TIME)
                        else:
                            await send_movie_file(update, context, title, url, file_id)
                        return MAIN_MENU
            except Exception as e:
                logger.error(f"Deep link error: {e}")
    
    # Welcome message with premium image
    msg = await context.bot.send_photo(
        chat_id=chat_id,
        photo=BOT_LOGO_URL,
        caption=(
            f"👋 **Hey {user.first_name}!**\n\n"
            f"⚡ Welcome to **Ur Movie Bot** ⚡\n\n"
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
    await auto_delete_message(context, chat_id, msg.message_id, MESSAGE_DELETE_TIME)
    return MAIN_MENU

async def search_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Search movies/series handler with auto-delete"""
    try:
        if not await check_rate_limit(update.effective_user.id):
            msg = await update.message.reply_text("⏰ Please wait before searching again.")
            await auto_delete_message(context, update.effective_chat.id, msg.message_id, 30)
            return MAIN_MENU
        
        user_message = update.message.text.strip()
        
        # Delete user's message
        try:
            await update.message.delete()
        except Exception as e:
            logger.warning(f"Failed to delete user message: {e}")
            pass
        
        # Search in database
        movies_found = get_movies_from_db(user_message, limit=10)
        
        if not movies_found:
            # Only send the "Not Found" message in private chat to avoid group spam
            if update.effective_chat.type != telegram.constants.ChatType.PRIVATE:
                return MAIN_MENU
            
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("🔍 Search Tips", callback_data="search_tips"),
                InlineKeyboardButton("📢 Channel", url=FILMFYBOX_CHANNEL_URL)
            ]])
            
            msg = await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=(
                    f"😔 **Not Found!**\n\n"
                    f"'{user_message}' isn't in our collection yet.\n\n"
                    f"💡 **Tips:**\n"
                    f"• Check spelling\n"
                    f"• Try full name\n"
                    f"• Add year (e.g., 2023)\n\n"
                    f"🔍 Try again with correct name!"
                ),
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
            await auto_delete_message(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_TIME)
            return MAIN_MENU
        
        # Single exact match case (redirect to quality selection or directly send file)
        elif len(movies_found) == 1:
            movie_id, title, url, file_id, is_series_flag = movies_found[0]
            
            # -------------------- FIX 1: SERIES LOGIC IMPROVEMENT --------------------
            # 1. Check if the title LOOKS like a series (e.g., contains S01E01)
            if is_series_flag:
                info = parse_series_info(title)
                base_title_to_use = info['base_title'] if info['base_title'] else title
                seasons_data = get_series_episodes(base_title_to_use)
                
                # 2. Show season selection ONLY if MULTIPLE seasons/episodes are found in the database.
                # If only one episode is found (i.e., seasons_data has one season with one episode), 
                # we treat it as a single file and fall through to the quality check below.
                total_episodes = sum(len(eps) for eps in seasons_data.values())

                if total_episodes > 1 or len(seasons_data) > 1:
                    context.user_data['series_data'] = seasons_data
                    context.user_data['base_title'] = base_title_to_use
                    
                    msg = await context.bot.send_message(
                        chat_id=update.effective_chat.id,
                        text=f"📺 **{base_title_to_use}**\n\n⬇️ Select Season:",
                        reply_markup=create_season_selection_keyboard(seasons_data, base_title_to_use),
                        parse_mode='Markdown'
                    )
                    await auto_delete_message(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_TIME)
                    return MAIN_MENU
            
            # -------------------- FIX 2: QUALITY CHECK (for single entry) --------------------
            # If it's not a multi-episode series, check qualities.
            qualities = get_all_movie_qualities(movie_id)
            
            # len(qualities) > 1 means there are multiple options (Watch Online + File or 2+ Files)
            if qualities and len(qualities) > 1:
                msg = await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"🎬 **{title}**\n\n⬇️ Select Quality:",
                    reply_markup=create_quality_selection_keyboard(movie_id, title, qualities),
                    parse_mode='Markdown'
                )
                await auto_delete_message(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_TIME)
            elif qualities:
                # Only one quality available (or just 'Watch Online') - send it
                quality, url_q, file_id_q, _ = qualities[0]
                await send_movie_file(update, context, f"{title} [{quality}]", url_q or url, file_id_q or file_id)
            else:
                # No quality options found in movie_files, use main movie entry (fallback)
                await send_movie_file(update, context, title, url, file_id)
        
        
        # Multiple matches case - show selection keyboard
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
        if update.effective_chat.type == telegram.constants.ChatType.PRIVATE:
            msg = await update.message.reply_text("❌ Something went wrong during search.")
            await auto_delete_message(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_TIME)
        return MAIN_MENU

async def group_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Silent group handler"""
    if not update.message or not update.message.text or update.message.from_user.is_bot:
        return
    
    message_text = update.message.text.strip()
    user = update.effective_user
    
    if len(message_text) < 4 or message_text.startswith('/'):
        return
    
    # Check if the user is mentioned. We should only respond if the query is a reasonable search term.
    movies_found = get_movies_from_db(message_text, limit=1)
    
    if not movies_found:
        return
    
    movie_id, title, _, _, is_series_flag = movies_found[0]
    score = fuzz.token_sort_ratio(_normalize_title_for_match(message_text), _normalize_title_for_match(title))
    
    if score < SIMILARITY_THRESHOLD: # Use the general similarity threshold
        return
    
    emoji = "📺" if is_series_flag else "🎬"
    keyboard = InlineKeyboardMarkup([[
        # Pass the message_id and chat_id to the callback to delete the group message later
        InlineKeyboardButton(f"✅ Get {emoji} in PM", callback_data=f"group_get_{movie_id}_{user.id}_{update.effective_chat.id}_{update.message.message_id}")
    ]])
    
    try:
        # Use user.mention_markdown() for proper markdown mention
        reply_msg = await update.message.reply_text(
            f"Hey {user.mention_markdown()}! 👋\n\n"
            f"{emoji} **{title}**\n\n"
            f"⚡ Click to get in PM ⬇️",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        
        # Schedule deletion of the group's search results prompt and the original message
        await delete_messages_after_delay(
            context, 
            update.effective_chat.id, 
            [reply_msg.message_id, update.message.message_id], 
            120
        )
        
    except Exception as e:
        logger.error(f"Group prompt error: {e}")

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all button callbacks"""
    query = update.callback_query
    
    try:
        await query.answer()
        
        # --- Membership Check & Navigation ---
        
        # Check membership callback
        if query.data == "check_membership":
            is_member = await check_user_membership(context, query.from_user.id)
            if is_member:
                await query.edit_message_caption(
                    caption=(
                        "✅ **Access Granted!**\n\n"
                        "⚡ Welcome to Ur Movie Bot!\n\n"
                        "🎬 You can now search for any movie or series.\n\n"
                        "💡 Just type the name and enjoy! ✨"
                    ),
                    reply_markup=get_main_menu_keyboard(),
                    parse_mode='Markdown'
                )
            else:
                await query.answer("❌ Please join both Channel and Group first!", show_alert=True)
            return

        # Simple navigation callbacks
        if query.data in ["about_bot", "help_bot", "search_tips"]:
             # To avoid repetition, implement a generic info function
             await send_info_message(query, query.data.split('_')[0], get_main_menu_keyboard)
             return
            
        # Back to start/main menu
        if query.data == "back_to_start":
            user = query.from_user
            await query.edit_message_caption(
                caption=(
                    f"👋 **Hey {user.first_name}!**\n\n"
                    f"⚡ Welcome to **Ur Movie Bot** ⚡\n\n"
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
        

        # --- Group Get Logic (Handle multiple parameters) ---
        if query.data.startswith("group_get_"):
            # ... (membership checks, fetching movie_data)
            
            title, url, file_id = movie_data

            # -------------------- FIX 1: SERIES LOGIC IMPROVEMENT (Group Get) --------------------
            if is_series(title):
                info = parse_series_info(title)
                base_title_to_use = info['base_title'] if info['base_title'] else title
                seasons_data = get_series_episodes(base_title_to_use)
                
                total_episodes = sum(len(eps) for eps in seasons_data.values())

                if total_episodes > 1 or len(seasons_data) > 1: # Only show selection if multiple found
                    # ... (rest of the series/season selection code - send to PM)
                    # ...
                    await query.edit_message_text("✅ Check your PM for Season Selection!")
                    return

            # -------------------- FIX 2: QUALITY CHECK (Group Get) --------------------
# It's a single movie/episode. Prompt for quality.
qualities = get_all_movie_qualities(movie_id)
 
if qualities and len(qualities) > 1:
    # ... (send quality selection keyboard to PM)
    pass # <-- ADD 'pass' HERE
else:
    # Send the default file (single quality or fallback)
    q_data = qualities[0] if qualities else (None, url, file_id, None)
    quality = q_data[0] if q_data[0] else ""
    await send_movie_file(dummy_update, context, f"{title} [{quality}]", q_data[1] or url, q_data[2] or file_id)

await query.edit_message_text("✅ Check your PM! Sent to your private chat.")

        # Movie selection (from multiple search results) - select_ handler
        if query.data.startswith("select_"):
            # ... (database fetching code)
            
            # The original code, with proper indentation restored for clarity:

def some_handler_function(update, context): # Assuming this is all inside a function
    # -------------------- FIX 3: SELECT HANDLER SERIES/QUALITY CHECK --------------------
    if is_series(title):
        info = parse_series_info(title)
        base_title_to_use = info['base_title'] if info['base_title'] else title
        seasons_data = get_series_episodes(base_title_to_use)
        
        total_episodes = sum(len(eps) for eps in seasons_data.values())

        if total_episodes > 1 or len(seasons_data) > 1:
            # ... (show season selection)
            return
        
    # Single movie/episode path (or single series entry)
    qualities = get_all_movie_qualities(movie_id)
    
    if qualities and len(qualities) > 1:
        # ... (show quality selection keyboard)
    elif qualities:
        # One quality available
        quality, url_q, file_id_q, _ = qualities[0]
        await query.edit_message_text(f"⚡ Sending **{title}** [{quality}]...", parse_mode='Markdown')
        await send_movie_file(update, context, f"{title} [{quality}]", url_q or url, file_id_q or file_id)
        await query.edit_message_text("✅ Sent!")
    else:
        # No quality options found
        await query.edit_message_text(f"⚡ Sending **{title}**...", parse_mode='Markdown')
        await send_movie_file(update, context, title, url, file_id)
        await query.edit_message_text("✅ Sent!")


    # Episode/movie selection - movie_ handler (for series episodes or a movie chosen from keyboard)
    if query.data.startswith("movie_"): # <-- FIX: Ensure this is correctly indented
        # ... (database fetching code)
           # -------------------- FIX 4: MOVIE HANDLER QUALITY CHECK --------------------

try: # <-- ADDED 'try' BLOCK START HERE

    qualities = get_all_movie_qualities(movie_id)
    
    if qualities and len(qualities) > 1:
        await query.edit_message_text(
            f"🎬 **{title}**\n\n⬇️ Select Quality:",
            reply_markup=create_quality_selection_keyboard(movie_id, title, qualities),
            parse_mode='Markdown'
        )
    elif qualities:
        # One quality available
        quality, url_q, file_id_q, _ = qualities[0]
        await query.edit_message_text(f"⚡ Sending **{title}** [{quality}]...", parse_mode='Markdown')
        await send_movie_file(update, context, f"{title} [{quality}]", url_q or url, file_id_q or file_id)
        await query.edit_message_text("✅ Sent!")
    else:
        # No quality options found
        await query.edit_message_text(f"⚡ Sending **{title}**...", parse_mode='Markdown')
        await send_movie_file(update, context, title, url, file_id)
        await query.edit_message_text("✅ Sent!")

# <-- The exception handler now correctly follows the 'try' block

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
        reply_markup=keyboard,
        parse_mode='Markdown'
    )
finally:
    if conn:
        conn.close()
    return

        # --- Movie Search Results Pagination & Selection ---
        
        # Movie selection (from multiple search results)
        if query.data.startswith("select_"):
            movie_id = int(query.data.replace("select_", ""))
            
            conn = get_db_connection()
            if not conn:
                await query.edit_message_text("❌ Database error. Please try searching again.")
                return
            
            try:
                cur = conn.cursor()
                cur.execute("SELECT title, url, file_id FROM movies WHERE id = %s", (movie_id,))
                result = cur.fetchone()
                cur.close()
                
                if not result:
                    await query.edit_message_text("❌ Movie not found in database!")
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
                        
                        await query.edit_message_text(
                            f"📺 **{base_title_to_use}**\n\n⬇️ Select Season:",
                            reply_markup=create_season_selection_keyboard(seasons_data, base_title_to_use),
                            parse_mode='Markdown'
                        )
                        return
                
                # Single movie/episode path
                qualities = get_all_movie_qualities(movie_id)
                if qualities and len(qualities) > 1:
                    await query.edit_message_text(
                        f"🎬 **{title}**\n\n⬇️ Select Quality:",
                        reply_markup=create_quality_selection_keyboard(movie_id, title, qualities),
                        parse_mode='Markdown'
                    )
                elif qualities:
                    # One quality available
                    quality, url_q, file_id_q, _ = qualities[0]
                    await query.edit_message_text(f"⚡ Sending **{title}** [{quality}]...", parse_mode='Markdown')
                    await send_movie_file(update, context, f"{title} [{quality}]", url_q or url, file_id_q or file_id)
                else:
                    # No quality options found
                    await query.edit_message_text(f"⚡ Sending **{title}**...", parse_mode='Markdown')
                    await send_movie_file(update, context, title, url, file_id)
            finally:
                if conn:
                    conn.close()
            return

        # Page navigation
        if query.data.startswith("page_"):
            page = int(query.data.replace("page_", ""))
            movies = context.user_data.get('search_results', [])
            if movies:
                await query.edit_message_text(
                    f"🔍 **Found {len(movies)} results**\n\n⬇️ Select one (Page {page+1}):",
                    reply_markup=create_movie_selection_keyboard(movies, page),
                    parse_mode='Markdown'
                )
            else:
                await query.edit_message_text("❌ Search results expired. Please search again.")
            return

        # Back to seasons
        if query.data == "back_to_seasons":
            seasons_data = context.user_data.get('series_data', {})
            base_title = context.user_data.get('base_title', 'Series')
            if seasons_data:
                await query.edit_message_text(
                    f"📺 **{base_title}**\n\n⬇️ Select Season:",
                    reply_markup=create_season_selection_keyboard(seasons_data, base_title),
                    parse_mode='Markdown'
                )
            else:
                await query.edit_message_text("❌ Series data expired. Please search again.")
            return
            
        # Season selection
        if query.data.startswith("season_"):
            # Fix: Callback data was "season_{season_num}_{base_title[:30]}", so split on the second underscore
            parts = query.data.split('_')
            # Fix: The season number is now in parts[1] since create_season_selection_keyboard was simplified
            season_num = int(parts[1]) 
            
            seasons_data = context.user_data.get('series_data', {})
            episodes = seasons_data.get(season_num, [])
            base_title = context.user_data.get('base_title', 'Series')

            if episodes:
                await query.edit_message_text(
                    f"📺 **{base_title} - Season {season_num}**\n\n⬇️ Select Episode:",
                    reply_markup=create_episode_selection_keyboard(episodes, season_num),
                    parse_mode='Markdown'
                )
            else:
                await query.edit_message_text("❌ Episodes not found for this season. Please try another season or search again.")
            return
            
        # Episode/movie selection (The actual ID to fetch content)
        if query.data.startswith("movie_"):
            # Fix: Ensure query.data is not a malformed string like "movie_{...}" which was a bad pattern
            movie_id = int(query.data.replace("movie_", ""))
            
            conn = get_db_connection()
            if not conn:
                await query.edit_message_text("❌ Database error. Please try again.")
                return
            
            try:
                cur = conn.cursor()
                cur.execute("SELECT title, url, file_id FROM movies WHERE id = %s", (movie_id,))
                result = cur.fetchone()
                cur.close()
                
                if not result:
                    await query.edit_message_text("❌ Episode/Movie data not found in database!")
                    return
                
                title, url, file_id = result
                qualities = get_all_movie_qualities(movie_id)
                
                if qualities and len(qualities) > 1:
                    await query.edit_message_text(
                        f"🎬 **{title}**\n\n⬇️ Select Quality:",
                        reply_markup=create_quality_selection_keyboard(movie_id, title, qualities),
                        parse_mode='Markdown'
                    )
                elif qualities:
                    # One quality available
                    quality, url_q, file_id_q, _ = qualities[0]
                    await query.edit_message_text(f"⚡ Sending **{title}** [{quality}]...", parse_mode='Markdown')
                    await send_movie_file(update, context, f"{title} [{quality}]", url_q or url, file_id_q or file_id)
                    await query.edit_message_text("✅ Sent!")
                else:
                    # No quality options found
                    await query.edit_message_text(f"⚡ Sending **{title}**...", parse_mode='Markdown')
                    await send_movie_file(update, context, title, url, file_id)
                    await query.edit_message_text("✅ Sent!")
            finally:
                if conn:
                    conn.close()
            return
            
        # Quality selection
        if query.data.startswith("quality_"):
            # Fix: Parse the URL-encoded quality string
            parts = query.data.split('_', 2) 
            if len(parts) < 3:
                 await query.answer("Invalid callback data structure for quality.", show_alert=True)
                 return
                 
            movie_id = int(parts[1]) # movie_id is the second part
            selected_quality_encoded = parts[2] # quality is the third part (the rest)
            selected_quality = urlunparse((selected_quality_encoded, '', '', '', '', '')) # Simple URL decode

            conn = get_db_connection()
            if not conn:
                await query.edit_message_text("❌ Database error. Please try again.")
                return
            
            title = "Movie"
            try:
                cur = conn.cursor()
                
                cur.execute("SELECT title FROM movies WHERE id = %s", (movie_id,))
                title_res = cur.fetchone()
                title = title_res[0] if title_res else "Movie"
                
                file_data = None
                
                if selected_quality == '🎬 Watch Online':
                    # Get the main URL from the movies table
                    cur.execute("SELECT url, file_id FROM movies WHERE id = %s", (movie_id,))
                    file_data = cur.fetchone()
                else:
                    # Get specific quality file data
                    cur.execute("""
                        SELECT url, file_id FROM movie_files
                        WHERE movie_id = %s AND quality = %s
                    """, (movie_id, selected_quality))
                    file_data = cur.fetchone()
                
                cur.close()
                
                if file_data:
                    url, file_id = file_data
                    
                    if not url and not file_id:
                        await query.edit_message_text(f"❌ **{title}** [{selected_quality}] link is missing or invalid!")
                        return

                    await query.edit_message_text(f"⚡ Sending **{title}** [{selected_quality}]...", parse_mode='Markdown')
                    await send_movie_file(update, context, f"{title} [{selected_quality}]", url, file_id)
                else:
                    await query.edit_message_text("❌ File not found for that quality!")
            finally:
                if conn:
                    conn.close()
            return
            
        # Cancel
        if query.data == "cancel_selection":
            await query.edit_message_text("❌ Cancelled selection.")
            return

    except Exception as e:
        logger.error(f"Callback error: {e}")
        try:
            await query.answer("❌ Error occurred processing selection!", show_alert=True)
            # Attempt to delete the message if it's not the main start message
            if query.message.caption or query.message.text:
                 await query.edit_message_text("❌ An error occurred. Please try searching again.")
        except Exception as edit_e:
             logger.error(f"Failed to edit message in callback error handler: {edit_e}")


# Helper function for sending info messages
async def send_info_message(query, info_type, back_keyboard_func):
    """Helper to send detailed info based on callback type"""
    
    caption = ""
    if info_type == "about":
        caption = (
            "ℹ️ **About Ur Movie Bot**\n\n"
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
        )
    elif info_type == "help":
         caption = (
            "❓ **How to Use**\n\n"
            "**Step 1:** Join Channel & Group ✅\n"
            "**Step 2:** Type movie/series name 🔍\n"
            "**Step 3:** Select from results 📋\n"
            "**Step 4:** Choose quality 🎥\n"
            "**Step 5:** Enjoy! 🍿\n\n"
            "━━━━━━━━━━━━━━━━━\n\n"
            "**📝 Tips:**\n"
            "• Use correct spelling\n"
            "• Add year for accuracy\n"
            "• For series: Include S and E numbers\n\n"
            "⚡ Need more help? Contact admin!"
        )
    elif info_type == "search":
        caption = (
            "🔍 **Search Tips**\n\n"
            "Maximize your search success:\n\n"
            "✅ **Good Searches:**\n"
            "`Inception 2010` (Title + Year)\n"
            "`Breaking Bad S03 E05` (Series S/E format)\n"
            "`Tenet` (Exact Title)\n\n"
            "❌ **Bad Searches:**\n"
            "`Inception full movie download` (Too long)\n"
            "`breakin bad` (Typo)\n"
            "`new movie 2024` (Too generic)\n\n"
            "💡 Try to keep it simple and accurate!"
        )
    
    await query.edit_message_caption(
        caption=caption,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔙 Back", callback_data="back_to_start")
        ]]),
        parse_mode='Markdown'
    )


async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main menu handler - redirects to search if text is received in private chat"""
    return await search_movies(update, context)

# ==================== ERROR HANDLER ====================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Error handler"""
    logger.error("Exception while handling an update:", exc_info=context.error)
    
    # If the update is a callback query, answer it
    if isinstance(update, Update) and update.callback_query:
        try:
             await update.callback_query.answer("❌ An internal error occurred.", show_alert=True)
             # Attempt to clean up the message the button was attached to
             await update.callback_query.edit_message_text("❌ An error occurred. Please try searching again.")
        except Exception as e:
            logger.error(f"Failed to handle error with callback_query: {e}")
            pass
        return

    if isinstance(update, Update) and update.effective_message:
        try:
            msg = await update.effective_message.reply_text("❌ Something went wrong. Please try again.")
            await auto_delete_message(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_TIME)
        except Exception as e:
             logger.error(f"Failed to reply to message in error handler: {e}")
             pass

# ==================== MAIN BOT ====================
def main():
    """Run the Telegram bot"""
    logger.info("Starting Ur Movie Bot...")
    
    # Setup database (assuming you have this file)
    try:
        from setup_database import setup_database
        setup_database()
        logger.info("Database setup executed (if available).")
    except ImportError:
        logger.warning("setup_database.py not found. Skipping database setup.")
    except Exception as e:
        logger.error(f"Error during database setup: {e}")

    # Around line 1439 in your original corrected code (or slightly before/after)

    # Fix: Use BOT_USERNAME from env if it exists, otherwise get it dynamically
    if not BOT_USERNAME or BOT_USERNAME == 'your_bot':  # <-- BOT_USERNAME is used here (read)
         # Fetch the bot's username dynamically after the application is built
         application = Application.builder()...
         try:
             bot_info = asyncio.run(application.bot.get_me())
             global BOT_USERNAME  # <-- 'global' declared AFTER use
             BOT_USERNAME = bot_info.username
             logger.info(f"Dynamically set BOT_USERNAME to @{BOT_USERNAME}")
         except Exception as e:
             logger.error(f"Failed to fetch bot username: {e}. Using default 'your_bot'.")
    else:
         application = Application.builder().token(TELEGRAM_BOT_TOKEN).read_timeout(30).write_timeout(30).build()
    
    # Conversation handler (only for private chat searches)
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start, filters=filters.ChatType.PRIVATE)],
        states={
            MAIN_MENU: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, 
                    main_menu # Will call search_movies indirectly
                )
            ],
            # SEARCHING state is effectively merged into MAIN_MENU for simplicity, 
            # as search_movies returns MAIN_MENU and the logic is stateless per search.
        },
        fallbacks=[CommandHandler('cancel', lambda u, c: u.message.reply_text("Cancelled."))],
        # The per_message=False is fine as you are not managing complex sequential input
        per_message=False,
        per_chat=True,
    )
    
    # Handlers
    application.add_handler(CallbackQueryHandler(button_callback))
    # Group messages use the simple handler, non-private messages are filtered out of the conv_handler
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS, group_message_handler))
    application.add_handler(conv_handler)
    
    # Add your existing admin command handlers here (placeholders)
    # application.add_handler(CommandHandler("addmovie", add_movie))
    # application.add_handler(CommandHandler("updateblog", update_blog))
    
    application.add_error_handler(error_handler)
    
    logger.info("Bot started successfully! 🎬⚡")
    # This is a blocking call and should be the last line
    application.run_polling()

if __name__ == '__main__':
    # Fix: Remove unused Flask setup from the main execution block
    # flask_thread = threading.Thread(target=run_flask)
    # flask_thread.daemon = True
    # flask_thread.start()
    
    main()
