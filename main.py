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
import time
from functools import wraps

from bs4 import BeautifulSoup
import telegram
import psycopg2
from typing import Optional, Dict, List
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

# ==================== FLASK APP (for Render/Web hosting) ====================
app = Flask(__name__)

@app.route("/")
def index():
    return "Ur Movie Bot is running ✅"

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

# Rate limiting
user_last_request = defaultdict(lambda: datetime.min)
REQUEST_COOLDOWN_MINUTES = int(os.environ.get('REQUEST_COOLDOWN_MINUTES', '10'))
SIMILARITY_THRESHOLD = int(os.environ.get('SIMILARITY_THRESHOLD', '80'))
MAX_REQUESTS_PER_MINUTE = int(os.environ.get('MAX_REQUESTS_PER_MINUTE', '10'))

# Auto delete delay (seconds) for normal bot messages
AUTO_DELETE_DELAY = int(os.environ.get('AUTO_DELETE_DELAY', '300'))  # default 5 minutes

# Membership cache for performance
membership_cache = {}
MEMBERSHIP_CACHE_TIME = 300  # 5 minutes

# User stats tracking
user_stats = defaultdict(lambda: {
    'searches': 0,
    'downloads': 0,
    'last_active': datetime.now(),
    'membership_warnings': 0
})

# Validate required environment variables
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN environment variable is not set")
    raise ValueError("TELEGRAM_BOT_TOKEN is not set.")

if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable is not set")
    raise ValueError("DATABASE_URL is not set.")

# ==================== UTILITY FUNCTIONS ====================
def preprocess_query(query):
    """Clean and normalize user query - UNCHANGED"""
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
    """Check if user is rate limited - UNCHANGED"""
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
    """Normalize title for fuzzy matching - UNCHANGED"""
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
    """Check if title is a series based on patterns - UNCHANGED"""
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
    """Parse series information from title - UNCHANGED"""
    try:
        info = {
            'base_title': title,
            'season': None,
            'episode': None,
            'is_series': False
        }
        
        match = re.search(r'S(\d+)\s*E\d+', title, re.IGNORECASE)
        if match:
            info['season'] = int(match.group(1))
            info['base_title'] = title[:match.start()].strip()
            info['is_series'] = True
            return info
        
        match = re.search(r'Season\s*(\d+)', title, re.IGNORECASE)
        if match:
            info['season'] = int(match.group(1))
            info['base_title'] = re.sub(r'Season\s*\d+.*', '', title, flags=re.IGNORECASE).strip()
            info['is_series'] = True
            
        return info
    except Exception as e:
        logger.error(f"Error in parse_series_info: {e}")
        return {'base_title': title, 'season': None, 'episode': None, 'is_series': False}

# ==================== ENHANCED FORCE JOIN CHECK ====================
async def check_user_membership(context, user_id):
    """Check if user is member of BOTH required channel and group"""
    try:
        # Check cache first for performance
        cache_key = f"membership_{user_id}"
        current_time = datetime.now()
        
        if cache_key in membership_cache:
            cached_time, is_member = membership_cache[cache_key]
            if current_time - cached_time < timedelta(seconds=MEMBERSHIP_CACHE_TIME):
                logger.debug(f"Using cached membership for user {user_id}: {is_member}")
                return is_member
        
        # Parallel check for both channel and group
        tasks = [
            context.bot.get_chat_member(chat_id=REQUIRED_CHANNEL_ID, user_id=user_id),
            context.bot.get_chat_member(chat_id=REQUIRED_GROUP_ID, user_id=user_id)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        channel_joined = False
        group_joined = False
        
        if not isinstance(results[0], Exception):
            channel_joined = results[0].status in ['member', 'administrator', 'creator']
        else:
            logger.warning(f"Could not check channel membership for {user_id}: {results[0]}")
        
        if not isinstance(results[1], Exception):
            group_joined = results[1].status in ['member', 'administrator', 'creator']
        else:
            logger.warning(f"Could not check group membership for {user_id}: {results[1]}")
        
        # Both must be joined
        is_member = channel_joined and group_joined
        
        # Cache the result
        membership_cache[cache_key] = (current_time, is_member)
        
        # Log for debugging
        if not is_member:
            logger.info(f"User {user_id} membership check failed - Channel: {channel_joined}, Group: {group_joined}")
            user_stats[user_id]['membership_warnings'] += 1
        
        return is_member
        
    except Exception as e:
        logger.error(f"Error checking membership for user {user_id}: {e}")
        return False

def get_force_join_keyboard():
    """Get premium styled force join keyboard"""
    try:
        keyboard = [
            [
                InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL),
                InlineKeyboardButton("💬 Join Group", url=FILMFYBOX_GROUP_URL)
            ],
            [InlineKeyboardButton("✅ I've Joined Both - Verify", callback_data="check_membership")],
            [InlineKeyboardButton("❓ Why Join?", callback_data="why_join_info")]
        ]
        return InlineKeyboardMarkup(keyboard)
    except Exception as e:
        logger.error(f"Error creating force join keyboard: {e}")
        return None

# ==================== DATABASE CONNECTION ====================
def get_db_connection():
    """Get database connection - UNCHANGED"""
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

# ==================== MOVIE SEARCH WITH SERIES SUPPORT - EXACT SAME LOGIC ====================
def get_movies_from_db(user_query, limit=10):
    """Search for movies/series in database - EXACT COPY FROM YOUR CODE"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return []
        
        cur = conn.cursor()
        logger.info(f"Searching for: '{user_query}'")
        
        # Exact match first
        cur.execute(
            "SELECT id, title, url, file_id FROM movies WHERE LOWER(title) LIKE LOWER(%s) ORDER BY title LIMIT %s",
            (f'%{user_query}%', limit)
        )
        exact_matches = cur.fetchall()
        
        if exact_matches:
            logger.info(f"Found {len(exact_matches)} exact matches")
            return exact_matches
        
        # Fuzzy search if no exact matches
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
                    filtered_movies.append(movie_dict[title])
        
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
    """Fetch all available qualities - EXACT COPY FROM YOUR CODE"""
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
                WHEN 'HD Quality' THEN 2
                WHEN 'Standard Quality' THEN 3
                WHEN 'Low Quality' THEN 4
                ELSE 5
            END DESC
        """, (movie_id,))
        results = cur.fetchall()
        cur.close()
        return results
    except Exception as e:
        logger.error(f"Error fetching movie qualities for {movie_id}: {e}")
        return []
    finally:
        if conn:
            conn.close()

def create_movie_selection_keyboard(movies, page=0, movies_per_page=5):
    """Create premium styled movie selection keyboard"""
    start_idx = page * movies_per_page
    end_idx = start_idx + movies_per_page
    current_movies = movies[start_idx:end_idx]

    keyboard = []

    for movie in current_movies:
        movie_id, title, url, file_id = movie
        # Add quality/type indicator
        emoji = "📺" if is_series(title) else "🎬"
        button_text = f"{emoji} {title}" if len(title) <= 35 else f"{emoji} {title[:32]}..."
        keyboard.append([InlineKeyboardButton(
            button_text,
            callback_data=f"movie_{movie_id}"
        )])

    nav_buttons = []
    total_pages = (len(movies) + movies_per_page - 1) // movies_per_page

    if page > 0:
        nav_buttons.append(InlineKeyboardButton("◀️ Previous", callback_data=f"page_{page-1}"))
    
    if total_pages > 1:
        nav_buttons.append(InlineKeyboardButton(f"📄 {page+1}/{total_pages}", callback_data="noop"))
    
    if end_idx < len(movies):
        nav_buttons.append(InlineKeyboardButton("Next ▶️", callback_data=f"page_{page+1}"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_selection")])

    return InlineKeyboardMarkup(keyboard)

def create_quality_selection_keyboard(movie_id, title, qualities):
    """Create premium quality selection keyboard"""
    keyboard = []

    # Quality emoji mapping for better UX
    quality_icons = {
        '4K': '💎',
        'HD Quality': '🔷', 
        'Standard Quality': '🟢',
        'Low Quality': '🟡'
    }

    for quality, url, file_id, file_size in qualities:
        callback_data = f"quality_{movie_id}_{quality}"
        
        icon = quality_icons.get(quality, '🎬')
        size_text = f" ({file_size})" if file_size else ""
        link_type = "📁" if file_id else "🔗"
        
        button_text = f"{icon} {quality}{size_text} {link_type}"
        
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])

    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_selection")])

    return InlineKeyboardMarkup(keyboard)

def get_series_episodes(base_title):
    """Get all episodes for a series - UNCHANGED"""
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
                        'episode': info.get('episode', 0)
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

def create_season_selection_keyboard(seasons_data, base_title):
    """Create season selection keyboard for series - UNCHANGED"""
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
    """Create episode selection keyboard - UNCHANGED"""
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

# ==================== AUTO DELETE HELPER ====================
async def delete_messages_after_delay(context, chat_id, message_ids, delay=60):
    """Delete messages after delay - UNCHANGED"""
    try:
        await asyncio.sleep(delay)
        for msg_id in message_ids:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except Exception as e:
                logger.debug(f"Could not delete message {msg_id}: {e}")
    except Exception as e:
        logger.error(f"Error in delete_messages_after_delay: {e}")

def schedule_delete(context, chat_id, message_ids, delay=None):
    """Helper to schedule auto-deletion - UNCHANGED"""
    try:
        if not message_ids:
            return
        if delay is None:
            delay = AUTO_DELETE_DELAY

        asyncio.create_task(
            delete_messages_after_delay(context, chat_id, message_ids, delay)
        )
    except Exception as e:
        logger.error(f"Error scheduling delete: {e}")

# ==================== PREMIUM SEND MOVIE WITH STRICT MEMBERSHIP CHECK ====================
async def send_movie_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE, movie_id: int, title: str, url: Optional[str] = None, file_id: Optional[str] = None):
    """Send movie WITH MANDATORY MEMBERSHIP CHECK"""
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id if update.effective_user else None
    
    # CRITICAL: Always check membership before sending ANY file
    if user_id:
        is_member = await check_user_membership(context, user_id)
        if not is_member:
            logger.warning(f"User {user_id} tried to access file without membership!")
            
            force_text = (
                "🚫 **MEMBERSHIP REQUIRED**\n\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "You MUST join both our Channel and Group\n"
                "to access any movie files!\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "📢 **Channel:** @filmfybox\n"
                "💬 **Group:** @Filmfybox002\n\n"
                "Join now and click verify ⬇️"
            )
            
            msg = await context.bot.send_message(
                chat_id=chat_id,
                text=force_text,
                reply_markup=get_force_join_keyboard(),
                parse_mode='Markdown'
            )
            schedule_delete(context, chat_id, [msg.message_id], 60)
            return

    # If no direct file/url, check for multiple qualities
    if not url and not file_id:
        qualities = get_all_movie_qualities(movie_id)
        if qualities:
            context.user_data['selected_movie_data'] = {
                'id': movie_id,
                'title': title,
                'qualities': qualities
            }
            selection_text = (
                f"✅ **Found: {title}**\n\n"
                f"🎯 **{len(qualities)} Qualities Available**\n"
                f"Select your preferred quality:"
            )
            keyboard = create_quality_selection_keyboard(movie_id, title, qualities)
            msg = await context.bot.send_message(
                chat_id=chat_id,
                text=selection_text,
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
            schedule_delete(context, chat_id, [msg.message_id], 300)
            return

    try:
        # Update user stats
        user_stats[user_id]['downloads'] += 1
        user_stats[user_id]['last_active'] = datetime.now()
        
        # Premium warning message
        warning_msg = await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "⚠️ **AUTO-DELETE WARNING**\n"
                "━━━━━━━━━━━━━━━━━\n"
                "❗ File deletes in **60 seconds**\n"
                "📤 **Forward NOW to save!**\n"
                "━━━━━━━━━━━━━━━━━"
            ),
            parse_mode='Markdown'
        )

        sent_msg = None
        caption_text = (
            f"🎬 **{title}**\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"📢 **Channel:** @filmfybox\n"
            f"💬 **Group:** @Filmfybox002\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"⏰ Auto-delete: **60 seconds**\n"
            f"💡 Forward to save permanently!"
        )
        
        join_keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("📢 Channel", url=FILMFYBOX_CHANNEL_URL),
            InlineKeyboardButton("💬 Group", url=FILMFYBOX_GROUP_URL)
        ]])

        if file_id:
            sent_msg = await context.bot.send_document(
                chat_id=chat_id,
                document=file_id,
                caption=caption_text,
                parse_mode='Markdown',
                reply_markup=join_keyboard
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
                    caption=caption_text,
                    parse_mode='Markdown',
                    reply_markup=join_keyboard
                )
            except Exception as e:
                logger.error(f"Copy private link failed {url}: {e}")
                sent_msg = await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🎬 **{title}**\n\n{caption_text}",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🎬 Watch Now", url=url),
                        InlineKeyboardButton("📢 Join", url=FILMFYBOX_CHANNEL_URL)
                    ]]),
                    parse_mode='Markdown'
                )
        elif url and url.startswith("https://t.me/"):
            try:
                parts = url.rstrip('/').split('/')
                username = parts[-2].lstrip("@")
                message_id = int(parts[-1])
                from_chat_id = f"@{username}"
                sent_msg = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=from_chat_id,
                    message_id=message_id,
                    caption=caption_text,
                    parse_mode='Markdown',
                    reply_markup=join_keyboard
                )
            except Exception as e:
                logger.error(f"Copy public link failed {url}: {e}")
                sent_msg = await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🎬 **{title}**\n\n{caption_text}",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🎬 Watch", url=url),
                        InlineKeyboardButton("📢 Join", url=FILMFYBOX_CHANNEL_URL)
                    ]]),
                    parse_mode='Markdown'
                )
        elif url:
            sent_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"🎬 **{title}**\n\n{caption_text}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🎬 Watch Now", url=url),
                    InlineKeyboardButton("📢 Join", url=FILMFYBOX_CHANNEL_URL)
                ]]),
                parse_mode='Markdown'
            )
        else:
            sent_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Sorry, **{title}** found but no file available."
            )

        if sent_msg:
            await delete_messages_after_delay(
                context,
                chat_id,
                [warning_msg.message_id, sent_msg.message_id],
                60
            )

    except Exception as e:
        logger.error(f"Error sending movie: {e}")
        await context.bot.send_message(chat_id=chat_id, text="❌ Failed to send file.")

async def send_movie_file(update, context, title, url=None, file_id=None):
    """Alternative send function with membership check"""
    chat_id = update.effective_chat.id if update.effective_chat else None
    user_id = update.effective_user.id if update.effective_user else None
    
    if not chat_id:
        return
    
    # ALWAYS CHECK MEMBERSHIP
    is_member = await check_user_membership(context, user_id)
    if not is_member:
        access_msg = await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "🚫 **ACCESS DENIED**\n\n"
                "Join our Channel and Group first:\n"
                "📢 @filmfybox\n"
                "💬 @Filmfybox002"
            ),
            reply_markup=get_force_join_keyboard(),
            parse_mode='Markdown'
        )
        schedule_delete(context, chat_id, [access_msg.message_id])
        return
    
    # Call the main send function
    await send_movie_to_user(update, context, 0, title, url, file_id)

# ==================== BOT HANDLERS ====================
async def start(update, context):
    """Premium start command"""
    try:
        # Handle deep links
        if context.args and context.args[0].startswith("movie_"):
            try:
                movie_id = int(context.args[0].split('_')[1])
                
                # CHECK MEMBERSHIP FIRST
                user_id = update.effective_user.id
                is_member = await check_user_membership(context, user_id)
                
                if not is_member:
                    join_msg = await update.message.reply_text(
                        "🚫 **Join Required!**\n\n"
                        "Join our Channel and Group to access movies:",
                        reply_markup=get_force_join_keyboard(),
                        parse_mode='Markdown'
                    )
                    schedule_delete(context, update.effective_chat.id, [join_msg.message_id])
                    return MAIN_MENU
                
                conn = get_db_connection()
                if conn:
                    cur = conn.cursor()
                    cur.execute("SELECT title, url, file_id FROM movies WHERE id = %s", (movie_id,))
                    movie_data = cur.fetchone()
                    cur.close()
                    conn.close()
                    
                    if movie_data:
                        title, url, file_id = movie_data
                        await send_movie_to_user(update, context, movie_id, title, url, file_id)
                        return MAIN_MENU
            except Exception as e:
                logger.error(f"Deep link error: {e}")
        
        chat_id = update.effective_chat.id
        bot_info = await context.bot.get_me()
        bot_username = bot_info.username

        start_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add to Group", url=f"https://t.me/{bot_username}?startgroup=true")],
            [
                InlineKeyboardButton("📢 Channel", url=FILMFYBOX_CHANNEL_URL),
                InlineKeyboardButton("💬 Group", url=FILMFYBOX_GROUP_URL)
            ],
            [
                InlineKeyboardButton("ℹ️ Help", callback_data="start_help"),
                InlineKeyboardButton("👑 About", callback_data="start_about")
            ]
        ])

        start_caption = (
            "✨ **Ur Movie Bot** ✨\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🎬 Movie & Series Bot\n"
            "🔍 Ultra‑fast search • Multi‑quality\n"
            "🛡 Auto‑delete privacy enabled\n"
            "📂 Seasons • Episodes • Clean UI\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "▶️ *Type any movie / series name to start...*\n"
            "`Avengers Endgame`\n"
            "`Stranger Things S01E01`\n"
            "`Landman Season 1`"
        )

        banner_msg = await update.message.reply_photo(
            photo="https://blogger.googleusercontent.com/img/b/R29vZ2xl/AVvXsEh_mmbgdGwsLw5sWXVDA5DnQjQ7IzDu3CurgHNCHBiG40XGy4gt51mk0_xwoAwGvQwKi6S_7NGhPtOkdV4gUyh47kKA5LvYHcA1ozxYLE44gblBOgQ7gqsccHimH-FbeDi0TfK7nEfNIhfo7rFwYFCbIPN29sTDRz2p34ZH7pldCYst4HYwGrfkXllJF0E/s1600/Gemini_Generated_Image_4fbjgh4fbjgh4fbj.png",
            caption=start_caption,
            parse_mode='Markdown',
            reply_markup=start_keyboard
        )
        schedule_delete(context, chat_id, [banner_msg.message_id])
        return MAIN_MENU
    except Exception as e:
        logger.error(f"Error in start: {e}")
        return MAIN_MENU

async def search_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Search for movies - EXACT FLOW FROM YOUR CODE"""
    try:
        if not update.message or not update.message.text:
            return MAIN_MENU

        query = update.message.text.strip()

        # Check for menu commands
        if query in ['🔍 Search Movies', '🙋 Request Movie', '📊 My Stats', '❓ Help']:
             return await main_menu(update, context)
        
        # Check membership BEFORE search
        user_id = update.effective_user.id
        is_member = await check_user_membership(context, user_id)
        
        if not is_member:
            join_msg = await update.message.reply_text(
                "🚫 **MEMBERSHIP REQUIRED**\n\n"
                "You must join BOTH:\n"
                "📢 Channel: @FilmfyBox\n"
                "💬 Group: @FilmfyBox002\n\n"
                "To search and download movies!",
                reply_markup=get_force_join_keyboard(),
                parse_mode='Markdown'
            )
            schedule_delete(context, update.effective_chat.id, [join_msg.message_id])
            return MAIN_MENU

        # 1. Search in DB (EXACT SAME LOGIC)
        movies = get_movies_from_db(query)

        # 2. If no movies found
        if not movies:
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🙋 Request This Movie", callback_data=f"request_{query[:20]}")],
                [InlineKeyboardButton("🔍 New Search", callback_data="new_search")]
            ])
            
            await update.message.reply_text(
                f"😕 **Not Found**\n\n"
                f"Couldn't find: `{query}`\n\n"
                f"Would you like to request it?",
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
            return MAIN_MENU

        # 3. Store results and show selection
        context.user_data['search_results'] = movies
        context.user_data['search_query'] = query

        keyboard = create_movie_selection_keyboard(movies, page=0)
        
        result_msg = await update.message.reply_text(
            f"🎬 **Found {len(movies)} results**\n"
            f"Search: `{query}`\n\n"
            f"👇 Select your movie:",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        
        # Update stats
        user_stats[user_id]['searches'] += 1
        user_stats[user_id]['last_active'] = datetime.now()
        
        return MAIN_MENU

    except Exception as e:
        logger.error(f"Error in search_movies: {e}")
        await update.message.reply_text("❌ Search failed.")
        return MAIN_MENU

async def group_message_handler(update, context):
    """Group handler WITH MEMBERSHIP WARNING"""
    try:
        if not update.message or not update.message.text or update.message.from_user.is_bot:
            return
        
        message_text = update.message.text.strip()
        user = update.effective_user
        
        if len(message_text) < 4 or message_text.startswith('/'):
            return
        
        movies_found = get_movies_from_db(message_text, limit=1)
        
        if not movies_found:
            return
        
        movie_id, title, _, _ = movies_found[0]
        score = fuzz.token_sort_ratio(_normalize_title_for_match(message_text), _normalize_title_for_match(title))
        
        if score < 85:
            return
        
        emoji = "📺" if is_series(title) else "🎬"
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(f"✅ Get {emoji} (Join Required)", callback_data=f"group_get_{movie_id}_{user.id}")
        ]])
        
        reply_msg = await update.message.reply_text(
            f"Hey {user.mention_markdown()}! 👋\n\n"
            f"{emoji} **{title}**\n\n"
            f"⚠️ **Note:** Channel & Group membership required!\n"
            f"Tap below to receive in PM ⬇️",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        
        schedule_delete(context, update.effective_chat.id, [reply_msg.message_id], 120)
    except Exception as e:
        logger.error(f"Group handler error: {e}")

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all callbacks - WITH MEMBERSHIP CHECKS"""
    try:
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id

        # Check membership verification
        if query.data == "check_membership":
            # Clear cache for fresh check
            cache_key = f"membership_{user_id}"
            if cache_key in membership_cache:
                del membership_cache[cache_key]
            
            is_member = await check_user_membership(context, user_id)
            if is_member:
                await query.edit_message_text(
                    "✅ **VERIFIED!**\n\n"
                    "Welcome to FilmfyBox Premium! 🎬\n"
                    "You can now search any movie...",
                    parse_mode='Markdown'
                )
                schedule_delete(context, query.message.chat.id, [query.message.message_id], 30)
            else:
                # Check which one is missing
                try:
                    channel_check = await context.bot.get_chat_member(REQUIRED_CHANNEL_ID, user_id)
                    channel_joined = channel_check.status in ['member', 'administrator', 'creator']
                except:
                    channel_joined = False
                
                try:
                    group_check = await context.bot.get_chat_member(REQUIRED_GROUP_ID, user_id)
                    group_joined = group_check.status in ['member', 'administrator', 'creator']
                except:
                    group_joined = False
                
                status_text = (
                    "❌ **NOT VERIFIED**\n\n"
                    f"📢 Channel: {'✅ Joined' if channel_joined else '❌ Not Joined'}\n"
                    f"💬 Group: {'✅ Joined' if group_joined else '❌ Not Joined'}\n\n"
                    "Please join BOTH to continue!"
                )
                
                await query.answer(status_text, show_alert=True)
            return

        # Movie selection WITH MEMBERSHIP CHECK
        elif query.data.startswith("movie_"):
            # CHECK MEMBERSHIP FIRST
            is_member = await check_user_membership(context, user_id)
            if not is_member:
                await query.answer("❌ Join our Channel and Group first!", show_alert=True)
                await query.edit_message_text(
                    "🚫 **MEMBERSHIP REQUIRED!**",
                    reply_markup=get_force_join_keyboard()
                )
                return

            movie_id = int(query.data.replace("movie_", ""))

            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT id, title FROM movies WHERE id = %s", (movie_id,))
            movie = cur.fetchone()
            cur.close()
            conn.close()

            if not movie:
                await query.edit_message_text("❌ Movie not found.")
                return

            movie_id, title = movie
            
            # Check for multiple qualities
            qualities = get_all_movie_qualities(movie_id)

            if not qualities:
                # No qualities in movie_files - use main table
                await query.edit_message_text(f"📤 Sending **{title}**...", parse_mode='Markdown')
                
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT url, file_id FROM movies WHERE id = %s", (movie_id,))
                result = cur.fetchone()
                url, file_id = result if result else (None, None)
                cur.close()
                conn.close()

                await send_movie_to_user(update, context, movie_id, title, url, file_id)
                return

            # Multiple qualities available
            context.user_data['selected_movie_data'] = {
                'id': movie_id,
                'title': title,
                'qualities': qualities
            }

            selection_text = f"✅ **{title}**\n\n🎯 **Choose Quality:**"
            keyboard = create_quality_selection_keyboard(movie_id, title, qualities)

            await query.edit_message_text(
                selection_text,
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
        
        # Quality selection
        elif query.data.startswith("quality_"):
            # MEMBERSHIP CHECK
            is_member = await check_user_membership(context, user_id)
            if not is_member:
                await query.answer("❌ Membership required!", show_alert=True)
                await query.edit_message_text(
                    "🚫 Join our Channel and Group!",
                    reply_markup=get_force_join_keyboard()
                )
                return

            parts = query.data.split('_')
            movie_id = int(parts[1])
            selected_quality = parts[2]

            movie_data = context.user_data.get('selected_movie_data')

            if not movie_data or movie_data.get('id') != movie_id:
                qualities = get_all_movie_qualities(movie_id)
                movie_data = {'id': movie_id, 'title': 'Movie', 'qualities': qualities}

            chosen_file = None
            for quality, url, file_id, file_size in movie_data['qualities']:
                if quality == selected_quality:
                    chosen_file = {'url': url, 'file_id': file_id}
                    break

            if not chosen_file:
                await query.edit_message_text("❌ Quality not available.")
                return

            title = movie_data['title']
            await query.edit_message_text(f"📤 Sending **{title}**...", parse_mode='Markdown')

            await send_movie_to_user(
                update,
                context,
                movie_id,
                title,
                chosen_file['url'],
                chosen_file['file_id']
            )
        
        # Group get WITH MEMBERSHIP CHECK
        elif query.data.startswith("group_get_"):
            parts = query.data.split('_')
            movie_id = int(parts[2])
            original_user_id = int(parts[3])
            
            if query.from_user.id != original_user_id:
                await query.answer("This button is not for you!", show_alert=True)
                return
            
            # CRITICAL MEMBERSHIP CHECK
            is_member = await check_user_membership(context, original_user_id)
            if not is_member:
                await query.edit_message_text(
                    "🚫 **MEMBERSHIP REQUIRED!**\n\n"
                    "Join BOTH to get movies:\n"
                    "📢 @filmfybox\n"
                    "💬 @Filmfybox002",
                    reply_markup=get_force_join_keyboard(),
                    parse_mode='Markdown'
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
                        
                        # Send to PM
                        if qualities and len(qualities) > 1:
                            await context.bot.send_message(
                                chat_id=original_user_id,
                                text=f"🎬 **{title}**\n\nSelect Quality:",
                                reply_markup=create_quality_selection_keyboard(movie_id, title, qualities),
                                parse_mode='Markdown'
                            )
                        else:
                            # Create dummy update for PM
                            dummy_update = Update(
                                update_id=0,
                                message=telegram.Message(
                                    message_id=0,
                                    date=datetime.now(),
                                    chat=telegram.Chat(id=original_user_id, type='private')
                                )
                            )
                            dummy_update._effective_user = query.from_user
                            await send_movie_to_user(dummy_update, context, movie_id, title, url, file_id)
                        
                        await query.edit_message_text("✅ Check your PM!")
                        
            except telegram.error.Forbidden:
                bot_username = (await context.bot.get_me()).username
                deep_link = f"https://t.me/{bot_username}?start=movie_{movie_id}"
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("🤖 Start Bot", url=deep_link)
                ]])
                await query.edit_message_text(
                    "❌ Please start the bot first!",
                    reply_markup=keyboard
                )
            return
        
        # Other callbacks
        elif query.data == "why_join_info":
            why_text = (
                "❓ **Why Join?**\n\n"
                "✅ **Benefits:**\n"
                "• Unlimited movie access\n"
                "• HD/4K quality files\n"
                "• Latest releases\n"
                "• 24/7 automated service\n"
                "• No ads or spam\n\n"
                "Join 50,000+ members!"
            )
            back_kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="check_membership")
            ]])
            await query.edit_message_text(why_text, reply_markup=back_kb, parse_mode='Markdown')
        
        elif query.data == "start_help":
            help_text = (
                "📖 **How to Use**\n\n"
                "1️⃣ Join Channel & Group\n"
                "2️⃣ Type movie name\n"
                "3️⃣ Select from results\n"
                "4️⃣ Choose quality\n"
                "5️⃣ Forward file to save!"
            )
            await query.edit_message_text(help_text, parse_mode='Markdown')
        
        elif query.data == "start_about":
            about_text = (
                "👑 **FilmfyBox Premium**\n\n"
                "🎬 Movies & Series Bot\n"
                "📊 Multi-quality support\n"
                "🛡 Privacy protected\n\n"
                "📢 @filmfybox\n"
                "💬 @Filmfybox002"
            )
            await query.edit_message_text(about_text, parse_mode='Markdown')
        
        elif query.data.startswith("page_"):
            page = int(query.data.replace("page_", ""))
            movies = context.user_data.get('search_results', [])
            if movies:
                await query.edit_message_text(
                    f"🔍 **{len(movies)} results**\n\nSelect:",
                    reply_markup=create_movie_selection_keyboard(movies, page),
                    parse_mode='Markdown'
                )
        
        elif query.data == "cancel_selection":
            await query.edit_message_text("❌ Cancelled.")
            schedule_delete(context, query.message.chat.id, [query.message.message_id], 5)
            
    except Exception as e:
        logger.error(f"Callback error: {e}")
        try:
            await query.answer("❌ Error occurred!", show_alert=True)
        except:
            pass

async def main_menu(update, context):
    """Main menu WITH MEMBERSHIP CHECK"""
    try:
        # Always check membership before search
        user_id = update.effective_user.id
        is_member = await check_user_membership(context, user_id)
        
        if not is_member:
            msg = await update.message.reply_text(
                "🚫 **JOIN REQUIRED!**\n\n"
                "Join our Channel and Group to search:",
                reply_markup=get_force_join_keyboard(),
                parse_mode='Markdown'
            )
            schedule_delete(context, update.effective_chat.id, [msg.message_id])
            return MAIN_MENU
        
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
                msg = await update.effective_message.reply_text("❌ Error occurred. Try again.")
                schedule_delete(context, update.effective_chat.id, [msg.message_id])
            except:
                pass
    except:
        pass

# ==================== MAIN BOT ====================

def run_flask():
    """Run Flask server"""
    try:
        port = int(os.environ.get("PORT", "10000"))
        logger.info(f"Starting Flask on port {port}")
        app.run(host="0.0.0.0", port=port)
    except Exception as e:
        logger.error(f"Flask error: {e}")

def main():
    """Run the bot"""
    try:
        logger.info("🚀 Starting Ur Movie Bot...")
        
        flask_thread = threading.Thread(target=run_flask, daemon=True)
        flask_thread.start()
        
        application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        
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
        
        logger.info("✅ Bot started! Join requirements enforced.")
        application.run_polling()
    except Exception as e:
        logger.error(f"Failed to start: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
