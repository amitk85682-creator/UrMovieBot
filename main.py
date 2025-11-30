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

# Premium features tracking
user_stats = defaultdict(lambda: {'searches': 0, 'downloads': 0, 'last_active': datetime.now()})
membership_cache = {}  # Cache membership status
MEMBERSHIP_CACHE_TIME = 300  # 5 minutes cache

# Validate required environment variables
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN environment variable is not set")
    raise ValueError("TELEGRAM_BOT_TOKEN is not set.")

if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable is not set")
    raise ValueError("DATABASE_URL is not set.")

# ==================== ENHANCED FORCE JOIN DECORATOR ====================
def require_membership(func):
    """Decorator to enforce membership check before any file sharing"""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id if update.effective_user else None
        
        if not user_id:
            return await func(update, context, *args, **kwargs)
        
        # Check membership with cache
        is_member = await check_user_membership_cached(context, user_id)
        
        if not is_member:
            # Send force join message
            chat_id = update.effective_chat.id
            
            force_join_text = (
                "🚫 **Access Denied - Membership Required!**\n\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "To access our premium content, you must join:\n\n"
                "📢 **Main Channel:** @filmfybox\n"
                "💬 **Support Group:** @Filmfybox002\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "✨ **Benefits of Joining:**\n"
                "• Unlimited movie downloads\n"
                "• Latest releases & exclusives\n"
                "• Multi-quality options\n"
                "• 24/7 support & updates\n\n"
                "👇 **Click below to join now!**"
            )
            
            keyboard = get_premium_force_join_keyboard()
            
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.answer("❌ Please join our channel and group first!", show_alert=True)
                await update.callback_query.edit_message_text(
                    force_join_text,
                    reply_markup=keyboard,
                    parse_mode='Markdown'
                )
            else:
                msg = await context.bot.send_message(
                    chat_id=chat_id,
                    text=force_join_text,
                    reply_markup=keyboard,
                    parse_mode='Markdown'
                )
                schedule_delete(context, chat_id, [msg.message_id], 60)
            
            return None
        
        return await func(update, context, *args, **kwargs)
    
    return wrapper

# ==================== ENHANCED MEMBERSHIP CHECK WITH CACHE ====================
async def check_user_membership_cached(context, user_id):
    """Check membership with caching for better performance"""
    try:
        # Check cache first
        cache_key = f"member_{user_id}"
        if cache_key in membership_cache:
            cached_time, is_member = membership_cache[cache_key]
            if datetime.now() - cached_time < timedelta(seconds=MEMBERSHIP_CACHE_TIME):
                return is_member
        
        # Actual membership check
        is_member = await check_user_membership(context, user_id)
        
        # Update cache
        membership_cache[cache_key] = (datetime.now(), is_member)
        
        return is_member
    except Exception as e:
        logger.error(f"Error in cached membership check: {e}")
        return False

async def check_user_membership(context, user_id):
    """Enhanced membership check with better error handling"""
    try:
        # Check both channel and group in parallel for faster response
        tasks = [
            context.bot.get_chat_member(chat_id=REQUIRED_CHANNEL_ID, user_id=user_id),
            context.bot.get_chat_member(chat_id=REQUIRED_GROUP_ID, user_id=user_id)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        channel_joined = False
        group_joined = False
        
        if not isinstance(results[0], Exception):
            channel_joined = results[0].status in ['member', 'administrator', 'creator']
        
        if not isinstance(results[1], Exception):
            group_joined = results[1].status in ['member', 'administrator', 'creator']
        
        # Log membership status for debugging
        if not (channel_joined and group_joined):
            logger.info(f"User {user_id} - Channel: {channel_joined}, Group: {group_joined}")
        
        return channel_joined and group_joined
        
    except Exception as e:
        logger.error(f"Error checking membership for user {user_id}: {e}")
        return False

def get_premium_force_join_keyboard():
    """Enhanced force join keyboard with better UI"""
    try:
        keyboard = [
            [
                InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL),
                InlineKeyboardButton("💬 Join Group", url=FILMFYBOX_GROUP_URL)
            ],
            [InlineKeyboardButton("✅ I've Joined Both - Verify Now", callback_data="verify_membership")],
            [InlineKeyboardButton("❓ Why Join?", callback_data="why_join")]
        ]
        return InlineKeyboardMarkup(keyboard)
    except Exception as e:
        logger.error(f"Error creating force join keyboard: {e}")
        return None

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

def update_user_stats(user_id, action='search'):
    """Track user statistics"""
    try:
        user_stats[user_id]['last_active'] = datetime.now()
        if action == 'search':
            user_stats[user_id]['searches'] += 1
        elif action == 'download':
            user_stats[user_id]['downloads'] += 1
    except Exception as e:
        logger.error(f"Error updating user stats: {e}")

# ==================== DATABASE CONNECTION ====================
def get_db_connection():
    """Get database connection with retry logic"""
    max_retries = 3
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            return conn
        except Exception as e:
            logger.error(f"Database connection attempt {i+1} failed: {e}")
            if i < max_retries - 1:
                time.sleep(1)
    return None

# ==================== AUTO DELETE HELPER ====================
async def delete_messages_after_delay(context, chat_id, message_ids, delay=60):
    """Delete messages after delay"""
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
    """Helper to schedule auto-deletion for messages"""
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

# ==================== MOVIE SEARCH FUNCTIONS ====================
def get_movies_from_db(user_query, limit=10):
    """Search for movies/series in database"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return []
        
        cur = conn.cursor()
        logger.info(f"Searching for: '{user_query}'")
        
        # Try exact match first
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
    """Fetch all available qualities for a movie"""
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
        # Add quality indicator
        quality_icon = "🎬" if file_id else "🔗"
        button_text = f"{quality_icon} {title}" if len(title) <= 35 else f"{quality_icon} {title[:32]}..."
        keyboard.append([InlineKeyboardButton(
            button_text,
            callback_data=f"movie_{movie_id}"
        )])

    # Navigation buttons
    nav_buttons = []
    total_pages = (len(movies) + movies_per_page - 1) // movies_per_page
    current_page = page + 1

    if page > 0:
        nav_buttons.append(InlineKeyboardButton("◀️ Previous", callback_data=f"page_{page-1}"))
    
    nav_buttons.append(InlineKeyboardButton(f"📄 {current_page}/{total_pages}", callback_data="page_info"))
    
    if end_idx < len(movies):
        nav_buttons.append(InlineKeyboardButton("Next ▶️", callback_data=f"page_{page+1}"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([
        InlineKeyboardButton("🔍 New Search", callback_data="new_search"),
        InlineKeyboardButton("❌ Cancel", callback_data="cancel_selection")
    ])

    return InlineKeyboardMarkup(keyboard)

def create_quality_selection_keyboard(movie_id, title, qualities):
    """Create premium quality selection keyboard"""
    keyboard = []

    # Quality icons mapping
    quality_icons = {
        '4K': '🔷',
        'HD Quality': '🔵',
        'Standard Quality': '🟢',
        'Low Quality': '🟡'
    }

    for quality, url, file_id, file_size in qualities:
        callback_data = f"quality_{movie_id}_{quality}"
        
        icon = quality_icons.get(quality, '🎬')
        size_text = f" • {file_size}" if file_size else ""
        link_type = "📁" if file_id else "🔗"
        
        button_text = f"{icon} {quality}{size_text} {link_type}"
        
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])

    keyboard.append([
        InlineKeyboardButton("🔙 Back", callback_data="back_to_search"),
        InlineKeyboardButton("❌ Cancel", callback_data="cancel_selection")
    ])

    return InlineKeyboardMarkup(keyboard)

# ==================== ENHANCED SEND MOVIE FUNCTION ====================
@require_membership
async def send_movie_file(update, context, title, url=None, file_id=None):
    """Premium send movie file with membership check"""
    try:
        chat_id = update.effective_chat.id if update.effective_chat else None
        user_id = update.effective_user.id if update.effective_user else None
        
        if not chat_id:
            logger.error("No chat_id found")
            return
        
        # Update user stats
        update_user_stats(user_id, 'download')
        
        # Premium styled warning message
        warning_text = (
            "⚠️ **Important Notice**\n"
            "━━━━━━━━━━━━━━━━━\n"
            "📌 File will **auto-delete** in 60 seconds\n"
            "📤 Please **forward** In Another Chat‼️\n"
            "━━━━━━━━━━━━━━━━━"
        )
        
        warning_msg = await context.bot.send_message(
            chat_id=chat_id,
            text=warning_text,
            parse_mode='Markdown'
        )
        
        # Premium caption
        caption_text = (
            f"🎬 <b>{title}</b>\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
            f"🚀 <b>ᴊᴏɪɴ ᴏᴜʀ ᴄʜᴀɴɴᴇʟs:</b>\n"
            f"📢 <a href='{CHANNEL_LINK}'>Main Channel</a> | 💬 <a href='{GROUP_LINK}'>Support Group</a>\n\n"
            f"⚠️ <i>Auto-delete in 60s. Forward explicitly!</i>"
        )
        
        sent_msg = None
        
        if file_id:
            # Send file with premium styling
            sent_msg = await context.bot.send_document(
                chat_id=chat_id,
                document=file_id,
                caption=caption,
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL),
                    InlineKeyboardButton("💬 Join Group", url=FILMFYBOX_GROUP_URL)
                ]])
            )
        elif url and url.startswith("https://t.me/"):
            try:
                # Handle Telegram links
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
                # Fallback to link button
                link_msg = await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🔗 **{title}**\n\n[Click here to watch]({url})",
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🎬 Watch Now", url=url),
                        InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL)
                    ]])
                )
                schedule_delete(context, chat_id, [warning_msg.message_id, link_msg.message_id], 60)
                return
        elif url:
            # External URL
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
            schedule_delete(context, chat_id, [warning_msg.message_id, link_msg.message_id], 60)
            return
        else:
            nofile_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Sorry, no file available for **{title}**\n\nPlease try another quality or contact support.",
                parse_mode='Markdown'
            )
            schedule_delete(context, chat_id, [warning_msg.message_id, nofile_msg.message_id], 60)
            return
        
        if sent_msg and warning_msg:
            schedule_delete(context, chat_id, [warning_msg.message_id, sent_msg.message_id], 60)
    
    except Exception as e:
        logger.error(f"Error sending file: {e}")
        try:
            err_msg = await context.bot.send_message(
                chat_id=chat_id,
                text="❌ Failed to send file. Please contact support."
            )
            schedule_delete(context, chat_id, [err_msg.message_id])
        except:
            pass

# ==================== BOT HANDLERS ====================
async def start(update, context):
    """Premium start command"""
    try:
        # Handle deep links
        if context.args and context.args[0].startswith("movie_"):
            # Require membership before processing deep link
            user_id = update.effective_user.id
            is_member = await check_user_membership_cached(context, user_id)
            
            if not is_member:
                force_msg = await update.message.reply_text(
                    "🚫 **Join Required!**\n\nPlease join our Channel and Group first to access movies.",
                    reply_markup=get_premium_force_join_keyboard(),
                    parse_mode='Markdown'
                )
                schedule_delete(context, update.effective_chat.id, [force_msg.message_id])
                return MAIN_MENU
            
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

        # Premium start keyboard
        start_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add to Group", url=f"https://t.me/{bot_username}?startgroup=true")],
            [
                InlineKeyboardButton("📢 Channel", url=FILMFYBOX_CHANNEL_URL),
                InlineKeyboardButton("💬 Group", url=FILMFYBOX_GROUP_URL)
            ],
            [
                InlineKeyboardButton("🔍 Search Tips", callback_data="search_tips"),
                InlineKeyboardButton("📊 My Stats", callback_data="my_stats")
            ],
            [
                InlineKeyboardButton("ℹ️ Help", callback_data="start_help"),
                InlineKeyboardButton("👑 Premium", callback_data="premium_info")
            ]
        ])

        # Premium welcome message
        start_caption = (
            "🎬 👋 Hey {user.first_name}!"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🤖 I'm **{BOT_NAME}**\n\n"
            "🔥 **Features:**\n"
            "• 🎞 Latest Movies & Series\n"
            "• 📺 Multiple Quality Options\n"
            "• ⚡ Lightning Fast Search\n"
            "• 🛡 Privacy Protected\n"
            "• 📂 Season & Episode Support\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "💡 **Quick Start:**\n"
            "Just type any movie name...\n\n"
            "📝 **Examples:**\n"
            "`Avatar 2`\n"
            "`Stranger Things S04`\n"
            "`RRR 2022`"
        )

        banner_msg = await update.message.reply_photo(
            photo="https://blogger.googleusercontent.com/img/b/R29vZ2xl/AVvXsEj35aShWJb06jx7Kz_v5hum9RJnhFF7DK1djZor59xWvCjBGRBh_NNjAgBi-IEhG5fSTPEt24gC9wsMVw_suit8hgmAC7SPbCwuh_gk4jywJlC2OCYJYvu6CoorlndlUITqBpIowR7xMA7AF-JQsponc_TUP1U95N2lobnUdK0W9kA9cGadqbRNNd1d5Fo/s1600/logo-design-for-flimfybox-a-cinematic-mo_OhkRefmbTCK6_RylGkOrAw_CtxTQGw_Tu6dY2kc64sagw.jpeg",
            caption=start_caption,
            parse_mode='Markdown',
            reply_markup=start_keyboard
        )
        schedule_delete(context, chat_id, [banner_msg.message_id], 600)  # 10 minutes for start message
        return MAIN_MENU
    except Exception as e:
        logger.error(f"Error in start: {e}")
        return MAIN_MENU

async def search_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enhanced movie search with membership check"""
    try:
        if not update.message or not update.message.text:
            return MAIN_MENU

        query = update.message.text.strip()
        user_id = update.effective_user.id
        
        # Check membership first
        is_member = await check_user_membership_cached(context, user_id)
        if not is_member:
            join_msg = await update.message.reply_text(
                "🚫 **Membership Required!**\n\n"
                "Join our Channel and Group to search movies:",
                reply_markup=get_premium_force_join_keyboard(),
                parse_mode='Markdown'
            )
            schedule_delete(context, update.effective_chat.id, [join_msg.message_id])
            return MAIN_MENU
        
        # Check rate limit
        if not await check_rate_limit(user_id):
            rate_msg = await update.message.reply_text(
                "⏱ **Slow down!**\n\nPlease wait a few seconds between searches.",
                parse_mode='Markdown'
            )
            schedule_delete(context, update.effective_chat.id, [rate_msg.message_id], 10)
            return MAIN_MENU
        
        # Update stats
        update_user_stats(user_id, 'search')
        
        # Show searching animation
        searching_msg = await update.message.reply_text("🔍 Searching...")
        
        # Search movies
        movies = get_movies_from_db(query)
        
        # Delete searching message
        try:
            await searching_msg.delete()
        except:
            pass
        
        if not movies:
            # Not found with request option
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🙋 Request This Movie", callback_data=f"request_{query[:50]}")],
                [InlineKeyboardButton("🔍 Try Another Search", callback_data="new_search")]
            ])
            
            not_found_msg = await update.message.reply_text(
                f"😕 **No Results Found**\n\n"
                f"Couldn't find: `{query}`\n\n"
                f"💡 **Tips:**\n"
                f"• Check spelling\n"
                f"• Try shorter keywords\n"
                f"• Remove year/quality tags",
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
            schedule_delete(context, update.effective_chat.id, [not_found_msg.message_id])
            return MAIN_MENU
        
        # Store results
        context.user_data['search_results'] = movies
        context.user_data['search_query'] = query
        
        # Send results
        result_text = (
            f"✅ **Found {len(movies)} Results**\n"
            f"🔍 Search: `{query}`\n\n"
            f"👇 Select your movie:"
        )
        
        keyboard = create_movie_selection_keyboard(movies, page=0)
        
        result_msg = await update.message.reply_text(
            result_text,
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        schedule_delete(context, update.effective_chat.id, [result_msg.message_id], 600)
        return MAIN_MENU

    except Exception as e:
        logger.error(f"Error in search_movies: {e}")
        await update.message.reply_text("❌ Search failed. Please try again.")
        return MAIN_MENU

async def group_message_handler(update, context):
    """Enhanced group handler with membership enforcement"""
    try:
        if not update.message or not update.message.text or update.message.from_user.is_bot:
            return
        
        message_text = update.message.text.strip()
        user = update.effective_user
        
        # Ignore short messages and commands
        if len(message_text) < 3 or message_text.startswith('/'):
            return
        
        # Search for movies
        movies_found = get_movies_from_db(message_text, limit=1)
        
        if not movies_found:
            return
        
        movie_id, title, _, _ = movies_found[0]
        
        # Create response with membership requirement notice
        emoji = "📺" if "S0" in title or "Season" in title else "🎬"
        
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(f"✅ Get {emoji} in PM", callback_data=f"group_get_{movie_id}_{user.id}")
        ]])
        
        reply_text = (
            f"Hey {user.mention_markdown()}! 👋\n\n"
            f"{emoji} **Found:** {title}\n\n"
            f"📌 **Note:** Channel & Group membership required\n"
            f"👇 Click to receive in private chat"
        )
        
        reply_msg = await update.message.reply_text(
            reply_text,
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        
        schedule_delete(context, update.effective_chat.id, [reply_msg.message_id], 120)
        
    except Exception as e:
        logger.error(f"Group handler error: {e}")

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enhanced callback handler with membership checks"""
    try:
        query = update.callback_query
        await query.answer()
        
        # Verify membership callback
        if query.data == "verify_membership" or query.data == "check_membership":
            user_id = query.from_user.id
            
            # Clear cache to force fresh check
            cache_key = f"member_{user_id}"
            if cache_key in membership_cache:
                del membership_cache[cache_key]
            
            is_member = await check_user_membership(context, user_id)
            
            if is_member:
                success_text = (
                    "✅ **Membership Verified!**\n\n"
                    "🎉 Welcome to Ur Movie!\n"
                    "You now have full access to:\n\n"
                    "• 🎬 All Movies & Series\n"
                    "• 📺 Multiple Qualities\n"
                    "• ⚡ Fast Downloads\n\n"
                    "Start searching now! Just type any movie name..."
                )
                await query.edit_message_text(success_text, parse_mode='Markdown')
                schedule_delete(context, query.message.chat.id, [query.message.message_id], 30)
            else:
                await query.answer(
                    "❌ Not joined yet! Please join both Channel and Group first.",
                    show_alert=True
                )
                
                # Show which one is missing
                channel_member = False
                group_member = False
                
                try:
                    cm = await context.bot.get_chat_member(REQUIRED_CHANNEL_ID, user_id)
                    channel_member = cm.status in ['member', 'administrator', 'creator']
                except:
                    pass
                
                try:
                    gm = await context.bot.get_chat_member(REQUIRED_GROUP_ID, user_id)
                    group_member = gm.status in ['member', 'administrator', 'creator']
                except:
                    pass
                
                status_text = (
                    "📊 **Membership Status:**\n\n"
                    f"📢 Channel: {'✅ Joined' if channel_member else '❌ Not Joined'}\n"
                    f"💬 Group: {'✅ Joined' if group_member else '❌ Not Joined'}\n\n"
                    "Please join both to continue!"
                )
                
                await query.edit_message_text(
                    status_text,
                    reply_markup=get_premium_force_join_keyboard(),
                    parse_mode='Markdown'
                )
            return
        
        # Why join callback
        elif query.data == "why_join":
            why_text = (
                "❓ **Why Join Our Community?**\n\n"
                "🎬 **Exclusive Benefits:**\n"
                "• Latest movies within hours of release\n"
                "• HD/4K quality options\n"
                "• No ads or spam\n"
                "• 24/7 automated service\n\n"
                "💬 **Community Perks:**\n"
                "• Request any movie\n"
                "• Get recommendations\n"
                "• Report issues directly\n"
                "• Vote for upcoming additions\n\n"
                "🛡 **100% Safe & Legal**\n"
                "Join 50,000+ movie lovers!"
            )
            back_keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="verify_membership")
            ]])
            await query.edit_message_text(why_text, reply_markup=back_keyboard, parse_mode='Markdown')
            return
        
        # Movie selection (with membership check)
        elif query.data.startswith("movie_"):
            # Check membership before processing
            user_id = query.from_user.id
            is_member = await check_user_membership_cached(context, user_id)
            
            if not is_member:
                await query.answer("❌ Please join our Channel and Group first!", show_alert=True)
                await query.edit_message_text(
                    "🚫 **Membership Required!**\n\nJoin to access movies:",
                    reply_markup=get_premium_force_join_keyboard(),
                    parse_mode='Markdown'
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
            qualities = get_all_movie_qualities(movie_id)
            
            if qualities and len(qualities) > 1:
                # Multiple qualities available
                context.user_data['selected_movie_data'] = {
                    'id': movie_id,
                    'title': title,
                    'qualities': qualities
                }
                
                selection_text = (
                    f"✅ **Selected:** {title}\n\n"
                    f"📊 **{len(qualities)} Qualities Available**\n"
                    f"Choose your preferred quality:"
                )
                
                keyboard = create_quality_selection_keyboard(movie_id, title, qualities)
                await query.edit_message_text(
                    selection_text,
                    reply_markup=keyboard,
                    parse_mode='Markdown'
                )
            else:
                # Single quality or direct file
                await query.edit_message_text(f"📤 Sending **{title}**...", parse_mode='Markdown')
                
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT url, file_id FROM movies WHERE id = %s", (movie_id,))
                url, file_id = cur.fetchone() or (None, None)
                cur.close()
                conn.close()
                
                await send_movie_file(update, context, title, url, file_id)
        
        # Quality selection (already has membership decorator in send_movie_file)
        elif query.data.startswith("quality_"):
            parts = query.data.split('_')
            movie_id = int(parts[1])
            selected_quality = parts[2]
            
            movie_data = context.user_data.get('selected_movie_data')
            
            if not movie_data:
                await query.edit_message_text("❌ Session expired. Please search again.")
                return
            
            chosen_file = None
            for quality, url, file_id, file_size in movie_data['qualities']:
                if quality == selected_quality:
                    chosen_file = {'url': url, 'file_id': file_id}
                    break
            
            if not chosen_file:
                await query.edit_message_text("❌ Quality not available.")
                return
            
            title = movie_data['title']
            await query.edit_message_text(f"📤 Sending **{title}** ({selected_quality})...", parse_mode='Markdown')
            
            await send_movie_file(update, context, title, chosen_file['url'], chosen_file['file_id'])
        
        # Group get button
        elif query.data.startswith("group_get_"):
            parts = query.data.split('_')
            movie_id = int(parts[2])
            original_user_id = int(parts[3])
            
            if query.from_user.id != original_user_id:
                await query.answer("❌ This button is for the original requester only!", show_alert=True)
                return
            
            # Check membership
            is_member = await check_user_membership_cached(context, original_user_id)
            if not is_member:
                await query.edit_message_text(
                    "🚫 **Join Required!**\n\n"
                    "You must join our Channel and Group first:",
                    reply_markup=get_premium_force_join_keyboard()
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
                            # Send quality selection to PM
                            context.user_data['selected_movie_data'] = {
                                'id': movie_id,
                                'title': title,
                                'qualities': qualities
                            }
                            
                            await context.bot.send_message(
                                chat_id=original_user_id,
                                text=f"🎬 **{title}**\n\nSelect Quality:",
                                reply_markup=create_quality_selection_keyboard(movie_id, title, qualities),
                                parse_mode='Markdown'
                            )
                        else:
                            # Send directly
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
                        
                        await query.edit_message_text("✅ Sent! Check your private chat.")
                        
            except telegram.error.Forbidden:
                bot_username = (await context.bot.get_me()).username
                deep_link = f"https://t.me/{bot_username}?start=movie_{movie_id}"
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("🤖 Start Bot", url=deep_link)
                ]])
                await query.edit_message_text(
                    "❌ **Can't send!**\n\nPlease start the bot first:",
                    reply_markup=keyboard,
                    parse_mode='Markdown'
                )
            return
        
        # My stats
        elif query.data == "my_stats":
            user_id = query.from_user.id
            stats = user_stats.get(user_id, {'searches': 0, 'downloads': 0})
            
            stats_text = (
                f"📊 **Your Statistics**\n"
                f"━━━━━━━━━━━━━━━━━\n"
                f"🔍 Searches: {stats['searches']}\n"
                f"📥 Downloads: {stats['downloads']}\n"
                f"━━━━━━━━━━━━━━━━━\n"
                f"Thank you for using FilmfyBox!"
            )
            
            back_keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="back_to_start")
            ]])
            
            await query.edit_message_text(stats_text, reply_markup=back_keyboard, parse_mode='Markdown')
            return
        
        # Premium info
        elif query.data == "premium_info":
            premium_text = (
                "👑 **Ur Movie Features**\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "✨ **What You Get:**\n"
                "• Unlimited movie searches\n"
                "• 4K/HD quality options\n"
                "• Priority support\n"
                "• Early access to new releases\n"
                "• No ads or delays\n\n"
                "💎 **How to Stay Premium:**\n"
                "Simply stay joined to our:\n"
                "📢 @filmfybox\n"
                "💬 @Filmfybox002\n\n"
                "That's it! Completely FREE!"
            )
            
            back_keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="back_to_start")
            ]])
            
            await query.edit_message_text(premium_text, reply_markup=back_keyboard, parse_mode='Markdown')
            return
        
        # Other callbacks...
        elif query.data == "new_search":
            await query.edit_message_text(
                "🔍 **New Search**\n\nType any movie or series name...",
                parse_mode='Markdown'
            )
            return
        
        elif query.data == "cancel_selection":
            await query.edit_message_text("❌ Cancelled.")
            schedule_delete(context, query.message.chat.id, [query.message.message_id], 5)
            return
        
        elif query.data.startswith("page_"):
            page = int(query.data.replace("page_", ""))
            movies = context.user_data.get('search_results', [])
            if movies:
                await query.edit_message_text(
                    f"🔍 **Found {len(movies)} results**\n\nSelect one:",
                    reply_markup=create_movie_selection_keyboard(movies, page),
                    parse_mode='Markdown'
                )
            return
            
    except Exception as e:
        logger.error(f"Callback error: {e}")
        try:
            await query.answer("❌ Error occurred!", show_alert=True)
        except:
            pass

async def main_menu(update, context):
    """Main menu handler with membership check"""
    try:
        # Check if user has joined before allowing search
        user_id = update.effective_user.id
        is_member = await check_user_membership_cached(context, user_id)
        
        if not is_member:
            join_msg = await update.message.reply_text(
                "🚫 **Join to Search!**\n\n"
                "You must be a member of our Channel and Group:",
                reply_markup=get_premium_force_join_keyboard(),
                parse_mode='Markdown'
            )
            schedule_delete(context, update.effective_chat.id, [join_msg.message_id])
            return MAIN_MENU
        
        return await search_movies(update, context)
    except Exception as e:
        logger.error(f"Error in main_menu: {e}")
        return MAIN_MENU

async def error_handler(update, context):
    """Enhanced error handler"""
    try:
        logger.error(f"Exception: {context.error}", exc_info=context.error)
        
        if isinstance(update, Update) and update.effective_message:
            try:
                error_text = (
                    "❌ **Oops! Something went wrong.**\n\n"
                    "Please try again or contact support if the issue persists."
                )
                msg = await update.effective_message.reply_text(error_text, parse_mode='Markdown')
                schedule_delete(context, update.effective_chat.id, [msg.message_id])
            except:
                pass
    except:
        pass

# ==================== MAIN BOT ====================
def run_flask():
    """Run Flask server for hosting"""
    try:
        port = int(os.environ.get("PORT", "10000"))
        logger.info(f"Starting Flask server on port {port}")
        app.run(host="0.0.0.0", port=port)
    except Exception as e:
        logger.error(f"Failed to start Flask server: {e}")

def main():
    """Run the premium Telegram bot"""
    try:
        logger.info("🚀 Starting Ur Movie Bot...")
        
        # Start Flask in background for web hosting
        flask_thread = threading.Thread(target=run_flask, daemon=True)
        flask_thread.start()
        
        # Build application with optimized settings
        application = (
            Application.builder()
            .token(TELEGRAM_BOT_TOKEN)
            .read_timeout(30)
            .write_timeout(30)
            .pool_timeout(30)
            .connect_timeout(30)
            .build()
        )
        
        # Conversation handler
        conv_handler = ConversationHandler(
            entry_points=[CommandHandler('start', start, filters=filters.ChatType.PRIVATE)],
            states={
                MAIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, main_menu)],
                SEARCHING: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, search_movies)],
            },
            fallbacks=[CommandHandler('cancel', lambda u, c: u.message.reply_text("❌ Cancelled."))],
            per_message=False,
            per_chat=True,
        )
        
        # Add handlers
        application.add_handler(CallbackQueryHandler(button_callback))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS, group_message_handler))
        application.add_handler(conv_handler)
        application.add_error_handler(error_handler)
        
        logger.info("✅ Ur Movie Bot started successfully!")
        logger.info("📢 Channel: @filmfybox")
        logger.info("💬 Group: @Filmfybox002")
        
        # Start polling
        application.run_polling(drop_pending_updates=True)
        
    except Exception as e:
        logger.error(f"❌ Failed to start bot: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
