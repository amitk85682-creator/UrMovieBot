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
from typing import Optional, Dict, List
from flask import Flask, request
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

@app.route('/')
def home():
    return "FilmfyBox Premium Bot is running! 🎬", 200

@app.route('/health')
def health():
    return "OK", 200

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
PORT = int(os.environ.get('PORT', 5000))

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

# Message tracking for deletion
message_tracker: Dict[int, List[int]] = defaultdict(list)

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
    """Search for movies in database - BOT 2 STYLE (4 Columns Only)"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return []
        
        cur = conn.cursor()
        logger.info(f"Searching for: '{user_query}'")
        
        # Exact Matches (Returns 4 items: id, title, url, file_id)
        cur.execute(
            "SELECT id, title, url, file_id FROM movies WHERE LOWER(title) LIKE LOWER(%s) ORDER BY title LIMIT %s",
            (f'%{user_query}%', limit)
        )
        exact_matches = cur.fetchall()
        
        if exact_matches:
            return exact_matches
        
        # Fuzzy Matches
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
            try: cur.close(); conn.close()
            except: pass
def get_all_movie_qualities(movie_id):
    """Fetch all available qualities (URL/File ID) including the main generic URL."""
    conn = get_db_connection()
    if not conn:
        return []

    try:
        cur = conn.cursor()
        
        # 1. Fetch specific qualities from movie_files table
        # We fetch 3 columns, but we will pad the result to 4 items below
        cur.execute("""
            SELECT quality, url, file_id
            FROM movie_files
            WHERE movie_id = %s AND (url IS NOT NULL OR file_id IS NOT NULL)
            ORDER BY CASE quality
                WHEN '4K' THEN 1
                WHEN 'HD Quality' THEN 2
                WHEN 'Standard Quality'  THEN 3
                WHEN 'SD Quality' THEN 4
                WHEN 'Low Quality'  THEN 5
                ELSE 6
            END
        """, (movie_id,))
        
        raw_results = cur.fetchall()
        
        # FIX: Convert 3-item tuples to 4-item tuples (adding None for file_size)
        # Structure: (quality, url, file_id, file_size)
        quality_results = []
        for row in raw_results:
            quality_results.append((row[0], row[1], row[2], None))

        # 2. Fetch the main generic URL from movies table
        cur.execute("SELECT url FROM movies WHERE id = %s", (movie_id,))
        main_res = cur.fetchone()
        
        final_results = []
        
        # Add the main URL to the top of the list if it exists
        if main_res and main_res[0] and main_res[0].strip():
            # Label it nicely, e.g., "Stream / Watch Online"
            # FIX: Added None as the 4th item here as well
            final_results.append(('Stream / Watch Online', main_res[0].strip(), None, None))
            
        # Add the rest of the qualities
        final_results.extend(quality_results)
        
        cur.close()
        return final_results
    except Exception as e:
        logger.error(f"Error fetching movie qualities for {movie_id}: {e}")
        return []
    finally:
        if conn:
            conn.close()
def create_quality_selection_keyboard(movie_id, title, qualities):

    """Create inline keyboard with quality selection buttons showing SIZE"""

    keyboard = []



    # Note: qualities tuple ab 4 items ka hai -> (quality, url, file_id, file_size)

    for quality, url, file_id, file_size in qualities:

        callback_data = f"quality_{movie_id}_{quality}"

        

        # Agar size available hai to dikhayein, nahi to sirf Quality dikhayein

        size_text = f" - {file_size}" if file_size else ""

        link_type = "File" if file_id else "Link"

        

        # Button text example: "🎬 720p - 1.4GB (Link)"

        button_text = f"🎬 {quality}{size_text} ({link_type})"

        

        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])



    keyboard.append([InlineKeyboardButton("❌ Cancel Selection", callback_data="cancel_selection")])



    return InlineKeyboardMarkup(keyboard)
# ==================== HELPER FUNCTION (FIXED FOR QUALITY CHOICE) ====================
async def send_movie_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE, movie_id: int, title: str, url: Optional[str] = None, file_id: Optional[str] = None):
    """
    Sends the movie file/link to the user with a warning and caption.
    This function expects the specific URL/File ID to be passed as arguments.
    """
    chat_id = update.effective_chat.id

    # ------------------- DATA FALLBACK REMOVED / CHECK -------------------
    # This logic is now handled in button_callback where the user selects the quality.
    # If the initial call (from search_movies single result) has no URL/File_ID,
    # we need to check if multi-quality files exist and prompt the user.
    if not url and not file_id:
        qualities = get_all_movie_qualities(movie_id)
        if qualities:
            # Re-engage the user for selection if files exist in multi-quality table
            context.user_data['selected_movie_data'] = {
                'id': movie_id,
                'title': title,
                'qualities': qualities
            }
            selection_text = f"✅ We found **{title}** in multiple qualities.\n\n⬇️ **Please choose the file quality:**"
            keyboard = create_quality_selection_keyboard(movie_id, title, qualities)
            await context.bot.send_message(
                chat_id=chat_id,
                text=selection_text,
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
            return # Exit after sending the selection prompt
    # ----------------------------------------------------------------------


    try:
        # Initial warning (auto-delete with media if media sent)
        warning_msg = await context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ ❌👉This file automatically❗️deletes after 1 minute❗️so please forward it to another chat👈❌",
            parse_mode='Markdown'
        )

        sent_msg = None
        name = title  # Use 'title' from the function arguments for the caption
        caption_text = (
            f"🎬 <b>{name}</b>\n\n"
            "🔗 <b>JOIN »</b> <a href='http://t.me/filmfybox'>FilmfyBox</a>\n\n"
            "🔹 <b>Please drop the movie name, and I’ll find it for you as soon as possible. 🎬✨👇</b>\n"
            "🔹 <b><a href='https://t.me/Filmfybox002'>FlimfyBox Chat</a></b>"
        )
        
        # Keyboard with a "Join Channel" button, to be attached to the media message
        join_channel_keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔗 Join Channel", url=FILMFYBOX_CHANNEL_URL)
        ]])


        # 1) file_id -> caption attached under media
        if file_id:
            sent_msg = await context.bot.send_document(
                chat_id=chat_id,
                document=file_id,
                caption=caption_text,
                parse_mode='HTML',
                reply_markup=join_channel_keyboard
            )

        # 2) Private channel message link: t.me/c/<chat_id>/<msg_id>
        elif url and url.startswith("https://t.me/c/"):
            try:
                parts = url.rstrip('/').split('/')
                from_chat_id = int("-100" + parts[-2])
                message_id = int(parts[-1])
                # Attach caption directly via copy_message
                sent_msg = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=from_chat_id,
                    message_id=message_id,
                    caption=caption_text,
                    parse_mode='HTML',
                    reply_markup=join_channel_keyboard
                )
            except Exception as e:
                logger.error(f"Copy private link failed {url}: {e}")
                # Fallback to sending a message with inline keyboard if copy fails
                sent_msg = await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🎬 Found: {name}\n\n{caption_text}",
                    reply_markup=get_movie_options_keyboard(name, url),
                    parse_mode='HTML'
                )

        # 3) Public channel message link: https://t.me/Username/123
        elif url and url.startswith("https://t.me/") and "/c/" not in url:
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
                    parse_mode='HTML',
                    reply_markup=join_channel_keyboard
                )
            except Exception as e:
                logger.error(f"Copy public link failed {url}: {e}")
                # Fallback to sending a message with inline keyboard if copy fails
                sent_msg = await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🎬 Found: {name}\n\n{caption_text}",
                    reply_markup=get_movie_options_keyboard(name, url),
                    parse_mode='HTML'
                )

        # 4) Normal external link
        elif url and url.startswith("http"):
            sent_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"🎉 Found it! '{name}' is available!\n\n{caption_text}",
                reply_markup=get_movie_options_keyboard(name, url),
                parse_mode='HTML'
            )

        # 5) Nothing valid to send
        else:
            sent_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Sorry, '{name}' found but no valid file or link is attached in the database."
            )

        # Auto-delete for media + warning
        if sent_msg:
            message_ids_to_delete = [warning_msg.message_id, sent_msg.message_id]

            asyncio.create_task(
                delete_messages_after_delay(
                    context,
                    chat_id,
                    message_ids_to_delete,
                    60 # 60 seconds delay
                )
            )

    except Exception as e:
        logger.error(f"Error sending movie to user: {e}")
        try:
            await context.bot.send_message(chat_id=chat_id, text="❌ Server failed to send file. Please report to Admin.")
        except Exception as e2:
            logger.error(f"Secondary send error: {e2}")

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

    """Create inline keyboard with quality selection buttons showing SIZE"""

    keyboard = []



    # Note: qualities tuple ab 4 items ka hai -> (quality, url, file_id, file_size)

    for quality, url, file_id, file_size in qualities:

        callback_data = f"quality_{movie_id}_{quality}"

        

        # Agar size available hai to dikhayein, nahi to sirf Quality dikhayein

        size_text = f" - {file_size}" if file_size else ""

        link_type = "File" if file_id else "Link"

        

        # Button text example: "🎬 720p - 1.4GB (Link)"

        button_text = f"🎬 {quality}{size_text} ({link_type})"

        

        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])



    keyboard.append([InlineKeyboardButton("❌ Cancel Selection", callback_data="cancel_selection")])



    return InlineKeyboardMarkup(keyboard)
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

# ==================== IMPROVED AUTO DELETE ====================
async def safe_delete_message(context, chat_id, message_id):
    """Safely delete a single message"""
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        return True
    except telegram.error.BadRequest as e:
        if "message to delete not found" in str(e).lower():
            logger.debug(f"Message {message_id} already deleted")
        elif "message can't be deleted" in str(e).lower():
            logger.debug(f"No permission to delete message {message_id}")
        else:
            logger.debug(f"Cannot delete message {message_id}: {e}")
    except Exception as e:
        logger.debug(f"Error deleting message {message_id}: {e}")
    return False

async def delete_messages_after_delay(context, chat_id, message_ids, delay=60):
    """Delete messages after delay with improved error handling"""
    try:
        await asyncio.sleep(delay)
        
        # Filter out None values and duplicates
        valid_message_ids = list(set(filter(None, message_ids)))
        
        for msg_id in valid_message_ids:
            await safe_delete_message(context, chat_id, msg_id)
            await asyncio.sleep(0.1)  # Small delay between deletions
            
    except Exception as e:
        logger.error(f"Error in delete_messages_after_delay: {e}")

def schedule_delete(context, chat_id, message_ids, delay=None):
    """Helper to schedule auto-deletion for messages"""
    try:
        if not message_ids:
            return
        
        # Filter out None values
        message_ids = [msg_id for msg_id in message_ids if msg_id is not None]
        
        if not message_ids:
            return
            
        if delay is None:
            delay = AUTO_DELETE_DELAY
            
        # Add to tracker
        message_tracker[chat_id].extend(message_ids)
        
        # Create deletion task
        asyncio.create_task(delete_messages_after_delay(context, chat_id, message_ids, delay))
        
    except Exception as e:
        logger.error(f"Error scheduling delete: {e}")

async def clear_chat_messages(context, chat_id):
    """Clear all tracked messages for a chat"""
    try:
        if chat_id in message_tracker:
            message_ids = message_tracker[chat_id]
            for msg_id in message_ids:
                await safe_delete_message(context, chat_id, msg_id)
            message_tracker[chat_id] = []
    except Exception as e:
        logger.error(f"Error clearing chat messages: {e}")

# ==================== SEND MOVIE FILE ====================
async def send_movie_file(update, context, title, url=None, file_id=None):
    """Send movie file with auto-delete and improved fallback logic"""
    try:
        chat_id = update.effective_chat.id if update.effective_chat else None
        user_id = update.effective_user.id if update.effective_user else None
        
        if not chat_id:
            return
        
        # Check Membership
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
            schedule_delete(context, chat_id, [access_msg.message_id])
            return
        
        # Send Warning Message
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
        
        # PRIORITY 1: Try sending by File ID
        if file_id:
            try:
                sent_msg = await context.bot.send_document(
                    chat_id=chat_id,
                    document=file_id,
                    caption=caption,
                    parse_mode='Markdown'
                )
            except telegram.error.BadRequest as e:
                logger.error(f"❌ Bad File ID for {title}: {e}")
                sent_msg = None # Reset to None to trigger fallback
            except Exception as e:
                logger.error(f"❌ Error sending document: {e}")
                sent_msg = None

        # PRIORITY 2: Fallback to URL (Copy Message) if File ID failed or didn't exist
        if not sent_msg and url:
            try:
                # Handle Private Channel Links (https://t.me/c/xxxx/xxx)
                if "/c/" in url:
                    parts = url.rstrip('/').split('/')
                    # Extract channel ID (adds -100 prefix)
                    channel_id_str = parts[-2]
                    from_chat_id = int("-100" + channel_id_str) if not channel_id_str.startswith("-100") else int(channel_id_str)
                    message_id = int(parts[-1])
                # Handle Public Channel Links (https://t.me/username/xxx)
                elif "t.me/" in url:
                    parts = url.rstrip('/').split('/')
                    from_chat_id = f"@{parts[-2]}"
                    message_id = int(parts[-1])
                else:
                    from_chat_id = None
                
                if from_chat_id:
                    sent_msg = await context.bot.copy_message(
                        chat_id=chat_id,
                        from_chat_id=from_chat_id,
                        message_id=message_id,
                        caption=caption,
                        parse_mode='Markdown'
                    )
            except Exception as e:
                logger.error(f"❌ Copy message failed for {title}: {e}")
                sent_msg = None
        
        # PRIORITY 3: If both failed, send a Link Button
        if not sent_msg and url:
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("🎬 Watch / Download Now", url=url)
            ]])
            sent_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"🎬 **{title}**\n\n❌ Could not upload file directly.\n👇 Click below to watch:",
                reply_markup=keyboard,
                parse_mode='Markdown'
            )

        # Final Error: Nothing worked
        if not sent_msg:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ **Error:** File not found for **{title}**.\n(Database ID or Link is invalid)",
                parse_mode='Markdown'
            )
        else:
            # Schedule delete if successful
            schedule_delete(context, chat_id, [warning_msg.message_id, sent_msg.message_id], 60)
            
    except Exception as e:
        logger.error(f"Critical error in send_movie_file: {e}")
        try:
            err_msg = await context.bot.send_message(
                chat_id=chat_id,
                text="❌ Failed to send file. Please try again."
            )
            schedule_delete(context, chat_id, [err_msg.message_id])
        except:
            pass

# ==================== BOT HANDLERS ====================
async def start(update, context):
    """Start command"""
    try:
        # Handle deep links
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
            "▶️ *Type any movie / series name to start...*\n"
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
        
        # Track message for deletion
        if banner_msg:
            schedule_delete(context, chat_id, [banner_msg.message_id])
            
        return MAIN_MENU
        
    except Exception as e:
        logger.error(f"Error in start: {e}")
        return MAIN_MENU

async def search_movies(update, context):
    """Search Handler - 100% Bot 2 Style (Direct File Delivery)"""
    try:
        chat_id = update.effective_chat.id
        
        # Rate Limit
        if not await check_rate_limit(update.effective_user.id):
            msg = await update.message.reply_text("⏳ Please wait a moment.")
            schedule_delete(context, chat_id, [msg.message_id], 5)
            return MAIN_MENU
        
        user_message = update.message.text.strip()
        movies_found = get_movies_from_db(user_message, limit=10)
        
        if not movies_found:
            if update.effective_chat.type == "private":
                keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL)]])
                msg = await update.message.reply_text(
                    f"🚫 **No Results Found**\n\n`{user_message}` not found.",
                    reply_markup=keyboard, parse_mode='Markdown'
                )
                schedule_delete(context, chat_id, [msg.message_id])
            return MAIN_MENU
        
        # === EXACT BOT 2 LOGIC APPLIED HERE ===
        elif len(movies_found) == 1:
            # Bot 1 returns 5 items. We ignore the last one (is_series_flag)
            movie_id, title, url, file_id, _ = movies_found[0]
            
            # Directly fetch qualities
            qualities = get_all_movie_qualities(movie_id)
            
            if qualities and len(qualities) > 1:
                # Store data and Show Buttons
                context.user_data['selected_movie_data'] = {
                    'id': movie_id,
                    'title': title,
                    'qualities': qualities
                }
                
                msg = await update.message.reply_text(
                    f"✅ **{title}** found!\n\n⬇️ **Select Quality:**",
                    reply_markup=create_quality_selection_keyboard(movie_id, title, qualities),
                    parse_mode='Markdown'
                )
                schedule_delete(context, chat_id, [msg.message_id])
                
            else:
                # Single file -> Send Directly
                final_url = url
                final_file_id = file_id
                display_title = title
                
                # Check if qualities list has data (even if 1 item) to get size/name
                if qualities:
                    q_name, q_url, q_file, q_size = qualities[0]
                    final_url = q_url if q_url else url
                    final_file_id = q_file if q_file else file_id
                    size_str = f" [{q_size}]" if q_size else ""
                    display_title = f"{title} [{q_name}]{size_str}"

                await send_movie_file(update, context, display_title, final_url, final_file_id)

        else:
            # Multiple results -> List
            context.user_data['search_results'] = movies_found
            msg = await update.message.reply_text(
                f"🔍 **Found {len(movies_found)} results**\n\nSelect one ⬇️",
                reply_markup=create_movie_selection_keyboard(movies_found),
                parse_mode='Markdown'
            )
            schedule_delete(context, chat_id, [msg.message_id])
        
        return MAIN_MENU
    
    except Exception as e:
        logger.error(f"Error in search: {e}")
        return MAIN_MENU
async def group_message_handler(update, context):
    """Silent group handler - only responds to potential movie searches"""
    try:
        if not update.message or not update.message.text or update.message.from_user.is_bot:
            return
        
        message_text = update.message.text.strip()
        user = update.effective_user
        
        # Ignore short messages and commands
        if len(message_text) < 4 or message_text.startswith('/'):
            return
        
        # Search for movies
        movies_found = get_movies_from_db(message_text, limit=1)
        
        if not movies_found:
            return
        
        movie_id, title, _, _, is_series_flag = movies_found[0]
        
        # Check similarity score
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
        
        # Schedule deletion after 2 minutes
        schedule_delete(context, update.effective_chat.id, [reply_msg.message_id], 120)
        
    except Exception as e:
        logger.error(f"Group handler error: {e}")

async def button_callback(update, context):
    """Handle all button callbacks"""
    try:
        query = update.callback_query
        await query.answer()
        chat_id = query.message.chat.id
        
        # Check membership callback
        if query.data == "check_membership":
            is_member = await check_user_membership(context, query.from_user.id)
            if is_member:
                await query.edit_message_text(
                    "✅ **Access Granted!**\n\n"
                    "Welcome to FilmfyBox Premium! 🎬\n"
                    "You can now search for movies and series.",
                    parse_mode='Markdown'
                )
                schedule_delete(context, chat_id, [query.message.message_id])
            else:
                await query.answer("❌ Please join both Channel and Group first!", show_alert=True)
            return
        
        # Help callback
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
            await query.edit_message_text(help_text, parse_mode='Markdown')
            return
        
        # About callback
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
            await query.edit_message_text(about_text, parse_mode='Markdown')
            return
        
        # Search tips callback
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
        
        # Group get callback
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
                            # Track PM message for deletion
                            schedule_delete(context, original_user_id, [pm_msg.message_id])
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
        
        # Movie selection callbacks
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
                    qualities = get_all_movie_qualities(movie_id)
                    
                    if qualities and len(qualities) > 1:
                        await query.edit_message_text(
                            f"🎬 **{title}**\n\nSelect Quality ⬇️",
                            reply_markup=create_quality_selection_keyboard(movie_id, title, qualities),
                            parse_mode='Markdown'
                        )
                    else:
                        await send_movie_file(update, context, title, url, file_id)
                        await query.edit_message_text("✅ Sent!")
            return
        
        # Season selection callbacks
        if query.data.startswith("season_"):
            parts = query.data.split('_', 2)
            season_num = int(parts[1])
            
            seasons_data = context.user_data.get('series_data', {})
            episodes = seasons_data.get(season_num, [])
            
            if episodes:
                await query.edit_message_text(
                    f"📺 Season {season_num}\n\nSelect Episode ⬇️",
                    reply_markup=create_episode_selection_keyboard(episodes, season_num),
                    parse_mode='Markdown'
                )
            return
        
        # Episode/Movie selection callbacks
        if query.data.startswith("movie_"):
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
                            f"🎬 **{title}**\n\nSelect Quality ⬇️",
                            reply_markup=create_quality_selection_keyboard(movie_id, title, qualities),
                            parse_mode='Markdown'
                        )
                    else:
                        await send_movie_file(update, context, title, url, file_id)
                        await query.edit_message_text("✅ Sent!")
            return
        
        # Quality Selection Callback (Corrected for 4-item tuple)
        if query.data.startswith("quality_"):
            parts = query.data.split('_', 2)
            movie_id = int(parts[1])
            selected_quality_safe = parts[2]
            
            # Retrieve stored data
            movie_data = context.user_data.get('selected_movie_data')
            
            # Refetch if missing
            if not movie_data or movie_data.get('id') != movie_id:
                qualities = get_all_movie_qualities(movie_id)
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT title FROM movies WHERE id = %s", (movie_id,))
                res = cur.fetchone()
                title = res[0] if res else "Movie"
                cur.close()
                conn.close()
                movie_data = {'id': movie_id, 'title': title, 'qualities': qualities}

            chosen_file = None
            
            # Find the matching quality
            for q_name, q_url, q_file, q_size in movie_data['qualities']:
                safe_q_name = q_name.replace(' ', '_').replace('/', '_')
                
                if safe_q_name == selected_quality_safe:
                    chosen_file = {
                        'url': q_url, 
                        'file_id': q_file, 
                        'quality': q_name, 
                        'size': q_size
                    }
                    break
            
            if chosen_file:
                size_str = f" [{chosen_file['size']}]" if chosen_file['size'] else ""
                display_title = f"{movie_data['title']} [{chosen_file['quality']}]{size_str}"
                
                await query.edit_message_text(f"📤 Sending **{display_title}**...", parse_mode='Markdown')
                await send_movie_file(update, context, display_title, chosen_file['url'], chosen_file['file_id'])
            else:
                await query.edit_message_text("❌ File not found.")
            
            return
        
        # Pagination callbacks
        if query.data.startswith("page_"):
            page = int(query.data.replace("page_", ""))
            movies = context.user_data.get('search_results', [])
            if movies:
                await query.edit_message_text(
                    f"🔍 **Found {len(movies)} results**\n\nSelect one ⬇️",
                    reply_markup=create_movie_selection_keyboard(movies, page),
                    parse_mode='Markdown'
                )
            return
        
        # Cancel callback
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
                chat_id = update.effective_chat.id
                msg = await update.effective_message.reply_text("❌ Something went wrong. Please try again.")
                schedule_delete(context, chat_id, [msg.message_id])
            except:
                pass
    except:
        pass

# ==================== ADMIN COMMANDS ====================
async def clear_all(update, context):
    """Admin command to clear all messages in a chat"""
    try:
        user_id = update.effective_user.id
        
        # Check if user is admin
        if user_id != ADMIN_USER_ID:
            return
            
        chat_id = update.effective_chat.id
        await clear_chat_messages(context, chat_id)
        
        msg = await update.message.reply_text("✅ All tracked messages cleared!")
        schedule_delete(context, chat_id, [msg.message_id], 5)
        
    except Exception as e:
        logger.error(f"Error in clear_all: {e}")

# ==================== RUN FLASK IN THREAD ====================
def run_flask():
    """Run Flask app in a separate thread"""
    app.run(host='0.0.0.0', port=PORT)

# ==================== MAIN BOT ====================
def main():
    """Run the Telegram bot with Flask web server"""
    try:
        logger.info("Starting FilmfyBox Premium Bot...")
        
        # Start Flask in a separate thread
        flask_thread = threading.Thread(target=run_flask, daemon=True)
        flask_thread.start()
        logger.info(f"Flask server started on port {PORT}")
        
        # Create bot application
        application = Application.builder().token(TELEGRAM_BOT_TOKEN).read_timeout(30).write_timeout(30).build()
        
        # Conversation handler for private chats
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
        
        # Add handlers
        application.add_handler(CallbackQueryHandler(button_callback))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS, group_message_handler))
        application.add_handler(CommandHandler('clearall', clear_all))  # Admin command
        application.add_handler(conv_handler)
        application.add_error_handler(error_handler)
        
        logger.info("Bot started successfully! 🎬")
        application.run_polling(drop_pending_updates=True)
        
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
