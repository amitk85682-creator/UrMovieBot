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
import html
from bs4 import BeautifulSoup
import telegram
import psycopg2
from typing import Optional
from flask import Flask, request, session, g
import google.generativeai as genai
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ChatMember,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler,
    CallbackQueryHandler,
)
from datetime import datetime, timedelta
from fuzzywuzzy import process, fuzz
from urllib.parse import urlparse, urlunparse, quote
from collections import defaultdict

# ==================== LOGGING SETUP ====================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ==================== CONVERSATION STATES ====================
MAIN_MENU, SEARCHING, REQUESTING = range(3)

# ==================== ENVIRONMENT VARIABLES ====================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DATABASE_URL = os.environ.get("DATABASE_URL")
BLOGGER_API_KEY = os.environ.get("BLOGGER_API_KEY")
BLOG_ID = os.environ.get("BLOG_ID")
UPDATE_SECRET_CODE = os.environ.get("UPDATE_SECRET_CODE", "default_secret_123")
ADMIN_USER_ID = int(os.environ.get("ADMIN_USER_ID", 0))
GROUP_CHAT_ID = os.environ.get("GROUP_CHAT_ID")
ADMIN_CHANNEL_ID = os.environ.get("ADMIN_CHANNEL_ID")

# Force Join Settings
REQUIRED_CHANNEL_ID = os.environ.get("REQUIRED_CHANNEL_ID", "@filmfybox")
REQUIRED_GROUP_ID = os.environ.get("REQUIRED_GROUP_ID", "@Filmfybox002")
FILMFYBOX_CHANNEL_URL = "https://t.me/filmfybox"
FILMFYBOX_GROUP_URL = "https://t.me/Filmfybox002"

# Rate limiting
user_last_request = defaultdict(lambda: datetime.min)
REQUEST_COOLDOWN_MINUTES = int(os.environ.get("REQUEST_COOLDOWN_MINUTES", "10"))
SIMILARITY_THRESHOLD = int(os.environ.get("SIMILARITY_THRESHOLD", "80"))
MAX_REQUESTS_PER_MINUTE = int(os.environ.get("MAX_REQUESTS_PER_MINUTE", "10"))

# Auto delete delay (seconds) for normal bot messages
AUTO_DELETE_DELAY = int(os.environ.get("AUTO_DELETE_DELAY", "300"))  # default 5 minutes


# Validate required environment variables
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN environment variable is not set")
    raise ValueError("TELEGRAM_BOT_TOKEN is not set.")

if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable is not set")
    raise ValueError("DATABASE_URL is not set.")


# ==================== UTILITY FUNCTIONS ====================
def esc(text) -> str:
    """HTML escape helper"""
    return html.escape(str(text)) if text is not None else ""


def preprocess_query(query):
    """Clean and normalize user query"""
    query = re.sub(r"[^\w\s-]", "", query)
    query = " ".join(query.split())
    stop_words = ["movie", "film", "full", "download", "watch", "online", "free"]
    words = query.lower().split()
    words = [w for w in words if w not in stop_words]
    return " ".join(words).strip()


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
    t = re.sub(r"[^\w\s]", " ", title)
    t = re.sub(r"\s+", " ", t).strip()
    return t.lower()


def is_series(title):
    """Check if title is a series based on patterns"""
    series_patterns = [
        r"S\d+\s*E\d+",  # S01E01, S1E1
        r"Season\s*\d+",  # Season 1
        r"Episode\s*\d+",  # Episode 1
        r"EP?\s*\d+",  # E01, EP01
        r"Part\s*\d+",  # Part 1
        r"\d+x\d+",  # 1x01
    ]
    return any(re.search(pattern, title, re.IGNORECASE) for pattern in series_patterns)


def parse_series_info(title):
    """Parse series information from title"""
    info = {"base_title": title, "season": None, "episode": None, "is_series": False}

    # Try to extract season and episode
    match = re.search(r"S(\d+)\s*E(\d+)", title, re.IGNORECASE)
    if match:
        info["season"] = int(match.group(1))
        info["episode"] = int(match.group(2))
        info["base_title"] = title[: match.start()].strip()
        info["is_series"] = True
        return info

    # Try other patterns
    match = re.search(r"Season\s*(\d+).*Episode\s*(\d+)", title, re.IGNORECASE)
    if match:
        info["season"] = int(match.group(1))
        info["episode"] = int(match.group(2))
        info["base_title"] = re.sub(
            r"Season\s*\d+.*Episode\s*\d+",
            "",
            title,
            flags=re.IGNORECASE,
        ).strip()
        info["is_series"] = True

    return info


# ==================== FORCE JOIN CHECK ====================
async def check_user_membership(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Check if user is member of required channel and group"""
    try:
        # Check channel membership
        channel_member = await context.bot.get_chat_member(
            chat_id=REQUIRED_CHANNEL_ID, user_id=user_id
        )
        channel_joined = channel_member.status in ["member", "administrator", "creator"]

        # Check group membership
        group_member = await context.bot.get_chat_member(
            chat_id=REQUIRED_GROUP_ID, user_id=user_id
        )
        group_joined = group_member.status in ["member", "administrator", "creator"]

        return channel_joined and group_joined
    except Exception as e:
        logger.error(f"Error checking membership for user {user_id}: {e}")
        return False


def get_force_join_keyboard():
    """Get keyboard for force join prompt"""
    keyboard = [
        [InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL)],
        [InlineKeyboardButton("💬 Join Group", url=FILMFYBOX_GROUP_URL)],
        [
            InlineKeyboardButton(
                "✅ I Joined, Check Again", callback_data="check_membership"
            )
        ],
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

        # Exact match
        cur.execute(
            "SELECT id, title, url, file_id FROM movies "
            "WHERE LOWER(title) LIKE LOWER(%s) "
            "ORDER BY title LIMIT %s",
            (f"%{user_query}%", limit),
        )
        exact_matches = cur.fetchall()

        if exact_matches:
            logger.info(f"Found {len(exact_matches)} exact matches")
            results = []
            for match in exact_matches:
                movie_id, title, url, file_id = match
                results.append((movie_id, title, url, file_id, is_series(title)))
            cur.close()
            conn.close()
            return results

        # Fuzzy matching
        cur.execute("SELECT id, title, url, file_id FROM movies")
        all_movies = cur.fetchall()

        if not all_movies:
            cur.close()
            conn.close()
            return []

        movie_titles = [movie[1] for movie in all_movies]
        movie_dict = {movie[1]: movie for movie in all_movies}

        matches = process.extract(
            user_query, movie_titles, scorer=fuzz.token_sort_ratio, limit=limit
        )

        filtered_movies = []
        for match in matches:
            if len(match) >= 2:
                title, score = match[0], match[1]
                if score >= 65 and title in movie_dict:
                    movie_data = movie_dict[title]
                    filtered_movies.append(
                        (
                            movie_data[0],  # id
                            movie_data[1],  # title
                            movie_data[2],  # url
                            movie_data[3],  # file_id
                            is_series(movie_data[1]),  # is_series check
                        )
                    )

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
            except Exception:
                pass


def get_all_movie_qualities(movie_id):
    """Get all quality options for a movie"""
    conn = get_db_connection()
    if not conn:
        return []

    try:
        cur = conn.cursor()

        # Get from movie_files table
        cur.execute(
            """
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
        """,
            (movie_id,),
        )

        quality_results = cur.fetchall()

        # Get main URL from movies table
        cur.execute("SELECT url FROM movies WHERE id = %s", (movie_id,))
        main_res = cur.fetchone()

        final_results = []

        # Add main URL if exists
        if main_res and main_res[0] and main_res[0].strip():
            final_results.append(("📺 Stream / Watch Online", main_res[0].strip(), None, None))

        # Add quality options
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

        # Find all episodes for this series
        cur.execute(
            """
            SELECT id, title FROM movies 
            WHERE title LIKE %s AND (
                title ~* 'S\\d+\\s*E\\d+' OR 
                title ~* 'Season\\s*\\d+' OR 
                title ~* 'Episode\\s*\\d+'
            )
            ORDER BY title
        """,
            (f"{base_title}%",),
        )

        episodes = cur.fetchall()

        # Organize by season
        seasons = defaultdict(list)
        for ep_id, title in episodes:
            info = parse_series_info(title)
            if info["season"]:
                seasons[info["season"]].append(
                    {"id": ep_id, "title": title, "episode": info["episode"] or 0}
                )

        # Sort episodes within each season
        for season in seasons:
            seasons[season].sort(key=lambda x: x["episode"])

        cur.close()
        conn.close()
        return dict(seasons)
    except Exception as e:
        logger.error(f"Error getting series episodes: {e}")
        return {}
    finally:
        if conn:
            conn.close()


# ==================== NETFLIX-STYLE KEYBOARDS ====================
def create_movie_selection_keyboard(movies, page=0, movies_per_page=5):
    """Create Netflix-style movie selection keyboard"""
    start_idx = page * movies_per_page
    end_idx = start_idx + movies_per_page
    current_movies = movies[start_idx:end_idx]

    keyboard = []

    for movie in current_movies:
        movie_id, title, url, file_id, is_series_flag = movie
        emoji = "📺" if is_series_flag else "🎬"
        short_title = title if len(title) <= 35 else f"{title[:32]}..."
        button_text = f"{emoji} {short_title}"
        keyboard.append(
            [InlineKeyboardButton(button_text, callback_data=f"select_{movie_id}")]
        )

    # Navigation
    nav_buttons = []
    if page > 0:
        nav_buttons.append(
            InlineKeyboardButton("◀️ Previous", callback_data=f"page_{page-1}")
        )
    if end_idx < len(movies):
        nav_buttons.append(
            InlineKeyboardButton("Next ▶️", callback_data=f"page_{page+1}")
        )

    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_selection")])

    return InlineKeyboardMarkup(keyboard)


def create_quality_selection_keyboard(movie_id, title, qualities):
    """Create Netflix-style quality selection keyboard"""
    keyboard = []

    for quality, url, file_id, file_size in qualities:
        size_text = f" • {file_size}" if file_size else ""
        link_type = "📱" if file_id else "🔗"
        button_text = f"{link_type} {quality}{size_text}"

        # Make quality value URL-safe for callback data
        safe_quality = quality.replace(" ", "_").replace("/", "_")
        keyboard.append(
            [
                InlineKeyboardButton(
                    button_text, callback_data=f"quality_{movie_id}_{safe_quality}"
                )
            ]
        )

    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_selection")])

    return InlineKeyboardMarkup(keyboard)


def create_season_selection_keyboard(seasons_data, base_title):
    """Create season selection keyboard for series"""
    keyboard = []

    for season_num in sorted(seasons_data.keys()):
        episodes = seasons_data[season_num]
        button_text = f"📂 Season {season_num} ({len(episodes)} episodes)"
        keyboard.append(
            [
                InlineKeyboardButton(
                    button_text, callback_data=f"season_{season_num}_{base_title[:30]}"
                )
            ]
        )

    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_selection")])

    return InlineKeyboardMarkup(keyboard)


def create_episode_selection_keyboard(episodes, season_num):
    """Create episode selection keyboard"""
    keyboard = []

    for ep in episodes:
        ep_num = ep.get("episode", 0)
        button_text = f"▶️ Episode {ep_num}" if ep_num else ep["title"][:40]
        keyboard.append(
            [InlineKeyboardButton(button_text, callback_data=f"movie_{ep['id']}")]
        )

    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="cancel_selection")])

    return InlineKeyboardMarkup(keyboard)


# ==================== AUTO-DELETE HELPERS ====================
async def delete_messages_after_delay(context, chat_id, message_ids, delay=60):
    """Delete messages after delay"""
    try:
        await asyncio.sleep(delay)
        for msg_id in message_ids:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except Exception as e:
                logger.error(f"Failed to delete message {msg_id}: {e}")
    except Exception as e:
        logger.error(f"Error in delete_messages_after_delay: {e}")


def schedule_delete(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_ids,
    delay: Optional[int] = None,
):
    """Helper to schedule auto-deletion for messages"""
    if not message_ids:
        return
    asyncio.create_task(
        delete_messages_after_delay(
            context,
            chat_id,
            message_ids,
            delay if delay is not None else AUTO_DELETE_DELAY,
        )
    )


# ==================== SEND MOVIE WITH AUTO-DELETE ====================
async def send_movie_file(
    update: Optional[Update],
    context: ContextTypes.DEFAULT_TYPE,
    title: str,
    url: Optional[str] = None,
    file_id: Optional[str] = None,
    chat_id: Optional[int] = None,
    user_id: Optional[int] = None,
):
    """
    Send movie file with auto-delete.
    Can be called with:
      - send_movie_file(update, context, ...)
      - send_movie_file(None, context, ..., chat_id=..., user_id=...)
    """
    if chat_id is None and update is not None:
        chat_id = update.effective_chat.id
    if user_id is None and update is not None:
        user_id = update.effective_user.id

    if chat_id is None or user_id is None:
        logger.error("send_movie_file called without chat_id/user_id")
        return

    # Check membership
    is_member = await check_user_membership(context, user_id)
    if not is_member:
        access_msg = await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "🚫 <b>Access Denied</b>\n\n"
                "To watch movies, you must join our:\n"
                "📢 Channel: @filmfybox\n"
                "💬 Group: @Filmfybox002"
            ),
            reply_markup=get_force_join_keyboard(),
            parse_mode="HTML",
        )
        schedule_delete(context, chat_id, [access_msg.message_id])
        return

    try:
        # Warning message
        warning_msg = await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "⚠️ <b>File will auto-delete in 60 seconds!</b>\n\n"
                "Please forward it to <b>Saved Messages</b>."
            ),
            parse_mode="HTML",
        )

        # Netflix-style caption
        caption = (
            f"🎬 <b>{esc(title)}</b>\n\n"
            "━━━━━━━━━━━━━━━━━\n"
            f"📢 Channel: <a href=\"{FILMFYBOX_CHANNEL_URL}\">FilmfyBox</a>\n"
            f"💬 Group: <a href=\"{FILMFYBOX_GROUP_URL}\">FilmfyBox Chat</a>\n"
            "━━━━━━━━━━━━━━━━━\n\n"
            "⏰ Auto-delete in: <b>60 seconds</b>"
        )

        sent_msg = None

        # Send file based on type
        if file_id:
            sent_msg = await context.bot.send_document(
                chat_id=chat_id,
                document=file_id,
                caption=caption,
                parse_mode="HTML",
            )
        elif url and url.startswith("https://t.me/c/"):
            try:
                parts = url.rstrip("/").split("/")
                from_chat_id = int("-100" + parts[-2])
                message_id = int(parts[-1])
                sent_msg = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=from_chat_id,
                    message_id=message_id,
                    caption=caption,
                    parse_mode="HTML",
                )
            except Exception as e:
                logger.error(f"Copy failed: {e}")
                link_msg = await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"🔗 <b>{esc(title)}</b>\n\n"
                        f'<a href="{esc(url)}">Click here to watch</a>'
                    ),
                    parse_mode="HTML",
                )
                schedule_delete(
                    context, chat_id, [warning_msg.message_id, link_msg.message_id], 60
                )
                return
        elif url and url.startswith("https://t.me/") and "/c/" not in url:
            try:
                parts = url.rstrip("/").split("/")
                username = parts[-2].lstrip("@")
                message_id = int(parts[-1])
                sent_msg = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=f"@{username}",
                    message_id=message_id,
                    caption=caption,
                    parse_mode="HTML",
                )
            except Exception as e:
                logger.error(f"Public copy failed: {e}")
                link_msg = await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"🔗 <b>{esc(title)}</b>\n\n"
                        f'<a href="{esc(url)}">Click here to watch</a>'
                    ),
                    parse_mode="HTML",
                )
                schedule_delete(
                    context, chat_id, [warning_msg.message_id, link_msg.message_id], 60
                )
                return
        elif url:
            keyboard = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("🎬 Watch Now", url=url),
                        InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL),
                    ]
                ]
            )
            link_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=caption,
                reply_markup=keyboard,
                parse_mode="HTML",
            )
            schedule_delete(
                context, chat_id, [warning_msg.message_id, link_msg.message_id], 60
            )
            return
        else:
            nofile_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Sorry, no file available for <b>{esc(title)}</b>",
                parse_mode="HTML",
            )
            schedule_delete(
                context, chat_id, [warning_msg.message_id, nofile_msg.message_id], 60
            )
            return

        # Auto-delete after 60 seconds
        if sent_msg:
            schedule_delete(
                context, chat_id, [warning_msg.message_id, sent_msg.message_id], 60
            )

    except Exception as e:
        logger.error(f"Error sending file: {e}")
        err_msg = await context.bot.send_message(
            chat_id=chat_id, text="❌ Failed to send file."
        )
        schedule_delete(context, chat_id, [err_msg.message_id])


# ==================== BOT HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command"""
    # Handle deep link
    if context.args and context.args[0].startswith("movie_"):
        try:
            movie_id = int(context.args[0].split("_")[1])
            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT title, url, file_id FROM movies WHERE id = %s",
                    (movie_id,),
                )
                movie_data = cur.fetchone()
                cur.close()
                conn.close()

                if movie_data:
                    title, url, file_id = movie_data
                    await send_movie_file(update, context, title, url, file_id)
                    return MAIN_MENU
        except Exception as e:
            logger.error(f"Deep link error: {e}")

    # Premium-style /start banner
    chat_id = update.effective_chat.id
    bot_info = await context.bot.get_me()
    bot_username = bot_info.username

    start_keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "➕ Add Me To Your Group",
                    url=f"https://t.me/{bot_username}?startgroup=true",
                )
            ],
            [
                InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL),
                InlineKeyboardButton("💬 Join Group", url=FILMFYBOX_GROUP_URL),
            ],
            [
                InlineKeyboardButton("ℹ️ Help", callback_data="start_help"),
                InlineKeyboardButton("👑 About", callback_data="start_about"),
            ],
        ]
    )

    start_caption = (
        "✨ <b>FilmfyBox Premium Bot</b> ✨\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "🎬 Netflix‑style Movie & Series Bot\n"
        "🧿 Ultra‑fast search • Multi‑quality\n"
        "🛡 Auto‑delete privacy enabled\n"
        "📂 Seasons • Episodes • Clean UI\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "▶️ <b>Type any movie / series name to start...</b>\n"
        "<code>Avengers Endgame</code>\n"
        "<code>Stranger Things S01E01</code>\n"
        "<code>KGF 2 2022</code>"
    )

    message = update.effective_message
    banner_msg = await message.reply_photo(
        photo=(
            "https://blogger.googleusercontent.com/img/b/"
            "R29vZ2xl/AVvXsEj35aShWJb06jx7Kz_v5hum9RJnhFF7DK1djZor59xWvCjBGRBh_NNjAgBi-"
            "IEhG5fSTPEt24gC9wsMVw_suit8hgmAC7SPbCwuh_gk4jywJlC2OCYJYvu6CoorlndlUITqBpIowR7xMA7AF-"
            "JQsponc_TUP1U95N2lobnUdK0W9kA9cGadqbRNNd1d5Fo/s1600/"
            "logo-design-for-flimfybox-a-cinematic-mo_OhkRefmbTCK6_RylGkOrAw_CtxTQGw_Tu6dY2kc64sagw.jpeg"
        ),
        caption=start_caption,
        parse_mode="HTML",
        reply_markup=start_keyboard,
    )
    schedule_delete(context, chat_id, [banner_msg.message_id])

    return MAIN_MENU


async def search_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Search movies/series handler"""
    chat_id = update.effective_chat.id
    try:
        if not await check_rate_limit(update.effective_user.id):
            msg = await update.message.reply_text(
                "⏳ Please wait a moment before searching again."
            )
            schedule_delete(context, chat_id, [msg.message_id])
            return MAIN_MENU

        user_message = update.message.text.strip()

        # Search in database
        movies_found = get_movies_from_db(user_message, limit=10)

        if not movies_found:
            # Silent in groups
            if update.effective_chat.type != "private":
                return MAIN_MENU

            # Not found message
            keyboard = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🔍 Search Tips", callback_data="search_tips"
                        ),
                        InlineKeyboardButton(
                            "📢 Join Channel", url=FILMFYBOX_CHANNEL_URL
                        ),
                    ]
                ]
            )

            msg = await update.message.reply_text(
                (
                    "🚫 <b>No Results Found</b>\n\n"
                    f"<code>{esc(user_message)}</code> is not in our "
                    "<b>FilmfyBox Premium Library</b> yet.\n\n"
                    "💡 <b>Try this:</b>\n"
                    "• Check spelling\n"
                    "• Use full movie / series name\n"
                    "• Add year for better accuracy\n"
                    "• Example: <code>Inception 2010</code>"
                ),
                reply_markup=keyboard,
                parse_mode="HTML",
            )
            schedule_delete(context, chat_id, [msg.message_id])
            return MAIN_MENU

        elif len(movies_found) == 1:
            # Single result
            movie_id, title, url, file_id, is_series_flag = movies_found[0]

            if is_series_flag:
                # Handle series
                info = parse_series_info(title)
                if info["is_series"] and info["base_title"]:
                    # Get all episodes
                    seasons_data = get_series_episodes(info["base_title"])
                    if seasons_data:
                        context.user_data["series_data"] = seasons_data
                        context.user_data["base_title"] = info["base_title"]

                        msg = await update.message.reply_text(
                            f"📺 <b>{esc(info['base_title'])}</b>\n\nSelect Season ⬇️",
                            reply_markup=create_season_selection_keyboard(
                                seasons_data, info["base_title"]
                            ),
                            parse_mode="HTML",
                        )
                        schedule_delete(context, chat_id, [msg.message_id])
                        return MAIN_MENU

            # Movie - show quality options
            qualities = get_all_movie_qualities(movie_id)
            if qualities and len(qualities) > 1:
                msg = await update.message.reply_text(
                    f"🎬 <b>{esc(title)}</b>\n\nSelect Quality ⬇️",
                    reply_markup=create_quality_selection_keyboard(
                        movie_id, title, qualities
                    ),
                    parse_mode="HTML",
                )
                schedule_delete(context, chat_id, [msg.message_id])
            elif qualities:
                # Single quality - send directly
                quality, url_q, file_id_q, _ = qualities[0]
                await send_movie_file(
                    update,
                    context,
                    f"{title} [{quality}]",
                    url_q or url,
                    file_id_q or file_id,
                )
            else:
                # No qualities - send main file
                await send_movie_file(update, context, title, url, file_id)

        else:
            # Multiple results
            context.user_data["search_results"] = movies_found
            msg = await update.message.reply_text(
                f"🔍 <b>Found {len(movies_found)} results</b>\n\nSelect one ⬇️",
                reply_markup=create_movie_selection_keyboard(movies_found),
                parse_mode="HTML",
            )
            schedule_delete(context, chat_id, [msg.message_id])

        return MAIN_MENU

    except Exception as e:
        logger.error(f"Error in search: {e}")
        if update.effective_chat.type == "private":
            msg = await update.message.reply_text(
                "❌ Something went wrong. Please try again."
            )
            schedule_delete(context, chat_id, [msg.message_id])
        return MAIN_MENU


async def group_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Silent group handler - only respond to exact matches"""
    if (
        not update.message
        or not update.message.text
        or update.message.from_user.is_bot
    ):
        return

    message_text = update.message.text.strip()
    user = update.effective_user

    # Ignore short messages and commands
    if len(message_text) < 4 or message_text.startswith("/"):
        return

    # Search for exact match
    movies_found = get_movies_from_db(message_text, limit=1)

    if not movies_found:
        # SILENT - no response
        return

    # Check match confidence
    movie_id, title, _, _, is_series_flag = movies_found[0]
    score = fuzz.token_sort_ratio(
        _normalize_title_for_match(message_text),
        _normalize_title_for_match(title),
    )

    if score < 85:
        # Not confident - stay silent
        return

    # High confidence match - send prompt
    emoji = "📺" if is_series_flag else "🎬"
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"✅ Get {emoji}", callback_data=f"group_get_{movie_id}_{user.id}"
                )
            ]
        ]
    )

    try:
        user_mention = user.mention_html()
        reply_msg = await update.message.reply_text(
            (
                f"Hey {user_mention}! 👋\n\n"
                f"{emoji} <b>{esc(title)}</b>\n\n"
                "Tap below to receive it in PM ⬇️"
            ),
            reply_markup=keyboard,
            parse_mode="HTML",
        )

        # Auto-delete after 2 minutes
        schedule_delete(
            context, update.effective_chat.id, [reply_msg.message_id], delay=120
        )
    except Exception as e:
        logger.error(f"Group prompt error: {e}")


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all button callbacks"""
    query = update.callback_query
    try:
        await query.answer()

        # Check membership button
        if query.data == "check_membership":
            is_member = await check_user_membership(context, query.from_user.id)
            if is_member:
                await query.edit_message_text(
                    (
                        "✅ <b>Access Granted!</b>\n\n"
                        "Welcome to FilmfyBox Premium! 🎬\n"
                        "You can now search for movies and series."
                    ),
                    parse_mode="HTML",
                )
                schedule_delete(
                    context, query.message.chat.id, [query.message.message_id]
                )
            else:
                await query.answer(
                    "❌ Please join both Channel and Group first!", show_alert=True
                )
            return

        # Start Help
        if query.data == "start_help":
            help_text = (
                "📖 <b>How to Use FilmfyBox</b>\n\n"
                "1️⃣ Type any movie / series name\n"
                "2️⃣ Choose the correct result\n"
                "3️⃣ Select your preferred quality\n"
                "4️⃣ File auto‑deletes in <b>60 sec</b> → Save to Saved Messages\n\n"
                "💡 <b>Examples:</b>\n"
                "<code>Avengers Endgame</code>\n"
                "<code>Stranger Things S01E01</code>\n"
                "<code>Mirzapur Season 1</code>"
            )
            msg = await context.bot.send_message(
                chat_id=query.message.chat.id,
                text=help_text,
                parse_mode="HTML",
            )
            schedule_delete(context, query.message.chat.id, [msg.message_id])
            return

        # Start About
        if query.data == "start_about":
            about_text = (
                "👑 <b>About FilmfyBox Premium</b>\n\n"
                "🎬 Auto‑organized Movies & Series\n"
                "🎞 Multiple quality options (4K / 1080p / 720p ...)\n"
                "📂 Season & Episode based navigation\n"
                "🛡 Full privacy with auto‑delete replies\n\n"
                "📢 Channel: @filmfybox\n"
                "💬 Group: @Filmfybox002"
            )
            msg = await context.bot.send_message(
                chat_id=query.message.chat.id,
                text=about_text,
                parse_mode="HTML",
            )
            schedule_delete(context, query.message.chat.id, [msg.message_id])
            return

        # Search tips
        if query.data == "search_tips":
            tips_text = (
                "🔍 <b>Smart Search Tips</b>\n\n"
                "✅ <b>Good Examples:</b>\n"
                "<code>Inception 2010</code>\n"
                "<code>Breaking Bad S01E01</code>\n"
                "<code>Stranger Things</code>\n\n"
                "❌ <b>Avoid:</b>\n"
                "• Emojis in name\n"
                "• Words like <code>movie</code>, <code>download</code>, <code>watch online</code>\n\n"
                "💡 Pro Tip: Copy exact title from Google / IMDb."
            )
            await query.edit_message_text(tips_text, parse_mode="HTML")
            schedule_delete(
                context, query.message.chat.id, [query.message.message_id]
            )
            return

        # Group get
        if query.data.startswith("group_get_"):
            parts = query.data.split("_")
            movie_id = int(parts[2])
            original_user_id = int(parts[3])

            if query.from_user.id != original_user_id:
                await query.answer("This button is not for you!", show_alert=True)
                return

            # Check membership first
            is_member = await check_user_membership(context, original_user_id)
            if not is_member:
                await query.edit_message_text(
                    "🚫 <b>Join Required!</b>\n\nPlease join our Channel and Group first.",
                    reply_markup=get_force_join_keyboard(),
                    parse_mode="HTML",
                )
                schedule_delete(
                    context, query.message.chat.id, [query.message.message_id]
                )
                return

            try:
                # Send to PM
                conn = get_db_connection()
                if conn:
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT title, url, file_id FROM movies WHERE id = %s",
                        (movie_id,),
                    )
                    movie_data = cur.fetchone()
                    cur.close()
                    conn.close()

                    if movie_data:
                        title, url, file_id = movie_data

                        # Check if series
                        if is_series(title):
                            info = parse_series_info(title)
                            seasons_data = get_series_episodes(info["base_title"])
                            if seasons_data:
                                context.user_data["series_data"] = seasons_data
                                context.user_data["base_title"] = info["base_title"]

                                pm_msg = await context.bot.send_message(
                                    chat_id=original_user_id,
                                    text=(
                                        f"📺 <b>{esc(info['base_title'])}</b>\n\n"
                                        "Select Season ⬇️"
                                    ),
                                    reply_markup=create_season_selection_keyboard(
                                        seasons_data, info["base_title"]
                                    ),
                                    parse_mode="HTML",
                                )
                                schedule_delete(
                                    context, original_user_id, [pm_msg.message_id]
                                )

                                await query.edit_message_text("✅ Check your PM!")
                                schedule_delete(
                                    context,
                                    query.message.chat.id,
                                    [query.message.message_id],
                                )
                                return

                        # Check qualities
                        qualities = get_all_movie_qualities(movie_id)
                        if qualities and len(qualities) > 1:
                            pm_msg = await context.bot.send_message(
                                chat_id=original_user_id,
                                text=(
                                    f"🎬 <b>{esc(title)}</b>\n\n"
                                    "Select Quality ⬇️"
                                ),
                                reply_markup=create_quality_selection_keyboard(
                                    movie_id, title, qualities
                                ),
                                parse_mode="HTML",
                            )
                            schedule_delete(
                                context, original_user_id, [pm_msg.message_id]
                            )
                        else:
                            # Directly send file
                            await send_movie_file(
                                None,
                                context,
                                title,
                                url,
                                file_id,
                                chat_id=original_user_id,
                                user_id=original_user_id,
                            )

                        await query.edit_message_text("✅ Check your PM!")
                        schedule_delete(
                            context, query.message.chat.id, [query.message.message_id]
                        )
                    else:
                        await query.edit_message_text("❌ Movie not found!")
                        schedule_delete(
                            context, query.message.chat.id, [query.message.message_id]
                        )

            except telegram.error.Forbidden:
                bot_username = (await context.bot.get_me()).username
                deep_link = f"https://t.me/{bot_username}?start=movie_{movie_id}"
                keyboard = InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton("🤖 Start Bot", url=deep_link),
                            InlineKeyboardButton(
                                "🔄 Try Again", callback_data=query.data
                            ),
                        ]
                    ]
                )
                await query.edit_message_text(
                    (
                        "❌ <b>Can't send message!</b>\n\n"
                        "Please start the bot first, then tap again."
                    ),
                    reply_markup=keyboard,
                    parse_mode="HTML",
                )
                schedule_delete(
                    context, query.message.chat.id, [query.message.message_id]
                )
            return

        # Movie selection from search list
        if query.data.startswith("select_"):
            movie_id = int(query.data.replace("select_", ""))

            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT title, url, file_id FROM movies WHERE id = %s",
                    (movie_id,),
                )
                result = cur.fetchone()
                cur.close()
                conn.close()

                if result:
                    title, url, file_id = result

                    if is_series(title):
                        info = parse_series_info(title)
                        seasons_data = get_series_episodes(info["base_title"])
                        if seasons_data:
                            context.user_data["series_data"] = seasons_data
                            context.user_data["base_title"] = info["base_title"]

                            await query.edit_message_text(
                                f"📺 <b>{esc(info['base_title'])}</b>\n\nSelect Season ⬇️",
                                reply_markup=create_season_selection_keyboard(
                                    seasons_data, info["base_title"]
                                ),
                                parse_mode="HTML",
                            )
                            schedule_delete(
                                context,
                                query.message.chat.id,
                                [query.message.message_id],
                            )
                            return

                    # Movie - show qualities
                    qualities = get_all_movie_qualities(movie_id)
                    if qualities and len(qualities) > 1:
                        await query.edit_message_text(
                            f"🎬 <b>{esc(title)}</b>\n\nSelect Quality ⬇️",
                            reply_markup=create_quality_selection_keyboard(
                                movie_id, title, qualities
                            ),
                            parse_mode="HTML",
                        )
                        schedule_delete(
                            context,
                            query.message.chat.id,
                            [query.message.message_id],
                        )
                    else:
                        await send_movie_file(update, context, title, url, file_id)
                        await query.edit_message_text("✅ Sent!")
                        schedule_delete(
                            context,
                            query.message.chat.id,
                            [query.message.message_id],
                        )
            return

        # Season selection
        if query.data.startswith("season_"):
            parts = query.data.split("_", 2)
            season_num = int(parts[1])

            seasons_data = context.user_data.get("series_data", {})
            episodes = seasons_data.get(season_num, [])

            if episodes:
                await query.edit_message_text(
                    f"📺 Season {season_num}\n\nSelect Episode ⬇️",
                    reply_markup=create_episode_selection_keyboard(episodes, season_num),
                    parse_mode="HTML",
                )
                schedule_delete(
                    context, query.message.chat.id, [query.message.message_id]
                )
            return

        # Episode selection (movie_ callback)
        if query.data.startswith("movie_") and not query.data.startswith("movie_{"):
            movie_id = int(query.data.replace("movie_", ""))

            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT title, url, file_id FROM movies WHERE id = %s",
                    (movie_id,),
                )
                result = cur.fetchone()
                cur.close()
                conn.close()

                if result:
                    title, url, file_id = result
                    qualities = get_all_movie_qualities(movie_id)

                    if qualities and len(qualities) > 1:
                        await query.edit_message_text(
                            f"🎬 <b>{esc(title)}</b>\n\nSelect Quality ⬇️",
                            reply_markup=create_quality_selection_keyboard(
                                movie_id, title, qualities
                            ),
                            parse_mode="HTML",
                        )
                        schedule_delete(
                            context,
                            query.message.chat.id,
                            [query.message.message_id],
                        )
                    else:
                        await send_movie_file(update, context, title, url, file_id)
                        await query.edit_message_text("✅ Sent!")
                        schedule_delete(
                            context,
                            query.message.chat.id,
                            [query.message.message_id],
                        )
            return

        # Quality selection
        if query.data.startswith("quality_"):
            parts = query.data.split("_", 2)
            movie_id = int(parts[1])
            selected_quality = parts[2].replace("_", " ")

            conn = get_db_connection()
            if conn:
                cur = conn.cursor()

                # Get movie title
                cur.execute("SELECT title FROM movies WHERE id = %s", (movie_id,))
                title_res = cur.fetchone()
                title = title_res[0] if title_res else "Movie"

                # Get specific quality file
                cur.execute(
                    """
                    SELECT url, file_id FROM movie_files
                    WHERE movie_id = %s AND quality = %s
                """,
                    (movie_id, selected_quality),
                )
                file_data = cur.fetchone()

                # Fallback to main movie URL if quality not in movie_files
                if not file_data:
                    cur.execute(
                        "SELECT url, file_id FROM movies WHERE id = %s", (movie_id,)
                    )
                    file_data = cur.fetchone()

                cur.close()
                conn.close()

                if file_data:
                    url, file_id = file_data
                    await query.edit_message_text(
                        f"📤 Sending <b>{esc(title)}</b> [{esc(selected_quality)}]...",
                        parse_mode="HTML",
                    )
                    schedule_delete(
                        context,
                        query.message.chat.id,
                        [query.message.message_id],
                    )
                    await send_movie_file(
                        update,
                        context,
                        f"{title} [{selected_quality}]",
                        url,
                        file_id,
                    )
                else:
                    await query.edit_message_text("❌ File not found!")
                    schedule_delete(
                        context,
                        query.message.chat.id,
                        [query.message.message_id],
                    )
            return

        # Navigation
        if query.data.startswith("page_"):
            page = int(query.data.replace("page_", ""))
            movies = context.user_data.get("search_results", [])
            if movies:
                await query.edit_message_text(
                    f"🔍 <b>Found {len(movies)} results</b>\n\nSelect one ⬇️",
                    reply_markup=create_movie_selection_keyboard(movies, page),
                    parse_mode="HTML",
                )
                schedule_delete(
                    context, query.message.chat.id, [query.message.message_id]
                )
            return

        # Cancel
        if query.data == "cancel_selection":
            await query.edit_message_text("❌ Cancelled.")
            schedule_delete(
                context, query.message.chat.id, [query.message.message_id]
            )
            return

    except Exception as e:
        logger.error(f"Callback error: {e}")
        try:
            await query.answer("❌ Error occurred!", show_alert=True)
        except Exception:
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
            msg = await update.effective_message.reply_text(
                "❌ Something went wrong. Please try again."
            )
            schedule_delete(context, update.effective_chat.id, [msg.message_id])
        except Exception:
            pass


# ==================== CANCEL HANDLER ====================
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("❌ Cancelled.")
    schedule_delete(context, update.effective_chat.id, [msg.message_id])
    return ConversationHandler.END


# ==================== MAIN BOT ====================
def main():
    """Run the Telegram bot"""
    logger.info("Starting FilmfyBox Bot...")

    # Optional: your existing setup_database()
    try:
        from setup_database import setup_database  # Use your existing setup
        setup_database()
    except Exception:
        pass

    application = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .read_timeout(30)
        .write_timeout(30)
        .build()
    )

    # Conversation handler
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start, filters=filters.ChatType.PRIVATE)],
        states={
            MAIN_MENU: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,
                    main_menu,
                )
            ],
            SEARCHING: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,
                    search_movies,
                )
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_message=False,
        per_chat=True,
    )

    # Handlers
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS,
            group_message_handler,
        )
    )
    application.add_handler(conv_handler)

    application.add_error_handler(error_handler)

    logger.info("Bot started successfully! 🎬")
    application.run_polling()


if __name__ == "__main__":
    main()
