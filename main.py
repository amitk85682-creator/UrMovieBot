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

# ==================== OPTIONAL FIXED DB URL (db_utils) ====================
try:
    # prefer db_utils' fixed URL if it exists
    import db_utils
    FIXED_DATABASE_URL = getattr(db_utils, "FIXED_DATABASE_URL", None)
except Exception:
    FIXED_DATABASE_URL = None

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

if not (DATABASE_URL or FIXED_DATABASE_URL):
    logger.error("No DATABASE_URL or FIXED_DATABASE_URL is configured")
    raise ValueError("DATABASE_URL / FIXED_DATABASE_URL is not set.")

# ==================== UTILITY FUNCTIONS ====================
def preprocess_query(query: str) -> str:
    """Clean and normalize user query"""
    if not query:
        return ""
    query = re.sub(r'[^\w\s-]', ' ', query)
    query = ' '.join(query.split())
    stop_words = ['movie', 'film', 'full', 'download', 'watch', 'online', 'free', 'फिल्म', 'मूवी', 'सीरीज']
    words = query.lower().split()
    words = [w for w in words if w not in stop_words]
    return ' '.join(words).strip()

def _normalize_title_for_match(title: str) -> str:
    """Normalize title for fuzzy / substring matching"""
    if not title:
        return ""
    t = title.lower()
    t = re.sub(r'[^\w\s]', ' ', t)   # remove punctuation
    t = re.sub(r'\s+', ' ', t).strip()
    return t

def _normalize_base_name(name: str) -> str:
    """Normalize base name so that 'Stranger Things', 'stranger.things' etc match."""
    if not name:
        return ""
    n = name.lower()
    n = re.sub(r'[^\w\s]', ' ', n)   # remove punctuation
    n = re.sub(r'\s+', ' ', n).strip()
    return n

async def delete_message_after_delay(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    delay: int = AUTO_DELETE_DELAY
):
    """Auto delete message after specified delay"""
    try:
        await asyncio.sleep(delay)
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info(f"Deleted message {message_id} from chat {chat_id}")
    except Exception as e:
        logger.error(f"Failed to delete message {message_id}: {e}")

# ==================== NEW HELPER FUNCTIONS ====================

def parse_info(title: str) -> dict:
    """
    Analyze a raw file title and extract:
    - base_name  (for grouping / Netflix-like catalog)
    - season
    - episode
    - quality
    - language
    """
    if not title:
        return {
            "base_name": "",
            "season": None,
            "episode": None,
            "quality": "HD",
            "language": "Unknown",
            "original_title": title
        }

    original_title = title
    title_lower = title.lower()

    # Normalize
    norm = re.sub(r'[.\-_]', ' ', title_lower)
    norm = re.sub(r'\s+', ' ', norm).strip()
    tokens = norm.split()

    quality_re = re.compile(r'^(?:\d{3,4}p|4k|2160p)$')
    season_token_re = re.compile(r'^s\d{1,2}$')
    episode_token_re = re.compile(r'^e\d{1,3}$')
    year_re = re.compile(r'^(19|20)\d{2}$')

    tech_words = {
        'season', 'seasons', 's', 'ep', 'eps', 'episode', 'episodes',
        'vol', 'volume', 'part', 'chapter', 'ch',
        'hdrip', 'webrip', 'webdl', 'web-dl', 'bluray', 'brrip',
        'dvdrip', 'cam', 'hdtc', 'hdcam',
        'proper', 'repack', 'uncut', 'complete', 'collection', 'pack'
    }
    lang_words = {
        'hindi', 'hin', 'urdu', 'tamil', 'telugu', 'malayalam', 'kannada',
        'english', 'eng',
        'dual', 'multi', 'dubbed', 'dub', 'subbed', 'sub'
    }

    # Base name tokens
    base_tokens = []
    for i, tok in enumerate(tokens):
        is_technical = False

        if quality_re.match(tok):
            is_technical = True
        elif season_token_re.match(tok) or episode_token_re.match(tok):
            is_technical = True
        elif tok in tech_words or tok in lang_words:
            is_technical = True
        elif tok in {'s', 'season'} and i + 1 < len(tokens) and tokens[i + 1].isdigit():
            is_technical = True
        elif tok in {'ep', 'episode'} and i + 1 < len(tokens) and tokens[i + 1].isdigit():
            is_technical = True
        elif year_re.match(tok):
            is_technical = False

        if is_technical:
            break
        base_tokens.append(tok)

    base_name = ' '.join(base_tokens).strip()
    if not base_name:
        base_name = norm

    # Season & Episode
    season = None
    episode = None

    season_match = re.search(r'(?:\bseason\b|\bs)(?:\s*|\.|\-)?(\d{1,2})', norm)
    if season_match:
        try:
            season = int(season_match.group(1))
        except ValueError:
            season = None

    ep_match = re.search(r'(?:\bepisode\b|\bep\b|\be)(?:\s*|\.|\-)?(\d{1,3})', norm)
    if ep_match:
        try:
            episode = int(ep_match.group(1))
        except ValueError:
            episode = None

    # Quality
    quality = "HD"
    if re.search(r'2160p|4k', title_lower):
        quality = "4K"
    elif "1080p" in title_lower:
        quality = "1080p"
    elif "720p" in title_lower:
        quality = "720p"
    elif "480p" in title_lower:
        quality = "480p"
    elif "cam" in title_lower or "hdcam" in title_lower or "hdtc" in title_lower:
        quality = "CAM"

    # Language
    language = "Unknown"
    if "multi audio" in title_lower or "multi-audio" in title_lower or "multi" in title_lower:
        language = "Multi-Audio"
    elif "dual audio" in title_lower or "dual" in title_lower:
        language = "Dual Audio"
    elif "hindi" in title_lower or "hin " in title_lower or title_lower.endswith(" hin"):
        language = "Hindi"
    elif "english" in title_lower or " eng" in title_lower:
        language = "English"

    return {
        "base_name": base_name,
        "season": season,
        "episode": episode,
        "quality": quality,
        "language": language,
        "original_title": original_title
    }

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
        cur.execute('''
            CREATE TABLE IF NOT EXISTS sync_info (
                id SERIAL PRIMARY KEY,
                last_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        ''')

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
                except Exception:
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
        if cur:
            cur.close()
        if conn:
            conn.close()

def get_movie_from_db(user_query: str):
    """Search a single best match movie in database (anchor), then we expand to all seasons/qualities."""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return None

        cur = conn.cursor()
        processed_query = preprocess_query(user_query)
        if not processed_query:
            cur.close()
            conn.close()
            return None

        logger.info(f"Searching for: '{processed_query}'")

        # Fetch all titles once
        cur.execute("SELECT id, title, url, file_id FROM movies")
        all_movies = cur.fetchall()
        if not all_movies:
            cur.close()
            conn.close()
            return None

        norm_query = _normalize_title_for_match(processed_query)

        # 1) Normalized substring match
        for movie in all_movies:
            mid, mtitle, murl, mfid = movie
            norm_title = _normalize_title_for_match(mtitle)
            if norm_query and norm_query in norm_title:
                cur.close()
                conn.close()
                return movie

        # 2) Fuzzy match as fallback
        movie_titles = [m[1] for m in all_movies]
        movie_dict = {m[1]: m for m in all_movies}

        matches = process.extract(processed_query, movie_titles, scorer=fuzz.token_sort_ratio, limit=1)
        if matches:
            best_title, score = matches[0]
            if score >= SIMILARITY_THRESHOLD and best_title in movie_dict:
                cur.close()
                conn.close()
                return movie_dict[best_title]

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
            except Exception:
                pass

def get_similar_movies(base_title: str):
    """
    Find all DB entries that belong to the same 'base title':
    e.g. all seasons/episodes/qualities of "Stranger Things"
    or all qualities of "Tere Ishk Mein (2024)".
    """
    try:
        conn = get_db_connection()
        if not conn:
            return []

        cur = conn.cursor()
        cur.execute("SELECT id, title, url, file_id FROM movies")
        rows = cur.fetchall()

        base_norm = _normalize_base_name(base_title)
        if not base_norm:
            return []

        candidates = []
        for row in rows:
            mid, title, url, fid = row
            info = parse_info(title)
            if _normalize_base_name(info['base_name']) == base_norm:
                candidates.append((row, info))

        # Sort nicely: Season → Episode → Quality → Title
        def quality_rank(q: str) -> int:
            mapping = {
                "CAM": 0,
                "480p": 1,
                "720p": 2,
                "HD": 2,
                "1080p": 3,
                "4K": 4
            }
            return mapping.get(q or "HD", 5)

        sorted_items = sorted(
            candidates,
            key=lambda item: (
                item[1]['season'] or 0,
                item[1]['episode'] or 0,
                quality_rank(item[1]['quality']),
                item[0][1]  # title
            )
        )

        results = [row for row, info in sorted_items]

        logger.info(f"Found {len(results)} matches for base '{base_norm}'")
        cur.close()
        conn.close()
        return results

    except Exception as e:
        logger.error(f"Error getting similar movies: {e}")
        return []

# ==================== KEYBOARD MARKUPS ====================
def get_start_keyboard():
    """Start menu keyboard exactly as per your image"""
    keyboard = [
        [
            InlineKeyboardButton(
                "➕ Add Me To Your Groups ➕",
                url=f"https://t.me/{(os.environ.get('BOT_USERNAME') or 'urmoviebot')}?startgroup=true"
            )
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
    """Premium Keyboard for File"""
    keyboard = [
        [
            InlineKeyboardButton("✨ ᴊᴏɪɴ ᴍᴏᴠɪᴇ ᴄʜᴀɴɴᴇʟ ✨", url=CHANNEL_LINK)
        ],
        [
            InlineKeyboardButton("💬 ᴊᴏɪɴ ɢʀᴏᴜᴘ", url=GROUP_LINK),
            InlineKeyboardButton(
                "♻️ sʜᴀʀᴇ ʙᴏᴛ",
                url=f"https://t.me/share/url?url=https://t.me/{os.environ.get('BOT_USERNAME', 'urmoviebot')}"
            )
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

# ==================== MOVIE DELIVERY FUNCTIONS ====================
async def send_movie_to_user(
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    movie_data: tuple,
    mode: str = "auto"
):
    """
    Smart Delivery System:

    mode="auto"   -> Decide Series vs Movie, show Seasons / Episodes / Qualities
    mode="final"  -> Send the actual file with premium feeling
    """
    try:
        movie_id, title, url, file_id = movie_data
        chat_id = user_id

        info = parse_info(title)
        base_name = info['base_name'] or title
        base_display = base_name.title()

        # AUTO MODE
        if mode == "auto":
            all_files = get_similar_movies(base_name)
            if not all_files:
                await send_movie_to_user(context, user_id, movie_data, mode="final")
                return

            parsed_all = {m[0]: parse_info(m[1]) for m in all_files}
            has_season = any(pi['season'] is not None for pi in parsed_all.values())
            has_episode = any(pi['episode'] is not None for pi in parsed_all.values())

            # ===== SERIES FLOW =====
            if has_season and has_episode:
                seasons_found = {
                    pi['season'] for pi in parsed_all.values() if pi['season'] is not None
                }
                sorted_seasons = sorted(seasons_found)

                if sorted_seasons:
                    keyboard = []
                    row = []
                    for s in sorted_seasons:
                        btn_text = f"📺 Season {s}"
                        row.append(InlineKeyboardButton(btn_text, callback_data=f"v_seas_{s}_{movie_id}"))
                        if len(row) == 3:
                            keyboard.append(row)
                            row = []
                    if row:
                        keyboard.append(row)

                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            f"🍿 <b>{base_display}</b>\n"
                            f"📺 TV Series\n\n"
                            f"📌 <b>Select a Season to continue:</b>"
                        ),
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode='HTML'
                    )
                    return

            # ===== MOVIE FLOW =====
            keyboard = []
            row = []
            seen = set()

            for mov in all_files:
                mid, m_title, _, _ = mov
                p_info = parsed_all[mid]
                q_text = p_info['quality'] or "HD"
                lang = p_info['language'] or "Unknown"

                if lang == "Hindi":
                    lang_label = "Hin"
                elif lang == "English":
                    lang_label = "Eng"
                elif lang == "Dual Audio":
                    lang_label = "Dual"
                elif lang == "Multi-Audio":
                    lang_label = "Multi"
                else:
                    lang_label = ""

                key = (q_text, lang_label)
                if key in seen:
                    continue
                seen.add(key)

                btn_text = f"📁 {q_text}" + (f" {lang_label}" if lang_label else "")
                row.append(InlineKeyboardButton(btn_text, callback_data=f"quality_{mid}"))

                if len(row) == 2:
                    keyboard.append(row)
                    row = []
            if row:
                keyboard.append(row)

            if not keyboard:
                await send_movie_to_user(context, user_id, movie_data, mode="final")
                return

            msg_text = (
                f"🎬 <b>{base_display}</b>\n\n"
                f"👇 <i>Select the quality you want:</i>"
            )
            await context.bot.send_message(
                chat_id=chat_id,
                text=msg_text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )
            return

        # FINAL MODE
        if mode == "final":
            loading_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    "⏳ <b>Processing your request...</b>\n"
                    "<i>Preparing your file, please wait.</i>"
                ),
                parse_mode='HTML'
            )
            await asyncio.sleep(0.5)

            caption_text = (
                f"🎬 <b>{title}</b>\n"
                f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
                f"💿 <b>Quality:</b> <i>High Definition</i>\n"
                f"🔊 <b>Language:</b> <i>Available as uploaded</i>\n"
                f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
                f"🚀 <b>Join Our Channels:</b>\n"
                f"📢 <a href='{CHANNEL_LINK}'>Main Channel</a> | "
                f"💬 <a href='{GROUP_LINK}'>Support Group</a>\n\n"
                f"⚠️ <i>Auto-delete in 60s. Forward explicitly if needed.</i>"
            )

            sent_msg = None

            try:
                # A) Direct file_id
                if file_id:
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=loading_msg.message_id,
                        text="📤 <b>Uploading file to you...</b>",
                        parse_mode='HTML'
                    )
                    sent_msg = await context.bot.send_document(
                        chat_id=chat_id,
                        document=file_id,
                        caption=caption_text,
                        parse_mode='HTML',
                        reply_markup=get_file_options_keyboard()
                    )

                # B) Telegram message link (public or private)
                elif url and "t.me" in url:
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=loading_msg.message_id,
                        text="🔄 <b>Retrieving from archive...</b>",
                        parse_mode='HTML'
                    )
                    try:
                        parsed = urlparse(url)
                        parts = parsed.path.strip("/").split("/")

                        if len(parts) >= 2:
                            if parts[0] == "c":
                                # /c/<internal_id>/<msg_id>
                                ch_id_str = parts[1]
                                from_chat_id = int("-100" + ch_id_str) if not ch_id_str.startswith("-100") else int(ch_id_str)
                                msg_id = int(parts[2])
                            else:
                                # /<username>/<msg_id>
                                username = parts[0]
                                from_chat_id = f"@{username}"
                                msg_id = int(parts[1])

                            sent_msg = await context.bot.copy_message(
                                chat_id=chat_id,
                                from_chat_id=from_chat_id,
                                message_id=msg_id,
                                caption=caption_text,
                                parse_mode='HTML',
                                reply_markup=get_file_options_keyboard()
                            )
                        else:
                            raise ValueError("Not a direct Telegram message link")
                    except Exception as e:
                        logger.error(f"Error copying from Telegram link: {e}")
                        sent_msg = await context.bot.send_message(
                            chat_id=chat_id,
                            text=f"🎬 <b>{title}</b>\n\n🔗 <b>Download Link:</b> {url}\n\n{caption_text}",
                            parse_mode='HTML',
                            reply_markup=get_file_options_keyboard()
                        )

                # C) Fallback: normal link / text
                else:
                    sent_msg = await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"🎬 <b>{title}</b>\n\n🔗 <b>Download Link:</b> {url}\n\n{caption_text}",
                        parse_mode='HTML',
                        reply_markup=get_file_options_keyboard()
                    )

            except Exception as e:
                logger.error(f"Failed to send file: {e}")
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="❌ <b>Error:</b> File removed or inaccessible.",
                    parse_mode='HTML'
                )

            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=loading_msg.message_id)
            except Exception:
                pass

            if sent_msg:
                timer_msg = await context.bot.send_message(
                    chat_id=chat_id,
                    text="⏳ <i>This message will self-destruct in 60 seconds.</i>",
                    parse_mode='HTML'
                )
                asyncio.create_task(delete_message_after_delay(context, chat_id, sent_msg.message_id, 60))
                asyncio.create_task(delete_message_after_delay(context, chat_id, timer_msg.message_id, 60))

    except Exception as e:
        logger.error(f"Send Movie Error: {e}")

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
                    await send_movie_to_user(context, chat_id, movie_data, mode="auto")
                else:
                    msg = await update.message.reply_text("❌ मूवी डेटाबेस में नहीं मिली।")
                    asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id))
        except Exception as e:
            logger.error(f"Error processing deep link: {e}")
            msg = await update.message.reply_text("❌ कुछ गलत हुआ। फिर से कोशिश करें।")
            asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id))
        return

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

    if len(message_text) < 4 or message_text.startswith('/'):
        return

    movie_data = get_movie_from_db(message_text)
    if not movie_data:
        return

    movie_id, title, _, _ = movie_data
    reply_text = f"@{user.username}, 🎬 **{title}** के लिए नीचे का बटन क्लिक करें:"

    msg = await update.message.reply_text(
        reply_text,
        parse_mode='Markdown',
        reply_markup=get_group_movie_button(movie_id)
    )
    asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id))

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline button callbacks with Smart Series/Season Logic"""
    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = query.message.chat_id

    try:
        # --- 1. FINAL FILE DELIVERY FROM QUALITY BUTTON ---
        if data.startswith("quality_"):
            movie_id = int(data.split("_")[1])
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT id, title, url, file_id FROM movies WHERE id = %s", (movie_id,))
            movie_data = cur.fetchone()
            cur.close()
            conn.close()

            if movie_data:
                try:
                    await query.message.delete()
                except Exception:
                    pass
                await send_movie_to_user(context, query.from_user.id, movie_data, mode="final")
            else:
                await query.message.edit_text("❌ File not found.")

        # --- 2. SELECT SEASON -> SHOW EPISODES / OR SEASON PACK QUALITIES ---
        elif data.startswith("v_seas_"):
            # Format: v_seas_{season_num}_{anchor_movie_id}
            parts = data.split("_")
            season_num = int(parts[2])
            anchor_id = int(parts[3])

            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT title FROM movies WHERE id = %s", (anchor_id,))
            res = cur.fetchone()
            cur.close()
            conn.close()

            if not res:
                await query.message.edit_text("❌ Series not found.")
                return

            base_title = parse_info(res[0])['base_name']
            all_files = get_similar_movies(base_title)

            # Group by episode
            episodes_map = {}
            for mov in all_files:
                p_info = parse_info(mov[1])
                if p_info['season'] == season_num and p_info['episode'] is not None:
                    ep_num = p_info['episode']
                    if ep_num not in episodes_map:
                        episodes_map[ep_num] = []
                    episodes_map[ep_num].append(mov)

            sorted_eps = sorted(episodes_map.keys())

            # अगर कोई episodes नहीं मिले, तो इस पूरे season को PACK मानकर quality-list दिखाओ
            if not sorted_eps:
                target_files = []
                for mov in all_files:
                    p_info = parse_info(mov[1])
                    if p_info['season'] == season_num:
                        target_files.append((mov, p_info))

                if not target_files:
                    await query.message.edit_text("❌ इस Season के लिए कोई फ़ाइल नहीं मिली.")
                    return

                keyboard = []
                seen = set()
                for mov, p in target_files:
                    mid, mtitle, _, _ = mov
                    q = p['quality'] or "HD"
                    lang = p['language'] or "Unknown"

                    if lang == "Hindi":
                        lang_label = "Hin"
                    elif lang == "English":
                        lang_label = "Eng"
                    elif lang == "Dual Audio":
                        lang_label = "Dual"
                    elif lang == "Multi-Audio":
                        lang_label = "Multi"
                    else:
                        lang_label = ""

                    key = (q, lang_label)
                    if key in seen:
                        continue
                    seen.add(key)

                    btn_txt = f"📁 {q}" + (f" {lang_label}" if lang_label else "")
                    keyboard.append([InlineKeyboardButton(btn_txt, callback_data=f"quality_{mid}")])

                keyboard.append([
                    InlineKeyboardButton("🔙 Back to Seasons", callback_data=f"back_seas_{anchor_id}")
                ])

                await query.message.edit_text(
                    text=(
                        f"🍿 <b>{base_title.title()}</b>\n"
                        f"📌 <b>Season {season_num}</b> (Complete Pack)\n\n"
                        f"👇 <i>Select Quality:</i>"
                    ),
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode='HTML'
                )
                return

            # Normal: show episodes list
            keyboard = []
            row = []
            for ep in sorted_eps:
                btn_txt = f"Ep {ep}"
                row.append(InlineKeyboardButton(btn_txt, callback_data=f"v_ep_{season_num}_{ep}_{anchor_id}"))
                if len(row) == 4:
                    keyboard.append(row)
                    row = []
            if row:
                keyboard.append(row)

            keyboard.append([
                InlineKeyboardButton("🔙 Back to Seasons", callback_data=f"back_seas_{anchor_id}")
            ])

            await query.message.edit_text(
                text=(
                    f"🍿 <b>{base_title.title()}</b>\n"
                    f"📌 <b>Season {season_num}</b>\n\n"
                    f"👇 <i>Select Episode:</i>"
                ),
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )

        # --- 3. SELECT EPISODE -> SHOW QUALITIES ---
        elif data.startswith("v_ep_"):
            # Format: v_ep_{season}_{ep}_{anchor_id}
            parts = data.split("_")
            season_num = int(parts[2])
            ep_num = int(parts[3])
            anchor_id = int(parts[4])

            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT title FROM movies WHERE id = %s", (anchor_id,))
            res = cur.fetchone()
            cur.close()
            conn.close()

            if not res:
                await query.message.edit_text("❌ Series not found.")
                return

            base_title = parse_info(res[0])['base_name']
            all_files = get_similar_movies(base_title)

            target_files = []
            for mov in all_files:
                p = parse_info(mov[1])
                if p['season'] == season_num and p['episode'] == ep_num:
                    target_files.append((mov, p))

            if not target_files:
                await query.message.edit_text("❌ इस Episode के लिए कोई File नहीं मिली.")
                return

            keyboard = []
            seen = set()
            for mov, p in target_files:
                mid, mtitle, _, _ = mov
                q = p['quality'] or "HD"
                lang = p['language'] or "Unknown"

                if lang == "Hindi":
                    lang_label = "Hin"
                elif lang == "English":
                    lang_label = "Eng"
                elif lang == "Dual Audio":
                    lang_label = "Dual"
                elif lang == "Multi-Audio":
                    lang_label = "Multi"
                else:
                    lang_label = ""

                key = (q, lang_label)
                if key in seen:
                    continue
                seen.add(key)

                btn_txt = f"📁 {q}" + (f" {lang_label}" if lang_label else "")
                keyboard.append([InlineKeyboardButton(btn_txt, callback_data=f"quality_{mid}")])

            keyboard.append([
                InlineKeyboardButton("🔙 Back to Episodes", callback_data=f"v_seas_{season_num}_{anchor_id}")
            ])

            await query.message.edit_text(
                text=(
                    f"🍿 <b>{base_title.title()}</b>\n"
                    f"📌 <b>S{season_num:02d} · E{ep_num:02d}</b>\n\n"
                    f"👇 <i>Select Quality:</i>"
                ),
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )

        # --- 4. BACK BUTTON: Seasons menu ---
        elif data.startswith("back_seas_"):
            anchor_id = int(data.split("_")[2])
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT id, title, url, file_id FROM movies WHERE id = %s", (anchor_id,))
            movie_data = cur.fetchone()
            cur.close()
            conn.close()

            if movie_data:
                await send_movie_to_user(context, query.from_user.id, movie_data, mode="auto")
                try:
                    await query.message.delete()
                except Exception:
                    pass

        # --- HELP ---
        elif data == "help":
            help_text = """
❓ **Help - कैसे उपयोग करें?**

1. ग्रुप में बस मूवी या सीरीज का नाम टाइप करें  
2. बॉट आपको "📂 Get File Here" बटन देगा  
3. बटन पर क्लिक करें - आप बॉट के प्राइवेट चैट में जाएंगे  
4. वहाँ Netflix जैसा मेनू मिलेगा:
   - Series: Season → Episode → Quality
   - Movie: Direct Quality select

⚠️ नोट:
- नाम जितना साफ़ होगा, रिज़ल्ट उतना बेहतर मिलेगा  
- सभी फाइनल messages 1 मिनट बाद ऑटो डिलीट हो जाते हैं
            """
            msg = await query.edit_message_caption(
                caption=help_text,
                parse_mode='Markdown',
                reply_markup=get_start_keyboard()
            )
            asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id))

        # --- ABOUT ---
        elif data == "about":
            about_text = f"""
ℹ️ **About {BOT_NAME}**

यह बॉट ग्रुप में सिर्फ नाम टाइप करने पर Netflix जैसा experience देता है:

- सीरीज: Season → Episode → Quality wise files  
- मूवी: Multiple qualities की clean list  
- Files प्राइवेट चैट में मिलती हैं, auto-delete के साथ

📢 चैनल: {CHANNEL_LINK}
👥 ग्रुप: {GROUP_LINK}
© MAINTAINED BY: FlimfyBox Team
            """
            msg = await query.edit_message_caption(
                caption=about_text,
                parse_mode='Markdown',
                reply_markup=get_start_keyboard()
            )
            asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id))

        # --- MEMBERSHIP CHECK ---
        elif data == "check_membership":
            msg = await query.edit_message_text(
                text="✅ आपको चैनल और ग्रुप में जॉइन होने का स्टेटस कन्फर्म हुआ!",
                reply_markup=None
            )
            asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id))

    except Exception as e:
        logger.error(f"Error in button callback: {e}")
        try:
            await query.edit_message_text("❌ कुछ गलत हुआ। फिर से कोशिश करें।")
        except Exception:
            pass

# ==================== ERROR HANDLER ====================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Log errors gracefully"""
    logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)

# ==================== FLASK APP ====================
flask_app = Flask(__name__)

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

    application = Application.builder()\
        .token(TELEGRAM_BOT_TOKEN)\
        .read_timeout(30)\
        .write_timeout(30)\
        .build()

    # Handlers
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(CommandHandler('start', start))

    # Group message handler
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS,
        group_message_handler
    ))

    # Private chat handler
    async def private_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Search movies when user sends text in private chat"""
        if not update.message or not update.message.text:
            return

        text = update.message.text
        movie = get_movie_from_db(text)

        if movie:
            await send_movie_to_user(context, update.effective_chat.id, movie, mode="auto")
        else:
            # silent
            pass

    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,
        private_message_handler
    ))

    application.add_error_handler(error_handler)

    # Start Flask in background
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    logger.info("Flask server started in background.")

    logger.info("Starting bot polling...")
    application.run_polling()

if __name__ == '__main__':
    main()
