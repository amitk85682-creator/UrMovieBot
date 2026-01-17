# -*- coding: utf-8 -*-
import os
import threading
import asyncio
import logging
import re
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any

import telegram
import psycopg2
from psycopg2 import pool
from flask import Flask
from collections import defaultdict

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatMember
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler,
    CallbackQueryHandler
)
from fuzzywuzzy import process, fuzz

# ==================== FLASK APP ====================
app = Flask(__name__)

@app.route("/")
def index():
    return "Ur Movie Bot is running ✅"

@app.route("/health")
def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

# ==================== LOGGING ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== STATES ====================
MAIN_MENU, SEARCHING = range(2)

# ==================== CONFIG ====================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.environ.get('DATABASE_URL')
ADMIN_USER_ID = int(os.environ.get('ADMIN_USER_ID', 0))

# Force Join Config
REQUIRED_CHANNEL = os.environ.get('REQUIRED_CHANNEL_ID', '-1003460387180')
REQUIRED_GROUP = os.environ.get('REQUIRED_GROUP_ID', '-1003330141433')
CHANNEL_URL = os.environ.get('CHANNEL_URL', 'https://t.me/FilmFyBoxMoviesHD')
GROUP_URL = os.environ.get('GROUP_URL', 'https://t.me/FlimfyBox')
FORCE_JOIN_ENABLED = True

# Auto delete delay
AUTO_DELETE_DELAY = 60

# Verified users cache
verified_users: Dict[int, Tuple[datetime, Dict]] = {}
VERIFICATION_CACHE_TIME = 3600  # 1 Hour

# Database connection pool
db_pool: Optional[pool.SimpleConnectionPool] = None

# Validate environment variables
if not TELEGRAM_BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN is not set")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set")

# ==================== DATABASE POOL ====================
def init_db_pool():
    """Initialize database connection pool"""
    global db_pool
    try:
        db_pool = pool.SimpleConnectionPool(1, 10, DATABASE_URL)
        logger.info("Database pool initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database pool: {e}")
        raise

def get_db():
    """Get database connection from pool"""
    global db_pool
    try:
        if db_pool is None:
            init_db_pool()
        return db_pool.getconn()
    except Exception as e:
        logger.error(f"DB connection error: {e}")
        return None

def release_db(conn):
    """Release connection back to pool"""
    global db_pool
    if db_pool and conn:
        try:
            db_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Error releasing connection: {e}")

# 👇👇👇 IS FUNCTION KO 'get_movie_by_id' KE NEECHE PASTE KARO 👇👇👇

# 👇👇👇 IS FUNCTION KO 'get_movie_by_id' KE NEECHE PASTE KARO 👇👇👇

def get_movies_fast_sql(query: str, limit: int = 5) -> List[Tuple]:
    """
    Smart SQL Search: Fast like SQL + Smart like FuzzyWuzzy.
    Handles typos using PostgreSQL 'pg_trgm' (Similarity).
    """
    conn = None
    try:
        conn = get_db() # Is script me connection pool use ho raha hai
        if not conn:
            return []

        cur = conn.cursor()
        
        # 1. Ensure Extension Enabled
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        
        # 2. Smart Query (SIMILARITY > 0.3)
        sql = """
            SELECT m.id, m.title, m.url, m.file_id, 
                   SIMILARITY(m.title, %s) as sim_score
            FROM movies m
            WHERE SIMILARITY(m.title, %s) > 0.3
            ORDER BY sim_score DESC
            LIMIT %s
        """
        
        cur.execute(sql, (query, query, limit))
        results = cur.fetchall()
        
        # Format results: (id, title, url, file_id) - Score hata rahe hain return ke liye
        final_results = [(r[0], r[1], r[2], r[3]) for r in results]
        
        cur.close()
        return final_results

    except Exception as e:
        logger.error(f"Smart SQL Search Error: {e}")
        return []
    finally:
        if conn:
            release_db(conn) # Connection pool me wapis
# ==================== MEMBERSHIP CHECK (FIXED) ====================
async def is_user_member(context, user_id: int, force_fresh: bool = False) -> Dict[str, Any]:
    """Check if user is member of channel and group"""
    
    if not FORCE_JOIN_ENABLED:
        return {'is_member': True, 'channel': True, 'group': True, 'error': None}
    
    current_time = datetime.now()
    
    # Check cache
    if not force_fresh and user_id in verified_users:
        last_checked, cached = verified_users[user_id]
        if (current_time - last_checked).total_seconds() < VERIFICATION_CACHE_TIME:
            logger.info(f"Cache hit for {user_id}")
            return cached
    
    result = {
        'is_member': False,
        'channel': False,
        'group': False,
        'channel_status': 'unknown',
        'group_status': 'unknown',
        'error': None
    }
    
    # ✅ VALID STATUSES - Only actual members
    VALID_MEMBER_STATUSES = [
        ChatMember.MEMBER,
        ChatMember.ADMINISTRATOR,
        ChatMember.OWNER,
        'member',
        'administrator',
        'creator'
    ]
    
    # ========== CHECK CHANNEL ==========
    try:
        channel_member = await context.bot.get_chat_member(
            chat_id=REQUIRED_CHANNEL,
            user_id=user_id
        )
        status = channel_member.status
        result['channel_status'] = str(status)
        
        # Check if valid member
        if status in VALID_MEMBER_STATUSES:
            result['channel'] = True
        else:
            result['channel'] = False
            
    except Exception as e:
        result['channel_status'] = f'error: {e}'
        logger.error(f"Channel check error: {e}")
        result['channel'] = False
    
    # ========== CHECK GROUP ==========
    try:
        group_member = await context.bot.get_chat_member(
            chat_id=REQUIRED_GROUP,
            user_id=user_id
        )
        status = group_member.status
        result['group_status'] = str(status)
        
        # Check if valid member
        if status in VALID_MEMBER_STATUSES:
            result['group'] = True
        else:
            result['group'] = False
            
    except Exception as e:
        result['group_status'] = f'error: {e}'
        logger.error(f"Group check error: {e}")
        result['group'] = False
    
    # ========== FINAL RESULT ==========
    result['is_member'] = result['channel'] and result['group']
    
    # Update cache
    verified_users[user_id] = (current_time, result)
    
    return result

def get_join_keyboard() -> InlineKeyboardMarkup:
    """Join buttons keyboard"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📢 Join Channel", url=CHANNEL_URL),
            InlineKeyboardButton("💬 Join Group", url=GROUP_URL)
        ],
        [InlineKeyboardButton("✅ Joined Both - Verify", callback_data="verify")]
    ])

def get_join_message(channel_status: bool, group_status: bool) -> str:
    """Generate join message based on what's missing"""
    if not channel_status and not group_status:
        missing = "Channel and Group both"
    elif not channel_status:
        missing = "Channel"
    else:
        missing = "Group"
    
    return (
        f"🚫 **Access Denied**\n\n"
        f"You haven't joined {missing}!\n\n"
        f"📢 Channel: {'✅' if channel_status else '❌'}\n"
        f"💬 Group: {'✅' if group_status else '❌'}\n\n"
        f"Join both, then click **Verify** button 👇"
    )

# ==================== DATABASE FUNCTIONS ====================
def search_movies(query: str, limit: int = 10) -> List[Tuple]:
    """Search movies in database: Checks Title AND Aliases"""
    conn = None
    try:
        conn = get_db()
        if not conn:
            return []
        
        cur = conn.cursor()
        
        flexible_query = query.strip().replace(" ", "%")
        search_term = f'%{flexible_query}%'
        
        sql_query = """
            SELECT DISTINCT m.id, m.title, m.url, m.file_id 
            FROM movies m
            LEFT JOIN movie_aliases ma ON m.id = ma.movie_id
            WHERE 
                m.title ILIKE %s OR 
                ma.alias ILIKE %s
            ORDER BY m.title 
            LIMIT %s
        """
        
        cur.execute(sql_query, (search_term, search_term, limit))
        results = cur.fetchall()
        
        if results:
            cur.close()
            return results

        # Fallback: Fuzzy Search
        cur.execute("SELECT id, title, url, file_id FROM movies")
        all_movies = cur.fetchall()
        cur.close()
        
        if not all_movies:
            return []
        
        titles = [m[1] for m in all_movies]
        matches = process.extract(query, titles, scorer=fuzz.token_sort_ratio, limit=limit)
        
        filtered = []
        for match in matches:
            if match[1] >= 50:
                for movie in all_movies:
                    if movie[1] == match[0]:
                        filtered.append(movie)
                        break
        
        return filtered
        
    except Exception as e:
        logger.error(f"Search error: {e}")
        return []
    finally:
        if conn:
            release_db(conn)

def get_movie_by_id(movie_id: int) -> Optional[Tuple]:
    """Get movie by ID"""
    conn = None
    try:
        conn = get_db()
        if not conn:
            return None
        
        cur = conn.cursor()
        cur.execute("SELECT id, title, url, file_id FROM movies WHERE id = %s", (movie_id,))
        result = cur.fetchone()
        cur.close()
        return result
    except Exception as e:
        logger.error(f"Get movie error: {e}")
        return None
    finally:
        if conn:
            release_db(conn)

# 👇 UPDATED FUNCTION FOR BATCH SUPPORT 👇
def get_movie_qualities(movie_id: int) -> List[Tuple]:
    """Get all qualities for a movie (Updated for Batch)"""
    conn = None
    try:
        conn = get_db()
        if not conn:
            return []
        
        cur = conn.cursor()
        # Simply get files, sorted by ID desc to show newest first
        cur.execute("""
            SELECT quality, url, file_id, file_size
            FROM movie_files
            WHERE movie_id = %s AND (url IS NOT NULL OR file_id IS NOT NULL)
            ORDER BY id DESC
        """, (movie_id,))
        results = cur.fetchall()
        cur.close()
        return results
    except Exception as e:
        logger.error(f"Quality fetch error: {e}")
        return []
    finally:
        if conn:
            release_db(conn)

def log_user_activity(user_id: int, activity_type: str, details: str = None):
    """Log user activity to database"""
    conn = None
    try:
        conn = get_db()
        if not conn:
            return
        
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO user_activity (user_id, activity_type, details, created_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT DO NOTHING
        """, (user_id, activity_type, details))
        conn.commit()
        cur.close()
    except Exception as e:
        logger.error(f"Log activity error: {e}")
    finally:
        if conn:
            release_db(conn)

# ==================== HELPER FUNCTIONS ====================
def is_series(title: str) -> bool:
    """Check if title is a series"""
    patterns = [r'S\d+\s*E\d+', r'Season\s*\d+', r'Episode\s*\d+', r'Ep\s*\d+']
    return any(re.search(p, title, re.IGNORECASE) for p in patterns)

async def safe_delete_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    """Safely delete a message"""
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except telegram.error.BadRequest:
        pass
    except Exception as e:
        logger.error(f"Delete message error: {e}")

async def auto_delete(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_ids: List[int], delay: int = 60):
    """Delete messages after delay"""
    await asyncio.sleep(delay)
    for msg_id in message_ids:
        await safe_delete_message(context, chat_id, msg_id)

def schedule_delete(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_ids: List[int], delay: int = None):
    """Schedule auto deletion"""
    if delay is None:
        delay = AUTO_DELETE_DELAY
    asyncio.create_task(auto_delete(context, chat_id, message_ids, delay))

# ==================== KEYBOARDS ====================
def movie_list_keyboard(movies: List[Tuple], page: int = 0, per_page: int = 5) -> InlineKeyboardMarkup:
    """Create movie selection keyboard"""
    start = page * per_page
    end = start + per_page
    current = movies[start:end]
    
    keyboard = []
    for movie_data in current:
        # Data unpack karo safely
        if len(movie_data) >= 2:
            movie_id = movie_data[0]
            title = movie_data[1]
        else:
            continue

        emoji = "📺" if "season" in title.lower() or "s0" in title.lower() else "🎬"
        display_title = f"{title[:35]}..." if len(title) > 35 else title
        
        # Button: 🎬 Movie Name
        keyboard.append([InlineKeyboardButton(f"{emoji} {display_title}", callback_data=f"m_{movie_id}")])
    
    # Navigation Buttons
    nav = []
    total_pages = (len(movies) + per_page - 1) // per_page
    
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"p_{page-1}"))
    
    # Page Indicator (Center)
    if total_pages > 1:
        nav.append(InlineKeyboardButton(f"📄 {page+1}/{total_pages}", callback_data="noop"))
        
    if end < len(movies):
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"p_{page+1}"))
    
    if nav:
        keyboard.append(nav)
    
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
    
    return InlineKeyboardMarkup(keyboard)
# 👇 UPDATED FUNCTION FOR SMART BUTTONS 👇
def quality_keyboard(movie_id: int, qualities: List[Tuple]) -> InlineKeyboardMarkup:
    """Create quality selection keyboard with Smart Label Logic"""
    keyboard = []
    
    for quality, url, file_id, size in qualities:
        # Icons logic
        icon = '🎬'
        q_lower = quality.lower()
        if '4k' in q_lower: icon = '💎'
        elif '1080p' in q_lower: icon = '🔷'
        elif '720p' in q_lower: icon = '🟢'
        elif '480p' in q_lower: icon = '🟡'

        # 👇 SMART LOGIC: Double Size Fix
        # Agar quality string me pehle se '[' aur ']' hai (Batch Upload), to size mat jodo
        if "[" in quality and "]" in quality:
            display_text = f"{icon} {quality}"
        else:
            # Purane data/Web panel ke liye size jodo
            size_text = f" ({size})" if size else ""
            display_text = f"{icon} {quality}{size_text}"

        keyboard.append([InlineKeyboardButton(
            display_text,
            callback_data=f"q_{movie_id}_{quality}"
        )])
    
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back")])
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
    
    return InlineKeyboardMarkup(keyboard)

def get_promo_buttons() -> InlineKeyboardMarkup:
    """Get promotional buttons"""
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("📢 Channel", url=CHANNEL_URL),
        InlineKeyboardButton("💬 Group", url=GROUP_URL)
    ]])

# ==================== SEND MOVIE ====================
async def send_movie(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    movie_id: int,
    title: str,
    url: Optional[str] = None,
    file_id: Optional[str] = None
):
    """Send movie file to user"""
    chat_id = update.effective_chat.id
    
    # If no direct file, check qualities
    if not url and not file_id:
        qualities = get_movie_qualities(movie_id)
        if qualities:
            context.user_data['movie'] = {
                'id': movie_id,
                'title': title,
                'qualities': qualities
            }
            msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"✅ **{title}**\n\n🎯 Choose quality:",
                reply_markup=quality_keyboard(movie_id, qualities),
                parse_mode='Markdown'
            )
            schedule_delete(context, chat_id, [msg.message_id], 300)
            return
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text="❌ No files available for this movie!"
            )
            return
    
    try:
        # Warning message
        warn = await context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ **Will be deleted in 60 seconds!**\n📤 Forward to save!",
            parse_mode='Markdown'
        )
        
        caption = (
            f"🎬 **{title}**\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"📢 [@FilmFyBox]({CHANNEL_URL})\n"
            f"⏰ Auto-delete: 60 sec"
        )
        
        buttons = get_promo_buttons()
        
        sent = None
        
        if file_id:
            try:
                sent = await context.bot.send_document(
                    chat_id=chat_id,
                    document=file_id,
                    caption=caption,
                    parse_mode='Markdown',
                    reply_markup=buttons
                )
            except telegram.error.BadRequest:
                # Try as video
                try:
                    sent = await context.bot.send_video(
                        chat_id=chat_id,
                        video=file_id,
                        caption=caption,
                        parse_mode='Markdown',
                        reply_markup=buttons
                    )
                except Exception as e:
                    logger.error(f"Send file_id failed: {e}")
                    
        elif url and "t.me/" in url:
            # Copy from telegram link
            try:
                parts = url.rstrip('/').split('/')
                if "/c/" in url:
                    from_chat = int("-100" + parts[-2])
                else:
                    from_chat = f"@{parts[-2]}"
                msg_id = int(parts[-1])
                
                sent = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=from_chat,
                    message_id=msg_id,
                    caption=caption,
                    parse_mode='Markdown',
                    reply_markup=buttons
                )
            except Exception as e:
                logger.error(f"Copy failed: {e}")
                sent = await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🎬 **{title}**\n\n🔗 [Watch Here]({url})",
                    parse_mode='Markdown',
                    reply_markup=buttons,
                    disable_web_page_preview=False
                )
        elif url:
            sent = await context.bot.send_message(
                chat_id=chat_id,
                text=f"🎬 **{title}**\n\n🔗 [Download]({url})",
                parse_mode='Markdown',
                reply_markup=buttons,
                disable_web_page_preview=False
            )
        
        if sent:
            # Get message ID properly
            msg_id = sent.message_id if hasattr(sent, 'message_id') else sent
            schedule_delete(context, chat_id, [warn.message_id, msg_id], AUTO_DELETE_DELAY)
            
            # Log activity
            log_user_activity(update.effective_user.id, 'movie_sent', title)
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text="❌ Could not send the file. Please try again later."
            )
            await safe_delete_message(context, chat_id, warn.message_id)
            
    except Exception as e:
        logger.error(f"Send movie error: {e}")
        await context.bot.send_message(
            chat_id=chat_id,
            text="❌ Failed to send file! Please try again."
        )

# ==================== HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start command handler"""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    # Log user activity
    log_user_activity(user_id, 'start', 'Bot started')
    
    # Handle deep links
    if context.args:
        arg = context.args[0]
        
        # Movie link: /start movie_123
        if arg.startswith("movie_"):
            # Check membership FIRST (fresh check)
            check = await is_user_member(context, user_id, force_fresh=True)
            
            if not check['is_member']:
                msg = await update.message.reply_text(
                    get_join_message(check['channel'], check['group']),
                    reply_markup=get_join_keyboard(),
                    parse_mode='Markdown'
                )
                schedule_delete(context, chat_id, [msg.message_id], 120)
                return MAIN_MENU
            
            # User is member, get movie
            try:
                movie_id = int(arg.split('_')[1])
                movie = get_movie_by_id(movie_id)
                
                if movie:
                    await send_movie(update, context, movie[0], movie[1], movie[2], movie[3])
                else:
                    await update.message.reply_text("❌ Movie not found!")
            except Exception as e:
                logger.error(f"Deep link error: {e}")
                await update.message.reply_text("❌ Invalid link!")
            
            return MAIN_MENU
        
        # Search link: /start q_Movie_Name
        if arg.startswith("q_"):
            query = arg[2:].replace("_", " ")
            
            # Check membership FIRST (fresh check)
            check = await is_user_member(context, user_id, force_fresh=True)
            
            if not check['is_member']:
                msg = await update.message.reply_text(
                    get_join_message(check['channel'], check['group']),
                    reply_markup=get_join_keyboard(),
                    parse_mode='Markdown'
                )
                schedule_delete(context, chat_id, [msg.message_id], 120)
                return MAIN_MENU
            
            # Process search
            context.user_data['query'] = query
            return await process_search(update, context, query)
    
    # Normal start - show welcome
    try:
        bot = await context.bot.get_me()
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add to Group", url=f"https://t.me/{bot.username}?startgroup=true")],
            [
                InlineKeyboardButton("📢 Channel", url=CHANNEL_URL),
                InlineKeyboardButton("💬 Group", url=GROUP_URL)
            ]
        ])
        
        welcome = (
            "🎬 **Ur Movie Bot**\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "🔍 Type movie or series name to search!\n\n"
            "📝 **Example:** `Avengers Endgame`\n\n"
            "⚡ **Features:**\n"
            "• Fast fuzzy search\n"
            "• Multiple quality options\n"
            "• Auto-delete for privacy\n\n"
            "Type any movie name to start! 👇"
        )
        
        await update.message.reply_text(
            welcome,
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Start command error: {e}")
        await update.message.reply_text(
            "🎬 Welcome to Ur Movie Bot!\n\nType any movie name to search."
        )
    
    return MAIN_MENU

async def process_search(update: Update, context: ContextTypes.DEFAULT_TYPE, query: str = None) -> int:
    """Process movie search"""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    if not query:
        if not update.message or not update.message.text:
            return MAIN_MENU
        query = update.message.text.strip()
    
    # Ignore commands
    if query.startswith('/'):
        return MAIN_MENU
    
    if len(query) < 2:
        await update.message.reply_text(
            "⚠️ Please enter at least 2 characters to search."
        )
        return MAIN_MENU
    
    # Membership check (use cache if available)
    check = await is_user_member(context, user_id, force_fresh=False)
    
    if check['error']:
        await update.message.reply_text(f"⚠️ Error: {check['error']}")
        return MAIN_MENU
    
    if not check['is_member']:
        msg = await update.message.reply_text(
            get_join_message(check['channel'], check['group']),
            reply_markup=get_join_keyboard(),
            parse_mode='Markdown'
        )
        schedule_delete(context, chat_id, [msg.message_id], 120)
        return MAIN_MENU
    
    # User is member - search movies
    searching_msg = await update.message.reply_text(
        f"🔍 Searching for `{query}`...",
        parse_mode='Markdown'
    )
    
    movies = search_movies(query)
    
    # Delete searching message
    await safe_delete_message(context, chat_id, searching_msg.message_id)
    
    if not movies:
        await update.message.reply_text(
            f"😕 `{query}` not found!\n\nTry a different name.",
            parse_mode='Markdown'
        )
        return MAIN_MENU
    
    # Log search activity
    log_user_activity(user_id, 'search', query)
    
    if len(movies) == 1:
        # Single result - send directly
        m = movies[0]
        await send_movie(update, context, m[0], m[1], m[2], m[3])
        return MAIN_MENU
    
    # Multiple results - show list
    context.user_data['results'] = movies
    context.user_data['query'] = query
    
    await update.message.reply_text(
        f"🔍 **{len(movies)} results** for `{query}`\n\nSelect one:",
        reply_markup=movie_list_keyboard(movies),
        parse_mode='Markdown'
    )
    
    return MAIN_MENU

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle text messages"""
    if not update.message or not update.message.text:
        return MAIN_MENU
    
    # Check if it's a group and bot is not mentioned
    if update.effective_chat.type in ['group', 'supergroup']:
        bot = await context.bot.get_me()
        if f"@{bot.username}" not in update.message.text:
            return MAIN_MENU
    
    return await process_search(update, context)

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle button callbacks"""
    query = update.callback_query
    
    try:
        await query.answer()
    except Exception as e:
        logger.warning(f"Could not answer callback: {e}")
    
    user_id = query.from_user.id
    chat_id = query.message.chat.id
    data = query.data
    
    # ============ NOOP (Page indicator) ============
    if data == "noop":
        await query.answer("📄 Page indicator", show_alert=False)
        return
    
    # ============ VERIFY BUTTON ============
    if data == "verify":
        await query.answer("🔍 Checking membership...", show_alert=True)
        # FORCE FRESH CHECK - Ignore cache completely
        check = await is_user_member(context, user_id, force_fresh=True)
        
        if check['is_member']:
            await query.edit_message_text(
                "✅ **Verified Successfully!**\n\n"
                "You can now search for any movie! 🎬\n"
                "Just type the movie name 👇",
                parse_mode='Markdown'
            )
            # Delete verification message after 10 seconds
            schedule_delete(context, chat_id, [query.message.message_id], 10)
        else:
            # Still not joined
            try:
                await query.edit_message_text(
                    get_join_message(check['channel'], check['group']),
                    reply_markup=get_join_keyboard(),
                    parse_mode='Markdown'
                )
            except telegram.error.BadRequest:
                # Message same, show popup
                await query.answer(
                    "❌ You haven't joined yet! Please join both first.",
                    show_alert=True
                )
        return
    
    # ============ BACK BUTTON ============
    if data == "back":
        movies = context.user_data.get('results', [])
        query_text = context.user_data.get('query', 'Search')
        
        if movies:
            await query.edit_message_text(
                f"🔍 **{len(movies)} results** for `{query_text}`\n\nSelect one:",
                reply_markup=movie_list_keyboard(movies),
                parse_mode='Markdown'
            )
        else:
            await query.edit_message_text("Type a movie name to search!")
        return
    
    # ============ MOVIE SELECTION ============
    if data.startswith("m_"):
        # Check membership (can use cache)
        check = await is_user_member(context, user_id, force_fresh=False)
        if not check['is_member']:
            await query.edit_message_text(
                get_join_message(check['channel'], check['group']),
                reply_markup=get_join_keyboard(),
                parse_mode='Markdown'
            )
            return
        
        try:
            movie_id = int(data[2:])
        except ValueError:
            await query.edit_message_text("❌ Invalid movie ID!")
            return
        
        movie = get_movie_by_id(movie_id)
        
        if not movie:
            await query.edit_message_text("❌ Movie not found!")
            return
        
        # Check qualities
        qualities = get_movie_qualities(movie_id)
        
        if qualities:
            context.user_data['movie'] = {
                'id': movie_id,
                'title': movie[1],
                'qualities': qualities
            }
            await query.edit_message_text(
                f"✅ **{movie[1]}**\n\n🎯 Choose quality:",
                reply_markup=quality_keyboard(movie_id, qualities),
                parse_mode='Markdown'
            )
        else:
            await query.edit_message_text(
                f"📤 Sending **{movie[1]}**...",
                parse_mode='Markdown'
            )
            await send_movie(update, context, movie[0], movie[1], movie[2], movie[3])
        
        return
    
    # ============ QUALITY SELECTION ============
    if data.startswith("q_"):
        # Check membership (can use cache)
        check = await is_user_member(context, user_id, force_fresh=False)
        if not check['is_member']:
            await query.edit_message_text(
                get_join_message(check['channel'], check['group']),
                reply_markup=get_join_keyboard(),
                parse_mode='Markdown'
            )
            return
        
        parts = data.split("_")
        if len(parts) < 3:
            await query.edit_message_text("❌ Invalid quality selection!")
            return
        
        try:
            movie_id = int(parts[1])
        except ValueError:
            await query.edit_message_text("❌ Invalid movie ID!")
            return
        
        quality = "_".join(parts[2:])
        
        movie_data = context.user_data.get('movie', {})
        
        # Find the quality
        url, file_id = None, None
        title = movie_data.get('title', 'Movie')
        
        for q, u, f, s in movie_data.get('qualities', []):
            if q == quality:
                url, file_id = u, f
                break
        
        if not url and not file_id:
            await query.edit_message_text("❌ Quality not available!")
            return
        
        await query.edit_message_text(
            f"📤 Sending **{title}** ({quality})...",
            parse_mode='Markdown'
        )
        
        await send_movie(update, context, movie_id, f"{title} ({quality})", url, file_id)
        return
    
    # ============ PAGINATION ============
    if data.startswith("p_"):
        try:
            page = int(data[2:])
        except ValueError:
            return
        
        movies = context.user_data.get('results', [])
        query_text = context.user_data.get('query', 'Search')
        
        if movies:
            await query.edit_message_text(
                f"🔍 **{len(movies)} results** for `{query_text}`\n\nSelect one:",
                reply_markup=movie_list_keyboard(movies, page),
                parse_mode='Markdown'
            )
        return
    
    # ============ CANCEL ============
    if data == "cancel":
        await query.edit_message_text("❌ Cancelled")
        schedule_delete(context, chat_id, [query.message.message_id], 5)
        return
    
    # ============ GROUP GET (DM) ============
    if data.startswith("g_"):
        parts = data.split("_")
        if len(parts) < 3:
            await query.answer("❌ Invalid request!", show_alert=True)
            return
        
        try:
            movie_id = int(parts[1])
            original_user = int(parts[2])
        except ValueError:
            await query.answer("❌ Invalid request!", show_alert=True)
            return
        
        if user_id != original_user:
            await query.answer("❌ This button is not for you!", show_alert=True)
            return
        
        # Check membership (fresh check for group actions)
        check = await is_user_member(context, user_id, force_fresh=True)
        if not check['is_member']:
            await query.edit_message_text(
                get_join_message(check['channel'], check['group']),
                reply_markup=get_join_keyboard(),
                parse_mode='Markdown'
            )
            return
        
        # Get movie details
        movie = get_movie_by_id(movie_id)
        
        if not movie:
            await query.edit_message_text("❌ Movie not found!")
            return
        
        await query.edit_message_text(
            f"📤 Sending **{movie[1]}** to your DM...",
            parse_mode='Markdown'
        )
        
        # Send in private chat
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"🎬 Here's your requested movie!"
            )
            
            # Create a simple object for send_movie
            class SimpleUpdate:
                def __init__(self, user, chat):
                    self.effective_user = user
                    self.effective_chat = chat
            
            class SimpleChat:
                def __init__(self, chat_id):
                    self.id = chat_id
            
            simple_update = SimpleUpdate(query.from_user, SimpleChat(user_id))
            await send_movie(simple_update, context, movie[0], movie[1], movie[2], movie[3])
            
            await query.edit_message_text(
                f"✅ **{movie[1]}** sent to your DM!",
                parse_mode='Markdown'
            )
            
        except telegram.error.Forbidden:
            await query.edit_message_text(
                "❌ Can't send DM! Please start the bot first:\n"
                f"1. Go to @{(await context.bot.get_me()).username}\n"
                "2. Press START\n"
                "3. Try again"
            )
        except Exception as e:
            logger.error(f"DM send error: {e}")
            await query.edit_message_text("❌ Failed to send DM. Please try again.")

# ==================== ADMIN COMMANDS ====================
async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get bot statistics (Admin only)"""
    if update.effective_user.id != ADMIN_USER_ID:
        return
    
    conn = None
    try:
        conn = get_db()
        if not conn:
            await update.message.reply_text("❌ Database connection failed!")
            return
        
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM movies")
        total_movies = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM movie_files")
        total_files = cur.fetchone()[0]
        
        # Try to get user count if table exists
        try:
            cur.execute("SELECT COUNT(DISTINCT user_id) FROM user_activity")
            total_users = cur.fetchone()[0]
        except:
            total_users = "N/A"
        
        cur.close()
        
        stats = (
            f"📊 **Bot Statistics**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🎬 Total Movies: {total_movies}\n"
            f"📁 Total Files: {total_files}\n"
            f"👥 Total Users: {total_users}\n"
            f"🔄 Cached Users: {len(verified_users)}\n"
            f"⏰ Cache Time: {VERIFICATION_CACHE_TIME}s\n"
        )
        
        await update.message.reply_text(stats, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Stats error: {e}")
        await update.message.reply_text(f"❌ Error getting stats: {e}")
    finally:
        if conn:
            release_db(conn)

async def admin_clear_cache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear user verification cache (Admin only)"""
    if update.effective_user.id != ADMIN_USER_ID:
        return
    
    count = len(verified_users)
    verified_users.clear()
    await update.message.reply_text(f"✅ Cache cleared! ({count} users)")

async def admin_check_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check specific user membership (Admin only)"""
    if update.effective_user.id != ADMIN_USER_ID:
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /checkuser USER_ID")
        return
    
    try:
        target_user_id = int(context.args[0])
        check = await is_user_member(context, target_user_id, force_fresh=True)
        
        msg = (
            f"👤 **User {target_user_id}**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📢 Channel: {'✅' if check['channel'] else '❌'}\n"
            f"💬 Group: {'✅' if check['group'] else '❌'}\n"
            f"✅ Is Member: {'Yes' if check['is_member'] else 'No'}\n"
        )
        
        if check['error']:
            msg += f"⚠️ Error: {check['error']}"
        
        await update.message.reply_text(msg, parse_mode='Markdown')
        
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID!")
    except Exception as e:
        logger.error(f"Check user error: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast message to all users (Admin only)"""
    if update.effective_user.id != ADMIN_USER_ID:
        return
    
    if not context.args:
        await update.message.reply_text(
            "Usage: /broadcast Your message here\n\n"
            "Supports Markdown formatting."
        )
        return
    
    message = ' '.join(context.args)
    
    conn = None
    try:
        conn = get_db()
        if not conn:
            await update.message.reply_text("❌ Database connection failed!")
            return
        
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT user_id FROM user_activity")
        users = cur.fetchall()
        cur.close()
        
        if not users:
            await update.message.reply_text("❌ No users found!")
            return
        
        success = 0
        failed = 0
        blocked = 0
        
        status = await update.message.reply_text(
            f"📤 Broadcasting to {len(users)} users..."
        )
        
        for i, (target_user_id,) in enumerate(users):
            try:
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text=message,
                    parse_mode='Markdown'
                )
                success += 1
            except telegram.error.Forbidden:
                blocked += 1
            except Exception as e:
                logger.error(f"Broadcast to {target_user_id} failed: {e}")
                failed += 1
            
            # Update status every 50 users
            if (i + 1) % 50 == 0:
                try:
                    await status.edit_text(
                        f"📤 Broadcasting... {i + 1}/{len(users)}\n"
                        f"✅ Success: {success} | ❌ Failed: {failed}"
                    )
                except:
                    pass
            
            # Small delay to avoid flood
            await asyncio.sleep(0.05)
        
        await status.edit_text(
            f"✅ **Broadcast Complete!**\n\n"
            f"📊 Total Users: {len(users)}\n"
            f"✅ Success: {success}\n"
            f"🚫 Blocked: {blocked}\n"
            f"❌ Failed: {failed}",
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logger.error(f"Broadcast error: {e}")
        await update.message.reply_text(f"❌ Broadcast failed: {e}")
    finally:
        if conn:
            release_db(conn)

async def admin_add_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a new movie (Admin only)"""
    if update.effective_user.id != ADMIN_USER_ID:
        return
    
    if len(context.args) < 2:
        await update.message.reply_text(
            "Usage: /addmovie \"Movie Title\" URL_or_FileID\n\n"
            "Example: /addmovie \"Avengers Endgame\" https://t.me/channel/123"
        )
        return
    
    # Parse arguments
    text = ' '.join(context.args)
    match = re.match(r'"([^"]+)"\s+(.+)', text)
    
    if not match:
        await update.message.reply_text("❌ Invalid format! Use quotes for title.")
        return
    
    title = match.group(1)
    url_or_file = match.group(2).strip()
    
    conn = None
    try:
        conn = get_db()
        if not conn:
            await update.message.reply_text("❌ Database connection failed!")
            return
        
        cur = conn.cursor()
        
        # Determine if it's a file_id or URL
        if url_or_file.startswith('http') or url_or_file.startswith('t.me'):
            cur.execute(
                "INSERT INTO movies (title, url) VALUES (%s, %s) RETURNING id",
                (title, url_or_file)
            )
        else:
            cur.execute(
                "INSERT INTO movies (title, file_id) VALUES (%s, %s) RETURNING id",
                (title, url_or_file)
            )
        
        movie_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        
        await update.message.reply_text(
            f"✅ Movie added successfully!\n\n"
            f"🆔 ID: {movie_id}\n"
            f"🎬 Title: {title}"
        )
        
    except Exception as e:
        logger.error(f"Add movie error: {e}")
        await update.message.reply_text(f"❌ Error adding movie: {e}")
    finally:
        if conn:
            release_db(conn)

# ==================== ERROR HANDLER ====================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors"""
    logger.error(f"Exception while handling an update: {context.error}")
    
    # Log the full traceback
    import traceback
    tb_string = ''.join(traceback.format_exception(None, context.error, context.error.__traceback__))
    logger.error(f"Traceback:\n{tb_string}")
    
    # Try to notify user
    if update and isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "❌ Something went wrong! Please try again later."
            )
        except:
            pass

# ==================== CANCEL HANDLER ====================
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancel current operation"""
    await update.message.reply_text("❌ Operation cancelled.")
    context.user_data.clear()
    return ConversationHandler.END

# ==================== HELP COMMAND ====================
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show help message"""
    help_text = (
        "🎬 **Ur Movie Bot Help**\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "**🔍 How to Search:**\n"
        "Just type any movie name!\n"
        "Example: `Avengers`\n\n"
        "**📱 Features:**\n"
        "• Fast search with fuzzy matching\n"
        "• Multiple quality options\n"
        "• Auto-delete for privacy\n"
        "• Support for Movies & Series\n\n"
        "**⚡ Commands:**\n"
        "/start - Start bot\n"
        "/help - Show this message\n\n"
        "**📢 Join Us:**\n"
        f"• Channel: {CHANNEL_URL}\n"
        f"• Group: {GROUP_URL}\n\n"
        "**💡 Tips:**\n"
        "• Join both Channel & Group for access\n"
        "• Files auto-delete in 60 seconds\n"
        "• Forward to save permanently\n\n"
        "Enjoy! 🍿"
    )
    
    await update.message.reply_text(help_text, parse_mode='Markdown')

# ==================== GROUP MENTION HANDLER ====================
# 👇👇👇 IS FUNCTION KO REPLACE KARO (Line ~1665) 👇👇👇

async def handle_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle messages in groups using FAST SQL Search.
    Agar movie database me hai to reply karega, nahi to chup rahega.
    """
    # 1. Basic Validation
    if not update.message or not update.message.text:
        return
    
    text = update.message.text.strip()
    
    # 2. Commands ignore karo
    if text.startswith('/'):
        return
    
    # 3. Bahut chote words ignore karo
    if len(text) < 2:
        return

    # 4. 🚀 FAST SEARCH CALL (Sirf SQL Check - No Python Lag)
    movies = get_movies_fast_sql(text, limit=5)

    if not movies:
        # 🤫 Agar movie nahi mili, to YAHIN RUK JAO.
        # Bot kuch reply nahi karega, group me shanti rahegi.
        return

    # 5. Results handling - FilmFyBox Style Keyboard
    # Hum seedha list dikhayenge, user click karega to Quality Menu khulega
    keyboard = movie_list_keyboard(movies, page=0)
    
    # Reply to user
    msg = await update.message.reply_text(
        f"🎬 **Found {len(movies)} results for '{text}'**\n👇 Select movie:",
        reply_markup=keyboard,
        parse_mode='Markdown'
    )
    
    # Auto-delete (60 Seconds)
    schedule_delete(context, update.effective_chat.id, [msg.message_id], delay=60)
# ==================== MAIN BOT SETUP ====================
def main():
    """Start the bot"""
    
    # Initialize database pool
    try:
        init_db_pool()
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        sys.exit(1)
    
    # Create application with proper settings
    application = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .read_timeout(30)
        .write_timeout(30)
        .connect_timeout(30)
        .pool_timeout(30)
        .build()
    )
    
    # Conversation handler for main flow
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('start', start),
            MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, handle_message)
        ],
        states={
            MAIN_MENU: [
                MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, handle_message),
                CallbackQueryHandler(handle_callback)
            ],
            SEARCHING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, handle_message),
                CallbackQueryHandler(handle_callback)
            ]
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            CommandHandler('start', start)
        ],
        allow_reentry=True,
        per_chat=True,
        per_user=True
    )
    
    # Add handlers in correct order
    application.add_handler(conv_handler)
    
    # Help command
    application.add_handler(CommandHandler('help', help_command))
    
    # Admin commands
    application.add_handler(CommandHandler('stats', admin_stats))
    application.add_handler(CommandHandler('clearcache', admin_clear_cache))
    application.add_handler(CommandHandler('checkuser', admin_check_user))
    application.add_handler(CommandHandler('broadcast', admin_broadcast))
    application.add_handler(CommandHandler('addmovie', admin_add_movie))
    
    # Group mention handler
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS,
        handle_group_message
    ))
    
    # Standalone callback handler for non-conversation callbacks
    application.add_handler(CallbackQueryHandler(handle_callback))
    
    # Error handler
    application.add_error_handler(error_handler)
    
    # Log startup
    logger.info("="*50)
    logger.info("🎬 Ur Movie Bot Starting...")
    logger.info(f"📢 Channel: {REQUIRED_CHANNEL}")
    logger.info(f"💬 Group: {REQUIRED_GROUP}")
    logger.info(f"⏰ Auto-delete: {AUTO_DELETE_DELAY}s")
    logger.info(f"🔄 Cache time: {VERIFICATION_CACHE_TIME}s")
    logger.info("="*50)
    
    # Start polling
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True
    )

# ==================== FLASK + BOT RUNNER ====================
def run_flask():
    """Run Flask server"""
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, threaded=True)

def cleanup():
    """Cleanup resources on exit"""
    global db_pool
    if db_pool:
        try:
            db_pool.closeall()
            logger.info("Database pool closed")
        except:
            pass

if __name__ == '__main__':
    try:
        # Check if running on server (has PORT env variable)
        if os.environ.get('PORT'):
            # Run Flask in separate thread
            flask_thread = threading.Thread(target=run_flask, daemon=True)
            flask_thread.start()
            logger.info(f"Flask server started on port {os.environ.get('PORT')}")
        
        # Start bot
        main()
        
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cleanup()
        sys.exit(0)
