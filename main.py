# -*- coding: utf-8 -*-

import os
import threading
import asyncio
import logging
import re
import sys
from datetime import datetime, timedelta

import telegram
import psycopg2
from typing import Optional
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

# Force Join Config - APNE CHANNEL/GROUP ID DAALO
REQUIRED_CHANNEL = os.environ.get('REQUIRED_CHANNEL_ID', '@filmfybox')
REQUIRED_GROUP = os.environ.get('REQUIRED_GROUP_ID', '@Filmfybox002')
CHANNEL_URL = 'https://t.me/FilmFyBoxMoviesHD'
GROUP_URL = 'https://t.me/FlimfyBox'

# Auto delete delay
AUTO_DELETE_DELAY = int(os.environ.get('AUTO_DELETE_DELAY', '300'))

# Validate
if not TELEGRAM_BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN is not set")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set")

# ==================== SIMPLE MEMBERSHIP CHECK ====================

async def is_user_member(context, user_id) -> dict:
    """
    Check if user is member of both channel and group
    Returns: {'is_member': bool, 'channel': bool, 'group': bool, 'error': str|None}
    """
    result = {
        'is_member': False,
        'channel': False,
        'group': False,
        'error': None
    }
    
    try:
        # Check Channel
        try:
            channel_member = await context.bot.get_chat_member(
                chat_id=REQUIRED_CHANNEL, 
                user_id=user_id
            )
            result['channel'] = channel_member.status in [
                ChatMember.MEMBER, 
                ChatMember.ADMINISTRATOR, 
                ChatMember.OWNER,
                'member', 'administrator', 'creator'
            ]
        except telegram.error.BadRequest as e:
            logger.warning(f"Channel check failed for {user_id}: {e}")
            result['channel'] = False
        except telegram.error.Forbidden:
            result['error'] = "Bot is not admin in channel!"
            logger.error("Bot is not admin in required channel!")
            return result
            
        # Check Group
        try:
            group_member = await context.bot.get_chat_member(
                chat_id=REQUIRED_GROUP, 
                user_id=user_id
            )
            result['group'] = group_member.status in [
                ChatMember.MEMBER, 
                ChatMember.ADMINISTRATOR, 
                ChatMember.OWNER,
                'member', 'administrator', 'creator'
            ]
        except telegram.error.BadRequest as e:
            logger.warning(f"Group check failed for {user_id}: {e}")
            result['group'] = False
        except telegram.error.Forbidden:
            result['error'] = "Bot is not admin in group!"
            logger.error("Bot is not admin in required group!")
            return result
        
        # Both must be True
        result['is_member'] = result['channel'] and result['group']
        
        return result
        
    except Exception as e:
        logger.error(f"Membership check error for {user_id}: {e}")
        result['error'] = str(e)
        return result

def get_join_keyboard():
    """Join buttons keyboard"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📢 Join Channel", url=CHANNEL_URL),
            InlineKeyboardButton("💬 Join Group", url=GROUP_URL)
        ],
        [InlineKeyboardButton("✅ Joined Both - Verify", callback_data="verify")]
    ])

def get_join_message(channel_status, group_status):
    """Generate join message based on what's missing"""
    if not channel_status and not group_status:
        missing = "Channel and Group dono"
    elif not channel_status:
        missing = "Channel"
    else:
        missing = "Group"
    
    return (
        f"🚫 **Access Denied**\n\n"
        f"Aapne {missing} join nahi kiya hai!\n\n"
        f"📢 Channel: {'✅' if channel_status else '❌'}\n"
        f"💬 Group: {'✅' if group_status else '❌'}\n\n"
        f"Dono join karo, phir **Verify** button dabao 👇"
    )

# ==================== DATABASE ====================

def get_db():
    """Get database connection"""
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        logger.error(f"DB connection error: {e}")
        return None

def search_movies(query, limit=10):
    """Search movies in database"""
    conn = None
    try:
        conn = get_db()
        if not conn:
            return []
        
        cur = conn.cursor()
        
        # Exact match first
        cur.execute(
            "SELECT id, title, url, file_id FROM movies WHERE LOWER(title) LIKE LOWER(%s) ORDER BY title LIMIT %s",
            (f'%{query}%', limit)
        )
        results = cur.fetchall()
        
        if results:
            cur.close()
            return results
        
        # Fuzzy search
        cur.execute("SELECT id, title, url, file_id FROM movies")
        all_movies = cur.fetchall()
        cur.close()
        
        if not all_movies:
            return []
        
        titles = [m[1] for m in all_movies]
        matches = process.extract(query, titles, scorer=fuzz.token_sort_ratio, limit=limit)
        
        filtered = []
        for match in matches:
            if match[1] >= 60:
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
            conn.close()

def get_movie_qualities(movie_id):
    """Get all qualities for a movie"""
    conn = None
    try:
        conn = get_db()
        if not conn:
            return []
        
        cur = conn.cursor()
        cur.execute("""
            SELECT quality, url, file_id, file_size
            FROM movie_files
            WHERE movie_id = %s AND (url IS NOT NULL OR file_id IS NOT NULL)
            ORDER BY CASE quality
                WHEN '4K' THEN 1
                WHEN 'HD Quality' THEN 2
                WHEN 'Standard Quality' THEN 3
                ELSE 4
            END
        """, (movie_id,))
        results = cur.fetchall()
        cur.close()
        return results
    except Exception as e:
        logger.error(f"Quality fetch error: {e}")
        return []
    finally:
        if conn:
            conn.close()

# ==================== HELPER FUNCTIONS ====================

def is_series(title):
    """Check if title is a series"""
    patterns = [r'S\d+\s*E\d+', r'Season\s*\d+', r'Episode\s*\d+']
    return any(re.search(p, title, re.IGNORECASE) for p in patterns)

async def auto_delete(context, chat_id, message_ids, delay=60):
    """Delete messages after delay"""
    await asyncio.sleep(delay)
    for msg_id in message_ids:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except:
            pass

def schedule_delete(context, chat_id, message_ids, delay=None):
    """Schedule auto deletion"""
    if delay is None:
        delay = AUTO_DELETE_DELAY
    asyncio.create_task(auto_delete(context, chat_id, message_ids, delay))

# ==================== KEYBOARDS ====================

def movie_list_keyboard(movies, page=0, per_page=5):
    """Create movie selection keyboard"""
    start = page * per_page
    end = start + per_page
    current = movies[start:end]
    
    keyboard = []
    for movie_id, title, url, file_id in current:
        emoji = "📺" if is_series(title) else "🎬"
        text = f"{emoji} {title[:35]}..." if len(title) > 35 else f"{emoji} {title}"
        keyboard.append([InlineKeyboardButton(text, callback_data=f"m_{movie_id}")])
    
    # Navigation
    nav = []
    total_pages = (len(movies) + per_page - 1) // per_page
    
    if page > 0:
        nav.append(InlineKeyboardButton("◀️", callback_data=f"p_{page-1}"))
    if total_pages > 1:
        nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
    if end < len(movies):
        nav.append(InlineKeyboardButton("▶️", callback_data=f"p_{page+1}"))
    
    if nav:
        keyboard.append(nav)
    
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
    
    return InlineKeyboardMarkup(keyboard)

def quality_keyboard(movie_id, qualities):
    """Create quality selection keyboard"""
    icons = {'4K': '💎', 'HD Quality': '🔷', 'Standard Quality': '🟢', 'Low Quality': '🟡'}
    
    keyboard = []
    for quality, url, file_id, size in qualities:
        icon = icons.get(quality, '🎬')
        size_text = f" ({size})" if size else ""
        keyboard.append([InlineKeyboardButton(
            f"{icon} {quality}{size_text}",
            callback_data=f"q_{movie_id}_{quality}"
        )])
    
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
    
    return InlineKeyboardMarkup(keyboard)

# ==================== SEND MOVIE ====================

async def send_movie(update, context, movie_id, title, url=None, file_id=None):
    """Send movie file to user"""
    chat_id = update.effective_chat.id
    
    # If no direct file, check qualities
    if not url and not file_id:
        qualities = get_movie_qualities(movie_id)
        if qualities:
            context.user_data['movie'] = {'id': movie_id, 'title': title, 'qualities': qualities}
            msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"✅ **{title}**\n\n🎯 Quality choose karo:",
                reply_markup=quality_keyboard(movie_id, qualities),
                parse_mode='Markdown'
            )
            schedule_delete(context, chat_id, [msg.message_id], 300)
            return
    
    try:
        # Warning message
        warn = await context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ **60 seconds me delete ho jayega!**\n📤 Forward karke save karo!",
            parse_mode='Markdown'
        )
        
        caption = (
            f"🎬 **{title}**\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"📢 [@FilmFyBox]({CHANNEL_URL})\n"
            f"⏰ Auto-delete: 60 sec"
        )
        
        buttons = InlineKeyboardMarkup([[
            InlineKeyboardButton("📢 Channel", url=CHANNEL_URL),
            InlineKeyboardButton("💬 Group", url=GROUP_URL)
        ]])
        
        sent = None
        
        if file_id:
            sent = await context.bot.send_document(
                chat_id=chat_id,
                document=file_id,
                caption=caption,
                parse_mode='Markdown',
                reply_markup=buttons
            )
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
                    reply_markup=buttons
                )
        elif url:
            sent = await context.bot.send_message(
                chat_id=chat_id,
                text=f"🎬 **{title}**\n\n🔗 [Download]({url})",
                parse_mode='Markdown',
                reply_markup=buttons
            )
        
        if sent:
            await auto_delete(context, chat_id, [warn.message_id, sent.message_id], 60)
            
    except Exception as e:
        logger.error(f"Send movie error: {e}")
        await context.bot.send_message(chat_id=chat_id, text="❌ File send nahi ho paya!")

# ==================== HANDLERS ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command"""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    # Handle deep links
    if context.args:
        arg = context.args[0]
        
        # Movie link: /start movie_123
        if arg.startswith("movie_"):
            # Check membership FIRST
            check = await is_user_member(context, user_id)
            
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
                conn = get_db()
                cur = conn.cursor()
                cur.execute("SELECT title, url, file_id FROM movies WHERE id = %s", (movie_id,))
                movie = cur.fetchone()
                cur.close()
                conn.close()
                
                if movie:
                    await send_movie(update, context, movie_id, movie[0], movie[1], movie[2])
                else:
                    await update.message.reply_text("❌ Movie not found!")
            except Exception as e:
                logger.error(f"Deep link error: {e}")
            
            return MAIN_MENU
        
        # Search link: /start q_Movie_Name
        if arg.startswith("q_"):
            query = arg[2:].replace("_", " ")
            
            # Check membership FIRST
            check = await is_user_member(context, user_id)
            
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
        "Movie ya Series ka naam type karo!\n\n"
        "Example: `Avengers Endgame`"
    )
    
    await update.message.reply_text(welcome, reply_markup=keyboard, parse_mode='Markdown')
    return MAIN_MENU

async def process_search(update: Update, context: ContextTypes.DEFAULT_TYPE, query: str = None):
    """Process movie search"""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    if not query:
        if not update.message or not update.message.text:
            return MAIN_MENU
        query = update.message.text.strip()
    
    if len(query) < 2:
        return MAIN_MENU
    
    # ============ MEMBERSHIP CHECK ============
    check = await is_user_member(context, user_id)
    
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
    # ==========================================
    
    # User is member - search movies
    movies = search_movies(query)
    
    if not movies:
        await update.message.reply_text(
            f"😕 `{query}` nahi mila!\n\nKuch aur search karo.",
            parse_mode='Markdown'
        )
        return MAIN_MENU
    
    if len(movies) == 1:
        # Single result - send directly
        m = movies[0]
        await send_movie(update, context, m[0], m[1], m[2], m[3])
        return MAIN_MENU
    
    # Multiple results - show list
    context.user_data['results'] = movies
    context.user_data['query'] = query
    
    await update.message.reply_text(
        f"🔍 **{len(movies)} results** for `{query}`\n\nSelect karo:",
        reply_markup=movie_list_keyboard(movies),
        parse_mode='Markdown'
    )
    
    return MAIN_MENU

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text messages"""
    return await process_search(update, context)

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button callbacks"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    chat_id = query.message.chat.id
    data = query.data
    
    # ============ VERIFY BUTTON ============
    if data == "verify":
        check = await is_user_member(context, user_id)
        
        if check['is_member']:
            await query.edit_message_text(
                "✅ **Verified!**\n\n"
                "Ab koi bhi movie search karo! 🎬",
                parse_mode='Markdown'
            )
            schedule_delete(context, chat_id, [query.message.message_id], 30)
        else:
            await query.edit_message_text(
                get_join_message(check['channel'], check['group']),
                reply_markup=get_join_keyboard(),
                parse_mode='Markdown'
            )
        return
    
    # ============ MOVIE SELECTION ============
    if data.startswith("m_"):
        # Check membership
        check = await is_user_member(context, user_id)
        if not check['is_member']:
            await query.edit_message_text(
                get_join_message(check['channel'], check['group']),
                reply_markup=get_join_keyboard(),
                parse_mode='Markdown'
            )
            return
        
        movie_id = int(data[2:])
        
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT id, title, url, file_id FROM movies WHERE id = %s", (movie_id,))
        movie = cur.fetchone()
        cur.close()
        conn.close()
        
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
                f"✅ **{movie[1]}**\n\n🎯 Quality choose karo:",
                reply_markup=quality_keyboard(movie_id, qualities),
                parse_mode='Markdown'
            )
        else:
            await query.edit_message_text(f"📤 Sending **{movie[1]}**...", parse_mode='Markdown')
            await send_movie(update, context, movie[0], movie[1], movie[2], movie[3])
        
        return
    
    # ============ QUALITY SELECTION ============
    if data.startswith("q_"):
        # Check membership
        check = await is_user_member(context, user_id)
        if not check['is_member']:
            await query.edit_message_text(
                get_join_message(check['channel'], check['group']),
                reply_markup=get_join_keyboard(),
                parse_mode='Markdown'
            )
            return
        
        parts = data.split("_")
        movie_id = int(parts[1])
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
        
        await query.edit_message_text(f"📤 Sending **{title}**...", parse_mode='Markdown')
        await send_movie(update, context, movie_id, title, url, file_id)
        return
    
    # ============ PAGINATION ============
    if data.startswith("p_"):
        page = int(data[2:])
        movies = context.user_data.get('results', [])
        query_text = context.user_data.get('query', 'Search')
        
        if movies:
            await query.edit_message_text(
                f"🔍 **{len(movies)} results** for `{query_text}`\n\nSelect karo:",
                reply_markup=movie_list_keyboard(movies, page),
                parse_mode='Markdown'
            )
        return
    
    # ============ CANCEL ============
    if data == "cancel":
        await query.edit_message_text("❌ Cancelled")
        schedule_delete(context, chat_id, [query.message.message_id], 5)
        return
    
    # ============ GROUP GET ============
    if data.startswith("g_"):
        parts = data.split("_")
        movie_id = int(parts[1])
        original_user = int(parts[2])
        
        if user_id != original_user:
            await query.answer("❌ Ye button tumhare liye nahi hai!", show_alert=True)
            return
        
        # Check membership
        check = await is_user_member(context, user_id)
        if not check['is_member']:
            await query.edit_message_text(
                get_join_message(check['channel'], check['group']),
                reply_markup=get_join_keyboard(),
                parse_mode='Markdown'
            )
            return
        
        # Get movie and send to PM
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute("SELECT title, url, file_id FROM movies WHERE id = %s", (movie_id,))
            movie = cur.fetchone()
            cur.close()
            conn.close()
            
            if movie:
                title, url, file_id = movie
                qualities = get_movie_qualities(movie_id)
                
                if qualities and len(qualities) > 1:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=f"✅ **{title}**\n\n🎯 Quality choose karo:",
                        reply_markup=quality_keyboard(movie_id, qualities),
                        parse_mode='Markdown'
                    )
                else:
                    # Create dummy update for PM
                    class DummyUpdate:
                        def __init__(self, user, chat):
                            self.effective_user = user
                            self.effective_chat = chat
                    
                    dummy_chat = type('obj', (object,), {'id': user_id})()
                    dummy = DummyUpdate(query.from_user, dummy_chat)
                    
                    await send_movie(dummy, context, movie_id, title, url, file_id)
                
                await query.edit_message_text(
                    f"✅ **{title}** sent to your PM! 📩",
                    parse_mode='Markdown'
                )
                schedule_delete(context, chat_id, [query.message.message_id], 60)
                
        except telegram.error.Forbidden:
            await query.edit_message_text(
                "❌ Pehle bot ko /start karo PM me!",
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Group get error: {e}")
            await query.answer("❌ Error!", show_alert=True)
        
        return

async def handle_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle group messages"""
    if not update.message or not update.message.text:
        return
    
    text = update.message.text.strip()
    user = update.effective_user
    
    if len(text) < 4 or text.startswith('/'):
        return
    
    # Search
    movies = search_movies(text, limit=1)
    
    if not movies:
        return
    
    movie_id, title, _, _ = movies[0]
    
    # Check similarity
    score = fuzz.token_sort_ratio(text.lower(), title.lower())
    if score < 85:
        return
    
    emoji = "📺" if is_series(title) else "🎬"
    
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton(f"✅ Get {emoji}", callback_data=f"g_{movie_id}_{user.id}")
    ]])
    
    msg = await update.message.reply_text(
        f"Hey {user.mention_markdown()}!\n\n"
        f"{emoji} **{title}**\n\n"
        f"PM me lene ke liye button dabao 👇",
        reply_markup=keyboard,
        parse_mode='Markdown'
    )
    
    schedule_delete(context, update.effective_chat.id, [msg.message_id], 120)

async def error_handler(update, context):
    """Handle errors"""
    logger.error(f"Error: {context.error}")

# ==================== MAIN ====================

def run_flask():
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)

def main():
    logger.info("🚀 Starting Bot...")
    logger.info(f"Channel: {REQUIRED_CHANNEL}")
    logger.info(f"Group: {REQUIRED_GROUP}")
    
    # Flask thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Bot
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Handlers
    conv = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            MAIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, handle_message)]
        },
        fallbacks=[],
        per_message=False,
        per_chat=True
    )
    
    application.add_handler(conv)
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS, 
        handle_group_message
    ))
    application.add_error_handler(error_handler)
    
    logger.info("✅ Bot Ready!")
    application.run_polling()

if __name__ == '__main__':
    main()
