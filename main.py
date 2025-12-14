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
AUTO_DELETE_DELAY = 60  

# Verified users cache  
verified_users = {}  
VERIFICATION_CACHE_TIME = 3600  # 1 Hour  

# Validate  
if not TELEGRAM_BOT_TOKEN:  
    raise ValueError("TELEGRAM_BOT_TOKEN is not set")  
if not DATABASE_URL:  
    raise ValueError("DATABASE_URL is not set")  

# ==================== MEMBERSHIP CHECK (FIXED) ====================  
async def is_user_member(context, user_id, force_fresh=False) -> dict:  
    """  
    Smart Check: Pehle Memory check karega, fir API.  
    force_fresh=True tab use hoga jab user 'Verify' button dabayega.  
    """  
    current_time = datetime.now()  
    
    # 1. Check Memory (Cache) first - BUT ONLY IF NOT FORCED  
    if not force_fresh and user_id in verified_users:  
        last_checked, cached_result = verified_users[user_id]  
        # Agar 1 ghante se kam hua hai, to wahi result use karo  
        if (current_time - last_checked).total_seconds() < VERIFICATION_CACHE_TIME:  
            logger.info(f"Cache hit for user {user_id}")  
            return cached_result  
    
    # 2. Result structure  
    result = {  
        'is_member': False,  
        'channel': False,  
        'group': False,  
        'error': None  
    }  
    
    try:  
        # Check Channel  
        try:  
            channel_member = await context.bot.get_chat_member(chat_id=REQUIRED_CHANNEL, user_id=user_id)  
            result['channel'] = channel_member.status in [  
                ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER,   
                'member', 'administrator', 'creator', 'restricted'  
            ]  
            logger.info(f"User {user_id} channel status: {channel_member.status}")  
        except telegram.error.BadRequest as e:  
            logger.warning(f"Channel check BadRequest for {user_id}: {e}")  
            result['channel'] = False  
        except telegram.error.Forbidden as e:  
            result['error'] = "Bot Channel me Admin nahi hai!"  
            logger.error(f"Channel Forbidden: {e}")  
            return result  
            
        # Check Group  
        try:  
            group_member = await context.bot.get_chat_member(chat_id=REQUIRED_GROUP, user_id=user_id)  
            result['group'] = group_member.status in [  
                ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER,  
                'member', 'administrator', 'creator', 'restricted'  
            ]  
            logger.info(f"User {user_id} group status: {group_member.status}")  
        except telegram.error.BadRequest as e:  
            logger.warning(f"Group check BadRequest for {user_id}: {e}")  
            result['group'] = False  
        except telegram.error.Forbidden as e:  
            result['error'] = "Bot Group me Admin nahi hai!"  
            logger.error(f"Group Forbidden: {e}")  
            return result  
        
        # Both must be True  
        result['is_member'] = result['channel'] and result['group']  
        
        # 3. Save to Memory - ALWAYS UPDATE CACHE WITH FRESH DATA  
        # Chahe member ho ya nahi, cache me update kar do  
        verified_users[user_id] = (current_time, result)  
        logger.info(f"Cache updated for user {user_id}: is_member={result['is_member']}")  
        
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
    patterns = [r'S\d+\sE\d+', r'Season\s\d+', r'Episode\s*\d+']  
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
    
    # ============ MEMBERSHIP CHECK (NOT FORCED - can use cache) ============  
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
    
    # ============ VERIFY BUTTON (MAIN FIX) ============  
    if data == "verify":  
        # FORCE FRESH CHECK - Ignore cache completely  
        check = await is_user_member(context, user_id, force_fresh=True)  
        
        if check['is_member']:  
            await query.edit_message_text(  
                "✅ **Verified Successfully!**\n\n"  
                "Ab aap koi bhi movie search kar sakte hain! 🎬\n"  
                "Bas movie ka naam likhiye 👇",  
                parse_mode='Markdown'  
            )  
            # Delete verification message after 10 seconds  
            schedule_delete(context, chat_id, [query.message.message_id], 10)  
        else:  
            # Agar abhi bhi join nahi kiya  
            try:  
                await query.edit_message_text(  
                    get_join_message(check['channel'], check['group']),  
                    reply_markup=get_join_keyboard(),  
                    parse_mode='Markdown'  
                )  
            except telegram.error.BadRequest:  
                # Message same hai, popup show karo  
                await query.answer("❌ Abhi bhi join nahi kiya! Pehle dono join karo.", show_alert=True)  
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
            
            # Create dummy update for send_movie  
            class DummyUpdate:  
                def __init__(self, user, chat):  
                    self.effective_user = user  
                    self.effective_chat = chat  
            
            dummy_chat = type('obj', (object,), {'id': chat_id})()  
            dummy = DummyUpdate(query.from_user, dummy_chat)  
            
            await send_movie(dummy, context, movie[0], movie[1], movie[2], movie[3])  
        
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
        
        # Create dummy update  
        class DummyUpdate:  
            def __init__(self, user, chat):  
                self.effective_user = user  
                self.effective_chat = chat  
        
        dummy_chat = type('obj', (object,), {'id': chat_id})()  
        dummy = DummyUpdate(query.from_user, dummy_chat)  
        
        await send_movie(dummy, context, movie_id, title, url, file_id)  
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
        
        # Check membership (fresh check recommended for group actions)
check = await is_user_member(context, user_id, force_fresh=True)
if not check['is_member']:
    await query.edit_message_text(
        get_join_message(check['channel'], check['group']),
        reply_markup=get_join_keyboard(),
        parse_mode='Markdown'
    )
    return  
        
        # User verified - get movie details  
        conn = get_db()  
        cur = conn.cursor()  
        cur.execute("SELECT title, url, file_id FROM movies WHERE id = %s", (movie_id,))  
        movie = cur.fetchone()  
        cur.close()  
        conn.close()  
        
        if not movie:  
            await query.edit_message_text("❌ Movie not found!")  
            return  
        
        await query.edit_message_text(f"📤 Sending **{movie[0]}** in DM...", parse_mode='Markdown')  
        
        # Send in private chat  
        try:  
            await context.bot.send_message(  
                chat_id=user_id,  
                text=f"🎬 {movie[0]}\n\nYe raha tumhara movie!"  
            )  
            await send_movie(query, context, movie_id, movie[0], movie[1], movie[2])  
        except Exception as e:  
            logger.error(f"DM send error: {e}")  
            await query.message.reply_text("❌ Tumhara DM band hai! Pehle /start karo private me.")  
        
        return  

# ==================== ADMIN COMMANDS ====================  
async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):  
    """Get bot statistics (Admin only)"""  
    if update.effective_user.id != ADMIN_USER_ID:  
        return  
    
    try:  
        conn = get_db()  
        cur = conn.cursor()  
        
        cur.execute("SELECT COUNT(*) FROM movies")  
        total_movies = cur.fetchone()[0]  
        
        cur.execute("SELECT COUNT(*) FROM movie_files")  
        total_files = cur.fetchone()[0]  
        
        cur.close()  
        conn.close()  
        
        stats = (  
            f"📊 **Bot Statistics**\n"  
            f"━━━━━━━━━━━━━━━━━━━━\n"  
            f"🎬 Total Movies: {total_movies}\n"  
            f"📁 Total Files: {total_files}\n"  
            f"👥 Cached Users: {len(verified_users)}\n"  
        )  
        
        await update.message.reply_text(stats, parse_mode='Markdown')  
        
    except Exception as e:  
        logger.error(f"Stats error: {e}")  
        await update.message.reply_text("❌ Error getting stats!")  

async def admin_clear_cache(update: Update, context: ContextTypes.DEFAULT_TYPE):  
    """Clear user verification cache (Admin only)"""  
    if update.effective_user.id != ADMIN_USER_ID:  
        return  
    
    verified_users.clear()  
    await update.message.reply_text("✅ Cache cleared!")  

async def admin_check_user(update: Update, context: ContextTypes.DEFAULT_TYPE):  
    """Check specific user membership (Admin only)"""  
    if update.effective_user.id != ADMIN_USER_ID:  
        return  
    
    if not context.args:  
        await update.message.reply_text("Usage: /checkuser USER_ID")  
        return  
    
    try:  
        user_id = int(context.args[0])  
        check = await is_user_member(context, user_id, force_fresh=True)  
        
        msg = (  
            f"👤 User {user_id}\n"  
            f"━━━━━━━━━━━━━━━━━━━━\n"  
            f"📢 Channel: {'✅' if check['channel'] else '❌'}\n"  
            f"💬 Group: {'✅' if check['group'] else '❌'}\n"
                        f"✅ Is Member: {'Yes' if check['is_member'] else 'No'}\n"  
        )  
        
        if check['error']:  
            msg += f"⚠️ Error: {check['error']}"  
        
        await update.message.reply_text(msg)  
        
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
        await update.message.reply_text("Usage: /broadcast Your message here")  
        return  
    
    message = ' '.join(context.args)  
    
    try:  
        conn = get_db()  
        cur = conn.cursor()  
        cur.execute("SELECT DISTINCT user_id FROM user_activity")  
        users = cur.fetchall()  
        cur.close()  
        conn.close()  
        
        success = 0  
        failed = 0  
        
        status = await update.message.reply_text("📤 Broadcasting...")  
        
        for (user_id,) in users:  
            try:  
                await context.bot.send_message(chat_id=user_id, text=message, parse_mode='Markdown')  
                success += 1  
            except Exception as e:  
                logger.error(f"Broadcast to {user_id} failed: {e}")  
                failed += 1  
            
            # Small delay to avoid flood  
            await asyncio.sleep(0.05)  
        
        await status.edit_text(  
            f"✅ Broadcast Complete!\n\n"  
            f"✅ Success: {success}\n"  
            f"❌ Failed: {failed}"  
        )  
        
    except Exception as e:  
        logger.error(f"Broadcast error: {e}")  
        await update.message.reply_text("❌ Broadcast failed!")  

# ==================== ERROR HANDLER ====================  
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):  
    """Handle errors"""  
    logger.error(f"Update {update} caused error {context.error}")  
    
    if update and update.effective_message:  
        try:  
            await update.effective_message.reply_text(  
                "❌ Kuch galat ho gaya! Bot admin ko contact karo."  
            )  
        except:  
            pass  

# ==================== CANCEL HANDLER ====================  
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):  
    """Cancel current operation"""  
    await update.message.reply_text("❌ Cancelled")  
    return ConversationHandler.END  

# ==================== HELP COMMAND ====================  
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):  
    """Show help message"""  
    help_text = (  
        "🎬 **Ur Movie Bot Help**\n"  
        "━━━━━━━━━━━━━━━━━━━━\n\n"  
        "**🔍 Movie Search:**\n"  
        "Bas movie ka naam type karo!\n"  
        "Example: `Avengers`\n\n"  
        "**📱 Features:**\n"  
        "• Fast search with fuzzy matching\n"  
        "• Multiple quality options\n"  
        "• Auto-delete for privacy\n"  
        "• Support for Movies & Series\n\n"  
        "**⚡ Commands:**\n"  
        "/start - Start bot\n"  
        "/help - Show this message\n\n"  
        "**📢 Channels:**\n"  
        f"Channel: {CHANNEL_URL}\n"  
                f"Group: {GROUP_URL}\n\n"  
        "**💡 Tips:**\n"  
        "• Join both Channel & Group for access\n"  
        "• Files auto-delete in 60 seconds\n"  
        "• Forward to save permanently\n\n"  
        "Enjoy! 🍿"  
    )  
    
    await update.message.reply_text(help_text, parse_mode='Markdown')  

# ==================== GROUP MENTION HANDLER ====================  
async def handle_group_mention(update: Update, context: ContextTypes.DEFAULT_TYPE):  
    """Handle when bot is mentioned in group"""  
    if not update.message or not update.message.text:  
        return  
    
    text = update.message.text  
    bot_username = (await context.bot.get_me()).username  
    
    # Check if bot is mentioned  
    if f"@{bot_username}" not in text:  
        return  
    
    # Extract movie name (remove bot mention)  
    query = text.replace(f"@{bot_username}", "").strip()  
    
    if len(query) < 2:  
        await update.message.reply_text(  
            "🎬 Movie ka naam mention karo!\n"  
            f"Example: `@{bot_username} Avengers`",  
            parse_mode='Markdown'  
        )  
        return  
    
    user_id = update.effective_user.id  
    
    # Search movies  
    movies = search_movies(query)  
    
    if not movies:  
        await update.message.reply_text(f"😕 `{query}` nahi mila!", parse_mode='Markdown')  
        return  
    
    if len(movies) == 1:  
        # Single result - send button to get in DM  
        movie = movies[0]  
        keyboard = InlineKeyboardMarkup([[  
            InlineKeyboardButton(  
                "📥 Get in DM",  
                callback_data=f"g_{movie[0]}_{user_id}"  
            )  
        ]])  
        
        await update.message.reply_text(  
            f"🎬 **{movie[1]}**\n\n"  
            f"Click button to get in your DM! 👇",  
            reply_markup=keyboard,  
            parse_mode='Markdown'  
        )  
    else:  
        # Multiple results - send deep link  
        bot_username = (await context.bot.get_me()).username  
        deep_link = f"https://t.me/{bot_username}?start=q_{query.replace(' ', '_')}"  
        
        keyboard = InlineKeyboardMarkup([[  
            InlineKeyboardButton(  
                f"📋 View {len(movies)} Results",  
                url=deep_link  
            )  
        ]])  
        
        await update.message.reply_text(  
            f"🔍 Found **{len(movies)} movies** for `{query}`\n\n"  
            f"Click button to select! 👇",  
            reply_markup=keyboard,  
            parse_mode='Markdown'  
        )  

# ==================== MAIN BOT SETUP ====================  
def main():  
    """Start the bot"""  
    
    # Create application  
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()  
    
    # Conversation handler  
    conv_handler = ConversationHandler(  
        entry_points=[  
            CommandHandler('start', start),  
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)  
        ],  
        states={  
            MAIN_MENU: [  
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message),  
                CallbackQueryHandler(handle_callback)  
            ],  
            SEARCHING: [  
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message),  
                CallbackQueryHandler(handle_callback)  
            ]  
        },  
        fallbacks=[CommandHandler('cancel', cancel)],  
        allow_reentry=True  
    )  
    
        # Add handlers
    application.add_handler(conv_handler)
    
    # Admin commands
    application.add_handler(CommandHandler('stats', admin_stats))
    application.add_handler(CommandHandler('clearcache', admin_clear_cache))
    application.add_handler(CommandHandler('checkuser', admin_check_user))
    application.add_handler(CommandHandler('broadcast', admin_broadcast))
    
    # Help command
    application.add_handler(CommandHandler('help', help_command))
    
    # Group mention handler (when bot is tagged)
    application.add_handler(MessageHandler(
        filters.TEXT & filters.ChatType.GROUPS,
        handle_group_mention
    ))
    
    # Callback query handler (for buttons)
    application.add_handler(CallbackQueryHandler(handle_callback))
    
    # Error handler
    application.add_error_handler(error_handler)
    
    # Log
    logger.info("Bot started successfully! 🎬")
    
    # Start polling
    application.run_polling(allowed_updates=Update.ALL_TYPES)

# ==================== FLASK + BOT RUNNER ====================
def run_flask():
    """Run Flask server"""
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

if __name__ == '__main__':
    # Check if running on server (has PORT env variable)
    if os.environ.get('PORT'):
        # Run Flask in separate thread
        flask_thread = threading.Thread(target=run_flask, daemon=True)
        flask_thread.start()
        logger.info("Flask server started in background")
    
    # Start bot
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        sys.exit(1)
