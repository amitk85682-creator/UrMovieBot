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
AUTO_DELETE_DELAY = int(os.environ.get('AUTO_DELETE_DELAY', '300'))

# Message tracking for deletion
message_tracker: Dict[int, List[int]] = defaultdict(list)

# Validate required environment variables
if not TELEGRAM_BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN is not set.")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set.")

# ==================== UTILITY FUNCTIONS ====================
def preprocess_query(query):
    """Clean and normalize user query, preserving Season/Episode numbers"""
    try:
        # Remove special chars but keep hyphens and spaces
        query = re.sub(r'[^\w\s\-]', ' ', query)
        query = ' '.join(query.split())
        
        stop_words = ['movie', 'film', 'full', 'download', 'watch', 'online', 'free', 'hindi', 'english', 'dual', 'audio']
        words = query.lower().split()
        
        # Keep words that are NOT stop words OR look like S01, E01, Season, Episode
        filtered_words = []
        for w in words:
            if w not in stop_words or re.match(r's\d+|e\d+|season|episode|\d+', w, re.IGNORECASE):
                filtered_words.append(w)
        
        return ' '.join(filtered_words).strip()
    except Exception as e:
        logger.error(f"Error in preprocess_query: {e}")
        return query

async def check_rate_limit(user_id):
    now = datetime.now()
    last_request = user_last_request[user_id]
    if now - last_request < timedelta(seconds=2):
        return False
    user_last_request[user_id] = now
    return True

def is_series(title):
    """Check if title is a series"""
    series_patterns = [
        r'S\d+\s*E\d+', r'Season\s*\d+', r'Episode\s*\d+', 
        r'EP?\s*\d+', r'Part\s*\d+', r'\d+x\d+', r'S\d+'
    ]
    return any(re.search(pattern, title, re.IGNORECASE) for pattern in series_patterns)

def parse_series_info(title):
    """Parse series info"""
    info = {'base_title': title, 'season': None, 'episode': None, 'is_series': False}
    
    # Match S01E01
    match = re.search(r'(.*?)\s*S(\d+)\s*E(\d+)', title, re.IGNORECASE)
    if match:
        info.update({'base_title': match.group(1).strip(), 'season': int(match.group(2)), 'episode': int(match.group(3)), 'is_series': True})
        return info
        
    # Match Season 1 Episode 1
    match = re.search(r'(.*?)\s*Season\s*(\d+)\s*Episode\s*(\d+)', title, re.IGNORECASE)
    if match:
        info.update({'base_title': match.group(1).strip(), 'season': int(match.group(2)), 'episode': int(match.group(3)), 'is_series': True})
        return info

    # Match Season 1
    match = re.search(r'(.*?)\s*Season\s*(\d+)', title, re.IGNORECASE)
    if match:
        info.update({'base_title': match.group(1).strip(), 'season': int(match.group(2)), 'is_series': True})
        return info
        
    return info

# ==================== FORCE JOIN CHECK ====================
async def check_user_membership(context, user_id):
    try:
        channel = await context.bot.get_chat_member(chat_id=REQUIRED_CHANNEL_ID, user_id=user_id)
        group = await context.bot.get_chat_member(chat_id=REQUIRED_GROUP_ID, user_id=user_id)
        return channel.status in ['member', 'administrator', 'creator'] and group.status in ['member', 'administrator', 'creator']
    except:
        return False

def get_force_join_keyboard():
    keyboard = [
        [InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL)],
        [InlineKeyboardButton("💬 Join Group", url=FILMFYBOX_GROUP_URL)],
        [InlineKeyboardButton("✅ I Joined, Check Again", callback_data="check_membership")]
    ]
    return InlineKeyboardMarkup(keyboard)

# ==================== DATABASE & SEARCH ====================
def get_db_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except:
        return None

def get_movies_from_db(user_query, limit=10):
    """
    Updated Logic: 
    1. Prioritize exact/fuzzy matches of the FULL query (e.g. "Landman Season 1 Episode 1").
    2. Only fallback to 'Series Grouping' if the user searches for the base name.
    """
    conn = get_db_connection()
    if not conn: return []
    
    try:
        cur = conn.cursor()
        processed_query = preprocess_query(user_query)
        logger.info(f"Searching for: {processed_query}")

        # 1. Try matching the full query first (Specific Episode Search)
        cur.execute(
            "SELECT id, title, url, file_id FROM movies WHERE LOWER(title) LIKE LOWER(%s) LIMIT %s",
            (f'%{processed_query}%', limit)
        )
        specific_matches = cur.fetchall()
        
        if specific_matches:
            # If we found specific episodes matching the full query, return them as "Movies" (single items)
            # We do NOT organize them into folders if the user asked for a specific one.
            cur.close()
            conn.close()
            return specific_matches

        # 2. Fuzzy match for specific titles
        cur.execute("SELECT id, title, url, file_id FROM movies")
        all_movies = cur.fetchall()
        movie_titles = [m[1] for m in all_movies]
        movie_dict = {m[1]: m for m in all_movies}
        
        matches = process.extract(processed_query, movie_titles, scorer=fuzz.token_sort_ratio, limit=limit*2)
        
        fuzzy_results = []
        for match in matches:
            if len(match) >= 2:
                title, score = match[0], match[1]
                if score >= 70:
                    fuzzy_results.append(movie_dict[title])

        # 3. Intelligent Organization
        # If fuzzy results contain "Season" or "Episode" and user query didn't specify one, organize it.
        # But if user query DID specify "Episode 1", filter for that.
        
        final_results = fuzzy_results[:limit]
        
        cur.close()
        conn.close()
        return final_results

    except Exception as e:
        logger.error(f"DB Error: {e}")
        return []
    finally:
        if conn: conn.close()

def get_all_movie_qualities(movie_id):
    """Get qualities like Bot 2"""
    conn = get_db_connection()
    if not conn: return []
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
                ELSE 6
            END
        """, (movie_id,))
        quality_results = cur.fetchall()
        
        cur.execute("SELECT url FROM movies WHERE id = %s", (movie_id,))
        main_res = cur.fetchone()
        
        final_results = []
        if main_res and main_res[0]:
            final_results.append(('📺 Stream / Watch Online', main_res[0], None, None))
            
        final_results.extend(quality_results)
        return final_results
    except:
        return []
    finally:
        if conn: conn.close()

def get_series_episodes(base_title):
    """Get hierarchy for series"""
    conn = get_db_connection()
    if not conn: return {}
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, title FROM movies WHERE title ILIKE %s ORDER BY title", (f'%{base_title}%',))
        episodes = cur.fetchall()
        
        seasons = defaultdict(list)
        for ep_id, title in episodes:
            if is_series(title):
                info = parse_series_info(title)
                season = info.get('season', 0)
                seasons[season].append({'id': ep_id, 'title': title, 'episode': info.get('episode', 0)})
        
        for s in seasons: seasons[s].sort(key=lambda x: x['episode'])
        return dict(seasons)
    finally:
        if conn: conn.close()

# ==================== KEYBOARDS ====================
def create_selection_keyboard(movies, page=0):
    keyboard = []
    start = page * 5
    end = start + 5
    
    for m in movies[start:end]:
        btn_text = f"🎬 {m[1][:40]}"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"select_{m[0]}")])
        
    nav = []
    if page > 0: nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"page_{page-1}"))
    if end < len(movies): nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"page_{page+1}"))
    if nav: keyboard.append(nav)
    
    return InlineKeyboardMarkup(keyboard)

def create_quality_keyboard(movie_id, qualities):
    keyboard = []
    for q, url, fid, size in qualities:
        size_txt = f" - {size}" if size else ""
        type_txt = "File" if fid else "Link"
        txt = f"🎬 {q}{size_txt} ({type_txt})"
        safe_q = q.replace(' ', '_').replace('/', '-')
        keyboard.append([InlineKeyboardButton(txt, callback_data=f"qual_{movie_id}_{safe_q}")])
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
    return InlineKeyboardMarkup(keyboard)

def create_season_keyboard(seasons, base_title):
    keyboard = []
    for s in sorted(seasons.keys()):
        txt = f"📂 Season {s} ({len(seasons[s])} Eps)"
        keyboard.append([InlineKeyboardButton(txt, callback_data=f"season_{s}_{base_title[:20]}")])
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
    return InlineKeyboardMarkup(keyboard)

def create_episode_keyboard(episodes, season_num):
    keyboard = []
    for ep in episodes:
        txt = f"▶️ Episode {ep['episode']}" if ep['episode'] else ep['title'][:30]
        keyboard.append([InlineKeyboardButton(txt, callback_data=f"select_{ep['id']}")])
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_series")])
    return InlineKeyboardMarkup(keyboard)

# ==================== SENDING LOGIC ====================
async def send_movie_result(update, context, movie_id, title, url, file_id):
    """
    Decides whether to send the file immediately or show quality options.
    Logic taken from Bot 2.
    """
    chat_id = update.effective_chat.id
    
    # 1. Check if membership is required
    if not await check_user_membership(context, update.effective_user.id):
        await context.bot.send_message(chat_id, "🚫 Join Channel & Group first!", reply_markup=get_force_join_keyboard())
        return

    # 2. Check for multiple qualities
    qualities = get_all_movie_qualities(movie_id)
    
    # If we have multiple qualities OR (no direct url/file provided but qualities exist)
    if len(qualities) > 1 or (not url and not file_id and qualities):
        context.user_data['qualities'] = qualities
        context.user_data['movie_title'] = title
        await context.bot.send_message(
            chat_id,
            f"🎬 **{title}**\n\nSelect Quality ⬇️",
            reply_markup=create_quality_keyboard(movie_id, qualities),
            parse_mode='Markdown'
        )
        return

    # 3. If only one quality or direct file, send it
    final_url = url
    final_file_id = file_id
    
    if qualities and not final_url and not final_file_id:
        # Fallback to first quality if main table is empty
        _, final_url, final_file_id, _ = qualities[0]

    await send_file_to_user(context, chat_id, title, final_url, final_file_id)

async def send_file_to_user(context, chat_id, title, url, file_id):
    try:
        caption = f"🎬 **{title}**\n\n⚠️ Auto-delete in 60s.\n📢 @filmfybox"
        
        msg = None
        if file_id:
            msg = await context.bot.send_document(chat_id, file_id, caption=caption, parse_mode='Markdown')
        elif url:
             keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("📺 Watch/Download", url=url)]])
             msg = await context.bot.send_message(chat_id, caption, reply_markup=keyboard, parse_mode='Markdown')
        
        if msg:
            asyncio.create_task(auto_delete(context, chat_id, msg.message_id))
            
    except Exception as e:
        logger.error(f"Send Error: {e}")
        await context.bot.send_message(chat_id, "❌ Error sending file.")

async def auto_delete(context, chat_id, msg_id):
    await asyncio.sleep(60)
    try:
        await context.bot.delete_message(chat_id, msg_id)
    except:
        pass

# ==================== HANDLERS ====================
async def start(update, context):
    # Deep link handling
    if context.args and context.args[0].startswith("movie_"):
        mid = int(context.args[0].split('_')[1])
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT title, url, file_id FROM movies WHERE id=%s", (mid,))
        row = cur.fetchone()
        conn.close()
        if row:
            await send_movie_result(update, context, mid, row[0], row[1], row[2])
        return MAIN_MENU

    await update.message.reply_text(
        "👋 **Welcome to FilmfyBox!**\n\nType any Movie or Series name.\nExample: `Landman Season 1 Episode 1`",
        parse_mode='Markdown'
    )
    return MAIN_MENU

async def search_handler(update, context):
    user_query = update.message.text.strip()
    
    if not await check_rate_limit(update.effective_user.id):
        await update.message.reply_text("⏳ Slow down...")
        return MAIN_MENU

    movies = get_movies_from_db(user_query)
    
    if not movies:
        await update.message.reply_text("❌ No results found. Try checking spelling.")
        return MAIN_MENU

    # LOGIC UPDATE:
    # If only 1 result found (e.g. specific episode), go straight to send logic
    if len(movies) == 1:
        m = movies[0]
        await send_movie_result(update, context, m[0], m[1], m[2], m[3])
        return MAIN_MENU

    # If multiple results:
    # Check if they are part of a series grouping (generic search like "Landman")
    series_map = defaultdict(list)
    for m in movies:
        if is_series(m[1]):
            info = parse_series_info(m[1])
            series_map[info['base_title']].append(m)

    # If nearly all results belong to ONE series, show series menu
    if len(series_map) == 1 and len(movies) > 1:
        base_title = list(series_map.keys())[0]
        seasons = get_series_episodes(base_title)
        context.user_data['seasons'] = seasons
        context.user_data['base_title'] = base_title
        
        await update.message.reply_text(
            f"📺 **{base_title}**\nSelect Season:",
            reply_markup=create_season_keyboard(seasons, base_title)
        )
        return MAIN_MENU

    # Otherwise, show standard list
    context.user_data['search_results'] = movies
    await update.message.reply_text(
        f"🔍 Found {len(movies)} results:",
        reply_markup=create_selection_keyboard(movies)
    )
    return MAIN_MENU

async def callback_handler(update, context):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "check_membership":
        if await check_user_membership(context, query.from_user.id):
            await query.edit_message_text("✅ Verified! You can search now.")
        else:
            await query.answer("❌ Not joined yet!", show_alert=True)

    elif data.startswith("select_"):
        mid = int(data.split("_")[1])
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT title, url, file_id FROM movies WHERE id=%s", (mid,))
        row = cur.fetchone()
        conn.close()
        if row:
            await query.delete_message()
            await send_movie_result(update, context, mid, row[0], row[1], row[2])

    elif data.startswith("qual_"):
        # qual_MOVIEID_QUALITY
        parts = data.split("_")
        mid = int(parts[1])
        # Reconstruct quality string (we replaced space with _ earlier)
        selected_q = parts[2].replace("_", " ").replace("-", "/")
        
        qualities = context.user_data.get('qualities', [])
        target = next((x for x in qualities if x[0] == selected_q), None)
        
        if target:
            await query.edit_message_text(f"🚀 Sending {context.user_data.get('movie_title')} ({selected_q})...")
            await send_file_to_user(context, query.message.chat.id, context.user_data.get('movie_title'), target[1], target[2])
        else:
            await query.edit_message_text("❌ File expired or not found.")

    elif data.startswith("season_"):
        s_num = int(data.split("_")[1])
        seasons = context.user_data.get('seasons')
        if seasons and s_num in seasons:
            await query.edit_message_text(
                f"📂 Season {s_num}",
                reply_markup=create_episode_keyboard(seasons[s_num], s_num)
            )

    elif data == "back_series":
        base = context.user_data.get('base_title')
        seasons = context.user_data.get('seasons')
        await query.edit_message_text(
            f"📺 **{base}**",
            reply_markup=create_season_keyboard(seasons, base)
        )

    elif data.startswith("page_"):
        page = int(data.split("_")[1])
        movies = context.user_data.get('search_results')
        await query.edit_message_text(
            "Results:", reply_markup=create_selection_keyboard(movies, page)
        )

    elif data == "cancel":
        await query.delete_message()

# ==================== GROUP HANDLER ====================
async def group_handler(update, context):
    msg = update.message.text.strip()
    if len(msg) < 4: return
    
    movies = get_movies_from_db(msg, limit=1)
    if movies:
        m = movies[0]
        # Only prompt if fuzzy score is high enough
        if fuzz.token_sort_ratio(msg, m[1]) > 80:
            btn = InlineKeyboardButton("✅ Get Movie", url=f"https://t.me/{context.bot.username}?start=movie_{m[0]}")
            reply = await update.message.reply_text(
                f"Found: **{m[1]}**",
                reply_markup=InlineKeyboardMarkup([[btn]]),
                parse_mode='Markdown'
            )
            asyncio.create_task(auto_delete(context, update.effective_chat.id, reply.message_id))

# ==================== FLASK RUNNER ====================
def run_flask():
    app.run(host='0.0.0.0', port=PORT)

# ==================== MAIN ====================
def main():
    threading.Thread(target=run_flask, daemon=True).start()
    
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    conv = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            MAIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_handler)]
        },
        fallbacks=[]
    )
    
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.TEXT, group_handler))
    app.add_handler(conv)
    
    logger.info("Bot Started")
    app.run_polling()

if __name__ == '__main__':
    main()
