from telegram.ext import ContextTypes
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from utils.parser import normalize
from utils.db import get_conn
import logging

log = logging.getLogger(__name__)

async def group_listener(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message: return
    text = update.message.text
    
    # Basic filters
    if len(text) < 3 or text.startswith("/"): return
    
    norm = normalize(text)
    conn = get_conn()
    cur = conn.cursor()

    # --- OPTIMIZED SEARCH (SQL based) ---
    # Python loop ki jagah DB ka 'pg_trgm' use kar rahe hain jo 100x fast hai
    # Hum check kar rahe hain ki movie ke paas direct file hai ya nahi
    cur.execute("""
        SELECT id, title, file_id, url
        FROM movies 
        WHERE title % %s 
        ORDER BY similarity(title, %s) DESC 
        LIMIT 1
    """, (norm, norm))
    
    best = cur.fetchone()
    cur.close()
    conn.close()

    # Agar match mila
    if best:
        # --- SMART BUTTON LOGIC ---
        # 1. Agar Purane DB style ka data hai (Direct File) -> 'Get' Button
        if best['file_id'] or best['url']:
            btn_text = "📂 Get File"
            cb_data = f"q_{best['id']}"
        
        # 2. Agar Naye DB style ka data hai (Multiple Qualities) -> 'Select Quality' Button
        # Hum 'seas_1_{id}' bhejenge jo menu open karega
        else:
            btn_text = "✨ Select Quality"
            cb_data = f"seas_1_{best['id']}"

        kb = [[InlineKeyboardButton(btn_text, callback_data=cb_data)]]
        
        await update.message.reply_text(
            f"Are you searching for **{best['title']}**?",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(kb)
        )
