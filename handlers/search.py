from telegram import Update
from telegram.ext import ContextTypes
from utils.parser import normalize
from utils.db import get_conn
from handlers.delivery import show_auto_menu
import logging

log = logging.getLogger(__name__)

async def search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text: return
    
    q_raw = update.message.text.strip()
    norm = normalize(q_raw)
    
    # 3 characters se kam par search na kare
    if len(norm) < 3: return

    conn = get_conn()
    cur = conn.cursor()

    # --- 1. SUPER FAST SQL SEARCH (Movies Table) ---
    # Python loop ki jagah DB se direct similarity check (pg_trgm)
    cur.execute("""
        SELECT id, title, url, file_id 
        FROM movies 
        WHERE title % %s 
        ORDER BY similarity(title, %s) DESC 
        LIMIT 1
    """, (norm, norm))
    
    best = cur.fetchone()

    # --- 2. Fallback: Search in Aliases (Agar title match na ho) ---
    # Agar user ne short name (e.g. 'KGF') likha aur title 'K.G.F Chapter 1' hai
    if not best:
        cur.execute("""
            SELECT m.id, m.title, m.url, m.file_id
            FROM movies m
            JOIN movie_aliases a ON m.id = a.movie_id
            WHERE a.alias % %s
            ORDER BY similarity(a.alias, %s) DESC
            LIMIT 1
        """, (norm, norm))
        best = cur.fetchone()

    cur.close()
    conn.close()

    # --- 3. Result Handling ---
    if not best:
        await update.message.reply_text("❌ Not found. Please check spelling.")
        return

    # User ko menu dikhana (Jo buttons.py se connect karega)
    await show_auto_menu(context, update.effective_chat.id, best)
