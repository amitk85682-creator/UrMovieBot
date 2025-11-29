from telegram.ext import ContextTypes
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from utils.parser import normalize
from utils.db import get_conn
from fuzzywuzzy import fuzz
import logging
log=logging.getLogger(__name__)

async def group_listener(update:Update, context:ContextTypes.DEFAULT_TYPE):
    if not update.message: return
    text=update.message.text
    if len(text)<4 or text.startswith("/"): return
    norm=normalize(text)
    conn=get_conn();cur=conn.cursor()
    cur.execute("SELECT id,title FROM movies")
    best=None;score=0
    for r in cur.fetchall():
        s=fuzz.token_sort_ratio(norm,normalize(r['title']))
        if s>score: score=s; best=r
    cur.close();conn.close()
    if best and score>88:
        kb=[[InlineKeyboardButton("📂 Get",callback_data=f"q_{best['id']}")]]
        await update.message.reply_text(f"Looks like you’re searching **{best['title']}**",
                                        parse_mode='Markdown',
                                        reply_markup=InlineKeyboardMarkup(kb))
