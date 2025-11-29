from telegram import Update
from telegram.ext import ContextTypes, filters
from utils.parser import normalize, parse_info
from utils.db import get_conn
from fuzzywuzzy import fuzz
from templates.keyboards import season_kb, quality_kb
from utils.helpers import auto_delete
import asyncio, logging
log=logging.getLogger(__name__)

async def search(update:Update, context:ContextTypes.DEFAULT_TYPE):
    q_raw=update.message.text.strip()
    norm=normalize(q_raw)
    if len(norm)<3: return
    conn=get_conn(); cur=conn.cursor()
    cur.execute("SELECT id,title,url,file_id FROM movies")
    all=list(cur.fetchall());cur.close();conn.close()

    # best anchor
    best=None; best_score=0
    for m in all:
        score=fuzz.token_sort_ratio(norm, normalize(m['title']))
        if score>best_score:
            best_score, best=m['id'],m

    if not best or best_score<60:
        await update.message.reply_text("❌ Not found.")
        return

    from handlers.delivery import show_auto_menu
    await show_auto_menu(context, update.effective_chat.id, best)
