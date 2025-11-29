from telegram import Update
from telegram.ext import ContextTypes
from utils.parser import parse_info
from utils.db import get_conn
from templates.keyboards import season_kb, quality_kb
from handlers.delivery import send_file, show_auto_menu
import logging
log=logging.getLogger(__name__)

async def buttons(update:Update, context:ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; await q.answer()
    data=q.data
    if data=="cancel":
        await q.message.delete(); return
    if data.startswith("seas_"):
        _,sn,anchor= data.split("_")
        season=int(sn); anchor=int(anchor)
        conn=get_conn();cur=conn.cursor()
        cur.execute("SELECT title FROM movies WHERE id=%s",(anchor,)); base=parse_info(cur.fetchone()['title'])['base']
        cur.close(); conn.close()
        fam=[m for m in show_family_cache(base)]  # you can cache if needed
        eps=sorted({parse_info(m['title'])['episode'] for m in fam if parse_info(m['title'])['season']==season and parse_info(m['title'])['episode']})
        if not eps:
            # Season pack
            qualities=[]
            for m in fam:
                p=parse_info(m['title'])
                if p['season']==season and p['quality']:
                    label=f"{p['quality']}"
                    qualities.append((label,f"q_{m['id']}"))
            await q.message.edit_text(f"Season {season} ‑ choose quality:",
                                      reply_markup=quality_kb(qualities),parse_mode='HTML')
            return
        # list episodes
        rows=[]; row=[]
        for e in eps:
            any_m=[m for m in fam if parse_info(m['title'])['season']==season and parse_info(m['title'])['episode']==e][0]
            row.append((f"E{e}",f"ep_{any_m['id']}"))
            if len(row)==4: rows.append(row); row=[]
        if row: rows.append(row)
        kb=[[InlineKeyboardButton(t,callback_data=c)] for t,c in [btn for r in rows for btn in r]]
        await q.message.edit_text(f"Season {season} ‑ select episode:", reply_markup=InlineKeyboardMarkup(kb))
    if data.startswith("ep_"):
        mid=int(data.split("_")[1])
        conn=get_conn();cur=conn.cursor(); cur.execute("SELECT id,title FROM movies WHERE id=%s",(mid,)); m=cur.fetchone();cur.close();conn.close()
        base=parse_info(m['title'])['base']; p_ep=parse_info(m['title'])
        fam=[m2 for m2 in show_family_cache(base) if parse_info(m2['title'])['season']==p_ep['season'] and parse_info(m2['title'])['episode']==p_ep['episode']]
        qmap=[]
        for m2 in fam:
            p=parse_info(m2['title'])
            qmap.append((p['quality'],f"q_{m2['id']}"))
        await q.message.edit_text("Select quality:",reply_markup=quality_kb(qmap))
    if data.startswith("q_"):
        mid=int(data.split("_")[1])
        conn=get_conn();cur=conn.cursor();cur.execute("SELECT title,url,file_id FROM movies WHERE id=%s",(mid,)); m=cur.fetchone();cur.close(); conn.close()
        await send_file(context,q.from_user.id,mid,m['title'],m['url'],m['file_id'])
        try: await q.message.delete()
        except: pass
