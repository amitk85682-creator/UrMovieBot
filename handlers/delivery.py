from utils.parser import parse_info
from utils.db import get_conn
from templates.keyboards import season_kb, quality_kb
from templates.captions import premium
from utils.helpers import auto_delete
import asyncio, logging
log=logging.getLogger(__name__)

def gather_family(base):
    conn=get_conn();cur=conn.cursor()
    cur.execute("SELECT id,title,url,file_id FROM movies")
    rows=cur.fetchall();cur.close();conn.close()
    fam=[]
    for r in rows:
        if parse_info(r['title'])['base']==base:
            fam.append(r)
    return fam

async def show_auto_menu(context,chat_id, anchor):
    info=parse_info(anchor['title'])
    fam=gather_family(info['base'])
    seasons=sorted({parse_info(m['title'])['season'] for m in fam if parse_info(m['title'])['season']})
    # series
    if seasons:
        await context.bot.send_message(chat_id,
            f"📺 <b>{info['base'].title()}</b>\nSelect Season:",
            reply_markup=season_kb(seasons, anchor['id']), parse_mode='HTML')
        return
    # movie
    qmap=[]
    seen=set()
    for m in fam:
        p=parse_info(m['title'])
        key=(p['quality'],p['language'])
        if key in seen: continue
        seen.add(key)
        label=f"{p['quality']} {p['language'][:3]}"
        qmap.append((label,f"q_{m['id']}"))
    await context.bot.send_message(chat_id,
        f"🎬 <b>{info['base'].title()}</b>\nSelect Quality:",
        reply_markup=quality_kb(qmap), parse_mode='HTML')

async def send_file(context,chat_id,id,title,url,file_id):
    from config import AUTO_DELETE_SEC
    from templates.captions import premium
    msg_wait=await context.bot.send_message(chat_id,"⏳ Preparing file…")
    sent=None
    try:
        if file_id:
            sent=await context.bot.send_document(chat_id,file_id,caption=premium(title),parse_mode='HTML')
        elif url and "t.me" in url:
            from urllib.parse import urlparse
            parts=urlparse(url).path.strip("/").split("/")
            if parts[0]=="c":
                from_chat=int("-100"+parts[1]); mid=int(parts[2])
            else:
                from_chat="@"+parts[0]; mid=int(parts[1])
            sent=await context.bot.copy_message(chat_id,from_chat,mid,caption=premium(title),parse_mode='HTML')
        else:
            await context.bot.send_message(chat_id,f"🔗 {url}\n\n{premium(title)}",parse_mode='HTML')
    except Exception as e:
        log.error(e); await context.bot.send_message(chat_id,"❌ Failed to send.")
    try: await context.bot.delete_message(chat_id,msg_wait.message_id)
    except: pass
    if sent:
        asyncio.create_task(auto_delete(context,chat_id,sent.message_id,AUTO_DELETE_SEC))
