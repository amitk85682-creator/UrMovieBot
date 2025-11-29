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

async def send_movie_to_user(context: ContextTypes.DEFAULT_TYPE, chat_id: int, movie_id, title, url=None, file_id=None):
    from templates.captions import premium
    from utils.helpers import auto_delete
    from config import CHANNEL_LINK
    import asyncio

    warning_msg = await context.bot.send_message(
        chat_id=chat_id,
        text="⚠️ ❌👉This file will be deleted automatically in 1 minute❗️Please forward to another chat if needed.",
        parse_mode='HTML'
    )

    sent_msg = None
    caption_text = premium(title)

    # Custom join button
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("🔗 Join Channel", url=CHANNEL_LINK)
    ]])

    try:
        # A) Send file using file_id
        if file_id:
            sent_msg = await context.bot.send_document(
                chat_id=chat_id,
                document=file_id,
                caption=caption_text,
                parse_mode='HTML',
                reply_markup=keyboard
            )

        # B) Telegram Private/Group Link (copy)
        elif url and url.startswith("https://t.me/"):
            try:
                parts = url.rstrip('/').split('/')
                if "/c/" in url:
                    # t.me/c/<chat_id>/<msg_id>
                    from_chat_id = int("-100" + parts[-2])
                else:
                    # public channel: t.me/username/<msg_id>
                    from_chat_id = "@" + parts[-2]
                msg_id = int(parts[-1])

                sent_msg = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=from_chat_id,
                    message_id=msg_id,
                    caption=caption_text,
                    parse_mode='HTML',
                    reply_markup=keyboard
                )
            except Exception as e:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🎬 {title}\n\n🔗 {url}",
                    reply_markup=keyboard,
                    parse_mode='HTML'
                )

        # C) Plain URL (fallback)
        elif url:
            sent_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"🎬 <b>{title}</b>\n\n🔗 <b>Download:</b> {url}",
                parse_mode='HTML',
                reply_markup=keyboard
            )

        # D) Nothing to send
        else:
            await context.bot.send_message(chat_id, "❌ File not available.")

    except Exception as e:
        logger.error(f"Error sending file: {e}")
        await context.bot.send_message(chat_id, "❌ Could not send file. Try again later.")

    if sent_msg:
        asyncio.create_task(auto_delete(context, chat_id, sent_msg.message_id, 60))
        asyncio.create_task(auto_delete(context, chat_id, warning_msg.message_id, 60))
