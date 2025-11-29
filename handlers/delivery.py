import logging
import asyncio
from urllib.parse import urlparse

from utils.db import get_conn
from utils.parser import parse_info
from templates.keyboards import season_kb, quality_kb
from templates.captions import premium
from utils.helpers import auto_delete
from config import AUTO_DELETE_SEC

log = logging.getLogger(__name__)

def gather_family(base_name: str):
    """
    Return all DB rows (dicts) whose parsed base equals base_name.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, title, url, file_id FROM movies")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    fam = []
    for r in rows:
        if parse_info(r['title'])['base'] == base_name:
            fam.append(r)
    return fam

async def show_auto_menu(context, chat_id: int, anchor_row: dict):
    """
    Netflix-like menu:
      - If series (seasons found) → show season buttons
      - Else (movie) → show unique quality options
    """
    info = parse_info(anchor_row['title'])
    fam = gather_family(info['base'])

    seasons = sorted({parse_info(m['title'])['season'] for m in fam if parse_info(m['title'])['season']})
    # If we have seasons -> show season list
    if seasons:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"📺 <b>{info['base'].title()}</b>\nSelect Season:",
            reply_markup=season_kb(seasons, anchor_row['id']),
            parse_mode='HTML'
        )
        return

    # Movie flow: show unique quality+lang options
    seen = set()
    q_map = []
    for m in fam:
        p = parse_info(m['title'])
        key = (p['quality'], p['language'])
        if key in seen:
            continue
        seen.add(key)
        label = f"{p['quality']}" + (f" {p['language'][:3]}" if p['language'] not in ("Unknown", "") else "")
        q_map.append((label, f"q_{m['id']}"))

    if not q_map:
        # Fallback: at least let them take the anchor item
        q_map = [(f"{info['quality']}", f"q_{anchor_row['id']}")]

    await context.bot.send_message(
        chat_id=chat_id,
        text=f"🎬 <b>{info['base'].title()}</b>\nSelect Quality:",
        reply_markup=quality_kb(q_map),
        parse_mode='HTML'
    )

async def send_file(context, chat_id: int, movie_id: int, title: str, url: str = None, file_id: str = None):
    """
    Deliver the actual file/link with Netflix-like caption.
    - file_id -> send_document
    - t.me public/private -> copy_message
    - http(s) link -> send as link with caption
    """
    # small wait message (optional)
    waiting = await context.bot.send_message(chat_id, "⏳ Preparing your file…")

    sent_msg = None
    caption_text = premium(title)

    try:
        if file_id:
            sent_msg = await context.bot.send_document(
                chat_id=chat_id,
                document=file_id,
                caption=caption_text,
                parse_mode='HTML'
            )

        elif url and "t.me" in url:
            try:
                parts = urlparse(url).path.strip("/").split("/")
                if parts[0] == "c":     # private channel: /c/<internal>/<msg>
                    from_chat_id = int("-100" + parts[1])
                    msg_id = int(parts[2])
                else:                    # public channel: /<username>/<msg>
                    from_chat_id = f"@{parts[0]}"
                    msg_id = int(parts[1])

                sent_msg = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=from_chat_id,
                    message_id=msg_id,
                    caption=caption_text,
                    parse_mode='HTML'
                )
            except Exception as e:
                log.error(f"Telegram copy failed, fallback to link: {e}")
                sent_msg = await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🎬 <b>{title}</b>\n\n🔗 {url}\n\n{caption_text}",
                    parse_mode='HTML'
                )

        elif url:
            sent_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"🎬 <b>{title}</b>\n\n🔗 {url}\n\n{caption_text}",
                parse_mode='HTML'
            )
        else:
            await context.bot.send_message(chat_id, "❌ File not available.")

    except Exception as e:
        log.error(f"send_file error: {e}")
        await context.bot.send_message(chat_id, "❌ Could not send file. Try again later.")

    try:
        await context.bot.delete_message(chat_id, waiting.message_id)
    except Exception:
        pass

    if sent_msg:
        asyncio.create_task(auto_delete(context, chat_id, sent_msg.message_id, AUTO_DELETE_SEC))

# Optional wrapper for compatibility with any previous calls
async def send_movie_to_user(context, chat_id: int, movie_id, title, url=None, file_id=None):
    await send_file(context, chat_id, movie_id, title, url, file_id)
