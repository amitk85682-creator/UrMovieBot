from telegram import Update
from telegram.ext import ContextTypes
from templates.keyboards import start_kb
from templates.captions import premium
from utils.db import get_conn
from utils.helpers import auto_delete
import asyncio, logging

log = logging.getLogger(__name__)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat_id = update.effective_chat.id

    # --- DEEP LINK LOGIC (Updated for Dual Table Support) ---
    if context.args and context.args[0].startswith("movie_"):
        try:
            movie_id = int(context.args[0].split("_")[1])
            conn = get_conn()
            cur = conn.cursor()
            
            # Movie ka data fetch karo
            cur.execute("SELECT id, title, url, file_id FROM movies WHERE id=%s", (movie_id,))
            m = cur.fetchone()
            cur.close()
            conn.close()

            if m:
                from handlers.delivery import send_file, show_auto_menu
                
                # Logic: Agar Main Table me file hai to direct bhejo
                if m['file_id'] or m['url']:
                    await send_file(context, chat_id, movie_id, m['title'], m['url'], m['file_id'])
                
                # Logic: Agar Main Table khali hai, iska matlab files 'movie_files' table me hongi
                # To hum user ko Quality Menu dikhayenge
                else:
                    await show_auto_menu(context, chat_id, m)
            else:
                await update.message.reply_text("❌ Movie link expired or not found.")
                
        except Exception as e:
            log.error(f"Deep link error: {e}")
        return

    # --- NORMAL START MESSAGE ---
    txt = f"👋 Hey {user.first_name}!\n\nI'm <b>Ur Movie Bot</b>. Type any movie/series name."
    msg = await update.message.reply_html(txt, reply_markup=start_kb())
    
    # Auto delete welcome message after 60 seconds
    asyncio.create_task(auto_delete(context, chat_id, msg.message_id, 60))
