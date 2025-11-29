utils/helpers.py

import asyncio
import logging
from telegram.error import BadRequest

log = logging.getLogger(__name__)

async def auto_delete(context, chat_id, msg_id, delay):
    try:
        await asyncio.sleep(delay)
        await context.bot.delete_message(chat_id, msg_id)
    except BadRequest:
        # Agar message pehle hi delete ho chuka hai, to error ignore karein
        pass
    except Exception as e:
        log.warning(f"Auto-delete failed: {e}")
