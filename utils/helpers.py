import asyncio, logging
log=logging.getLogger(__name__)

async def auto_delete(context, chat_id, msg_id, delay):
    try:
        await asyncio.sleep(delay)
        await context.bot.delete_message(chat_id, msg_id)
    except Exception as e:
        log.warning(f"auto-delete failed: {e}")
