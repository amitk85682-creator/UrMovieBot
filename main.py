import logging, threading
from telegram.ext import Application, MessageHandler, CommandHandler, CallbackQueryHandler, filters
from utils import db
from config import TOKEN
from handlers import start, search, buttons, group_listener, err
from flask import Flask
from config import UPDATE_SECRET
import os

logging.basicConfig(level=logging.INFO)

# --- Telegram App ---
app=Application.builder().token(TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CallbackQueryHandler(buttons))
app.add_handler(MessageHandler(filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND, search))
app.add_handler(MessageHandler(filters.TEXT & filters.ChatType.GROUPS & ~filters.COMMAND, group_listener))
app.add_error_handler(err)

# --- Flask (keep-alive + manual update) ---
flask=Flask(__name__)
@flask.route("/")
def home(): return "Running!"
@flask.route(f"/{UPDATE_SECRET}")
def update():
    from utils.db import get_conn
    # call your update_movies_in_db() if needed
    return "updated"

def run_flask():
    flask.run(host="0.0.0.0", port=int(os.getenv("PORT",8080)))

if __name__=="__main__":
    db.setup()
    threading.Thread(target=run_flask, daemon=True).start()
    app.run_polling()
