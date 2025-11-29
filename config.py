import os
from dotenv import load_dotenv
load_dotenv()

# Core
TOKEN          = os.getenv("TELEGRAM_BOT_TOKEN")
BOT_USERNAME   = os.getenv("BOT_USERNAME", "urmoviebot")

# DB
DATABASE_URL   = os.getenv("DATABASE_URL")
FIXED_DATABASE_URL = os.getenv("FIXED_DATABASE_URL")

# Admin
ADMIN_ID       = int(os.getenv("ADMIN_USER_ID", 0))
UPDATE_SECRET  = os.getenv("UPDATE_SECRET_CODE", "secret123")

# Links
CHANNEL_LINK   = os.getenv("CHANNEL_LINK", "https://t.me/filmfybox")
GROUP_LINK     = os.getenv("GROUP_LINK",   "https://t.me/Filmfybox002")

# Behaviour
AUTO_DELETE_SEC = 60
SIMILARITY      = 85
