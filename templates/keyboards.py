from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from config import CHANNEL_LINK, GROUP_LINK, BOT_USERNAME

def start_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Me To Group", url=f"https://t.me/{BOT_USERNAME}?startgroup=true")],
        [InlineKeyboardButton("📢 Channel", url=CHANNEL_LINK),
         InlineKeyboardButton("👥 Group",   url=GROUP_LINK)]
    ])

def quality_kb(q_map):
    """
    q_map: list of (btn_text, callback)
    Optimized: Shows 2 buttons per row for compact view.
    """
    rows = []
    row = []
    for txt, cb in q_map:
        row.append(InlineKeyboardButton(txt, callback_data=cb))
        # Har line me 2 buttons dikhayega
        if len(row) == 2:
            rows.append(row)
            row = []
    
    # Agar koi button bach gaya to use add karo
    if row:
        rows.append(row)
        
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
    return InlineKeyboardMarkup(rows)

def season_kb(seasons, anchor_id):
    rows = []
    row = []
    for s in seasons:
        row.append(InlineKeyboardButton(f"Season {s}", callback_data=f"seas_{s}_{anchor_id}"))
        # Har line me 3 Seasons dikhayega (S1, S2, S3)
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
    return InlineKeyboardMarkup(rows)
