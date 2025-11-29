import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from utils.parser import parse_info
from utils.db import get_conn
from templates.keyboards import season_kb, quality_kb
from handlers.delivery import send_file, show_auto_menu, gather_family

log = logging.getLogger(__name__)

async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data

    if data == "cancel":
        try:
            await q.message.delete()
        except Exception:
            pass
        return

    # --- SEASON SELECTION ---
    if data.startswith("seas_"):
        # data: seas_<season>_<anchorId>
        _, season_str, anchor_str = data.split("_", 2)
        season = int(season_str)
        anchor_id = int(anchor_str)

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT title FROM movies WHERE id = %s", (anchor_id,))
        anchor = cur.fetchone()
        cur.close()
        conn.close()

        if not anchor:
            await q.message.edit_text("❌ Series not found.")
            return

        base = parse_info(anchor['title'])['base']
        fam = gather_family(base)

        # Collect episodes
        eps_set = set()
        for m in fam:
            # Handle mixed objects (movies table or joined row)
            t = m.get('title') or m.get('name') 
            if not t: continue
            
            p = parse_info(t)
            if p['season'] == season and p['episode'] is not None:
                eps_set.add(p['episode'])
        episodes = sorted(eps_set)

        # 1. Season Pack Logic (No episodes found for this season)
        if not episodes:
            seen = set()
            qmap = []
            for m in fam:
                t = m.get('title') or m.get('name')
                if not t: continue
                
                p = parse_info(t)
                if p['season'] == season:
                    key = (p['quality'], p.get('language', ''))
                    if key in seen: continue
                    seen.add(key)
                    
                    label = f"{p['quality']}" + (f" {p['language'][:3]}" if p.get('language') not in ("Unknown", "", None) else "")
                    # 'q_' callback handles both tables now
                    qmap.append((label, f"q_{m['id']}"))

            if not qmap:
                await q.message.edit_text("❌ No files found for this season.")
                return

            await q.message.edit_text(
                text=f"📦 Season {season} — choose quality:",
                reply_markup=quality_kb(qmap),
                parse_mode='HTML'
            )
            return

        # 2. Episode Grid Logic
        rows = []
        row = []
        for e in episodes:
            # Pick one representative for the episode button
            pick = next((m for m in fam if parse_info(m.get('title', ''))['season'] == season and parse_info(m.get('title', ''))['episode'] == e), None)
            if not pick: continue
            
            row.append(InlineKeyboardButton(f"Ep {e:02d}", callback_data=f"ep_{pick['id']}"))
            if len(row) == 4:
                rows.append(row)
                row = []
        if row:
            rows.append(row)

        kb = InlineKeyboardMarkup(rows + [[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]])
        await q.message.edit_text(
            text=f"📺 Season {season} — select episode:",
            reply_markup=kb,
            parse_mode='HTML'
        )
        return

    # --- EPISODE QUALITY SELECTION ---
    if data.startswith("ep_"):
        mid = int(data.split("_")[1])

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, title FROM movies WHERE id = %s", (mid,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            await q.message.edit_text("❌ Episode not found.")
            return

        p0 = parse_info(row['title'])
        base = p0['base']
        fam = gather_family(base)

        # Filter family for specific SxxExx
        targets = []
        for m in fam:
            t = m.get('title')
            if not t: continue
            p = parse_info(t)
            if p['season'] == p0['season'] and p['episode'] == p0['episode']:
                targets.append((m, p))

        seen = set()
        qmap = []
        for m, p in targets:
            key = (p['quality'], p.get('language', ''))
            if key in seen: continue
            seen.add(key)
            
            label = f"{p['quality']}" + (f" {p['language'][:3]}" if p.get('language') not in ("Unknown", "", None) else "")
            qmap.append((label, f"q_{m['id']}"))

        if not qmap:
            await q.message.edit_text("❌ No files found for this episode.")
            return

        await q.message.edit_text("Select quality:", reply_markup=quality_kb(qmap))
        return

    # --- FINAL FILE DELIVERY (CRITICAL UPDATE) ---
    if data.startswith("q_"):
        mid = int(data.split("_")[1])
        conn = get_conn()
        cur = conn.cursor()

        # Step 1: Try finding in 'movies' table (Legacy/Direct)
        cur.execute("SELECT title, url, file_id FROM movies WHERE id = %s", (mid,))
        m = cur.fetchone()

        # Step 2: If not found or empty, try 'movie_files' table (New Structure)
        # Note: We join with movies table to get the Base Title
        if not m or (not m['file_id'] and not m['url']):
            cur.execute("""
                SELECT m.title, mf.url, mf.file_id, mf.quality
                FROM movie_files mf
                JOIN movies m ON mf.movie_id = m.id
                WHERE mf.id = %s
            """, (mid,))
            mf = cur.fetchone()
            
            if mf:
                # Construct a title combining Movie Name + Quality
                full_title = f"{mf['title']} - {mf['quality']}"
                m = {'title': full_title, 'url': mf['url'], 'file_id': mf['file_id']}

        cur.close()
        conn.close()

        if not m or (not m['file_id'] and not m['url']):
            await q.message.edit_text("❌ File not found in database.")
            return

        # Send to user
        await send_file(context, q.from_user.id, mid, m['title'], m['url'], m['file_id'])

        # Clean up menu
        try:
            await q.message.delete()
        except Exception:
            pass
