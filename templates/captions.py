from config import CHANNEL_LINK, GROUP_LINK

def premium(title:str)->str:
    return (
        f"🎬 <b>{title}</b>\n"
        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        f"💿 <b>Quality:</b> High Definition\n"
        f"🔊 <b>Lang:</b> Hindi / English\n"
        f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
        f"📢 <a href='{CHANNEL_LINK}'>Join Channel</a> | "
        f"<a href='{GROUP_LINK}'>Support Group</a>\n\n"
        f"⚠️ <i>Auto-delete in 60 s</i>"
    )
