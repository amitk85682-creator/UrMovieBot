import re

def normalize(text:str)->str:
    t = re.sub(r'[^\w\s]', ' ', text or '')
    return re.sub(r'\s+', ' ', t).lower().strip()

def parse_info(title:str):
    """
    Return dict: base, season, episode, quality, language
    """
    tl = title.lower()
    # quality
    q="HD"
    for p,qname in [("2160p","4K"),("4k","4K"),("1080p","1080p"),("720p","720p"),("480p","480p"),("cam","CAM")]:
        if p in tl: q=qname; break

    # season / episode
    s=e=None
    m=re.search(r'(?:s|season)[ ._-]?(\d{1,2})', tl);  s=int(m.group(1)) if m else None
    m=re.search(r'(?:e|ep|episode)[ ._-]?(\d{1,3})', tl); e=int(m.group(1)) if m else None

    # base
    base = re.split(r'\s(?:s\d{1,2}|season|2160p|1080p|720p|480p|cam)\b', tl)[0]
    base = re.sub(r'[.\-_]', ' ', base).strip()

    lang="Hindi" if "hindi" in tl else ("Dual" if "dual" in tl else "English")
    return dict(base=base,season=s,episode=e,quality=q,language=lang,raw=title)
