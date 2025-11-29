import re

def normalize(text: str) -> str:
    # Thoda behtar safai taaki old DB ke brackets wagera remove ho sake
    t = re.sub(r'[^\w\s\-\.]', ' ', text or '') 
    return re.sub(r'\s+', ' ', t).lower().strip()

def parse_info(title: str):
    """
    Return dict: base, season, episode, quality, language
    Updated to support Legacy DB formats (Standard Quality, HD Quality, etc.)
    """
    tl = normalize(title)

    # --- 1. Quality Parsing (Old DB + New DB Support) ---
    q = "HD" # Default fallback
    
    # Check for specific Old DB terms & New terms
    if "4k" in tl or "2160p" in tl: 
        q = "4K"
    elif "1080p" in tl or "hd quality" in tl or "fhd" in tl: 
        q = "1080p"
    elif "720p" in tl or "standard quality" in tl: 
        q = "720p"
    elif "480p" in tl or "sd quality" in tl: 
        q = "480p"
    elif "360p" in tl or "low quality" in tl: 
        q = "360p"
    elif "cam" in tl: 
        q = "CAM"

    # --- 2. Season / Episode Parsing ---
    s, e = None, None
    
    # Season extraction (s01 or season 1)
    ms = re.search(r'(?:s|season)[ ._-]?(\d{1,2})', tl)
    s = int(ms.group(1)) if ms else None

    # Episode extraction (e01 or episode 1)
    me = re.search(r'(?:e|ep|episode)[ ._-]?(\d{1,3})', tl)
    e = int(me.group(1)) if me else None

    # --- 3. Language Parsing ---
    # Purane bot me Hindi content jyada tha, use detect karega
    lang = "English"
    if "hindi" in tl: 
        lang = "Hindi"
    elif "dual" in tl or "multi" in tl: 
        lang = "Dual"

    # --- 4. Base Title Extraction ---
    # Saare technical words hata kar sirf Movie ka naam nikalna
    # Isme Old DB ke keywords bhi add kiye hain taaki naam clean mile
    tech_terms = [
        r's\d{1,2}', r'season\s*\d+',           # Seasons
        r'e\d{1,3}', r'episode\s*\d+',          # Episodes
        r'2160p', r'1080p', r'720p', r'480p', r'360p', # Resolutions
        r'hd quality', r'standard quality',     # Old DB Resolutions
        r'sd quality', r'low quality',          # Old DB Resolutions
        r'cam', r'hindi', r'dual', r'audio',    # Misc
        r'\b(19|20)\d{2}\b'                     # Year (e.g., 2023) ko bhi naam se hata dega
    ]
    
    # Pattern banakar split karega
    split_pattern = r'|'.join(tech_terms)
    base = re.split(split_pattern, tl)[0]
    
    # Cleaning special chars
    base = re.sub(r'[.\-_]', ' ', base).strip()

    return dict(base=base, season=s, episode=e, quality=q, language=lang, raw=title)
