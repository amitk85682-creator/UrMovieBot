import psycopg2, logging, os
from psycopg2.extras import RealDictCursor
from config import DATABASE_URL, FIXED_DATABASE_URL

log = logging.getLogger(__name__)

def get_conn():
    dsn = FIXED_DATABASE_URL or DATABASE_URL
    return psycopg2.connect(dsn, cursor_factory=RealDictCursor)

def setup():
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm;')

        # 1. Movies Table (Updated to match Old DB columns like description/file_size)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS movies(
            id SERIAL PRIMARY KEY,
            title TEXT UNIQUE,
            url TEXT,
            file_id TEXT,
            description TEXT,
            file_size TEXT
        );""")

        # 2. Movie Files Table (Ye sabse zaroori hai purane files ke liye)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS movie_files (
            id SERIAL PRIMARY KEY,
            movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE,
            quality TEXT NOT NULL,
            file_id TEXT,
            url TEXT,
            file_size TEXT,
            UNIQUE(movie_id, quality)
        );""")

        # 3. Aliases Table (Search improvement ke liye)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS movie_aliases (
            id SERIAL PRIMARY KEY,
            movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE,
            alias TEXT NOT NULL,
            UNIQUE(movie_id, alias)
        );""")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_info(
            id SERIAL PRIMARY KEY,
            last_sync TIMESTAMP DEFAULT NOW()
        );""")

        conn.commit()
        cur.close(); conn.close()
        log.info("DB ready and Compatible with Old Data ✅")
    except Exception as e:
        log.error(f"DB setup failed: {e}")

# --- Helper Function to Fetch Movie with Files ---
# Naye bot ko search karte waqt ise use karna chahiye
def get_movie_with_files(title_query):
    conn = get_conn()
    if not conn: return None
    try:
        cur = conn.cursor()
        # Ye query 'movies' aur 'movie_files' dono ko join karke data layegi
        cur.execute("""
            SELECT m.id, m.title, m.file_id as main_file, 
                   mf.quality, mf.file_id as sub_file, mf.url as sub_url
            FROM movies m
            LEFT JOIN movie_files mf ON m.id = mf.movie_id
            WHERE m.title ILIKE %s OR m.title ILIKE %s
        """, (f"{title_query}", f"%{title_query}%"))
        
        results = cur.fetchall()
        return results
    except Exception as e:
        log.error(f"Search Error: {e}")
        return []
    finally:
        conn.close()
