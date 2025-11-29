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

        cur.execute("""
        CREATE TABLE IF NOT EXISTS movies(
            id SERIAL PRIMARY KEY,
            title TEXT UNIQUE,
            url TEXT,
            file_id TEXT
        );""")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_info(
            id SERIAL PRIMARY KEY,
            last_sync TIMESTAMP DEFAULT NOW()
        );""")

        conn.commit()
        cur.close(); conn.close()
        log.info("DB ready ✅")
    except Exception as e:
        log.error(f"DB setup failed: {e}")
