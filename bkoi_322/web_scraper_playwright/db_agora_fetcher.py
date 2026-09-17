import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

env_path = Path(__file__).resolve().parent / ".env"

STAGING_HOST = os.getenv("STAGING_HOST")
STAGING_PORT = os.getenv("STAGING_PORT", "5432")
STAGING_USER = os.getenv("STAGING_USER")
STAGING_PASS = os.getenv("STAGING_PASS")
STAGING_DB   = os.getenv("STAGING_DB")

def get_db_connection():
    """Establish connection to PostgreSQL staging database."""
    return psycopg2.connect(
        host=STAGING_HOST,
        port=STAGING_PORT,
        user=STAGING_USER,
        password=STAGING_PASS,
        dbname=STAGING_DB
    )

def fetch_agora_outlets():
    """Fetch all Agora outlet records from public.places table."""
    query = """
        SELECT p.* 
        FROM public.places AS p
        WHERE p.business_name ILIKE %s;
    """
    params = ("%Agora%",)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                results = cur.fetchall()
                return [dict(row) for row in results]
    except Exception as e:
        print(f"[!] Database Connection Error: {e}")
        return []

if __name__ == "__main__":
    outlets = fetch_agora_outlets()
    print(f"[+] Total Agora outlet records found: {len(outlets)}")
    
    if outlets:
        print("\n--- First Result Sample ---")
        for key, value in outlets[0].items():       
            print(f"{key}: {value}")