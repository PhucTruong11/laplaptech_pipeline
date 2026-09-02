import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    dbname=os.getenv("POSTGRES_DATABASE")
)
conn.autocommit = True
cur = conn.cursor()

try:
    print("Dropping dbt schemas...")
    cur.execute("DROP SCHEMA IF EXISTS public_bronze CASCADE;")
    cur.execute("DROP SCHEMA IF EXISTS public_silver CASCADE;")
    cur.execute("DROP SCHEMA IF EXISTS public_gold CASCADE;")
    print("Dropped successfully.")
except Exception as e:
    print(f"Error: {e}")
finally:
    cur.close()
    conn.close()
