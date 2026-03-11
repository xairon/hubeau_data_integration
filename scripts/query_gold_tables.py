import os
import psycopg2

conn = psycopg2.connect(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    user=os.getenv("PG_USER", "postgres"),
    password=os.getenv("PG_PASSWORD", ""),
    dbname=os.getenv("PG_DB", "postgres"),
)
cur = conn.cursor()
cur.execute(
    "SELECT table_name FROM information_schema.tables "
    "WHERE table_schema = 'gold' AND table_type = 'BASE TABLE' "
    "ORDER BY table_name;"
)
rows = [r[0] for r in cur.fetchall()]
print(rows)
cur.close()
conn.close()
