import psycopg2

conn = psycopg2.connect(
    host="dib-2019006065",
    port=49502,
    user="postgres",
    password="REDACTED",
    dbname="postgres",
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
