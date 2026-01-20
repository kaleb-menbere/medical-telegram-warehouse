import sqlite3

DB_PATH = "../data/telegram_warehouse.db"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# List tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

print("📦 Tables in database:")
for t in tables:
    print("-", t[0])

print("\n📊 Row counts:")
for t in tables:
    table = t[0]
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table}: {count}")
    except Exception as e:
        print(f"{table}: ERROR ({e})")

conn.close()
