# scripts/create_database.py
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

try:
    # Connect to PostgreSQL default database
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",  # Connect to default database
        user="postgres",
        password="password"   # Use your actual PostgreSQL password
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    
    cursor = conn.cursor()
    
    # Create database if it doesn't exist
    cursor.execute("CREATE DATABASE telegram_warehouse;")
    print("✅ Database 'telegram_warehouse' created successfully!")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Error: {e}")
    print("\nTrying to check if database already exists...")
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="telegram_warehouse",
            user="postgres",
            password="postgres"
        )
        print("✅ Database 'telegram_warehouse' already exists!")
        conn.close()
    except:
        print("❌ Could not create or connect to database.")
        print("\nPossible issues:")
        print("1. PostgreSQL is not running")
        print("2. Wrong password (default is often 'postgres')")
        print("3. PostgreSQL service needs to be started")