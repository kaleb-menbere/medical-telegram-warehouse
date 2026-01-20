# scripts/recreate_and_load.py
import psycopg2
import json
import os
from datetime import datetime

def recreate_and_load():
    conn = psycopg2.connect(
        host="localhost",
        database="telegram_warehouse",
        user="postgres",
        password="password"
    )
    
    cursor = conn.cursor()
    
    # Create raw schema
    cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    
    # Drop existing table if it exists
    cursor.execute("DROP TABLE IF EXISTS raw.telegram_messages;")
    
    # Create table with correct schema based on your JSON structure
    create_table_sql = """
    CREATE TABLE raw.telegram_messages (
        message_id BIGINT PRIMARY KEY,
        channel_id BIGINT,
        channel_username VARCHAR(255),
        channel_name VARCHAR(255),
        message_date TIMESTAMP,
        message_text TEXT,
        has_media BOOLEAN,
        media_type VARCHAR(100),
        image_path VARCHAR(500),
        views INTEGER,
        forwards INTEGER,
        replies INTEGER,
        edited BOOLEAN,
        edit_date TIMESTAMP,
        pinned BOOLEAN,
        scraped_at TIMESTAMP,
        scraping_session_id VARCHAR(100),
        raw_data JSONB
    );
    """
    cursor.execute(create_table_sql)
    print("✅ Table created with correct schema")
    
    # Now load data from JSON files
    base_dir = "data/raw/telegram_messages"
    total_messages = 0
    
    # Walk through all date directories
    for date_dir in os.listdir(base_dir):
        date_path = os.path.join(base_dir, date_dir)
        if not os.path.isdir(date_path):
            continue
            
        for json_file in os.listdir(date_path):
            if json_file.endswith('.json'):
                full_path = os.path.join(date_path, json_file)
                print(f"📄 Loading {full_path}")
                
                try:
                    with open(full_path, 'r', encoding='utf-8') as f:
                        messages = json.load(f)
                        
                        if not isinstance(messages, list):
                            print(f"⚠️  Skipping: Expected list, got {type(messages)}")
                            continue
                        
                        for message in messages:
                            # Use the exact field names from your JSON
                            insert_sql = """
                            INSERT INTO raw.telegram_messages 
                            (message_id, channel_id, channel_username, channel_name, 
                             message_date, message_text, has_media, media_type, image_path,
                             views, forwards, replies, edited, edit_date, pinned,
                             scraped_at, scraping_session_id, raw_data)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """
                            
                            # Convert string dates to datetime objects
                            message_date_str = message.get('message_date')
                            edit_date_str = message.get('edit_date')
                            scraped_at_str = message.get('scraped_at')
                            
                            try:
                                message_date = datetime.fromisoformat(message_date_str.replace('Z', '+00:00')) if message_date_str else None
                            except:
                                message_date = None
                                
                            try:
                                edit_date = datetime.fromisoformat(edit_date_str.replace('Z', '+00:00')) if edit_date_str else None
                            except:
                                edit_date = None
                                
                            try:
                                scraped_at = datetime.fromisoformat(scraped_at_str.replace('Z', '+00:00')) if scraped_at_str else None
                            except:
                                scraped_at = None
                            
                            cursor.execute(insert_sql, (
                                message.get('message_id'),
                                message.get('channel_id'),
                                message.get('channel_username'),
                                message.get('channel_name'),
                                message_date,
                                message.get('message_text', ''),
                                message.get('has_media', False),
                                message.get('media_type', ''),
                                message.get('image_path', ''),
                                message.get('views', 0),
                                message.get('forwards', 0),
                                message.get('replies', 0),
                                message.get('edited', False),
                                edit_date,
                                message.get('pinned', False),
                                scraped_at,
                                message.get('scraping_session_id'),
                                json.dumps(message)
                            ))
                            total_messages += 1
                            
                except Exception as e:
                    print(f"❌ Error: {e}")
                    continue
    
    conn.commit()
    
    # Verify the load
    cursor.execute("SELECT COUNT(*) FROM raw.telegram_messages;")
    count = cursor.fetchone()[0]
    
    print(f"\n✅ Successfully loaded {count} messages")
    
    # Show some stats
    cursor.execute("""
        SELECT 
            channel_name,
            COUNT(*) as total_messages,
            SUM(CASE WHEN has_media THEN 1 ELSE 0 END) as with_media,
            AVG(views) as avg_views,
            MIN(message_date) as first_post,
            MAX(message_date) as last_post
        FROM raw.telegram_messages
        GROUP BY channel_name
        ORDER BY total_messages DESC;
    """)
    
    print("\n📊 Summary by Channel:")
    for row in cursor.fetchall():
        print(f"  {row[0]}:")
        print(f"    Total messages: {row[1]}")
        print(f"    With media: {row[2]}")
        print(f"    Avg views: {row[3]:.0f}")
        print(f"    Date range: {row[4]} to {row[5]}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    recreate_and_load()