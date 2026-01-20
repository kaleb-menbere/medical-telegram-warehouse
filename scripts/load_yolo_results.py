import psycopg2
import pandas as pd
import os

def load_yolo_results():
    """
    Load YOLO detection results into PostgreSQL database.
    """
    
    csv_file = "data/yolo_detections.csv"
    
    if not os.path.exists(csv_file):
        print(f"❌ CSV file not found: {csv_file}")
        print("Run yolo_detect.py first to generate detections.")
        return
    
    # Read CSV
    df = pd.read_csv(csv_file)
    print(f"📄 Loaded {len(df)} detections from CSV")
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        database="telegram_warehouse",
        user="postgres",
        password="password"  
    )
    
    cursor = conn.cursor()
    
    # Create table for YOLO results
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS raw.yolo_detections (
        detection_id SERIAL PRIMARY KEY,
        message_id BIGINT,
        channel_name VARCHAR(255),
        image_path VARCHAR(500),
        detected_objects TEXT,
        num_detections INTEGER,
        image_category VARCHAR(50),
        confidence_score FLOAT,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_sql)
    
    # Clear existing data
    cursor.execute("TRUNCATE TABLE raw.yolo_detections;")
    
    # Insert data
    insert_sql = """
    INSERT INTO raw.yolo_detections 
    (message_id, channel_name, image_path, detected_objects, 
     num_detections, image_category, confidence_score)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    for _, row in df.iterrows():
        cursor.execute(insert_sql, (
            row['message_id'],
            row['channel_name'],
            row['image_path'],
            row['detected_objects'],
            row['num_detections'],
            row['image_category'],
            row['confidence_score']
        ))
    
    conn.commit()
    
    # Verify
    cursor.execute("SELECT COUNT(*) FROM raw.yolo_detections;")
    count = cursor.fetchone()[0]
    print(f"✅ Loaded {count} detections into raw.yolo_detections")
    
    # Show summary
    print("\n📊 Database Summary:")
    cursor.execute("""
        SELECT 
            image_category,
            COUNT(*) as count,
            ROUND(AVG(confidence_score)::numeric, 2) as avg_confidence,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as percentage
        FROM raw.yolo_detections
        GROUP BY image_category
        ORDER BY count DESC;
    """)
    
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} images (avg confidence: {row[2]}, {row[3]}%)")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    load_yolo_results()