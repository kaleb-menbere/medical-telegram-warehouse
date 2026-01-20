import psycopg2
import pandas as pd
import os
from getpass import getpass

def analyze_yolo_results():
    """
    Analyze YOLO detection results and answer business questions.
    """
    
    # Try to get password from environment variable first
    db_password = os.getenv('DB_PASSWORD')
    
    if not db_password:
        # If not in environment, try common passwords or ask
        common_passwords = ['postgres', 'password', 'admin', 'root']
        
        for pwd in common_passwords:
            try:
                # Test connection with common password
                conn = psycopg2.connect(
                    host="localhost",
                    database="telegram_warehouse",
                    user="postgres",
                    password=pwd,
                    connect_timeout=2
                )
                conn.close()
                db_password = pwd
                print(f"✓ Using password: {pwd}")
                break
            except:
                continue
        
        if not db_password:
            # Ask user for password
            db_password = getpass("Enter PostgreSQL password for user 'postgres': ")
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="telegram_warehouse",
            user="postgres",
            password=db_password
        )
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\nTrying common passwords...")
        return
    
    print("📊 YOLO DETECTION ANALYSIS")
    print("=" * 60)
    
    # 1. Category distribution
    print("\n1. IMAGE CATEGORY DISTRIBUTION:")
    query1 = """
    SELECT 
        image_category,
        COUNT(*) as image_count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as percentage,
        ROUND(AVG(confidence_score)::numeric, 2) as avg_confidence
    FROM raw.yolo_detections
    GROUP BY image_category
    ORDER BY image_count DESC;
    """
    
    try:
        df1 = pd.read_sql_query(query1, conn)
        print(df1.to_string(index=False))
    except Exception as e:
        print(f"Error: {e}")
    
    # 2. Do "promotional" posts get more views?
    print("\n2. ENGAGEMENT BY IMAGE CATEGORY:")
    query2 = """
    WITH category_stats AS (
        SELECT 
            i.image_category,
            COUNT(*) as post_count,
            ROUND(AVG(f.view_count)::numeric, 0) as avg_views,
            ROUND(AVG(f.forward_count)::numeric, 1) as avg_forwards,
            ROUND(AVG(i.confidence_score)::numeric, 2) as avg_confidence
        FROM medical_warehouse_marts.fct_image_detections i
        JOIN medical_warehouse_marts.fct_messages f ON i.message_id = f.message_id
        GROUP BY i.image_category
    )
    SELECT * FROM category_stats
    ORDER BY avg_views DESC;
    """
    
    try:
        df2 = pd.read_sql_query(query2, conn)
        print(df2.to_string(index=False))
    except Exception as e:
        print(f"Error: {e}")
    
    # 3. Which channels use more visual content?
    print("\n3. VISUAL CONTENT BY CHANNEL:")
    query3 = """
    SELECT 
        c.channel_name,
        c.channel_type,
        COUNT(DISTINCT f.message_id) as total_posts,
        COUNT(DISTINCT i.message_id) as posts_with_images,
        ROUND(100.0 * COUNT(DISTINCT i.message_id) / NULLIF(COUNT(DISTINCT f.message_id), 0), 1) as image_percentage,
        STRING_AGG(DISTINCT i.image_category, ', ') as detected_categories
    FROM medical_warehouse_marts.fct_messages f
    JOIN medical_warehouse_marts.dim_channels c ON f.channel_key = c.channel_key
    LEFT JOIN medical_warehouse_marts.fct_image_detections i ON f.message_id = i.message_id
    GROUP BY c.channel_name, c.channel_type
    ORDER BY image_percentage DESC NULLS LAST;
    """
    
    try:
        df3 = pd.read_sql_query(query3, conn)
        print(df3.to_string(index=False))
    except Exception as e:
        print(f"Error: {e}")
    
    # 4. Most common detected objects
    print("\n4. MOST COMMON DETECTED OBJECTS:")
    query4 = """
    WITH objects AS (
        SELECT 
            TRIM(UNNEST(STRING_TO_ARRAY(detected_objects, ','))) as object_name
        FROM raw.yolo_detections
        WHERE detected_objects != ''
    )
    SELECT 
        object_name,
        COUNT(*) as detection_count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as percentage
    FROM objects
    GROUP BY object_name
    ORDER BY detection_count DESC
    LIMIT 10;
    """
    
    try:
        df4 = pd.read_sql_query(query4, conn)
        print(df4.to_string(index=False))
    except Exception as e:
        print(f"Error: {e}")
    
    # 5. Limitations analysis
    print("\n5. YOLO MODEL LIMITATIONS ANALYSIS:")
    print("   • Pre-trained YOLOv8 detects general objects (person, bottle, etc.)")
    print("   • Cannot identify specific medical products or drugs")
    print("   • May misclassify domain-specific items")
    print("   • Confidence scores may vary based on image quality")
    
    conn.close()
    
    print("\n" + "=" * 60)
    print("✅ Analysis complete! Key insights:")
    print("   - Check which image categories get most engagement")
    print("   - See which channels use more visual content")
    print("   - Understand YOLO's capabilities and limitations")

if __name__ == "__main__":
    analyze_yolo_results()