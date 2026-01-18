# scripts/load_raw_data.py
import json
import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_raw_data():
    """
    Load JSON files from data lake into PostgreSQL raw schema
    """
    # Database connection (use environment variables in production)
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'medical_warehouse')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')
    
    # Create connection string
    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    # Data lake directory
    data_lake_path = "data/raw/telegram_messages"
    
    try:
        # Connect to PostgreSQL
        engine = create_engine(connection_string)
        
        # Create raw schema if not exists
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
            logger.info("Raw schema created/verified")
        
        # List all JSON files
        json_files = []
        for root, dirs, files in os.walk(data_lake_path):
            for file in files:
                if file.endswith('.json'):
                    json_files.append(os.path.join(root, file))
        
        if not json_files:
            logger.warning(f"No JSON files found in {data_lake_path}")
            return
        
        logger.info(f"Found {len(json_files)} JSON files to load")
        
        # Read and combine all JSON files
        all_data = []
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # If data is a list of messages, extend; if single dict, append
                    if isinstance(data, list):
                        all_data.extend(data)
                    else:
                        all_data.append(data)
            except Exception as e:
                logger.error(f"Error reading {json_file}: {e}")
        
        if not all_data:
            logger.warning("No data to load")
            return
        
        # Convert to DataFrame
        df = pd.json_normalize(all_data)
        
        # Ensure all required columns exist
        expected_columns = ['message_id', 'channel_name', 'message_date', 
                           'message_text', 'has_media', 'views', 'forwards']
        
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None
        
        # Select only the columns we need
        df = df[expected_columns + [col for col in df.columns if col not in expected_columns]]
        
        # Create table in PostgreSQL
        df.to_sql('telegram_messages', engine, schema='raw', 
                 if_exists='replace', index=False, method='multi')
        
        logger.info(f"Successfully loaded {len(df)} records to raw.telegram_messages")
        
        # Add indexes for better query performance
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_message_id ON raw.telegram_messages(message_id);
                CREATE INDEX IF NOT EXISTS idx_channel_name ON raw.telegram_messages(channel_name);
                CREATE INDEX IF NOT EXISTS idx_message_date ON raw.telegram_messages(message_date);
            """))
            logger.info("Indexes created on raw table")
            
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

if __name__ == "__main__":
    load_raw_data()