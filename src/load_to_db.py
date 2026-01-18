"""
Load scraped Telegram data into database (SQLite for development, PostgreSQL for production).
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, Boolean, Text, Float, MetaData
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import structlog

# Load environment variables
load_dotenv()

# Configure logging
logger = structlog.get_logger()


class DataLoader:
    def __init__(self, use_sqlite=True):
        """Initialize database loader."""
        self.use_sqlite = use_sqlite
        
        if self.use_sqlite:
            # SQLite configuration
            self.db_path = Path('./data/telegram_warehouse.db')
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self.connection_string = f"sqlite:///{self.db_path}"
            logger.info(f"Using SQLite database: {self.db_path}")
        else:
            # PostgreSQL configuration
            self.db_config = {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': os.getenv('DB_PORT', '5432'),
                'database': os.getenv('DB_NAME', 'telegram_warehouse'),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD', 'postgres')
            }
            self.connection_string = (
                f"postgresql://{self.db_config['user']}:{self.db_config['password']}"
                f"@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            )
            logger.info(f"Using PostgreSQL database: {self.db_config['database']}")
        
        # Data directories
        self.base_dir = Path(os.getenv('DATA_DIR', './data'))
        self.raw_dir = self.base_dir / 'raw'
        self.messages_dir = self.raw_dir / 'telegram_messages'
        self.channels_dir = self.raw_dir / 'channels'
        
        # Create database connection
        self.engine = self.create_engine()
        self.metadata = MetaData()
        
    def create_engine(self):
        """Create SQLAlchemy engine."""
        try:
            engine = create_engine(self.connection_string)
            # Test connection
            with engine.connect() as conn:
                logger.info(f"Connected to database successfully")
            return engine
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
    
    def create_tables(self):
        """Create raw schema tables."""
        logger.info("Creating database tables...")
        
        # Define raw.telegram_messages table
        self.raw_messages = Table(
            'telegram_messages',
            self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('message_id', Integer, nullable=False),
            Column('channel_id', Integer),
            Column('channel_username', String(255)),
            Column('channel_name', String(255)),
            Column('message_date', DateTime),
            Column('message_text', Text),
            Column('message_raw', Text if self.use_sqlite else JSONB),  # JSONB for PostgreSQL, Text for SQLite
            Column('has_media', Boolean),
            Column('media_type', String(100)),
            Column('image_path', String(500)),
            Column('views', Integer),
            Column('forwards', Integer),
            Column('replies', Integer),
            Column('edited', Boolean),
            Column('edit_date', DateTime),
            Column('pinned', Boolean),
            Column('via_bot', Integer),
            Column('scraped_at', DateTime),
            Column('scraping_session_id', String(100)),
            Column('loaded_at', DateTime, default=datetime.now),
            
            # SQLite doesn't support schemas, so we'll use prefixes
            schema=None if self.use_sqlite else 'raw'
        )
        
        # Define raw.telegram_channels table
        self.raw_channels = Table(
            'telegram_channels',
            self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('channel_id', Integer, unique=True),
            Column('channel_username', String(255)),
            Column('channel_name', String(255)),
            Column('description', Text),
            Column('participants_count', Integer),
            Column('date_created', DateTime),
            Column('scraped_at', DateTime),
            Column('is_verified', Boolean),
            Column('is_scam', Boolean),
            Column('total_messages', Integer),
            Column('channel_raw', Text if self.use_sqlite else JSONB),
            Column('loaded_at', DateTime, default=datetime.now),
            
            schema=None if self.use_sqlite else 'raw'
        )
        
        # Create tables
        try:
            self.metadata.create_all(self.engine, checkfirst=True)
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            raise
    
    def load_channels(self):
        """Load channel information from JSON files."""
        logger.info("Loading channel data...")
        
        channel_files = list(self.channels_dir.glob('*_info.json'))
        
        if not channel_files:
            logger.warning("No channel files found")
            return 0
        
        channels_data = []
        for filepath in channel_files:
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    channel_info = json.load(f)
                
                # Prepare data for database
                channel_data = {
                    'channel_id': channel_info.get('channel_id'),
                    'channel_username': channel_info.get('channel_username'),
                    'channel_name': channel_info.get('channel_name'),
                    'description': channel_info.get('description'),
                    'participants_count': channel_info.get('participants_count'),
                    'date_created': self.parse_datetime(channel_info.get('date_created')),
                    'scraped_at': self.parse_datetime(channel_info.get('scraped_at')),
                    'is_verified': channel_info.get('is_verified', False),
                    'is_scam': channel_info.get('is_scam', False),
                    'total_messages': channel_info.get('total_messages', 0),
                    'channel_raw': json.dumps(channel_info)
                }
                
                channels_data.append(channel_data)
                
            except Exception as e:
                logger.error(f"Error loading channel file {filepath}: {str(e)}")
        
        if channels_data:
            try:
                # Use pandas to load data
                df = pd.DataFrame(channels_data)
                
                # Table name depends on database type
                table_name = 'raw_telegram_channels' if self.use_sqlite else 'raw.telegram_channels'
                
                df.to_sql(
                    table_name.replace('.', '_').replace('raw_', '') if self.use_sqlite else table_name.split('.')[1],
                    self.engine,
                    if_exists='replace' if self.use_sqlite else 'append',  # Replace for SQLite, append for PostgreSQL
                    index=False,
                    method=None  # Let pandas handle it
                )
                
                logger.info(f"Loaded {len(channels_data)} channels to database")
                return len(channels_data)
                
            except Exception as e:
                logger.error(f"Error saving channels to database: {str(e)}")
        
        return 0
    
    def load_messages(self):
        """Load messages from JSON files."""
        logger.info("Loading message data...")
        
        # Find all JSON files
        json_files = []
        for date_dir in self.messages_dir.iterdir():
            if date_dir.is_dir():
                for json_file in date_dir.glob('*.json'):
                    json_files.append(json_file)
        
        if not json_files:
            logger.warning("No message files found")
            return 0
        
        total_messages = 0
        for filepath in json_files:
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    messages = json.load(f)
                
                # Prepare data for database
                messages_data = []
                for message in messages:
                    message_data = {
                        'message_id': message.get('message_id'),
                        'channel_id': message.get('channel_id'),
                        'channel_username': message.get('channel_username'),
                        'channel_name': message.get('channel_name'),
                        'message_date': self.parse_datetime(message.get('message_date')),
                        'message_text': message.get('message_text'),
                        'message_raw': json.dumps(message),
                        'has_media': message.get('has_media', False),
                        'media_type': message.get('media_type'),
                        'image_path': message.get('image_path'),
                        'views': message.get('views', 0),
                        'forwards': message.get('forwards', 0),
                        'replies': message.get('replies', 0),
                        'edited': message.get('edited', False),
                        'edit_date': self.parse_datetime(message.get('edit_date')),
                        'pinned': message.get('pinned', False),
                        'via_bot': message.get('via_bot'),
                        'scraped_at': self.parse_datetime(message.get('scraped_at')),
                        'scraping_session_id': message.get('scraping_session_id')
                    }
                    messages_data.append(message_data)
                
                if messages_data:
                    # Use pandas to load data
                    df = pd.DataFrame(messages_data)
                    
                    # Table name depends on database type
                    table_name = 'raw_telegram_messages' if self.use_sqlite else 'raw.telegram_messages'
                    
                    df.to_sql(
                        table_name.replace('.', '_').replace('raw_', '') if self.use_sqlite else table_name.split('.')[1],
                        self.engine,
                        if_exists='replace' if self.use_sqlite else 'append',  # Replace for SQLite, append for PostgreSQL
                        index=False,
                        method=None
                    )
                    
                    total_messages += len(messages_data)
                    logger.info(f"Loaded {len(messages_data)} messages from {filepath.name}")
                    
            except Exception as e:
                logger.error(f"Error loading message file {filepath}: {str(e)}")
        
        logger.info(f"Total messages loaded: {total_messages}")
        return total_messages
    
    def parse_datetime(self, dt_str):
        """Parse datetime string to datetime object."""
        if not dt_str:
            return None
        
        try:
            # Handle various datetime formats
            for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
                try:
                    return datetime.strptime(dt_str, fmt)
                except ValueError:
                    continue
            
            # If none of the formats work, try ISO format
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
            
        except Exception:
            return None
    
    def create_sample_queries(self):
        """Create sample queries to verify data."""
        logger.info("\nCreating sample queries...")
        
        queries = [
            ("Total channels", "SELECT COUNT(*) as channel_count FROM telegram_channels"),
            ("Total messages", "SELECT COUNT(*) as message_count FROM telegram_messages"),
            ("Messages with media", "SELECT COUNT(*) as media_count FROM telegram_messages WHERE has_media = TRUE"),
            ("Top channels by message count", """
                SELECT channel_name, COUNT(*) as message_count 
                FROM telegram_messages 
                GROUP BY channel_name 
                ORDER BY message_count DESC
            """),
            ("Average views per channel", """
                SELECT channel_name, AVG(views) as avg_views 
                FROM telegram_messages 
                GROUP BY channel_name 
                ORDER BY avg_views DESC
            """)
        ]
        
        for query_name, query in queries:
            try:
                result = pd.read_sql_query(query, self.engine)
                logger.info(f"{query_name}: {result.iloc[0,0]}")
            except Exception as e:
                logger.error(f"Error running query '{query_name}': {str(e)}")
    
    def run(self):
        """Run the complete loading process."""
        logger.info("Starting data loading...")
        
        try:
            # Create tables
            self.create_tables()
            
            # Load data
            channels_loaded = self.load_channels()
            messages_loaded = self.load_messages()
            
            # Create sample queries
            self.create_sample_queries()
            
            # Summary
            logger.info("\n" + "="*60)
            logger.info("LOADING SUMMARY")
            logger.info("="*60)
            logger.info(f"Database type: {'SQLite' if self.use_sqlite else 'PostgreSQL'}")
            logger.info(f"Channels loaded: {channels_loaded}")
            logger.info(f"Messages loaded: {messages_loaded}")
            logger.info(f"Total records: {channels_loaded + messages_loaded}")
            logger.info("="*60)
            logger.info("✅ Data loading completed successfully!")
            
            # Save database info
            if self.use_sqlite:
                logger.info(f"Database file: {self.db_path.absolute()}")
                logger.info(f"File size: {self.db_path.stat().st_size / 1024:.1f} KB")
            
        except Exception as e:
            logger.error(f"❌ Data loading failed: {str(e)}")
            raise


def main():
    """Main entry point."""
    print("Telegram Data Loader")
    print("="*60)
    print("Options:")
    print("1. Use SQLite (recommended for development)")
    print("2. Use PostgreSQL (requires running database)")
    print("="*60)
    
    choice = input("Choose option (1 or 2): ").strip()
    use_sqlite = choice == "1"
    
    try:
        loader = DataLoader(use_sqlite=use_sqlite)
        loader.run()
        
        print("\n✅ Data loaded successfully!")
        print("\nNext steps:")
        if use_sqlite:
            print("1. Database saved to: data/telegram_warehouse.db")
            print("2. You can explore it with: sqlite3 data/telegram_warehouse.db")
        else:
            print("1. Data is in PostgreSQL database: telegram_warehouse")
            print("2. Connect with: psql -U postgres -d telegram_warehouse")
        print("\nProceed to dbt transformations...")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()