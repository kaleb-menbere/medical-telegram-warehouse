"""
Telegram Data Scraper for Ethiopian Medical Businesses
Extracts messages and images from Telegram channels and stores them in a data lake.
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any
import traceback

# Add the parent directory to sys.path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import aiofiles
    from telethon import TelegramClient
    from telethon.tl.types import (
        Message,
        MessageMediaPhoto,
        MessageMediaDocument,
        Channel
    )
    from telethon.tl.functions.channels import GetFullChannelRequest
    from telethon.errors import (
        FloodWaitError,
        ChannelPrivateError,
        ChatAdminRequiredError
    )
    import pandas as pd
    from dotenv import load_dotenv
    from tqdm import tqdm
    import structlog
except ImportError as e:
    print(f"❌ Missing required package: {e}")
    print("Please install requirements with: pip install -r requirements.txt")
    sys.exit(1)

# Load environment variables
load_dotenv()

# Configure structured logging
logger = structlog.get_logger()


def make_naive(dt: datetime) -> datetime:
    """Convert an aware datetime to naive datetime in UTC."""
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


class TelegramScraper:
    def __init__(self):
        """Initialize the Telegram scraper with configuration."""
        # Telegram API credentials
        self.api_id = os.getenv('TELEGRAM_API_ID')
        self.api_hash = os.getenv('TELEGRAM_API_HASH')
        self.phone = os.getenv('TELEGRAM_PHONE')
        
        if not all([self.api_id, self.api_hash, self.phone]):
            raise ValueError(
                "Missing Telegram API credentials in .env file. "
                "Please set TELEGRAM_API_ID, TELEGRAM_API_HASH, and TELEGRAM_PHONE"
            )
        
        # Project directories
        self.base_dir = Path(os.getenv('DATA_DIR', './data'))
        self.raw_dir = self.base_dir / 'raw'
        self.images_dir = self.raw_dir / 'images'
        self.messages_dir = self.base_dir / 'raw' / 'telegram_messages'
        self.logs_dir = Path(os.getenv('LOG_DIR', './logs'))
        
        # Create directories
        for directory in [self.raw_dir, self.images_dir, self.messages_dir, self.logs_dir]:
            directory.mkdir(parents=True, exist_ok=True)
        
        # Scraping configuration
        self.max_messages = int(os.getenv('MAX_MESSAGES_PER_CHANNEL', 100))
        self.days_back = int(os.getenv('SCRAPE_DAYS_BACK', 7))
        self.max_retries = int(os.getenv('MAX_RETRIES', 3))
        
        # Target Telegram channels (with their usernames or links)
        self.channels = [
            'chemed_ethiopia',  # CheMed Telegram Channel
            'lobelia4cosmetics',  # Lobelia Cosmetics
            'tikvahpharma',  # Tikvah Pharma
        ]
        
        # Additional channels from et.tgstat.com/medicine (add more as needed)
        self.additional_channels = [
            # Add more Ethiopian medical channels here
            # Example: 'pharma_ethiopia'
        ]
        
        # Combine all channels
        self.all_channels = self.channels + self.additional_channels
        
        # Initialize Telegram client
        self.client = None
        self.session_file = 'telegram_scraper.session'
        
        # Statistics
        self.scraping_stats = {
            'start_time': None,
            'end_time': None,
            'total_messages': 0,
            'total_images': 0,
            'channels_success': 0,
            'channels_failed': 0,
            'channel_details': []
        }
    
    async def connect(self):
        """Connect to Telegram with retry logic."""
        retries = 0
        while retries < self.max_retries:
            try:
                logger.info(f"Connecting to Telegram (attempt {retries + 1}/{self.max_retries})")
                
                self.client = TelegramClient(
                    self.session_file,
                    int(self.api_id),
                    self.api_hash
                )
                
                await self.client.start(phone=self.phone)
                
                # Test connection
                me = await self.client.get_me()
                logger.info(f"Connected to Telegram as: {me.username or me.phone}")
                return True
                
            except FloodWaitError as e:
                wait_time = e.seconds
                logger.warning(f"Flood wait error: Need to wait {wait_time} seconds")
                await asyncio.sleep(wait_time)
                retries += 1
                
            except Exception as e:
                logger.error(f"Connection error: {str(e)}")
                retries += 1
                if retries < self.max_retries:
                    await asyncio.sleep(5)  # Wait before retry
        
        logger.error("Failed to connect to Telegram after all retries")
        return False
    
    async def get_channel_info(self, channel_username: str) -> Optional[Dict]:
        """Get detailed information about a Telegram channel."""
        try:
            logger.info(f"Fetching info for channel: {channel_username}")
            
            # Remove @ symbol if present
            if channel_username.startswith('@'):
                channel_username = channel_username[1:]
            
            # Try to get the channel entity
            try:
                entity = await self.client.get_entity(channel_username)
            except ValueError:
                # Try with @ symbol
                entity = await self.client.get_entity(f'@{channel_username}')
            
            if isinstance(entity, Channel):
                # Get full channel details
                full_channel = await self.client(GetFullChannelRequest(channel=entity))
                
                channel_info = {
                    'channel_id': entity.id,
                    'channel_username': entity.username or channel_username,
                    'channel_name': entity.title,
                    'description': entity.about if hasattr(entity, 'about') else '',
                    'participants_count': entity.participants_count if hasattr(entity, 'participants_count') else 0,
                    'date_created': entity.date.isoformat() if entity.date else None,
                    'scraped_at': datetime.now(timezone.utc).isoformat(),
                    'is_verified': entity.verified if hasattr(entity, 'verified') else False,
                    'is_scam': entity.scam if hasattr(entity, 'scam') else False,
                    'total_messages': full_channel.full_chat.read_inbox_max_id if hasattr(full_channel.full_chat, 'read_inbox_max_id') else 0,
                }
                
                logger.info(f"Channel info retrieved: {channel_info['channel_name']}")
                return channel_info
            else:
                logger.warning(f"Entity is not a channel: {channel_username}")
                return None
                
        except ChannelPrivateError:
            logger.error(f"Channel is private: {channel_username}")
            return None
        except ChatAdminRequiredError:
            logger.error(f"Admin rights required for: {channel_username}")
            return None
        except Exception as e:
            logger.error(f"Error getting channel info for {channel_username}: {str(e)}")
            return None
    
    async def download_media(self, message: Message, channel_name: str) -> Optional[str]:
        """Download media from a message if present."""
        if not message.media:
            return None
        
        try:
            # Create channel-specific image directory
            channel_image_dir = self.images_dir / channel_name
            channel_image_dir.mkdir(parents=True, exist_ok=True)
            
            # Check if it's a photo
            if isinstance(message.media, MessageMediaPhoto):
                # Generate filename
                timestamp = make_naive(message.date).strftime('%Y%m%d_%H%M%S')
                filename = f"{message.id}_{timestamp}.jpg"
                filepath = channel_image_dir / filename
                
                # Download the image
                await message.download_media(file=str(filepath))
                
                logger.debug(f"Downloaded image: {filepath}")
                return str(filepath.relative_to(self.base_dir))
            
            # Check if it's a document with image extension
            elif isinstance(message.media, MessageMediaDocument):
                document = message.media.document
                if document.mime_type and 'image' in document.mime_type:
                    # Get file extension from mime type or attributes
                    ext = '.jpg'  # default
                    for attr in document.attributes:
                        if hasattr(attr, 'file_name') and attr.file_name:
                            ext = Path(attr.file_name).suffix or '.jpg'
                    
                    timestamp = make_naive(message.date).strftime('%Y%m%d_%H%M%S')
                    filename = f"{message.id}_{timestamp}{ext}"
                    filepath = channel_image_dir / filename
                    
                    await message.download_media(file=str(filepath))
                    
                    logger.debug(f"Downloaded document image: {filepath}")
                    return str(filepath.relative_to(self.base_dir))
            
            return None
            
        except Exception as e:
            logger.error(f"Error downloading media for message {message.id}: {str(e)}")
            return None
    
    def extract_message_data(self, message: Message, channel_info: Dict) -> Dict:
        """Extract relevant data from a Telegram message."""
        try:
            message_date = message.date
            if message_date:
                message_date_naive = make_naive(message_date)
                message_date_str = message_date_naive.isoformat()
            else:
                message_date_str = None
            
            edit_date = None
            if message.edit_date:
                edit_date = make_naive(message.edit_date).isoformat()
            
            message_data = {
                'message_id': message.id,
                'channel_id': channel_info['channel_id'],
                'channel_username': channel_info['channel_username'],
                'channel_name': channel_info['channel_name'],
                'message_date': message_date_str,
                'message_text': message.text or '',
                'message_raw': message.to_dict() if hasattr(message, 'to_dict') else {},
                
                # Media information
                'has_media': message.media is not None,
                'media_type': type(message.media).__name__ if message.media else None,
                'image_path': None,
                
                # Engagement metrics
                'views': message.views if hasattr(message, 'views') and message.views else 0,
                'forwards': message.forwards if hasattr(message, 'forwards') and message.forwards else 0,
                'replies': message.replies.replies if hasattr(message, 'replies') and message.replies else 0,
                
                # Additional metadata
                'edited': message.edit_date is not None,
                'edit_date': edit_date,
                'pinned': message.pinned,
                'via_bot': message.via_bot_id if hasattr(message, 'via_bot_id') else None,
                
                # Scraping metadata
                'scraped_at': datetime.now(timezone.utc).isoformat(),
                'scraping_session_id': self.scraping_stats['start_time']
            }
            
            return message_data
            
        except Exception as e:
            logger.error(f"Error extracting message data: {str(e)}")
            return {}
    
    async def scrape_channel_messages(self, channel_username: str, channel_info: Dict) -> List[Dict]:
        """Scrape messages from a Telegram channel."""
        messages = []
        images_downloaded = 0
        
        try:
            logger.info(f"Starting to scrape messages from {channel_username}")
            
            # Get channel entity
            entity = await self.client.get_entity(channel_username)
            
            # Calculate date range for scraping
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=self.days_back)
            
            # Make dates naive for comparison
            end_date_naive = make_naive(end_date)
            start_date_naive = make_naive(start_date)
            
            logger.info(f"Scraping messages from {start_date_naive.date()} to {end_date_naive.date()}")
            
            # Use tqdm for progress bar
            with tqdm(total=self.max_messages, desc=f"Scraping {channel_username}", unit="msg") as pbar:
                message_count = 0
                
                async for message in self.client.iter_messages(
                    entity,
                    limit=self.max_messages,
                    offset_date=end_date,
                    reverse=False
                ):
                    # Skip messages older than our date range
                    if message.date:
                        message_date_naive = make_naive(message.date)
                        if message_date_naive < start_date_naive:
                            break
                    
                    # Skip service messages
                    if message.action:
                        continue
                    
                    # Extract message data
                    message_data = self.extract_message_data(message, channel_info)
                    
                    if message_data:
                        # Download media if present
                        if message.media:
                            image_path = await self.download_media(message, channel_username)
                            if image_path:
                                message_data['image_path'] = image_path
                                images_downloaded += 1
                        
                        messages.append(message_data)
                    
                    message_count += 1
                    pbar.update(1)
                    
                    # Add small delay to avoid rate limiting
                    if message_count % 50 == 0:
                        await asyncio.sleep(1)
            
            logger.info(f"Scraped {len(messages)} messages from {channel_username} ({images_downloaded} images downloaded)")
            return messages, images_downloaded
            
        except FloodWaitError as e:
            logger.error(f"Flood wait error for {channel_username}: {e.seconds} seconds")
            raise
        except Exception as e:
            logger.error(f"Error scraping messages from {channel_username}: {str(e)}")
            logger.debug(traceback.format_exc())
            return [], 0
    
    async def save_messages_to_json(self, channel_username: str, messages: List[Dict]):
        """Save scraped messages to JSON file with date-based partitioning."""
        if not messages:
            logger.warning(f"No messages to save for {channel_username}")
            return
        
        try:
            # Group messages by date
            messages_by_date = {}
            for message in messages:
                if message.get('message_date'):
                    try:
                        msg_date = datetime.fromisoformat(message['message_date']).date()
                        date_str = msg_date.strftime('%Y-%m-%d')
                        
                        if date_str not in messages_by_date:
                            messages_by_date[date_str] = []
                        messages_by_date[date_str].append(message)
                    except ValueError as e:
                        logger.warning(f"Invalid date format in message {message.get('message_id')}: {e}")
                        continue
            
            # Save each date's messages to separate file
            for date_str, date_messages in messages_by_date.items():
                # Create date directory
                date_dir = self.messages_dir / date_str
                date_dir.mkdir(parents=True, exist_ok=True)
                
                # Generate filename
                safe_channel_name = channel_username.replace('/', '_').replace('\\', '_')
                filename = f"{safe_channel_name}_{date_str}.json"
                filepath = date_dir / filename
                
                # Save to JSON file
                async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                    await f.write(json.dumps(date_messages, indent=2, ensure_ascii=False, default=str))
                
                logger.info(f"Saved {len(date_messages)} messages to {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving messages to JSON for {channel_username}: {str(e)}")
    
    async def save_channel_info(self, channel_info: Dict):
        """Save channel information to a JSON file."""
        if not channel_info:
            return
        
        try:
            # Create channels directory
            channels_dir = self.raw_dir / 'channels'
            channels_dir.mkdir(parents=True, exist_ok=True)
            
            # Save channel info
            safe_username = channel_info['channel_username'].replace('/', '_').replace('\\', '_')
            filename = f"{safe_username}_info.json"
            filepath = channels_dir / filename
            
            async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(channel_info, indent=2, ensure_ascii=False, default=str))
            
            logger.info(f"Saved channel info to {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving channel info: {str(e)}")
    
    def save_scraping_summary(self):
        """Save scraping summary to a JSON file."""
        try:
            summary_file = self.logs_dir / f"scraping_summary_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
            
            summary = {
                **self.scraping_stats,
                'end_time': datetime.now(timezone.utc).isoformat(),
                'duration_seconds': (
                    datetime.now(timezone.utc) - datetime.fromisoformat(self.scraping_stats['start_time']).replace(tzinfo=timezone.utc)
                ).total_seconds() if self.scraping_stats['start_time'] else 0,
                'configuration': {
                    'max_messages_per_channel': self.max_messages,
                    'days_back': self.days_back,
                    'max_retries': self.max_retries,
                    'channels_targeted': len(self.all_channels)
                }
            }
            
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, default=str)
            
            logger.info(f"Scraping summary saved to {summary_file}")
            
            # Also print summary to console
            self.print_summary(summary)
            
        except Exception as e:
            logger.error(f"Error saving scraping summary: {str(e)}")
    
    def print_summary(self, summary: Dict):
        """Print scraping summary to console."""
        print("\n" + "="*60)
        print("SCRAPING SUMMARY")
        print("="*60)
        print(f"Start Time: {summary['start_time']}")
        print(f"End Time: {summary['end_time']}")
        print(f"Duration: {summary['duration_seconds']:.2f} seconds")
        print(f"Total Channels: {len(summary['channel_details'])}")
        print(f"Successful: {summary['channels_success']}")
        print(f"Failed: {summary['channels_failed']}")
        print(f"Total Messages: {summary['total_messages']}")
        print(f"Total Images: {summary['total_images']}")
        print("-"*60)
        
        for channel in summary['channel_details']:
            status = "✓" if channel['success'] else "✗"
            print(f"{status} {channel['channel']}: {channel['messages_scraped']} msgs, {channel['images_downloaded']} imgs")
        
        print("="*60)
    
    async def scrape_single_channel(self, channel: str) -> Dict:
        """Scrape a single Telegram channel."""
        channel_result = {
            'channel': channel,
            'messages_scraped': 0,
            'images_downloaded': 0,
            'success': False,
            'error': None
        }
        
        try:
            logger.info(f"\n{'='*50}")
            logger.info(f"Processing channel: {channel}")
            
            # Get channel information
            channel_info = await self.get_channel_info(channel)
            if not channel_info:
                channel_result['error'] = "Failed to get channel info"
                logger.error(f"Failed to get info for channel: {channel}")
                return channel_result
            
            # Save channel info
            await self.save_channel_info(channel_info)
            
            # Scrape messages
            messages, images_downloaded = await self.scrape_channel_messages(channel, channel_info)
            
            # Save messages
            if messages:
                await self.save_messages_to_json(channel, messages)
            
            # Update result
            channel_result['messages_scraped'] = len(messages)
            channel_result['images_downloaded'] = images_downloaded
            channel_result['success'] = True
            
            logger.info(f"✓ Successfully scraped {channel}: {len(messages)} messages, {images_downloaded} images")
            
            # Rate limiting - be gentle with the API
            await asyncio.sleep(2)
            
            return channel_result
            
        except FloodWaitError as e:
            error_msg = f"Flood wait error: {e.seconds} seconds"
            channel_result['error'] = error_msg
            logger.error(f"✗ {error_msg} for channel: {channel}")
            
            # Wait as instructed by Telegram
            await asyncio.sleep(e.seconds)
            
        except Exception as e:
            error_msg = str(e)
            channel_result['error'] = error_msg
            logger.error(f"✗ Failed to scrape {channel}: {error_msg}")
            logger.debug(traceback.format_exc())
        
        return channel_result
    
    async def scrape_all_channels(self):
        """Scrape all configured Telegram channels."""
        # Initialize statistics
        self.scraping_stats = {
            'start_time': datetime.now(timezone.utc).isoformat(),
            'end_time': None,
            'total_messages': 0,
            'total_images': 0,
            'channels_success': 0,
            'channels_failed': 0,
            'channel_details': []
        }
        
        logger.info("Starting Telegram scraping process")
        logger.info(f"Target channels: {self.all_channels}")
        logger.info(f"Max messages per channel: {self.max_messages}")
        logger.info(f"Days to look back: {self.days_back}")
        
        # Connect to Telegram
        if not await self.connect():
            logger.error("Failed to connect to Telegram. Exiting.")
            return
        
        # Scrape each channel
        for channel in self.all_channels:
            channel_result = await self.scrape_single_channel(channel)
            
            # Update statistics
            self.scraping_stats['channel_details'].append(channel_result)
            self.scraping_stats['total_messages'] += channel_result['messages_scraped']
            self.scraping_stats['total_images'] += channel_result['images_downloaded']
            
            if channel_result['success']:
                self.scraping_stats['channels_success'] += 1
            else:
                self.scraping_stats['channels_failed'] += 1
        
        # Save summary
        self.scraping_stats['end_time'] = datetime.now(timezone.utc).isoformat()
        self.save_scraping_summary()
        
        # Disconnect
        await self.client.disconnect()
        logger.info("Disconnected from Telegram")
        
        logger.info("\n" + "="*50)
        logger.info("Scraping completed!")
        logger.info(f"Total messages scraped: {self.scraping_stats['total_messages']}")
        logger.info(f"Total images downloaded: {self.scraping_stats['total_images']}")
        logger.info(f"Successful channels: {self.scraping_stats['channels_success']}/{len(self.all_channels)}")
        logger.info("="*50)
    
    def run(self):
        """Run the scraper synchronously."""
        try:
            asyncio.run(self.scrape_all_channels())
        except KeyboardInterrupt:
            logger.info("\nScraping interrupted by user")
            sys.exit(0)
        except Exception as e:
            logger.error(f"Fatal error in scraper: {str(e)}")
            logger.debug(traceback.format_exc())
            sys.exit(1)


def main():
    """Main entry point for the scraper."""
    print("Medical Telegram Warehouse - Data Scraper")
    print("="*50)
    
    try:
        scraper = TelegramScraper()
        scraper.run()
    except ValueError as e:
        print(f"Configuration error: {e}")
        print("\nPlease ensure your .env file contains:")
        print("TELEGRAM_API_ID=your_api_id")
        print("TELEGRAM_API_HASH=your_api_hash")
        print("TELEGRAM_PHONE=your_phone_number")
        sys.exit(1)


if __name__ == "__main__":
    main()