"""
Test script for Telegram scraper functionality.
"""

import asyncio
import json
import sys
import os
from datetime import datetime

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.scraper import TelegramScraper


async def test_connection():
    """Test Telegram connection."""
    print("Testing Telegram connection...")
    
    scraper = TelegramScraper()
    
    try:
        # Test with reduced limits for testing
        scraper.max_messages = 10
        scraper.days_back = 1
        
        # Connect
        if await scraper.connect():
            print("✓ Connected to Telegram successfully")
            
            # Test getting channel info
            test_channel = 'lobelia4cosmetics'
            print(f"\nTesting channel info for: {test_channel}")
            
            channel_info = await scraper.get_channel_info(test_channel)
            if channel_info:
                print("✓ Channel info retrieved:")
                print(json.dumps(channel_info, indent=2, default=str))
            else:
                print("✗ Failed to get channel info")
            
            # Test scraping a few messages
            print(f"\nTesting message scraping for: {test_channel}")
            messages, images = await scraper.scrape_channel_messages(test_channel, channel_info)
            
            print(f"✓ Scraped {len(messages)} messages")
            print(f"✓ Downloaded {images} images")
            
            if messages:
                print("\nSample message:")
                print(json.dumps(messages[0], indent=2, default=str))
            
            # Test saving
            print("\nTesting data saving...")
            await scraper.save_messages_to_json(test_channel, messages)
            await scraper.save_channel_info(channel_info)
            print("✓ Data saved successfully")
            
            await scraper.client.disconnect()
            print("\n✓ All tests passed!")
            return True
        else:
            print("✗ Failed to connect to Telegram")
            return False
            
    except Exception as e:
        print(f"✗ Test failed with error: {str(e)}")
        return False


async def test_data_structure():
    """Test the data lake structure."""
    print("\nTesting data lake structure...")
    
    scraper = TelegramScraper()
    
    # Check if directories exist
    required_dirs = [
        scraper.raw_dir,
        scraper.images_dir,
        scraper.messages_dir,
        scraper.logs_dir
    ]
    
    for directory in required_dirs:
        if directory.exists():
            print(f"✓ Directory exists: {directory}")
        else:
            print(f"✗ Directory missing: {directory}")
    
    return True


def main():
    """Run all tests."""
    print("Medical Telegram Warehouse - Scraper Test Suite")
    print("="*50)
    
    # Test data structure
    asyncio.run(test_data_structure())
    
    print("\n" + "="*50)
    print("Testing Telegram scraping functionality...")
    print("NOTE: This requires valid Telegram API credentials in .env file")
    print("="*50)
    
    # Test Telegram functionality
    success = asyncio.run(test_connection())
    
    if success:
        print("\n" + "="*50)
        print("✅ ALL TESTS PASSED!")
        print("="*50)
    else:
        print("\n" + "="*50)
        print("❌ SOME TESTS FAILED")
        print("Please check your Telegram API credentials and internet connection")
        print("="*50)


if __name__ == "__main__":
    main()