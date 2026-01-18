"""
Simple script to run the Telegram scraper with options.
"""

import argparse
import os
import sys
from pathlib import Path


def setup_environment(test_mode=False, limit_messages=None, days_back=None):
    """Set up environment variables for scraping."""
    env_vars = {}
    
    if test_mode:
        env_vars['MAX_MESSAGES_PER_CHANNEL'] = '10'
        env_vars['SCRAPE_DAYS_BACK'] = '1'
        print("Running in TEST mode (10 messages, 1 day back)")
    
    if limit_messages:
        env_vars['MAX_MESSAGES_PER_CHANNEL'] = str(limit_messages)
    
    if days_back:
        env_vars['SCRAPE_DAYS_BACK'] = str(days_back)
    
    # Set environment variables
    for key, value in env_vars.items():
        os.environ[key] = value
        print(f"Set {key}={value}")


def main():
    """Parse arguments and run scraper."""
    parser = argparse.ArgumentParser(
        description='Run Telegram scraper for Ethiopian medical channels'
    )
    
    parser.add_argument(
        '--test',
        action='store_true',
        help='Run in test mode (limited messages)'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of messages per channel'
    )
    
    parser.add_argument(
        '--days',
        type=int,
        help='Number of days to look back'
    )
    
    parser.add_argument(
        '--channels',
        nargs='+',
        help='Specific channels to scrape'
    )
    
    parser.add_argument(
        '--config',
        action='store_true',
        help='Check configuration before running'
    )
    
    args = parser.parse_args()
    
    print("Medical Telegram Warehouse - Scraper Runner")
    print("="*60)
    
    # Check configuration if requested
    if args.config:
        print("Running configuration check...")
        import subprocess
        result = subprocess.run([sys.executable, 'scripts/config.py'], 
                              capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            print("Configuration check failed. Please fix issues before running.")
            sys.exit(1)
    
    # Set up environment
    setup_environment(args.test, args.limit, args.days)
    
    # If specific channels are provided, modify the scraper
    if args.channels:
        print(f"\nTargeting specific channels: {args.channels}")
        # This would require modifying the scraper class
        # For now, we'll just print a message
        print("Note: Channel specification requires code modification")
    
    # Run the scraper
    print("\n" + "="*60)
    print("Starting Telegram scraper...")
    print("="*60)
    
    try:
        from src.scraper import TelegramScraper
        scraper = TelegramScraper()
        
        # Override channels if specified
        if args.channels:
            scraper.all_channels = args.channels
        
        scraper.run()
        
    except Exception as e:
        print(f"\n❌ Error running scraper: {str(e)}")
        sys.exit(1)
    
    print("\n" + "="*60)
    print("Scraper completed successfully!")
    print("="*60)


if __name__ == "__main__":
    main() 