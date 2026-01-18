"""
Verify the collected Telegram data.
"""

import json
from pathlib import Path
import pandas as pd
from datetime import datetime
import sys

def verify_data_structure():
    """Verify the data lake structure and contents."""
    print("Verifying Data Collection")
    print("="*60)
    
    base_path = Path('./data/raw')
    
    # Check directory structure
    print("\n1. Directory Structure:")
    directories = [
        base_path / 'images',
        base_path / 'telegram_messages',
        base_path / 'channels'
    ]
    
    for directory in directories:
        if directory.exists():
            print(f"   ✓ {directory.relative_to('.')}")
        else:
            print(f"   ✗ {directory.relative_to('.')} - MISSING")
    
    # Check images
    print("\n2. Images Collected:")
    images_dir = base_path / 'images'
    if images_dir.exists():
        channel_dirs = list(images_dir.iterdir())
        print(f"   Found {len(channel_dirs)} channels with images:")
        for channel_dir in channel_dirs:
            if channel_dir.is_dir():
                images = list(channel_dir.glob('*.jpg'))
                print(f"   - {channel_dir.name}: {len(images)} images")
    
    # Check JSON files
    print("\n3. Message JSON Files:")
    messages_dir = base_path / 'telegram_messages'
    if messages_dir.exists():
        date_dirs = list(messages_dir.iterdir())
        total_files = 0
        total_messages = 0
        
        for date_dir in date_dirs:
            if date_dir.is_dir():
                json_files = list(date_dir.glob('*.json'))
                total_files += len(json_files)
                print(f"\n   Date: {date_dir.name}")
                for json_file in json_files:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        messages = json.load(f)
                    total_messages += len(messages)
                    print(f"   - {json_file.name}: {len(messages)} messages")
        
        print(f"\n   Total: {total_files} JSON files, {total_messages} messages")
    
    # Check channel info
    print("\n4. Channel Information:")
    channels_dir = base_path / 'channels'
    if channels_dir.exists():
        channel_files = list(channels_dir.glob('*_info.json'))
        print(f"   Found {len(channel_files)} channel info files:")
        for channel_file in channel_files:
            with open(channel_file, 'r', encoding='utf-8') as f:
                channel_info = json.load(f)
            print(f"   - {channel_info['channel_name']} (@{channel_info['channel_username']})")
    
    # Load and analyze sample data
    print("\n5. Sample Data Analysis:")
    
    # Find first JSON file
    sample_file = None
    for date_dir in messages_dir.iterdir():
        if date_dir.is_dir():
            json_files = list(date_dir.glob('*.json'))
            if json_files:
                sample_file = json_files[0]
                break
    
    if sample_file:
        print(f"   Analyzing: {sample_file.name}")
        with open(sample_file, 'r', encoding='utf-8') as f:
            messages = json.load(f)
        
        if messages:
            # Convert to DataFrame for analysis
            df = pd.DataFrame(messages)
            
            print(f"   Total messages in file: {len(df)}")
            print(f"   Date range: {df['message_date'].min()} to {df['message_date'].max()}")
            print(f"   Channels: {df['channel_name'].unique()}")
            print(f"   Messages with media: {df['has_media'].sum()}")
            print(f"   Average views: {df['views'].mean():.0f}")
            print(f"   Average forwards: {df['forwards'].mean():.0f}")
            
            # Show sample message
            print("\n   Sample Message:")
            sample = messages[0]
            print(f"     ID: {sample['message_id']}")
            print(f"     Channel: {sample['channel_name']}")
            print(f"     Date: {sample['message_date']}")
            print(f"     Text: {sample['message_text'][:100]}...")
            print(f"     Views: {sample['views']}")
            print(f"     Forwards: {sample['forwards']}")
            print(f"     Has Image: {sample['has_media']}")
            if sample['image_path']:
                print(f"     Image: {sample['image_path']}")
    
    print("\n" + "="*60)
    print("✅ Data verification complete!")
    print("="*60)

def analyze_content():
    """Analyze the content of scraped messages."""
    print("\nContent Analysis")
    print("="*60)
    
    messages_dir = Path('./data/raw/telegram_messages')
    
    # Collect all messages
    all_messages = []
    
    for date_dir in messages_dir.iterdir():
        if date_dir.is_dir():
            for json_file in date_dir.glob('*.json'):
                with open(json_file, 'r', encoding='utf-8') as f:
                    messages = json.load(f)
                    all_messages.extend(messages)
    
    if not all_messages:
        print("No messages found for analysis")
        return
    
    df = pd.DataFrame(all_messages)
    
    print(f"Total Messages Analyzed: {len(df)}")
    print(f"Date Range: {df['message_date'].min()} to {df['message_date'].max()}")
    
    # Channel stats
    print("\nChannel Statistics:")
    channel_stats = df.groupby('channel_name').agg({
        'message_id': 'count',
        'views': 'mean',
        'forwards': 'mean',
        'has_media': 'sum'
    }).round(2)
    
    channel_stats.columns = ['total_messages', 'avg_views', 'avg_forwards', 'images_count']
    print(channel_stats)
    
    # Media analysis
    print(f"\nMedia Analysis:")
    print(f"  Messages with media: {df['has_media'].sum()} ({df['has_media'].mean()*100:.1f}%)")
    
    # Extract keywords from messages (simple approach)
    medical_keywords = ['pill', 'tablet', 'cream', 'medicine', 'pharma', 'drug', 'health', 'medical', 'cosmetic']
    
    keyword_counts = {}
    for keyword in medical_keywords:
        count = df['message_text'].str.contains(keyword, case=False).sum()
        if count > 0:
            keyword_counts[keyword] = count
    
    print(f"\nMedical Keywords Found:")
    for keyword, count in sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {keyword}: {count} messages")
    
    print("\n" + "="*60)

def main():
    """Main verification function."""
    print("Medical Telegram Warehouse - Data Verification")
    print("="*60)
    
    verify_data_structure()
    analyze_content()
    
    print("\n✅ All checks completed successfully!")
    print("\nNext steps:")
    print("1. Review the collected data")
    print("2. Check logs/scraping_summary_*.json for detailed stats")
    print("3. Move to Task 2: Data Modeling and Transformation")
     
if __name__ == "__main__":
    main()