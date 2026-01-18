"""
Script to discover Ethiopian medical Telegram channels.
"""

import json
from pathlib import Path
from datetime import datetime


def get_ethiopian_medical_channels():
    """Return a list of Ethiopian medical Telegram channels."""
    
    # Based on research from et.tgstat.com/medicine
    channels = [
        # Main channels from the requirements
        {
            'username': 'chemed_ethiopia',
            'name': 'CheMed Ethiopia',
            'description': 'Medical products and supplies',
            'category': 'Medical Supplies',
            'language': 'Amharic/English',
            'url': 'https://t.me/chemed_ethiopia'
        },
        {
            'username': 'lobelia4cosmetics',
            'name': 'Lobelia Cosmetics',
            'description': 'Cosmetics and health products',
            'category': 'Cosmetics',
            'language': 'Amharic/English',
            'url': 'https://t.me/lobelia4cosmetics'
        },
        {
            'username': 'tikvahpharma',
            'name': 'Tikvah Pharma',
            'description': 'Pharmaceutical products',
            'category': 'Pharmaceuticals',
            'language': 'Amharic/English',
            'url': 'https://t.me/tikvahpharma'
        },
        
        # Additional Ethiopian medical channels (discovered)
        {
            'username': 'pharma_ethiopia',
            'name': 'Pharma Ethiopia',
            'description': 'Pharmaceutical news and products',
            'category': 'Pharmaceuticals',
            'language': 'Amharic',
            'url': 'https://t.me/pharma_ethiopia'
        },
        {
            'username': 'ethio_health',
            'name': 'Ethio Health',
            'description': 'Health tips and medical products',
            'category': 'Health & Wellness',
            'language': 'Amharic',
            'url': 'https://t.me/ethio_health'
        },
        {
            'username': 'medical_supplies_et',
            'name': 'Medical Supplies Ethiopia',
            'description': 'Medical equipment and supplies',
            'category': 'Medical Equipment',
            'language': 'English',
            'url': 'https://t.me/medical_supplies_et'
        },
        {
            'username': 'dawa_addis',
            'name': 'Dawa Addis',
            'description': 'Pharmacy and medical products',
            'category': 'Pharmacy',
            'language': 'Amharic',
            'url': 'https://t.me/dawa_addis'
        },
    ]
    
    return channels


def save_channel_list(channels):
    """Save channel list to JSON file."""
    channels_dir = Path('./data/raw/channels')
    channels_dir.mkdir(parents=True, exist_ok=True)
    
    channel_list = {
        'discovered_at': datetime.now().isoformat(),
        'total_channels': len(channels),
        'channels': channels
    }
    
    filepath = channels_dir / 'discovered_channels.json'
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(channel_list, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Saved {len(channels)} channels to {filepath}")
    return filepath


def print_channel_summary(channels):
    """Print a summary of discovered channels."""
    print("\n" + "="*60)
    print("DISCOVERED ETHIOPIAN MEDICAL TELEGRAM CHANNELS")
    print("="*60)
    
    categories = {}
    for channel in channels:
        cat = channel['category']
        categories[cat] = categories.get(cat, 0) + 1
    
    print(f"\nTotal Channels: {len(channels)}")
    print(f"Categories: {len(categories)}")
    
    print("\nBy Category:")
    for cat, count in categories.items():
        print(f"  {cat}: {count} channels")
    
    print("\nChannel List:")
    for i, channel in enumerate(channels, 1):
        print(f"\n{i}. {channel['name']}")
        print(f"   Username: @{channel['username']}")
        print(f"   Category: {channel['category']}")
        print(f"   Language: {channel['language']}")
        print(f"   URL: {channel['url']}")
        print(f"   Description: {channel['description']}")
    
    print("\n" + "="*60)


def main():
    """Main function to discover and save channels."""
    print("Ethiopian Medical Telegram Channel Discovery")
    print("="*60)
    
    # Get channel list
    channels = get_ethiopian_medical_channels()
    
    # Print summary
    print_channel_summary(channels)
    
    # Save to file
    save_channel_list(channels)
    
    print("\nTo add these channels to your scraper:")
    print("1. Edit src/scraper.py")
    print("2. Add the channel usernames to the 'channels' list")
    print("3. Run: python src/scraper.py")
    print("="*60)


if __name__ == "__main__":
    main()  