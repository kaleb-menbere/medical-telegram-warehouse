"""
Configuration helper script for Telegram scraper.
"""

import os
import sys
from pathlib import Path


def check_environment():
    """Check if all required environment variables are set."""
    print("Checking environment configuration...")
    print("-" * 50)
    
    required_vars = [
        'TELEGRAM_API_ID',
        'TELEGRAM_API_HASH', 
        'TELEGRAM_PHONE'
    ]
    
    missing_vars = []
    
    for var in required_vars:
        value = os.getenv(var)
        if value and value != f"your_{var.lower()}":
            print(f"✓ {var}: {'*' * len(value)}")
        elif value == f"your_{var.lower()}":
            print(f"✗ {var}: NOT CONFIGURED (still has placeholder value)")
            missing_vars.append(var)
        else:
            print(f"✗ {var}: NOT SET")
            missing_vars.append(var)
    
    print("-" * 50)
    
    if missing_vars:
        print(f"❌ Missing {len(missing_vars)} configuration variables")
        print("\nPlease update your .env file with:")
        for var in missing_vars:
            print(f"  {var}=your_value_here")
        return False
    else:
        print("✅ All required environment variables are configured")
        return True


def check_directories():
    """Check if all required directories exist."""
    print("\nChecking directory structure...")
    print("-" * 50)
    
    required_dirs = [
        './data/raw/images',
        './data/raw/telegram_messages',
        './logs',
        './src',
        './scripts'
    ]
    
    missing_dirs = []
    
    for dir_path in required_dirs:
        if Path(dir_path).exists():
            print(f"✓ {dir_path}")
        else:
            print(f"✗ {dir_path}")
            missing_dirs.append(dir_path)
    
    print("-" * 50)
    
    if missing_dirs:
        print(f"⚠️  Missing {len(missing_dirs)} directories")
        print("\nCreate them with:")
        for dir_path in missing_dirs:
            print(f"  mkdir -p {dir_path}")
        return False
    else:
        print("✅ All required directories exist")
        return True


def get_telegram_credentials_guide():
    """Print guide for getting Telegram credentials."""
    print("\n" + "="*60)
    print("HOW TO GET TELEGRAM API CREDENTIALS")
    print("="*60)
    print("\n1. Go to https://my.telegram.org")
    print("2. Log in with your phone number")
    print("3. Click on 'API Development Tools'")
    print("4. Fill in the form to create a new application:")
    print("   - App title: Medical Telegram Warehouse")
    print("   - Short name: medtelegram")
    print("   - URL: (can be empty or your GitHub URL)")
    print("   - Platform: Desktop")
    print("   - Description: Data pipeline for medical businesses")
    print("\n5. You will get:")
    print("   - api_id: A number")
    print("   - api_hash: A string of characters")
    print("\n6. Add these to your .env file:")
    print("   TELEGRAM_API_ID=your_api_id")
    print("   TELEGRAM_API_HASH=your_api_hash")
    print("   TELEGRAM_PHONE=+251XXXXXXXXX (your phone with country code)")
    print("="*60)


def main():
    """Main configuration checker."""
    print("Medical Telegram Warehouse - Configuration Check")
    print("="*60)
    
    # Check if .env file exists
    env_file = Path('.env')
    if not env_file.exists():
        print("❌ .env file not found!")
        print("\nCreate .env file with:")
        print("cp .env.example .env")
        print("\nThen edit it with your credentials")
        get_telegram_credentials_guide()
        sys.exit(1)
    
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    # Run checks
    env_ok = check_environment()
    dirs_ok = check_directories()
    
    if not env_ok:
        get_telegram_credentials_guide()
    
    print("\n" + "="*60)
    if env_ok and dirs_ok:
        print("✅ Configuration is ready!")
        print("\nTo run the scraper:")
        print("  python src/scraper.py")
        print("\nTo test the scraper:")
        print("  python src/test_scraper.py")
    else:
        print("❌ Configuration needs attention")
        print("\nPlease fix the issues above before running the scraper.")
    print("="*60)
 

if __name__ == "__main__":
    main()    