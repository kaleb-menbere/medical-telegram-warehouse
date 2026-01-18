#!/usr/bin/env python3
"""
Simple Telegram connection test
"""
import asyncio
import os
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv()

async def test_connection():
    print("🔧 Testing Telegram Connection...")
    print("-" * 40)
    
    api_id = os.getenv('TELEGRAM_API_ID', '34314272')
    api_hash = os.getenv('TELEGRAM_API_HASH', '755aa767749b741157eef616a7daf097')
    
    print(f"API ID: {api_id}")
    print(f"API Hash: {api_hash[:10]}...")
    
    client = TelegramClient('test_session', int(api_id), api_hash)
    
    try:
        await client.start()
        print("✅ Connected to Telegram!")
        
        me = await client.get_me()
        print(f"👤 You are: {me.first_name} {me.last_name or ''}")
        print(f"📱 Phone: {me.phone}")
        print(f"🆔 Username: {me.username or 'None'}")
        
        # Try to get a public channel
        try:
            channel = await client.get_entity('telegram')
            print(f"📢 Test channel: {channel.title}")
        except:
            print("⚠️  Could not access test channel")
        
        await client.disconnect()
        print("✅ Test completed successfully!")
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\n🔧 Troubleshooting:")
        print("1. Check your API credentials in .env file")
        print("2. Make sure you have internet connection")
        print("3. If in restricted region, try VPN")
   
if __name__ == "__main__":
    asyncio.run(test_connection())