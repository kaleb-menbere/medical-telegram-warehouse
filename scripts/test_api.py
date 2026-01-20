import requests
import json

BASE_URL = "http://localhost:8000"

def test_api_endpoints():
    """Test all API endpoints"""
    
    print("🧪 Testing Telegram Analytics API")
    print("=" * 50)
    
    endpoints = [
        ("/", "Root endpoint"),
        ("/api/health", "Health check"),
        ("/api/channels", "List channels"),
        ("/api/reports/top-products?limit=5", "Top products"),
        ("/api/reports/visual-content", "Visual content stats"),
        ("/api/messages/recent?limit=3", "Recent messages"),
        ("/api/channels/Lobelia pharmacy and cosmetics/activity?days=3", "Channel activity"),
        ("/api/search/messages?query=paracetamol&limit=3", "Message search"),
    ]
    
    for endpoint, description in endpoints:
        try:
            response = requests.get(f"{BASE_URL}{endpoint}", timeout=10)
            status = "✅" if response.status_code == 200 else "❌"
            print(f"{status} {description}: {response.status_code}")
            
            if response.status_code != 200:
                print(f"   Error: {response.text[:100]}")
            else:
                data = response.json()
                if isinstance(data, list):
                    print(f"   Results: {len(data)} items")
                elif isinstance(data, dict) and "total_count" in data:
                    print(f"   Results: {data['total_count']} total, {len(data.get('messages', []))} returned")
                elif isinstance(data, dict):
                    print(f"   Status: {data.get('status', 'N/A')}")
                    
        except requests.exceptions.ConnectionError:
            print(f"❌ {description}: Cannot connect to API")
            print("   Make sure the API is running: python -m api.main")
            break
        except Exception as e:
            print(f"❌ {description}: Error - {str(e)}")
    
    print("\n" + "=" * 50)
    print("📋 API Documentation available at: http://localhost:8000/docs")

if __name__ == "__main__":
    test_api_endpoints()