import requests
import json
from datetime import datetime

BASE_URL = "http://localhost:8000"

def verify_task4_complete():
    """Verify that Task 4 (Analytical API) is complete and all endpoints work."""
    
    print("✅ TASK 4 COMPLETION VERIFICATION")
    print("=" * 60)
    
    # Test all required endpoints
    endpoints = [
        {
            "url": "/api/reports/top-products?limit=10",
            "method": "GET",
            "description": "Endpoint 1: Top Products",
            "min_items": 1
        },
        {
            "url": "/api/channels/Lobelia pharmacy and cosmetics/activity",
            "method": "GET",
            "description": "Endpoint 2: Channel Activity",
            "min_items": 1
        },
        {
            "url": "/api/search/messages?query=paracetamol&limit=20",
            "method": "GET",
            "description": "Endpoint 3: Message Search",
            "min_items": 0  # Might not find paracetamol
        },
        {
            "url": "/api/reports/visual-content",
            "method": "GET",
            "description": "Endpoint 4: Visual Content Stats",
            "min_items": 1
        },
        {
            "url": "/api/health",
            "method": "GET",
            "description": "Health Check Endpoint",
            "min_items": 1
        },
        {
            "url": "/docs",
            "method": "GET",
            "description": "OpenAPI Documentation",
            "min_items": 0  # HTML response
        }
    ]
    
    all_passed = True
    
    for endpoint in endpoints:
        try:
            response = requests.get(f"{BASE_URL}{endpoint['url']}", timeout=10)
            
            if response.status_code == 200:
                # Check if it returns data (for JSON endpoints)
                if endpoint['url'] != '/docs':  # Skip docs as it returns HTML
                    data = response.json()
                    
                    if isinstance(data, list):
                        item_count = len(data)
                        status = f"✓ PASS ({item_count} items)"
                        if item_count < endpoint['min_items']:
                            status = f"⚠️ WARNING ({item_count} items, expected {endpoint['min_items']}+)"
                    elif isinstance(data, dict):
                        if 'messages' in data:
                            item_count = len(data['messages'])
                            status = f"✓ PASS ({item_count} messages)"
                        else:
                            status = "✓ PASS (object response)"
                    else:
                        status = "✓ PASS"
                else:
                    status = "✓ PASS (HTML docs)"
                    
                print(f"{status} - {endpoint['description']}")
            else:
                print(f"❌ FAIL ({response.status_code}) - {endpoint['description']}")
                all_passed = False
                
        except Exception as e:
            print(f"❌ ERROR - {endpoint['description']}: {str(e)}")
            all_passed = False
    
    # Test sample queries
    print("\n📊 SAMPLE QUERY RESULTS:")
    
    sample_queries = [
        ("Top 3 products", "/api/reports/top-products?limit=3"),
        ("Channel list", "/api/channels"),
        ("Visual content analysis", "/api/reports/visual-content"),
    ]
    
    for query_name, query_url in sample_queries:
        try:
            response = requests.get(f"{BASE_URL}{query_url}", timeout=5)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list):
                    print(f"  {query_name}: {len(data)} results")
                    if data and query_name == "Visual content analysis":
                        for item in data[:2]:  # Show first 2
                            print(f"    • {item.get('channel_name')}: {item.get('image_percentage')}% images")
                elif isinstance(data, dict):
                    print(f"  {query_name}: Object response")
            else:
                print(f"  {query_name}: Failed ({response.status_code})")
        except:
            print(f"  {query_name}: Error")
    
    # API Documentation check
    print("\n📚 API DOCUMENTATION:")
    print(f"  Interactive Docs: {BASE_URL}/docs")
    print(f"  ReDoc: {BASE_URL}/redoc")
    print(f"  OpenAPI JSON: {BASE_URL}/openapi.json")
    
    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 TASK 4 COMPLETE! All endpoints working correctly.")
        print("   Next: Proceed to Task 5 (Pipeline Orchestration)")
    else:
        print("⚠️  TASK 4 INCOMPLETE. Some endpoints failed.")
        print("   Check the errors above and fix before proceeding.")

if __name__ == "__main__":
    verify_task4_complete()