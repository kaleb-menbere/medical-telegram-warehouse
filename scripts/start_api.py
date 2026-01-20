import subprocess
import time
import requests
import sys

def start_api():
    """Start the FastAPI server and wait for it to be ready"""
    
    print("🚀 Starting Telegram Analytics API...")
    
    # Start the API server in a subprocess
    api_process = subprocess.Popen([
        sys.executable, "-m", "uvicorn",
        "api.main:app",
        "--host", "0.0.0.0",
        "--port", "8000",
        "--reload"
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    print("⏳ Waiting for API to start...")
    
    # Wait for API to be ready
    max_attempts = 10
    for attempt in range(max_attempts):
        try:
            response = requests.get("http://localhost:8000/api/health", timeout=2)
            if response.status_code == 200:
                print("✅ API started successfully!")
                print("📚 Documentation: http://localhost:8000/docs")
                print("🛑 Press Ctrl+C to stop the server")
                
                # Keep the script running
                try:
                    api_process.wait()
                except KeyboardInterrupt:
                    print("\n🛑 Stopping API server...")
                    api_process.terminate()
                    api_process.wait()
                    print("✅ API server stopped")
                break
        except requests.exceptions.ConnectionError:
            time.sleep(1)
            if attempt == max_attempts - 1:
                print("❌ Failed to start API")
                api_process.terminate()
                sys.exit(1)

if __name__ == "__main__":
    start_api()