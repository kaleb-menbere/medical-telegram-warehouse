import subprocess
import sys
import time
import webbrowser

def start_dagster():
    """Start Dagster webserver"""
    
    print("🚀 Starting Dagster Orchestration UI...")
    print("=" * 50)
    
    # Create orchestration directory if it doesn't exist
    import os
    os.makedirs("orchestration", exist_ok=True)
    
    # Start Dagster
    try:
        process = subprocess.Popen(
            [
                sys.executable, "-m", "dagster",
                "dev",
                "-f", "orchestration/pipeline.py",
                "--host", "0.0.0.0",
                "--port", "3000"
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        print("⏳ Dagster starting... (this may take a moment)")
        print("📱 UI will be available at: http://localhost:3000")
        
        # Wait a bit and open browser
        time.sleep(5)
        webbrowser.open("http://localhost:3000")
        
        print("\n🛑 Press Ctrl+C to stop Dagster")
        
        # Wait for process
        try:
            process.wait()
        except KeyboardInterrupt:
            print("\n🛑 Stopping Dagster...")
            process.terminate()
            process.wait()
            print("✅ Dagster stopped")
            
    except Exception as e:
        print(f"❌ Error starting Dagster: {e}")
        print("\n💡 Try manually: dagster dev -f orchestration/pipeline.py")

if __name__ == "__main__":
    start_dagster()