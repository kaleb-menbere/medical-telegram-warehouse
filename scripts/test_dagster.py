import os
import sys

def test_dagster_setup():
    """Test if Dagster is properly installed and configured"""
    
    print("🧪 Testing Dagster Setup")
    print("=" * 50)
    
    # Check Dagster installation
    try:
        import dagster
        print(f"✅ Dagster version: {dagster.__version__}")
    except ImportError:
        print("❌ Dagster not installed")
        print("   Run: pip install dagster dagster-webserver")
        return False
    
    # Check pipeline file exists
    pipeline_file = "orchestration/pipeline.py"
    if os.path.exists(pipeline_file):
        print(f"✅ Pipeline file found: {pipeline_file}")
    else:
        print(f"❌ Pipeline file not found: {pipeline_file}")
        print("   Create the orchestration directory and pipeline.py")
        return False
    
    # Check dependencies
    dependencies = ["psycopg2", "pandas", "subprocess"]
    for dep in dependencies:
        try:
            __import__(dep)
            print(f"✅ Dependency: {dep}")
        except ImportError:
            print(f"⚠️  Missing dependency: {dep}")
    
    print("\n🚀 Setup Instructions:")
    print("1. Start Dagster UI: dagster dev -f orchestration/pipeline.py")
    print("2. Open browser: http://localhost:3000")
    print("3. Run the 'telegram_data_pipeline' job")
    
    print("\n📋 Quick test command:")
    print("   dagster job execute -f orchestration/pipeline.py")
    
    return True

if __name__ == "__main__":
    test_dagster_setup()