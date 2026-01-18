# scripts/setup_dbt.py
import subprocess
import os
import sys

def run_dbt_commands():
    """Run dbt commands in sequence"""
    commands = [
        "dbt deps",           # Install dependencies
        "dbt seed",           # Load seed data (if any)
        "dbt run",            # Build models
        "dbt test",           # Run tests
        "dbt docs generate",  # Generate documentation
    ]
    
    for cmd in commands:
        print(f"\n{'='*50}")
        print(f"Running: {cmd}")
        print('='*50)
        
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Error running {cmd}:")
            print(result.stderr)
            sys.exit(1)
        else:
            print(result.stdout)
    
    print("\n" + "="*50)
    print("dbt project setup completed successfully!")
    print("To view documentation, run: dbt docs serve")
    print("="*50)

if __name__ == "__main__":
    # Set environment variables
    os.environ['DB_HOST'] = 'localhost'
    os.environ['DB_PORT'] = '5432'
    os.environ['DB_NAME'] = 'medical_warehouse'
    os.environ['DB_USER'] = 'postgres'
    os.environ['DB_PASSWORD'] = 'password'
    os.environ['DB_SCHEMA'] = 'dbt_transform'
    
    run_dbt_commands()