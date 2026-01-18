import sys
import pkg_resources

# Required packages
required_packages = [
    'telethon',
    'pandas',
    'pydantic',
    'python-dotenv',
    'psycopg2-binary',
    'sqlalchemy',
    'tqdm'
]

print("Testing package installation...")
print("=" * 50)

all_installed = True
for package in required_packages:
    try:
        dist = pkg_resources.get_distribution(package)
        print(f"✓ {package} ({dist.version})")
    except pkg_resources.DistributionNotFound:
        print(f"✗ {package} - NOT INSTALLED")
        all_installed = False

print("=" * 50)
if all_installed:
    print("✅ All packages installed successfully!")
else:
    print("❌ Some packages are missing. Please install them manually.")
    sys.exit(1)