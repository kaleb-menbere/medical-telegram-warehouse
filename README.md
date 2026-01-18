# Medical Telegram Warehouse

## Project Overview
This project builds a data pipeline for scraping and analyzing Ethiopian medical business data from Telegram channels.

## Quick Start

### 1. Prerequisites
- Python 3.9+
- PostgreSQL
- Telegram API credentials

### 2. Setup

```bash
# Clone the repository
git clone <repository-url>
cd medical-telegram-warehouse

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your credentials

# Start PostgreSQL with Docker
docker-compose up -d postgres

# Run the scraper
python src/scraper.py