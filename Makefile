.PHONY: setup test clean scrape

setup:
	@echo "Setting up environment..."
	python -m venv venv
	@echo "Run: source venv/bin/activate (or venv\Scripts\activate on Windows)"

install:
	@echo "Installing packages..."
	pip install -r requirements.txt

test:
	@echo "Testing Telegram connection..."
	python scripts/test_telegram.py

scrape:
	@echo "Starting scraper..."
	python src/scraper.py

clean:
	@echo "Cleaning up..."
	rm -rf __pycache__ */__pycache__ */*/__pycache__
	rm -f *.pyc */*.pyc */*/*.pyc