@echo off
echo Medical Telegram Warehouse - Scraper
echo ====================================

REM Check if virtual environment exists
if not exist ".venv\Scripts\activate.bat" (
    echo Creating virtual environment...
    python -m venv .venv
)

REM Activate virtual environment
call .venv\Scripts\activate.bat

REM Install requirements if needed
if not exist ".venv\Lib\site-packages\telethon" (
    echo Installing requirements...
    pip install -r requirements.txt
)

REM Run configuration check
echo.
echo Running configuration check...
python scripts\config.py

REM Ask to continue
echo.
set /p continue="Do you want to continue with scraping? (y/n): "
if /i not "%continue%"=="y" (
    echo Scraping cancelled.
    pause
    exit /b 0
)

REM Run the scraper
echo.
echo Starting Telegram scraper...
echo ============================
python src\scraper.py

pause