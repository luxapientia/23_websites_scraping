@echo off
echo ========================================
echo Automotive Wheels Scraper - Setup
echo ========================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8+ from https://www.python.org/
    pause
    exit /b 1
)

echo [1/4] Checking Python version...
python --version

echo.
echo [2/4] Installing dependencies...
pip install -r requirements.txt

if errorlevel 1 (
    echo ERROR: Failed to install dependencies
    pause
    exit /b 1
)

echo.
echo [3/4] Creating directories...
if not exist "data" mkdir data
if not exist "data\raw" mkdir data\raw
if not exist "data\processed" mkdir data\processed
if not exist "data\checkpoints" mkdir data\checkpoints
if not exist "logs" mkdir logs

echo.
echo [4/4] Running tests...
python test_scraper.py detection

echo.
echo ========================================
echo Setup Complete!
echo ========================================
echo.
echo Next steps:
echo   1. Test single product: python test_scraper.py tascaparts
echo   2. Run full scraper: python main.py
echo.
echo For more info, see README.md or QUICKSTART.md
echo.
pause

