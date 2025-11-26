#!/bin/bash

echo "========================================"
echo "Automotive Wheels Scraper - Setup"
echo "========================================"
echo ""

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python 3 is not installed"
    echo "Please install Python 3.8+ from https://www.python.org/"
    exit 1
fi

echo "[1/4] Checking Python version..."
python3 --version

echo ""
echo "[2/4] Installing dependencies..."
pip3 install -r requirements.txt

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to install dependencies"
    exit 1
fi

echo ""
echo "[3/4] Creating directories..."
mkdir -p data/raw
mkdir -p data/processed
mkdir -p data/checkpoints
mkdir -p logs

echo ""
echo "[4/4] Running tests..."
python3 test_scraper.py detection

echo ""
echo "========================================"
echo "Setup Complete!"
echo "========================================"
echo ""
echo "Next steps:"
echo "  1. Test single product: python3 test_scraper.py tascaparts"
echo "  2. Run full scraper: python3 main.py"
echo ""
echo "For more info, see README.md or QUICKSTART.md"
echo ""

