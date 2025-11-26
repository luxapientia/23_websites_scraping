@echo off
echo ========================================
echo Automotive Wheels Scraper
echo ========================================
echo.
echo Starting the scraper...
echo This may take 1-3 days to complete all 23 sites.
echo.
echo Press Ctrl+C at any time to stop (progress will be saved)
echo.
pause

python main.py

echo.
echo ========================================
echo Scraping Complete!
echo ========================================
echo.
echo Check the output in: data\processed\
echo.
pause

