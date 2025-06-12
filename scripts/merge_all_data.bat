@echo off
echo ============================================
echo DART 2023-2024 Data Merge Script
echo ============================================
echo.

set DART_API_KEY=ea93b96a1c65f8122dc6a71957dbf871655ef780
echo [INFO] API Key set successfully

echo [INFO] Step 1: Merging 2023-2024 team files...
python dart_2023_2024_downloader.py --merge-only

echo.
echo [INFO] Step 2: Merging with existing 2015-2022 data...
python dart_2023_2024_downloader.py --merge-all

echo.
echo [SUCCESS] Merge completed!
echo.
echo Generated files:
echo   1. data\dart_statements_2023_2024_merged.xlsx
echo   2. data\dart_statements_2015_2024_merged.xlsx
echo.
pause
