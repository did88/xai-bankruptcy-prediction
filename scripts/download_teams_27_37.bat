@echo off
echo ============================================
echo DART 2023-2024 Teams 27-37 Batch Downloader
echo ============================================
echo.

REM Set API key
set DART_API_KEY=ea93b96a1c65f8122dc6a71957dbf871655ef780

echo [INFO] API Key set successfully
echo [INFO] API Key: %DART_API_KEY:~0,10%...

set SCRIPT_NAME=download_teams_27_37.py

if not exist "%SCRIPT_NAME%" (
    echo [ERROR] %SCRIPT_NAME% file not found.
    echo Please check if the script file is in current directory.
    pause
    exit /b 1
)

echo.
echo [INFO] This will download teams 27-37 for 2023-2024 data
echo [WARNING] This process may take 2-4 hours depending on network speed
echo.

set /p confirm="Continue with teams 27-37 download? (y/N): "
if /i not "%confirm%"=="y" (
    echo Download cancelled.
    pause
    exit /b 0
)

echo.
set /p workers="Number of workers (default 10, press Enter to skip): "
if "%workers%"=="" set workers=10

echo.
echo [INFO] Starting teams 27-37 download...
echo [INFO] Workers: %workers%
echo [INFO] Estimated time: 2-4 hours
echo [INFO] You can press Ctrl+C to stop anytime
echo.

if "%workers%"=="10" (
    python %SCRIPT_NAME%
) else (
    python %SCRIPT_NAME% --workers %workers%
)

echo.
echo [INFO] Download process completed!
echo Check the output above for detailed results.
echo.
pause
