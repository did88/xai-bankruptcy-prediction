@echo off
echo ============================================
echo DART 2023-2024 Financial Data Downloader
echo ============================================
echo.

REM Check if .env file exists and load it
if exist "..\..env" (
    echo [INFO] Loading .env file...
    for /f "usebackq tokens=1,2 delims==" %%a in ("..\..env") do (
        if "%%a"=="DART_API_KEY" set DART_API_KEY=%%b
    )
    REM Remove spaces from the value
    for /f "tokens=* delims= " %%a in ("%DART_API_KEY%") do set DART_API_KEY=%%a
    echo [INFO] API key loaded from .env file
)

REM Check API Key
if "%DART_API_KEY%"=="" (
    echo [ERROR] DART_API_KEY environment variable not set.
    echo    Method 1: set DART_API_KEY=your_api_key
    echo    Method 2: Add DART_API_KEY=your_api_key to .env file
    echo    Method 3: Check if .env file exists in parent directory
    pause
    exit /b 1
)

echo [INFO] API Key: %DART_API_KEY:~0,10%...

REM Python script filename
set SCRIPT_NAME=dart_2023_2024_downloader.py

REM Check script exists
if not exist "%SCRIPT_NAME%" (
    echo [ERROR] %SCRIPT_NAME% file not found.
    echo    Please check if the script file is in current directory.
    pause
    exit /b 1
)

echo Available Options:
echo    1. Show team information
echo    2. Download specific team
echo    3. Merge 2023-2024 files
echo    4. Merge all data (2015-2024)
echo    5. Download all teams sequentially
echo    6. Exit
echo.

:menu
set /p choice="Select option (1-6): "

if "%choice%"=="1" goto list_teams
if "%choice%"=="2" goto download_team
if "%choice%"=="3" goto merge_2023_2024
if "%choice%"=="4" goto merge_all
if "%choice%"=="5" goto download_all_teams
if "%choice%"=="6" goto exit
goto invalid_choice

:list_teams
echo.
echo [INFO] Checking team information...
python %SCRIPT_NAME% --list-teams
echo.
goto menu

:download_team
echo.
set /p team_num="Enter team number to download: "
set /p workers="Number of workers (default 10, press Enter to skip): "
if "%workers%"=="" set workers=10

echo.
echo [INFO] Starting team %team_num% download...
python %SCRIPT_NAME% --team %team_num% --workers %workers%
echo.
echo Complete! Press any key to continue...
pause >nul
goto menu

:merge_2023_2024
echo.
echo [INFO] Merging 2023-2024 team files...
python %SCRIPT_NAME% --merge-only
echo.
echo Complete! Press any key to continue...
pause >nul
goto menu

:merge_all
echo.
echo [INFO] Merging all data (2015-2024)...
python %SCRIPT_NAME% --merge-all
echo.
echo Complete! Press any key to continue...
pause >nul
goto menu

:download_all_teams
echo.
echo [WARNING] Full team download takes a very long time.
set /p confirm="Continue? (y/N): "
if /i not "%confirm%"=="y" goto menu

set /p workers="Number of workers (default 10): "
if "%workers%"=="" set workers=10

echo.
echo [INFO] Starting sequential download of all teams...
echo    Workers: %workers%
echo.

REM Usually around 30 teams
for /l %%i in (1,1,30) do (
    echo.
    echo =============================================
    echo Downloading team %%i... (Progress: %%i/30)
    echo =============================================
    python %SCRIPT_NAME% --team %%i --workers %workers%
    
    if errorlevel 1 (
        echo [WARNING] Error in team %%i download, continuing...
    ) else (
        echo [SUCCESS] Team %%i completed
    )
)

echo.
echo [SUCCESS] All teams download completed!
echo [INFO] Now running file merge...
python %SCRIPT_NAME% --merge-only

echo.
echo Complete! Press any key to continue...
pause >nul
goto menu

:invalid_choice
echo.
echo [ERROR] Invalid choice. Please select 1-6.
echo.
goto menu

:exit
echo.
echo [INFO] Exiting script.
pause
exit /b 0
