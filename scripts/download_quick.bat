@echo off
echo ============================================
echo DART 2023-2024 Financial Data Downloader
echo ============================================
echo.

REM Set API key directly (will be loaded from .env by Python script)
set DART_API_KEY=ea93b96a1c65f8122dc6a71957dbf871655ef780

echo [INFO] API Key set successfully
echo [INFO] API Key: %DART_API_KEY:~0,10%...

set SCRIPT_NAME=dart_2023_2024_downloader.py

if not exist "%SCRIPT_NAME%" (
    echo [ERROR] %SCRIPT_NAME% file not found.
    pause
    exit /b 1
)

:menu
echo.
echo Options:
echo 1. List teams
echo 2. Download team
echo 3. Merge 2023-2024
echo 4. Merge all
echo 5. Download all
echo 6. Exit
echo.
set /p choice="Select (1-6): "

if "%choice%"=="1" (
    python %SCRIPT_NAME% --list-teams
    pause
    goto menu
)
if "%choice%"=="2" (
    set /p team="Team number: "
    set /p workers="Workers (default 10): "
    if "%workers%"=="" set workers=10
    python %SCRIPT_NAME% --team %team% --workers %workers%
    pause
    goto menu
)
if "%choice%"=="3" (
    python %SCRIPT_NAME% --merge-only
    pause
    goto menu
)
if "%choice%"=="4" (
    python %SCRIPT_NAME% --merge-all
    pause
    goto menu
)
if "%choice%"=="5" (
    echo WARNING: This will take many hours!
    set /p confirm="Continue? (y/N): "
    if /i "%confirm%"=="y" (
        set /p workers="Workers (default 10): "
        if "%workers%"=="" set workers=10
        for /l %%i in (1,1,30) do (
            echo Downloading team %%i...
            python %SCRIPT_NAME% --team %%i --workers %workers%
        )
        python %SCRIPT_NAME% --merge-only
    )
    pause
    goto menu
)
if "%choice%"=="6" exit /b 0

echo Invalid choice
goto menu
