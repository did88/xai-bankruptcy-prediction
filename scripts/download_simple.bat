@echo off
echo ============================================
echo DART 2023-2024 Data Downloader
echo ============================================
echo.

if "%DART_API_KEY%"=="" (
    echo ERROR: DART_API_KEY not set
    echo Set: set DART_API_KEY=your_key
    pause
    exit /b 1
)

set SCRIPT_NAME=dart_2023_2024_downloader.py

if not exist "%SCRIPT_NAME%" (
    echo ERROR: %SCRIPT_NAME% not found
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
    goto menu
)
if "%choice%"=="2" (
    set /p team="Team number: "
    python %SCRIPT_NAME% --team %team%
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
        for /l %%i in (1,1,30) do (
            echo Downloading team %%i...
            python %SCRIPT_NAME% --team %%i
        )
        python %SCRIPT_NAME% --merge-only
    )
    pause
    goto menu
)
if "%choice%"=="6" exit /b 0

echo Invalid choice
goto menu
