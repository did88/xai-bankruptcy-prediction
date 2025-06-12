@echo off
echo ============================================
echo DART 2023-2024년 재무제표 다운로드 스크립트
echo ============================================
echo.

REM API 키 확인
if "%DART_API_KEY%"=="" (
    echo ❌ 오류: DART_API_KEY 환경변수가 설정되지 않았습니다.
    echo    설정 방법: set DART_API_KEY=your_api_key
    echo    또는 .env 파일에 DART_API_KEY=your_api_key 추가
    pause
    exit /b 1
)

REM Python 스크립트 파일명
set SCRIPT_NAME=dart_2023_2024_downloader.py

REM 스크립트 존재 확인
if not exist "%SCRIPT_NAME%" (
    echo ❌ 오류: %SCRIPT_NAME% 파일을 찾을 수 없습니다.
    echo    현재 디렉토리에 스크립트 파일이 있는지 확인하세요.
    pause
    exit /b 1
)

echo 📋 사용 가능한 옵션:
echo    1. 팀 정보 확인
echo    2. 특정 팀 다운로드
echo    3. 2023-2024년 파일 병합
echo    4. 전체 데이터 병합 (2015-2024)
echo    5. 전체 팀 순차 다운로드
echo    6. 종료
echo.

:menu
set /p choice="선택하세요 (1-6): "

if "%choice%"=="1" goto list_teams
if "%choice%"=="2" goto download_team
if "%choice%"=="3" goto merge_2023_2024
if "%choice%"=="4" goto merge_all
if "%choice%"=="5" goto download_all_teams
if "%choice%"=="6" goto exit
goto invalid_choice

:list_teams
echo.
echo 📊 팀 정보 확인 중...
python %SCRIPT_NAME% --list-teams
echo.
goto menu

:download_team
echo.
set /p team_num="다운로드할 팀 번호를 입력하세요: "
set /p workers="동시 작업 수 (기본값 10, 엔터로 스킵): "
if "%workers%"=="" set workers=10

echo.
echo 🚀 팀 %team_num% 다운로드 시작...
python %SCRIPT_NAME% --team %team_num% --workers %workers%
echo.
echo 완료! 계속하려면 아무 키나 누르세요...
pause >nul
goto menu

:merge_2023_2024
echo.
echo 📊 2023-2024년 팀별 파일 병합 중...
python %SCRIPT_NAME% --merge-only
echo.
echo 완료! 계속하려면 아무 키나 누르세요...
pause >nul
goto menu

:merge_all
echo.
echo 🔄 전체 데이터 병합 중 (2015-2024년)...
python %SCRIPT_NAME% --merge-all
echo.
echo 완료! 계속하려면 아무 키나 누르세요...
pause >nul
goto menu

:download_all_teams
echo.
echo ⚠️ 전체 팀 다운로드는 시간이 매우 오래 걸립니다.
set /p confirm="계속하시겠습니까? (y/N): "
if /i not "%confirm%"=="y" goto menu

set /p workers="동시 작업 수 (기본값 10): "
if "%workers%"=="" set workers=10

echo.
echo 🚀 전체 팀 순차 다운로드 시작...
echo    동시 작업 수: %workers%
echo.

REM 팀 개수 확인 (일반적으로 30개 팀 정도)
for /l %%i in (1,1,30) do (
    echo.
    echo =============================================
    echo 팀 %%i 다운로드 중... ^(진행률: %%i/30^)
    echo =============================================
    python %SCRIPT_NAME% --team %%i --workers %workers%
    
    if errorlevel 1 (
        echo ⚠️ 팀 %%i 다운로드 중 오류 발생, 계속 진행...
    ) else (
        echo ✅ 팀 %%i 완료
    )
)

echo.
echo 🎉 전체 팀 다운로드 완료!
echo 📊 이제 파일 병합을 실행하세요...
python %SCRIPT_NAME% --merge-only

echo.
echo 완료! 계속하려면 아무 키나 누르세요...
pause >nul
goto menu

:invalid_choice
echo.
echo ❌ 잘못된 선택입니다. 1-6 중에서 선택하세요.
echo.
goto menu

:exit
echo.
echo 👋 스크립트를 종료합니다.
pause
exit /b 0
