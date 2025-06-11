@echo off
echo 🚀 담당자별 DART 다운로드 스크립트
echo =====================================

:menu
echo.
echo 👥 담당자를 선택하세요:
echo 1. 담당자 1 (팀 1-7)
echo 2. 담당자 2 (팀 8-14) 
echo 3. 담당자 3 (팀 15-21)
echo 4. 담당자 4 (팀 22-26)
echo 5. 전체 병합
echo 0. 종료
echo.

set /p choice="선택 (0-5): "

if "%choice%"=="1" goto person1
if "%choice%"=="2" goto person2
if "%choice%"=="3" goto person3
if "%choice%"=="4" goto person4
if "%choice%"=="5" goto merge
if "%choice%"=="0" goto exit
goto menu

:person1
echo.
echo 👤 담당자 1: 팀 1-7 다운로드 시작
echo ⏰ 예상 소요시간: 7분
echo.
for /L %%i in (1,1,7) do (
    echo 📊 팀 %%i 다운로드 중...
    python cached_team_downloader.py --team %%i --skip-validation
    if errorlevel 1 (
        echo ⚠️ 팀 %%i 실패, 재시도...
        python cached_team_downloader.py --team %%i --skip-validation
    )
)
echo ✅ 담당자 1 완료!
goto menu

:person2
echo.
echo 👤 담당자 2: 팀 8-14 다운로드 시작  
echo ⏰ 예상 소요시간: 7분
echo.
for /L %%i in (8,1,14) do (
    echo 📊 팀 %%i 다운로드 중...
    python cached_team_downloader.py --team %%i --skip-validation
    if errorlevel 1 (
        echo ⚠️ 팀 %%i 실패, 재시도...
        python cached_team_downloader.py --team %%i --skip-validation
    )
)
echo ✅ 담당자 2 완료!
goto menu

:person3
echo.
echo 👤 담당자 3: 팀 15-21 다운로드 시작
echo ⏰ 예상 소요시간: 7분  
echo.
for /L %%i in (15,1,21) do (
    echo 📊 팀 %%i 다운로드 중...
    python cached_team_downloader.py --team %%i --skip-validation
    if errorlevel 1 (
        echo ⚠️ 팀 %%i 실패, 재시도...
        python cached_team_downloader.py --team %%i --skip-validation
    )
)
echo ✅ 담당자 3 완료!
goto menu

:person4
echo.
echo 👤 담당자 4: 팀 22-26 다운로드 시작
echo ⏰ 예상 소요시간: 5분
echo.
for /L %%i in (22,1,26) do (
    echo 📊 팀 %%i 다운로드 중...
    python cached_team_downloader.py --team %%i --skip-validation
    if errorlevel 1 (
        echo ⚠️ 팀 %%i 실패, 재시도...
        python cached_team_downloader.py --team %%i --skip-validation
    )
)
echo ✅ 담당자 4 완료!
goto menu

:merge
echo.
echo 🔗 모든 팀 파일 병합 중...
python team_dart_downloader_fixed.py --merge-only
if errorlevel 1 (
    echo ❌ 병합 실패
) else (
    echo ✅ 병합 완료!
    echo 📁 최종 파일: data/dart_statements_merged.xlsx
)
goto menu

:exit
echo.
echo 👋 다운로드 스크립트를 종료합니다.
pause
