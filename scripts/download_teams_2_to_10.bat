@echo off
echo 🚀 DART 팀별 다운로드 배치 시작...

REM 팀 2부터 10까지 순차 실행
for /L %%i in (2,1,10) do (
    echo.
    echo 📊 팀 %%i 다운로드 시작...
    python team_dart_downloader_fixed.py --team %%i --skip-validation
    if errorlevel 1 (
        echo ❌ 팀 %%i 다운로드 실패
        pause
        exit /b 1
    ) else (
        echo ✅ 팀 %%i 다운로드 완료
    )
)

echo.
echo 🎉 팀 2-10 다운로드 모두 완료!
echo 📁 파일 위치: data/team_downloads/
pause
