@echo off
echo 🚀 전체 DART 팀 다운로드 배치 시작...

REM 먼저 팀 목록 확인
echo 📋 팀 목록 확인 중...
python team_dart_downloader_fixed.py --list-teams

echo.
echo 📊 팀 2부터 26까지 순차 다운로드 시작...
echo ⏰ 예상 소요시간: 약 25-30분

REM 팀 2부터 26까지 순차 실행
for /L %%i in (2,1,26) do (
    echo.
    echo 🔄 팀 %%i 다운로드 시작... [%%i/26]
    python team_dart_downloader_fixed.py --team %%i --skip-validation
    if errorlevel 1 (
        echo ❌ 팀 %%i 다운로드 실패
        echo 계속하려면 Enter를 누르고, 중단하려면 Ctrl+C를 누르세요.
        pause
    ) else (
        echo ✅ 팀 %%i 다운로드 완료
    )
)

echo.
echo 🔗 모든 팀 파일 병합 중...
python team_dart_downloader_fixed.py --merge-only

if errorlevel 1 (
    echo ❌ 병합 실패
) else (
    echo ✅ 병합 완료! 
    echo 📁 최종 파일: data/dart_statements_merged.xlsx
)

echo.
echo 🎉 전체 DART 다운로드 완료!
pause
