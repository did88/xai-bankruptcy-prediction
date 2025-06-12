@echo off
echo ============================================
echo 디버깅 테스트 배치 파일
echo ============================================
echo.

echo 현재 디렉토리: %CD%
echo.

echo Python 버전 확인:
python --version
echo.

echo 환경변수 확인:
echo DART_API_KEY = %DART_API_KEY%
echo.

echo 필요한 파일 존재 확인:
if exist "dart_2023_2024_downloader.py" (
    echo ✅ dart_2023_2024_downloader.py 존재
) else (
    echo ❌ dart_2023_2024_downloader.py 없음
)

if exist "dart_bulk_downloader.py" (
    echo ✅ dart_bulk_downloader.py 존재
) else (
    echo ❌ dart_bulk_downloader.py 없음
)

echo.
echo Python 모듈 확인:
python -c "import pandas; print('pandas OK')" 2>nul || echo "❌ pandas 모듈 없음"
python -c "import aiohttp; print('aiohttp OK')" 2>nul || echo "❌ aiohttp 모듈 없음"
python -c "import openpyxl; print('openpyxl OK')" 2>nul || echo "❌ openpyxl 모듈 없음"

echo.
echo 테스트 완료! 아무 키나 누르면 종료됩니다...
pause >nul
