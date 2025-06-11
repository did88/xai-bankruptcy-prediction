#!/bin/bash

# 담당자별 팀 다운로드 스크립트

echo "🚀 DART 전체 데이터 다운로드 - 4명 분산 처리"
echo "====================================================="

# 담당자 1: 팀 1-7
echo "👤 담당자 1: 팀 1-7 처리 중..."
for team in {1..7}; do
    echo "📊 팀 $team 다운로드..."
    python cached_team_downloader.py --team $team --skip-validation
    if [ $? -ne 0 ]; then
        echo "❌ 팀 $team 실패, 재시도..."
        python cached_team_downloader.py --team $team --skip-validation
    fi
done

echo "✅ 담당자 1 완료!"
