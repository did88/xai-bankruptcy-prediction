#!/bin/bash

echo "============================================"
echo "DART 2023-2024년 재무제표 다운로드 스크립트"
echo "============================================"
echo

# API 키 확인
if [ -z "$DART_API_KEY" ]; then
    echo "❌ 오류: DART_API_KEY 환경변수가 설정되지 않았습니다."
    echo "   설정 방법: export DART_API_KEY=your_api_key"
    echo "   또는 .env 파일에 DART_API_KEY=your_api_key 추가"
    exit 1
fi

# Python 스크립트 파일명
SCRIPT_NAME="dart_2023_2024_downloader.py"

# 스크립트 존재 확인
if [ ! -f "$SCRIPT_NAME" ]; then
    echo "❌ 오류: $SCRIPT_NAME 파일을 찾을 수 없습니다."
    echo "   현재 디렉토리에 스크립트 파일이 있는지 확인하세요."
    exit 1
fi

# 함수 정의
show_menu() {
    echo "📋 사용 가능한 옵션:"
    echo "   1. 팀 정보 확인"
    echo "   2. 특정 팀 다운로드"
    echo "   3. 2023-2024년 파일 병합"
    echo "   4. 전체 데이터 병합 (2015-2024)"
    echo "   5. 전체 팀 순차 다운로드"
    echo "   6. 종료"
    echo
}

list_teams() {
    echo
    echo "📊 팀 정보 확인 중..."
    python3 "$SCRIPT_NAME" --list-teams
    echo
}

download_team() {
    echo
    read -p "다운로드할 팀 번호를 입력하세요: " team_num
    read -p "동시 작업 수 (기본값 10, 엔터로 스킵): " workers
    workers=${workers:-10}

    echo
    echo "🚀 팀 $team_num 다운로드 시작..."
    python3 "$SCRIPT_NAME" --team "$team_num" --workers "$workers"
    echo
    echo "완료! 계속하려면 엔터를 누르세요..."
    read
}

merge_2023_2024() {
    echo
    echo "📊 2023-2024년 팀별 파일 병합 중..."
    python3 "$SCRIPT_NAME" --merge-only
    echo
    echo "완료! 계속하려면 엔터를 누르세요..."
    read
}

merge_all() {
    echo
    echo "🔄 전체 데이터 병합 중 (2015-2024년)..."
    python3 "$SCRIPT_NAME" --merge-all
    echo
    echo "완료! 계속하려면 엔터를 누르세요..."
    read
}

download_all_teams() {
    echo
    echo "⚠️ 전체 팀 다운로드는 시간이 매우 오래 걸립니다."
    read -p "계속하시겠습니까? (y/N): " confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        return
    fi

    read -p "동시 작업 수 (기본값 10): " workers
    workers=${workers:-10}

    echo
    echo "🚀 전체 팀 순차 다운로드 시작..."
    echo "   동시 작업 수: $workers"
    echo

    # 팀 개수 확인 (일반적으로 30개 팀 정도)
    for i in {1..30}; do
        echo
        echo "============================================="
        echo "팀 $i 다운로드 중... (진행률: $i/30)"
        echo "============================================="
        
        if python3 "$SCRIPT_NAME" --team "$i" --workers "$workers"; then
            echo "✅ 팀 $i 완료"
        else
            echo "⚠️ 팀 $i 다운로드 중 오류 발생, 계속 진행..."
        fi
    done

    echo
    echo "🎉 전체 팀 다운로드 완료!"
    echo "📊 이제 파일 병합을 실행하세요..."
    python3 "$SCRIPT_NAME" --merge-only

    echo
    echo "완료! 계속하려면 엔터를 누르세요..."
    read
}

# 메인 루프
while true; do
    show_menu
    read -p "선택하세요 (1-6): " choice

    case $choice in
        1)
            list_teams
            ;;
        2)
            download_team
            ;;
        3)
            merge_2023_2024
            ;;
        4)
            merge_all
            ;;
        5)
            download_all_teams
            ;;
        6)
            echo
            echo "👋 스크립트를 종료합니다."
            exit 0
            ;;
        *)
            echo
            echo "❌ 잘못된 선택입니다. 1-6 중에서 선택하세요."
            echo
            ;;
    esac
done
