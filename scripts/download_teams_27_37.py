"""DART 2023-2024년 팀 27-37 일괄 다운로드 스크립트"""

import asyncio
import os
from pathlib import Path
from datetime import datetime
import argparse

# 기존 모듈 import
from dart_bulk_downloader import (
    fetch_corp_codes,
    filter_kospi_kosdaq_non_financial,
    fetch_bulk_statements,
    save_to_excel
)


def split_corps_for_teams(corp_codes, chunk_size=100):
    """기업 코드를 팀별로 분할"""
    chunks = []
    for i in range(0, len(corp_codes), chunk_size):
        team_num = i // chunk_size + 1
        chunk = corp_codes[i:i + chunk_size]
        chunks.append((team_num, chunk))
    return chunks


async def download_team_data(api_key, team_num, corp_codes, years, output_dir, workers=10):
    """특정 팀의 2023-2024년 데이터 다운로드"""
    print(f"\n🚀 팀 {team_num} 다운로드 시작 - {len(corp_codes)}개 기업")
    
    start_time = datetime.now()
    
    try:
        # 데이터 다운로드
        statements = await fetch_bulk_statements(api_key, corp_codes, years, workers)
        
        # 저장 경로 생성
        filename = f"dart_statements_2023_2024_team_{team_num:02d}.xlsx"
        output_path = output_dir / filename
        
        # 엑셀로 저장
        if not statements.empty:
            save_to_excel(statements, output_path)
            elapsed = datetime.now() - start_time
            print(f"✅ 팀 {team_num} 완료 - {elapsed.total_seconds():.1f}초 소요")
            print(f"   저장 위치: {output_path}")
            print(f"   데이터 행 수: {len(statements):,}")
            return True
        else:
            print(f"⚠️ 팀 {team_num} - 다운로드된 데이터가 없습니다")
            return False
            
    except Exception as e:
        elapsed = datetime.now() - start_time
        print(f"❌ 팀 {team_num} 오류 - {elapsed.total_seconds():.1f}초 소요")
        print(f"   오류 내용: {str(e)}")
        return False


async def download_teams_batch(api_key, start_team=27, end_team=37, workers=10):
    """팀 27-37 일괄 다운로드"""
    
    print("=" * 50)
    print(f"DART 2023-2024년 팀 {start_team}-{end_team} 일괄 다운로드")
    print("=" * 50)
    print()
    
    # 출력 디렉토리 생성
    output_dir = Path(__file__).resolve().parent.parent / "data" / "team_downloads"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 기업 코드 가져오기
    print("📋 기업 코드 목록 다운로드 중...")
    corp_df = await fetch_corp_codes(api_key)
    target_df = filter_kospi_kosdaq_non_financial(corp_df)
    
    # 전체 기업 코드
    all_corp_codes = target_df["corp_code"].unique().tolist()
    print(f"   전체 대상 기업: {len(all_corp_codes):,}개")
    
    # 팀별로 분할
    team_chunks = split_corps_for_teams(all_corp_codes, chunk_size=100)
    total_teams = len(team_chunks)
    print(f"   총 팀 수: {total_teams}")
    
    # 다운로드할 팀 범위 확인
    teams_to_download = []
    for team_num in range(start_team, end_team + 1):
        if team_num <= total_teams:
            team_data = next((chunk for chunk in team_chunks if chunk[0] == team_num), None)
            if team_data:
                teams_to_download.append(team_data)
        else:
            print(f"⚠️ 팀 {team_num}은(는) 존재하지 않습니다 (최대 팀: {total_teams})")
    
    if not teams_to_download:
        print("❌ 다운로드할 팀이 없습니다.")
        return
    
    print(f"\n📊 다운로드 계획:")
    print(f"   대상 팀: {len(teams_to_download)}개 팀")
    print(f"   연도: 2023-2024")
    print(f"   동시 작업 수: {workers}")
    
    total_corps = sum(len(corps) for _, corps in teams_to_download)
    print(f"   총 기업 수: {total_corps:,}개")
    print(f"   예상 요청 수: {total_corps * 2:,}개")
    
    # 사용자 확인
    response = input(f"\n계속 진행하시겠습니까? (y/N): ")
    if response.lower() != 'y':
        print("다운로드를 취소합니다.")
        return
    
    # 연도 설정
    years = range(2023, 2025)  # 2023, 2024년
    
    # 팀별 다운로드 실행
    success_count = 0
    failed_teams = []
    
    total_start_time = datetime.now()
    
    for i, (team_num, corp_codes) in enumerate(teams_to_download, 1):
        print(f"\n{'='*20} 진행률: {i}/{len(teams_to_download)} {'='*20}")
        
        # 기존 파일 확인
        filename = f"dart_statements_2023_2024_team_{team_num:02d}.xlsx"
        output_path = output_dir / filename
        
        if output_path.exists():
            print(f"⏭️ 팀 {team_num} - 이미 다운로드됨 (건너뜀)")
            success_count += 1
            continue
        
        # 다운로드 실행
        success = await download_team_data(
            api_key=api_key,
            team_num=team_num,
            corp_codes=corp_codes,
            years=years,
            output_dir=output_dir,
            workers=workers
        )
        
        if success:
            success_count += 1
        else:
            failed_teams.append(team_num)
        
        # 중간 휴식 (API 부하 방지)
        if i < len(teams_to_download):
            print("   💤 3초 대기 중...")
            await asyncio.sleep(3)
    
    # 결과 요약
    total_elapsed = datetime.now() - total_start_time
    print(f"\n{'='*50}")
    print(f"📊 다운로드 완료 요약")
    print(f"{'='*50}")
    print(f"   총 소요 시간: {total_elapsed.total_seconds():.1f}초")
    print(f"   성공한 팀: {success_count}/{len(teams_to_download)}")
    
    if failed_teams:
        print(f"   실패한 팀: {failed_teams}")
        print(f"\n💡 실패한 팀은 다음 명령어로 재시도 가능:")
        for team in failed_teams:
            print(f"      python dart_2023_2024_downloader.py --team {team}")
    else:
        print(f"   🎉 모든 팀 다운로드 성공!")
    
    # 자동 병합 제안
    if success_count > 0:
        merge_response = input(f"\n파일 병합을 실행하시겠습니까? (y/N): ")
        if merge_response.lower() == 'y':
            print("\n📊 파일 병합 중...")
            # 병합 스크립트 실행
            import subprocess
            try:
                result = subprocess.run([
                    'python', 'dart_2023_2024_downloader.py', '--merge-only'
                ], capture_output=True, text=True)
                
                if result.returncode == 0:
                    print("✅ 파일 병합 완료!")
                else:
                    print(f"❌ 병합 중 오류: {result.stderr}")
            except Exception as e:
                print(f"❌ 병합 실행 오류: {e}")


async def main():
    parser = argparse.ArgumentParser(description='DART 2023-2024년 팀 27-37 일괄 다운로드')
    parser.add_argument('--start', type=int, default=27, help='시작 팀 번호 (기본값: 27)')
    parser.add_argument('--end', type=int, default=37, help='종료 팀 번호 (기본값: 37)')
    parser.add_argument('--workers', type=int, default=10, help='동시 작업 수 (기본값: 10)')
    
    args = parser.parse_args()
    
    # API 키 확인
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        print("❌ 오류: DART_API_KEY 환경변수를 설정하세요")
        print("   설정 방법: set DART_API_KEY=your_api_key")
        return
    
    print(f"🔑 API 키: {api_key[:10]}... (처음 10자리)")
    
    # 다운로드 실행
    await download_teams_batch(
        api_key=api_key,
        start_team=args.start,
        end_team=args.end,
        workers=args.workers
    )


if __name__ == "__main__":
    asyncio.run(main())
