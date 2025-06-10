"""팀별 DART 재무제표 병렬 다운로드 스크립트"""

import asyncio
import os
from pathlib import Path
from typing import List, Tuple
import pandas as pd
from datetime import datetime
import argparse

# 기존 모듈 import (dart_bulk_downloader.py가 같은 디렉토리에 있다고 가정)
from dart_bulk_downloader import (
    fetch_corp_codes,
    filter_kospi_kosdaq_non_financial,
    fetch_bulk_statements,
    save_to_excel
)


def split_corps_for_teams(corp_codes: List[str], chunk_size: int = 100) -> List[Tuple[int, List[str]]]:
    """기업 코드를 팀별로 분할"""
    chunks = []
    for i in range(0, len(corp_codes), chunk_size):
        team_num = i // chunk_size + 1
        chunk = corp_codes[i:i + chunk_size]
        chunks.append((team_num, chunk))
    return chunks


async def download_team_data(
    api_key: str,
    team_num: int,
    corp_codes: List[str],
    years: range,
    output_dir: Path,
    workers: int = 10
) -> Path:
    """특정 팀의 데이터 다운로드"""
    print(f"🚀 팀 {team_num} 다운로드 시작 - {len(corp_codes)}개 기업")
    
    start_time = datetime.now()
    
    # 데이터 다운로드
    statements = await fetch_bulk_statements(api_key, corp_codes, years, workers)
    
    # 저장 경로 생성
    filename = f"dart_statements_team_{team_num:02d}.xlsx"
    output_path = output_dir / filename
    
    # 엑셀로 저장
    save_to_excel(statements, output_path)
    
    elapsed = datetime.now() - start_time
    print(f"✅ 팀 {team_num} 완료 - {elapsed.total_seconds():.1f}초 소요")
    print(f"   저장 위치: {output_path}")
    print(f"   데이터 행 수: {len(statements):,}")
    
    return output_path


def merge_team_files(team_files: List[Path], output_path: Path) -> None:
    """팀별 파일들을 하나로 병합"""
    print("\n📊 팀별 파일 병합 중...")
    
    all_data = []
    for file_path in sorted(team_files):
        if file_path.exists():
            df = pd.read_excel(file_path, engine='openpyxl')
            all_data.append(df)
            print(f"   - {file_path.name}: {len(df):,}행")
    
    if all_data:
        merged_df = pd.concat(all_data, ignore_index=True)
        save_to_excel(merged_df, output_path)
        print(f"\n✅ 병합 완료!")
        print(f"   전체 데이터: {len(merged_df):,}행")
        print(f"   저장 위치: {output_path}")
    else:
        print("❌ 병합할 파일이 없습니다.")


async def main():
    parser = argparse.ArgumentParser(description='DART 재무제표 팀별 다운로드')
    parser.add_argument('--team', type=int, help='팀 번호 (예: 1, 2, 3...)')
    parser.add_argument('--merge-only', action='store_true', help='병합만 수행')
    parser.add_argument('--list-teams', action='store_true', help='팀 분할 정보 표시')
    parser.add_argument('--workers', type=int, default=10, help='동시 작업 수 (기본값: 10)')
    parser.add_argument('--start-year', type=int, default=2015, help='시작 연도 (기본값: 2015)')
    parser.add_argument('--end-year', type=int, default=2022, help='종료 연도 (기본값: 2022)')
    
    args = parser.parse_args()
    
    # API 키 확인
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        raise EnvironmentError("DART_API_KEY 환경변수를 설정하세요")
    
    # 출력 디렉토리 생성
    output_dir = Path(__file__).resolve().parent.parent / "data" / "team_downloads"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 병합만 수행하는 경우
    if args.merge_only:
        team_files = list(output_dir.glob("dart_statements_team_*.xlsx"))
        if team_files:
            final_output = output_dir.parent / "dart_statements_merged.xlsx"
            merge_team_files(team_files, final_output)
        else:
            print("병합할 팀 파일이 없습니다.")
        return
    
    # 기업 코드 가져오기
    print("📋 기업 코드 목록 다운로드 중...")
    corp_df = await fetch_corp_codes(api_key)
    target_df = filter_kospi_kosdaq_non_financial(corp_df)
    
    # 전체 기업 코드 (필요에 따라 조정 가능)
    all_corp_codes = target_df["corp_code"].unique().tolist()
    print(f"   전체 대상 기업: {len(all_corp_codes):,}개")
    
    # 팀별로 분할
    team_chunks = split_corps_for_teams(all_corp_codes, chunk_size=100)
    
    # 팀 정보 표시
    if args.list_teams:
        print("\n📊 팀별 할당 정보:")
        for team_num, corps in team_chunks:
            print(f"   팀 {team_num}: {len(corps)}개 기업 (인덱스 {(team_num-1)*100} ~ {team_num*100-1})")
        print(f"\n💡 사용법: python {Path(__file__).name} --team [팀번호]")
        return
    
    # 특정 팀 다운로드
    if args.team:
        team_data = next((chunk for chunk in team_chunks if chunk[0] == args.team), None)
        if team_data:
            team_num, corp_codes = team_data
            years = range(args.start_year, args.end_year + 1)
            
            print(f"\n🎯 팀 {team_num} 다운로드 설정:")
            print(f"   - 기업 수: {len(corp_codes)}개")
            print(f"   - 연도: {args.start_year} ~ {args.end_year}")
            print(f"   - 예상 요청 수: {len(corp_codes) * len(years):,}개")
            print(f"   - 동시 작업 수: {args.workers}")
            
            await download_team_data(
                api_key=api_key,
                team_num=team_num,
                corp_codes=corp_codes,
                years=years,
                output_dir=output_dir,
                workers=args.workers
            )
        else:
            print(f"❌ 팀 {args.team}을(를) 찾을 수 없습니다.")
            print(f"   사용 가능한 팀: 1 ~ {len(team_chunks)}")
    else:
        print("\n사용법:")
        print(f"  1. 팀 정보 확인: python {Path(__file__).name} --list-teams")
        print(f"  2. 팀별 다운로드: python {Path(__file__).name} --team [팀번호]")
        print(f"  3. 파일 병합: python {Path(__file__).name} --merge-only")
        print("\n옵션:")
        print("  --workers N: 동시 작업 수 설정 (기본값: 10)")
        print("  --start-year YYYY: 시작 연도 (기본값: 2015)")
        print("  --end-year YYYY: 종료 연도 (기본값: 2022)")
        print("\n💡 참고: 2023년 이후 데이터는 아직 없을 수 있습니다.")


if __name__ == "__main__":
    asyncio.run(main())
