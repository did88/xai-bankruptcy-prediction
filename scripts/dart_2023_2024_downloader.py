"""DART 2023-2024년 재무제표 다운로드 스크립트
기존 2015-2022년 데이터와 동일한 조건으로 2023-2024년 데이터를 수집합니다.
"""

import asyncio
import os
from pathlib import Path
from typing import List, Tuple
import pandas as pd
from datetime import datetime
import argparse

# 기존 모듈 import
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


async def download_team_data_2023_2024(
    api_key: str,
    team_num: int,
    corp_codes: List[str],
    years: range,
    output_dir: Path,
    workers: int = 10
) -> Path:
    """특정 팀의 2023-2024년 데이터 다운로드"""
    print(f"🚀 팀 {team_num} (2023-2024) 다운로드 시작 - {len(corp_codes)}개 기업")
    
    start_time = datetime.now()
    
    # 데이터 다운로드
    statements = await fetch_bulk_statements(api_key, corp_codes, years, workers)
    
    # 저장 경로 생성 (2023-2024 구분을 위해 파일명 수정)
    filename = f"dart_statements_2023_2024_team_{team_num:02d}.xlsx"
    output_path = output_dir / filename
    
    # 엑셀로 저장
    save_to_excel(statements, output_path)
    
    elapsed = datetime.now() - start_time
    print(f"✅ 팀 {team_num} (2023-2024) 완료 - {elapsed.total_seconds():.1f}초 소요")
    print(f"   저장 위치: {output_path}")
    print(f"   데이터 행 수: {len(statements):,}")
    
    return output_path


def merge_team_files_2023_2024(team_files: List[Path], output_path: Path) -> None:
    """2023-2024년 팀별 파일들을 하나로 병합"""
    print("\n📊 2023-2024년 팀별 파일 병합 중...")
    
    all_data = []
    for file_path in sorted(team_files):
        if file_path.exists():
            df = pd.read_excel(file_path, engine='openpyxl')
            all_data.append(df)
            print(f"   - {file_path.name}: {len(df):,}행")
    
    if all_data:
        merged_df = pd.concat(all_data, ignore_index=True)
        save_to_excel(merged_df, output_path)
        print(f"\n✅ 2023-2024년 데이터 병합 완료!")
        print(f"   전체 데이터: {len(merged_df):,}행")
        print(f"   저장 위치: {output_path}")
        
        # 연도별 데이터 분포 확인
        if 'bsns_year' in merged_df.columns:
            year_counts = merged_df['bsns_year'].value_counts().sort_index()
            print(f"   연도별 분포:")
            for year, count in year_counts.items():
                print(f"     {year}년: {count:,}개 기업")
    else:
        print("❌ 병합할 파일이 없습니다.")


def merge_with_existing_data(
    new_data_path: Path, 
    existing_data_path: Path, 
    final_output_path: Path
) -> None:
    """2023-2024년 새 데이터를 기존 2015-2022년 데이터와 병합"""
    print("\n🔄 기존 데이터와 새 데이터 병합 중...")
    
    # 기존 데이터 로드
    if existing_data_path.exists():
        print(f"   기존 데이터 로드: {existing_data_path}")
        existing_df = pd.read_excel(existing_data_path, engine='openpyxl')
        print(f"   기존 데이터: {len(existing_df):,}행")
        
        if 'bsns_year' in existing_df.columns:
            existing_years = sorted(existing_df['bsns_year'].unique())
            print(f"   기존 연도 범위: {min(existing_years)} ~ {max(existing_years)}")
    else:
        print(f"   ⚠️ 기존 데이터 파일을 찾을 수 없습니다: {existing_data_path}")
        existing_df = pd.DataFrame()
    
    # 새 데이터 로드
    if new_data_path.exists():
        print(f"   새 데이터 로드: {new_data_path}")
        new_df = pd.read_excel(new_data_path, engine='openpyxl')
        print(f"   새 데이터: {len(new_df):,}행")
        
        if 'bsns_year' in new_df.columns:
            new_years = sorted(new_df['bsns_year'].unique())
            print(f"   새 연도 범위: {min(new_years)} ~ {max(new_years)}")
    else:
        print(f"   ❌ 새 데이터 파일을 찾을 수 없습니다: {new_data_path}")
        return
    
    # 데이터 병합
    if not existing_df.empty:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        print(f"   병합된 전체 데이터: {len(combined_df):,}행")
    else:
        combined_df = new_df
        print(f"   기존 데이터가 없어서 새 데이터만 저장: {len(combined_df):,}행")
    
    # 최종 파일 저장
    save_to_excel(combined_df, final_output_path)
    print(f"✅ 최종 병합 완료!")
    print(f"   저장 위치: {final_output_path}")
    
    # 연도별 분포 확인
    if 'bsns_year' in combined_df.columns:
        year_counts = combined_df['bsns_year'].value_counts().sort_index()
        print(f"   최종 연도별 분포:")
        for year, count in year_counts.items():
            print(f"     {year}년: {count:,}개 기업")


async def main():
    parser = argparse.ArgumentParser(description='DART 2023-2024년 재무제표 다운로드')
    parser.add_argument('--team', type=int, help='팀 번호 (예: 1, 2, 3...)')
    parser.add_argument('--merge-only', action='store_true', help='2023-2024년 팀별 파일 병합만 수행')
    parser.add_argument('--merge-all', action='store_true', help='기존 데이터와 새 데이터 전체 병합')
    parser.add_argument('--list-teams', action='store_true', help='팀 분할 정보 표시')
    parser.add_argument('--workers', type=int, default=10, help='동시 작업 수 (기본값: 10)')
    
    args = parser.parse_args()
    
    # API 키 확인
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        raise EnvironmentError("DART_API_KEY 환경변수를 설정하세요")
    
    # 출력 디렉토리 생성
    output_dir = Path(__file__).resolve().parent.parent / "data" / "team_downloads"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 기존 데이터와 새 데이터 전체 병합
    if args.merge_all:
        new_data_path = output_dir.parent / "dart_statements_2023_2024_merged.xlsx"
        existing_data_path = output_dir.parent / "dart_statements_merged.xlsx"  # 기존 2015-2022 데이터
        final_output_path = output_dir.parent / "dart_statements_2015_2024_merged.xlsx"
        
        merge_with_existing_data(new_data_path, existing_data_path, final_output_path)
        return
    
    # 2023-2024년 팀별 파일 병합만 수행
    if args.merge_only:
        team_files = list(output_dir.glob("dart_statements_2023_2024_team_*.xlsx"))
        if team_files:
            final_output = output_dir.parent / "dart_statements_2023_2024_merged.xlsx"
            merge_team_files_2023_2024(team_files, final_output)
        else:
            print("병합할 2023-2024년 팀 파일이 없습니다.")
        return
    
    # 기업 코드 가져오기
    print("📋 기업 코드 목록 다운로드 중...")
    corp_df = await fetch_corp_codes(api_key)
    target_df = filter_kospi_kosdaq_non_financial(corp_df)
    
    # 전체 기업 코드 (기존과 동일한 조건)
    all_corp_codes = target_df["corp_code"].unique().tolist()
    print(f"   전체 대상 기업: {len(all_corp_codes):,}개")
    
    # 팀별로 분할 (기존과 동일)
    team_chunks = split_corps_for_teams(all_corp_codes, chunk_size=100)
    
    # 팀 정보 표시
    if args.list_teams:
        print("\n📊 팀별 할당 정보 (2023-2024년 다운로드용):")
        for team_num, corps in team_chunks:
            print(f"   팀 {team_num}: {len(corps)}개 기업 (인덱스 {(team_num-1)*100} ~ {team_num*100-1})")
        print(f"\n💡 사용법:")
        print(f"   1. 팀별 다운로드: python {Path(__file__).name} --team [팀번호]")
        print(f"   2. 2023-2024 파일 병합: python {Path(__file__).name} --merge-only")
        print(f"   3. 전체 데이터 병합: python {Path(__file__).name} --merge-all")
        return
    
    # 특정 팀 다운로드 (2023-2024년)
    if args.team:
        team_data = next((chunk for chunk in team_chunks if chunk[0] == args.team), None)
        if team_data:
            team_num, corp_codes = team_data
            years = range(2023, 2025)  # 2023, 2024년
            
            print(f"\n🎯 팀 {team_num} (2023-2024년) 다운로드 설정:")
            print(f"   - 기업 수: {len(corp_codes)}개")
            print(f"   - 연도: 2023 ~ 2024")
            print(f"   - 예상 요청 수: {len(corp_codes) * 2:,}개")  # 2년치
            print(f"   - 동시 작업 수: {args.workers}")
            
            await download_team_data_2023_2024(
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
        print("\n💡 2023-2024년 데이터 다운로드 사용법:")
        print(f"  1. 팀 정보 확인: python {Path(__file__).name} --list-teams")
        print(f"  2. 팀별 다운로드: python {Path(__file__).name} --team [팀번호]")
        print(f"  3. 2023-2024 파일 병합: python {Path(__file__).name} --merge-only")
        print(f"  4. 전체 데이터 병합: python {Path(__file__).name} --merge-all")
        print("\n옵션:")
        print("  --workers N: 동시 작업 수 설정 (기본값: 10)")
        print("\n📝 참고:")
        print("  - 기존 2015-2022년 데이터와 동일한 필터링 조건 적용")
        print("  - KOSPI/KOSDAQ 비금융업종 기업 대상")
        print("  - 연결재무제표(CFS) 우선, 없으면 개별재무제표(OFS) 수집")


if __name__ == "__main__":
    asyncio.run(main())
