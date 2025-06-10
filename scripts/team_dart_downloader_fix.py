import asyncio
import os
import time
from pathlib import Path
from typing import List, Tuple
import pandas as pd
from datetime import datetime
import argparse
import requests

from dart_bulk_downloader import (
    fetch_corp_codes,
    filter_kospi_kosdaq_non_financial,
    fetch_bulk_statements,
    save_to_excel,
    DART_SINGLE_ACCOUNT_URL,
)

# ✅ 동기 요청에 대한 초당 14~15회 제한 함수
def rate_limited_get(url, params, delay=0.07):
    time.sleep(delay)
    return requests.get(url, params=params, timeout=10)

# ✅ 유효 보고서 존재 여부 확인 (연도별 순차 요청 + 제한 적용)
def has_report_for_any_year(api_key: str, corp_code: str, years: range) -> bool:
    for year in years:
        params = {
            "crtfc_key": api_key,
            "corp_code": corp_code,
            "bsns_year": year,
            "reprt_code": "11011",
        }
        try:
            resp = rate_limited_get(DART_SINGLE_ACCOUNT_URL, params)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "000":
                    return True
        except Exception:
            continue
    return False

# ✅ 기업을 팀 단위로 나누기
def split_corps_for_teams(corp_codes: List[str], chunk_size: int = 500) -> List[Tuple[int, List[str]]]:
    chunks = []
    for i in range(0, len(corp_codes), chunk_size):
        team_num = i // chunk_size + 1
        chunk = corp_codes[i:i + chunk_size]
        chunks.append((team_num, chunk))
    return chunks

# ✅ 각 팀별 데이터 다운로드 실행
async def download_team_data(api_key: str, team_num: int, corp_codes: List[str], years: range, output_dir: Path, workers: int = 10) -> Path:
    print(f"🚀 팀 {team_num} 다운로드 시작 - {len(corp_codes)}개 기업")
    start_time = datetime.now()

    statements = await fetch_bulk_statements(api_key, corp_codes, years, workers)
    filename = f"dart_statements_team_{team_num:02d}.xlsx"
    output_path = output_dir / filename
    save_to_excel(statements, output_path)

    elapsed = datetime.now() - start_time
    print(f"✅ 팀 {team_num} 완료 - {elapsed.total_seconds():.1f}초 소요")
    print(f"   저장 위치: {output_path}")
    print(f"   데이터 행 수: {len(statements):,}")
    return output_path

# ✅ 팀별 파일 병합
def merge_team_files(team_files: List[Path], output_path: Path) -> None:
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

# ✅ 메인 실행 함수
async def main():
    parser = argparse.ArgumentParser(description='DART 재무제표 팀별 다운로드')
    parser.add_argument('--team', type=int, help='팀 번호 (1, 2, ...)')
    parser.add_argument('--merge-only', action='store_true', help='병합만 수행')
    parser.add_argument('--list-teams', action='store_true', help='팀 분할 정보 표시')
    parser.add_argument('--workers', type=int, default=10, help='동시 작업 수')
    parser.add_argument('--start-year', type=int, default=2015)
    parser.add_argument('--end-year', type=int, default=2022)
    args = parser.parse_args()

    if not args.team and not args.merge_only and not args.list_teams:
        print("\n사용법:")
        print(f"  python {Path(__file__).name} --list-teams")
        print(f"  python {Path(__file__).name} --team [팀번호]")
        print(f"  python {Path(__file__).name} --merge-only")
        return

    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        raise EnvironmentError("DART_API_KEY 환경변수를 설정하세요")

    output_dir = Path(__file__).resolve().parent.parent / "data" / "team_downloads"
    output_dir.mkdir(parents=True, exist_ok=True)

    print("📋 기업 코드 목록 다운로드 중...")
    corp_df = await fetch_corp_codes(api_key)
    target_df = filter_kospi_kosdaq_non_financial(corp_df)
    years = range(args.start_year, args.end_year + 1)

    print("\n📤 유효한 사업보고서가 있는 기업 필터링 중...")
    valid_corp_codes = []
    for i, corp_code in enumerate(target_df["corp_code"]):
        if has_report_for_any_year(api_key, corp_code, years):
            valid_corp_codes.append(corp_code)
        if (i + 1) % 100 == 0:
            print(f"   진행 중: {i + 1} / {len(target_df)}")

    print(f"✅ 유효 기업 수: {len(valid_corp_codes)}개")
    if len(valid_corp_codes) == 0:
        print("❌ 사용할 수 있는 기업이 없습니다.")
        return

    all_corp_codes = valid_corp_codes
    team_chunks = split_corps_for_teams(all_corp_codes, chunk_size=100)

    if args.list_teams:
        print("\n📊 팀별 할당 정보:")
        for team_num, corps in team_chunks:
            print(f"   팀 {team_num}: {len(corps)}개 기업")
        return

    if args.team:
        team_data = next((chunk for chunk in team_chunks if chunk[0] == args.team), None)
        if team_data:
            team_num, corp_codes = team_data
            print(f"\n🌟 팀 {team_num} 다운로드 설정:")
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
            print(f"❌ 팀 {args.team} 정보를 찾을 수 없습니다.")

if __name__ == "__main__":
    asyncio.run(main())
