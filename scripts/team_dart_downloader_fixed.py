"""팀별 DART 재무제표 병렬 다운로드 스크립트 (개선 버전)"""

import asyncio
import os
import time
import logging
from pathlib import Path
from typing import List, Tuple, Optional
import pandas as pd
from datetime import datetime
import argparse
import requests
from tqdm import tqdm

from dart_bulk_downloader import (
    fetch_corp_codes,
    filter_kospi_kosdaq_non_financial,
    fetch_bulk_statements,
    save_to_excel,
    DART_SINGLE_ACCOUNT_URL,
)

# 로깅 설정
def setup_logging(log_dir: Path) -> logging.Logger:
    """로깅 시스템 설정"""
    log_dir.mkdir(parents=True, exist_ok=True)
    
    logger = logging.getLogger('dart_downloader')
    logger.setLevel(logging.INFO)
    
    # 파일 핸들러
    file_handler = logging.FileHandler(
        log_dir / f"dart_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # 포매터
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# 동기 요청에 대한 속도 제한 함수 (개선된 버전)
def rate_limited_get(url: str, params: dict, delay: float = 0.07, timeout: int = 10) -> Optional[requests.Response]:
    """속도 제한이 적용된 GET 요청"""
    time.sleep(delay)
    try:
        return requests.get(url, params=params, timeout=timeout)
    except requests.exceptions.RequestException:
        return None

# 재시도 로직이 포함된 유효 보고서 확인
def has_report_for_any_year(api_key: str, corp_code: str, years: range, max_retries: int = 2) -> bool:
    """유효한 보고서가 있는지 확인 (재시도 로직 포함)"""
    for year in years:
        params = {
            "crtfc_key": api_key,
            "corp_code": corp_code,
            "bsns_year": year,
            "reprt_code": "11011",
        }
        
        for attempt in range(max_retries):
            try:
                resp = rate_limited_get(url=DART_SINGLE_ACCOUNT_URL, params=params)
                if resp and resp.status_code == 200:
                    data = resp.json()
                    if data.get("status") == "000":
                        return True
                break  # 성공하면 재시도 중단
            except Exception:
                if attempt == max_retries - 1:
                    continue  # 최종 시도 실패 시 다음 연도로
                time.sleep(1)  # 재시도 전 대기
    return False

# 기업을 팀 단위로 나누기
def split_corps_for_teams(corp_codes: List[str], chunk_size: int = 100) -> List[Tuple[int, List[str]]]:
    """기업 코드를 팀별로 분할"""
    chunks = []
    for i in range(0, len(corp_codes), chunk_size):
        team_num = i // chunk_size + 1
        chunk = corp_codes[i:i + chunk_size]
        chunks.append((team_num, chunk))
    return chunks

# 데이터 검증 함수
def validate_financial_data(df: pd.DataFrame) -> bool:
    """재무데이터 유효성 검증"""
    if df.empty:
        return False
    
    required_columns = ['corp_code', 'bsns_year']
    return all(col in df.columns for col in required_columns)

# 각 팀별 데이터 다운로드 실행
async def download_team_data(
    api_key: str,
    team_num: int,
    corp_codes: List[str],
    years: range,
    output_dir: Path,
    workers: int = 10,
    logger: Optional[logging.Logger] = None
) -> Optional[Path]:
    """팀별 데이터 다운로드 실행"""
    if not logger:
        logger = logging.getLogger('dart_downloader')
    
    logger.info(f"팀 {team_num} 다운로드 시작 - {len(corp_codes)}개 기업")
    start_time = datetime.now()

    try:
        statements = await fetch_bulk_statements(api_key, corp_codes, years, workers)

        if not validate_financial_data(statements):
            logger.warning(f"팀 {team_num}: 수집된 데이터가 없거나 유효하지 않아 파일을 저장하지 않습니다.")
            return None

        logger.info(f"팀 {team_num}: 수집된 재무제표 수 - {len(statements):,}행")

        filename = f"dart_statements_team_{team_num:02d}.xlsx"
        output_path = output_dir / filename
        save_to_excel(statements, output_path)

        elapsed = datetime.now() - start_time
        logger.info(f"팀 {team_num} 완료 - {elapsed.total_seconds():.1f}초 소요")
        logger.info(f"저장 위치: {output_path}")
        
        return output_path
        
    except Exception as e:
        logger.error(f"팀 {team_num} 다운로드 중 오류 발생: {e}")
        return None

# 팀별 파일 병합
def merge_team_files(team_files: List[Path], output_path: Path, logger: Optional[logging.Logger] = None) -> None:
    """팀별 파일들을 하나로 병합"""
    if not logger:
        logger = logging.getLogger('dart_downloader')
    
    logger.info("팀별 파일 병합 시작...")
    
    all_data = []
    for file_path in sorted(team_files):
        if file_path.exists():
            try:
                df = pd.read_excel(file_path, engine='openpyxl')
                all_data.append(df)
                logger.info(f"파일 로드: {file_path.name} - {len(df):,}행")
            except Exception as e:
                logger.error(f"파일 로드 실패: {file_path.name} - {e}")
    
    if all_data:
        merged_df = pd.concat(all_data, ignore_index=True)
        save_to_excel(merged_df, output_path)
        logger.info(f"병합 완료! 전체 데이터: {len(merged_df):,}행")
        logger.info(f"저장 위치: {output_path}")
    else:
        logger.warning("병합할 파일이 없습니다.")

# 메인 실행 함수
async def main():
    parser = argparse.ArgumentParser(description='DART 재무제표 팀별 다운로드 (개선 버전)')
    parser.add_argument('--team', type=int, help='팀 번호 (1, 2, ...)')
    parser.add_argument('--merge-only', action='store_true', help='병합만 수행')
    parser.add_argument('--list-teams', action='store_true', help='팀 분할 정보 표시')
    parser.add_argument('--workers', type=int, default=10, help='동시 작업 수 (기본값: 10)')
    parser.add_argument('--start-year', type=int, default=2015, help='시작 연도 (기본값: 2015)')
    parser.add_argument('--end-year', type=int, default=2022, help='종료 연도 (기본값: 2022)')
    parser.add_argument('--chunk-size', type=int, default=100, help='팀당 기업 수 (기본값: 100)')
    parser.add_argument('--skip-validation', action='store_true', help='유효성 검증 건너뛰기')
    
    args = parser.parse_args()

    # 출력 및 로그 디렉토리 설정
    base_dir = Path(__file__).resolve().parent.parent
    output_dir = base_dir / "data" / "team_downloads"
    log_dir = base_dir / "logs"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 로깅 설정
    logger = setup_logging(log_dir)

    if not args.team and not args.merge_only and not args.list_teams:
        print("\n사용법:")
        print(f"  python {Path(__file__).name} --list-teams")
        print(f"  python {Path(__file__).name} --team [팀번호]")
        print(f"  python {Path(__file__).name} --merge-only")
        print("\n옵션:")
        print("  --workers N: 동시 작업 수 설정")
        print("  --chunk-size N: 팀당 기업 수 설정")
        print("  --skip-validation: 유효성 검증 건너뛰기")
        return

    # API 키 확인
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        logger.error("DART_API_KEY 환경변수를 설정하세요")
        raise EnvironmentError("DART_API_KEY 환경변수를 설정하세요")

    # 병합만 수행하는 경우
    if args.merge_only:
        team_files = list(output_dir.glob("dart_statements_team_*.xlsx"))
        if team_files:
            final_output = output_dir.parent / "dart_statements_merged.xlsx"
            merge_team_files(team_files, final_output, logger)
        else:
            logger.warning("병합할 팀 파일이 없습니다.")
        return

    # 기업 코드 목록 다운로드
    logger.info("기업 코드 목록 다운로드 중...")
    corp_df = await fetch_corp_codes(api_key)
    target_df = filter_kospi_kosdaq_non_financial(corp_df)
    years = range(args.start_year, args.end_year + 1)

    # 유효성 검증 (옵션)
    if not args.skip_validation:
        logger.info("유효한 사업보고서가 있는 기업 필터링 중...")
        valid_corp_codes = []
        
        # 진행률 표시
        with tqdm(total=len(target_df), desc="유효성 검증") as pbar:
            for corp_code in target_df["corp_code"]:
                if has_report_for_any_year(api_key, corp_code, years):
                    valid_corp_codes.append(corp_code)
                pbar.update(1)
        
        logger.info(f"유효 기업 수: {len(valid_corp_codes)}개")
        if len(valid_corp_codes) == 0:
            logger.error("사용할 수 있는 기업이 없습니다.")
            return
        all_corp_codes = valid_corp_codes
    else:
        all_corp_codes = target_df["corp_code"].tolist()
        logger.info(f"전체 기업 수: {len(all_corp_codes)}개 (유효성 검증 건너뜀)")

    # 팀별 분할
    team_chunks = split_corps_for_teams(all_corp_codes, chunk_size=args.chunk_size)

    if args.list_teams:
        logger.info("팀별 할당 정보:")
        for team_num, corps in team_chunks:
            logger.info(f"팀 {team_num}: {len(corps)}개 기업")
        return

    if args.team:
        team_data = next((chunk for chunk in team_chunks if chunk[0] == args.team), None)
        if team_data:
            team_num, corp_codes = team_data
            logger.info(f"팀 {team_num} 다운로드 설정:")
            logger.info(f"- 기업 수: {len(corp_codes)}개")
            logger.info(f"- 연도: {args.start_year} ~ {args.end_year}")
            logger.info(f"- 예상 요청 수: {len(corp_codes) * len(years):,}개")
            logger.info(f"- 동시 작업 수: {args.workers}")

            result_path = await download_team_data(
                api_key=api_key,
                team_num=team_num,
                corp_codes=corp_codes,
                years=years,
                output_dir=output_dir,
                workers=args.workers,
                logger=logger
            )
            
            if result_path:
                logger.info(f"팀 {team_num} 다운로드 성공적으로 완료")
            else:
                logger.error(f"팀 {team_num} 다운로드 실패")
        else:
            logger.error(f"팀 {args.team} 정보를 찾을 수 없습니다.")
            logger.info(f"사용 가능한 팀: 1 ~ {len(team_chunks)}")

if __name__ == "__main__":
    asyncio.run(main())
