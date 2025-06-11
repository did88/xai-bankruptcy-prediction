"""DART 다운로드 공통 유틸리티 함수들"""

import logging
import time
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional
import requests
import pandas as pd

from config import (
    MAX_ERROR_LOGS, LOG_FORMAT, API_TIMEOUT, MAX_RETRIES,
    DART_SINGLE_ACCOUNT_URL
)

def setup_logging(log_dir: Path, script_name: str = "dart_downloader") -> logging.Logger:
    """표준화된 로깅 시스템 설정"""
    log_dir.mkdir(parents=True, exist_ok=True)
    
    logger = logging.getLogger(script_name)
    
    # 기존 핸들러 제거 (중복 방지)
    if logger.handlers:
        logger.handlers.clear()
    
    logger.setLevel(logging.INFO)
    
    # 파일 핸들러
    log_filename = f"{script_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    file_handler = logging.FileHandler(log_dir / log_filename, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # 포매터
    formatter = logging.Formatter(LOG_FORMAT)
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def rate_limited_get(url: str, params: dict, delay: float = 0.07) -> Optional[requests.Response]:
    """속도 제한이 적용된 GET 요청"""
    time.sleep(delay)
    try:
        return requests.get(url, params=params, timeout=API_TIMEOUT)
    except requests.exceptions.RequestException as e:
        logging.getLogger().debug(f"Request failed: {e}")
        return None

def has_report_for_any_year(api_key: str, corp_code: str, years: range) -> bool:
    """유효한 보고서가 있는지 확인 (재시도 로직 포함)"""
    for year in years:
        params = {
            "crtfc_key": api_key,
            "corp_code": corp_code,
            "bsns_year": year,
            "reprt_code": "11011",
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                resp = rate_limited_get(url=DART_SINGLE_ACCOUNT_URL, params=params)
                if resp and resp.status_code == 200:
                    data = resp.json()
                    if data.get("status") == "000":
                        return True
                break  # 성공하면 재시도 중단
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    logging.getLogger().debug(f"Final attempt failed for {corp_code} {year}: {e}")
                    continue
                time.sleep(1)  # 재시도 전 대기
    return False

def split_corps_for_teams(corp_codes: List[str], chunk_size: int = 100) -> List[Tuple[int, List[str]]]:
    """기업 코드를 팀별로 분할"""
    chunks = []
    for i in range(0, len(corp_codes), chunk_size):
        team_num = i // chunk_size + 1
        chunk = corp_codes[i:i + chunk_size]
        chunks.append((team_num, chunk))
    return chunks

def validate_financial_data(df: pd.DataFrame) -> bool:
    """재무데이터 유효성 검증"""
    if df.empty:
        return False
    
    required_columns = ['corp_code', 'bsns_year']
    return all(col in df.columns for col in required_columns)

def get_project_paths(script_path: Path) -> dict:
    """프로젝트 경로들을 표준화해서 반환"""
    base_dir = script_path.resolve().parent.parent
    
    return {
        'base': base_dir,
        'data': base_dir / "data",
        'team_downloads': base_dir / "data" / "team_downloads",
        'logs': base_dir / "logs",
        'scripts': base_dir / "scripts"
    }

def format_duration(seconds: float) -> str:
    """초를 읽기 쉬운 형태로 포맷"""
    if seconds < 60:
        return f"{seconds:.1f}초"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}분"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}시간"

def safe_mkdir(path: Path) -> None:
    """안전한 디렉토리 생성"""
    try:
        path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logging.getLogger().error(f"디렉토리 생성 실패: {path} - {e}")
        raise
