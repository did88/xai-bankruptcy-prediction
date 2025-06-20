"""Utilities for bulk downloading DART financial statements."""

from __future__ import annotations

import asyncio
import io
import zipfile
from collections import deque
from pathlib import Path
from typing import Iterable, List
import xml.etree.ElementTree as ET

import aiohttp
import pandas as pd
from dotenv import load_dotenv
import argparse
import os

# Load API key from .env at project root
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

DART_CORPCODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
DART_SINGLE_ACCOUNT_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"

class RateLimiter:
    """Simple asyncio rate limiter for API calls."""

    def __init__(self, max_calls: int, period: float) -> None:
        self.max_calls = max_calls
        self.period = period
        self.calls: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            now = asyncio.get_event_loop().time()
            while self.calls and self.calls[0] <= now - self.period:
                self.calls.popleft()
            if len(self.calls) >= self.max_calls:
                wait_for = self.period - (now - self.calls[0])
                await asyncio.sleep(wait_for)
                now = asyncio.get_event_loop().time()
                while self.calls and self.calls[0] <= now - self.period:
                    self.calls.popleft()
            self.calls.append(now)

async def fetch_corp_codes(api_key: str) -> pd.DataFrame:
    """Download and parse DART corp code list with error handling."""
    url = f"{DART_CORPCODE_URL}?crtfc_key={api_key}"
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            print(f"기업 코드 다운로드 시도 {attempt + 1}/{max_retries}...")
            
            # 타임아웃과 헤더 설정
            timeout = aiohttp.ClientTimeout(total=120, connect=30)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.get(url) as resp:
                    print(f"HTTP 상태 코드: {resp.status}")
                    print(f"Content-Type: {resp.headers.get('Content-Type', 'Unknown')}")
                    
                    resp.raise_for_status()
                    data = await resp.read()
                    print(f"응답 데이터 크기: {len(data)} bytes")
                    
                    # 응답이 ZIP 파일인지 확인
                    if len(data) < 10:
                        raise ValueError(f"응답 데이터가 너무 짧습니다: {len(data)} bytes")
                    
                    # ZIP 파일 매직 넘버 확인 (PK)
                    if not data.startswith(b'PK'):
                        # 텍스트 응답인 경우 내용 확인
                        try:
                            text_response = data.decode('utf-8')[:500]
                            print(f"비ZIP 응답 내용 (처음 500자): {text_response}")
                        except:
                            print(f"비ZIP 응답 내용 (hex): {data[:50].hex()}")
                        
                        if attempt < max_retries - 1:
                            wait_time = (attempt + 1) * 5
                            print(f"ZIP 파일이 아닙니다. {wait_time}초 후 재시도...")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            raise ValueError("ZIP 파일 형식이 아닌 응답을 받았습니다")

            # ZIP 파일 처리
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                xml_data = zf.read("CORPCODE.xml").decode("utf-8")
            
            root = ET.fromstring(xml_data)
            data_list = []
            
            for corp in root.findall('.//list'):
                corp_dict = {}
                for child in corp:
                    corp_dict[child.tag] = child.text
                data_list.append(corp_dict)
            
            df = pd.DataFrame(data_list)
            print(f"Available columns: {list(df.columns)}")
            print(f"✅ 기업 코드 다운로드 성공: {len(df)}개 기업")
            return df
            
        except asyncio.TimeoutError:
            print(f"❌ 시도 {attempt + 1}: 타임아웃 (120초)")
        except aiohttp.ClientError as e:
            print(f"❌ 시도 {attempt + 1}: 네트워크 오류 - {e}")
        except zipfile.BadZipFile as e:
            print(f"❌ 시도 {attempt + 1}: ZIP 파일 오류 - {e}")
        except Exception as e:
            print(f"❌ 시도 {attempt + 1}: 기타 오류 - {e}")
        
        if attempt < max_retries - 1:
            wait_time = (attempt + 1) * 10  # 점진적으로 대기 시간 증가
            print(f"⏳ {wait_time}초 대기 후 재시도...")
            await asyncio.sleep(wait_time)
    
    raise RuntimeError(f"기업 코드 다운로드가 {max_retries}번 모두 실패했습니다")

def filter_kospi_kosdaq_non_financial(df: pd.DataFrame) -> pd.DataFrame:
    if "stock_code" not in df.columns:
        print("Error: 'stock_code' column not found!")
        return pd.DataFrame()
    
    has_stock = df["stock_code"].notna() & (df["stock_code"].str.strip() != "")
    valid_stock = has_stock & df["stock_code"].str.match(r'^\d{6}$', na=False)
    
    print(f"Total firms: {len(df):,}")
    print(f"Firms with stock codes: {has_stock.sum():,}")
    print(f"Firms with valid 6-digit codes: {valid_stock.sum():,}")
    
    if "corp_name" in df.columns:
        financial_keywords = "금융|은행|보험|증권|캐피탈|투자|자산운용|신용|저축|카드|리스|신탁|펀드"
        is_financial = df["corp_name"].str.contains(financial_keywords, na=False, case=False)
        non_financial = valid_stock & ~is_financial
        
        print(f"Financial firms filtered out: {(valid_stock & is_financial).sum():,}")
        print(f"Final non-financial firms: {non_financial.sum():,}")
        
        return df[non_financial]
    else:
        return df[valid_stock]

async def fetch_single_statement(
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    api_key: str,
    corp_code: str,
    year: int,
    max_retries: int = 2,
) -> pd.DataFrame:
    # 연결재무제표와 개별재무제표 모두 시도
    fs_types = [("CFS", "연결"), ("OFS", "개별")]
    
    for fs_div, fs_name in fs_types:
        params = {
            "crtfc_key": api_key,
            "corp_code": corp_code,
            "bsns_year": year,
            "reprt_code": "11011",
            "fs_div": fs_div,
        }
        
        for attempt in range(max_retries):
            await rate_limiter.wait()
            
            try:
                async with session.get(DART_SINGLE_ACCOUNT_URL, params=params) as resp:
                    resp.raise_for_status()
                    data = await resp.json()

                if data.get("status") == "000":
                    list_data = data.get("list", [])
                    if list_data:  # 데이터가 있으면 바로 반환
                        df = pd.DataFrame(list_data)
                        # API 응답에 corp_code와 bsns_year가 없으면 추가
                        if 'corp_code' not in df.columns:
                            df['corp_code'] = corp_code
                        if 'bsns_year' not in df.columns:
                            df['bsns_year'] = year
                        df['fs_div'] = fs_div  # 재무제표 구분 추가
                        return df
                else:
                    if attempt == max_retries - 1:  # 마지막 시도에서만 로그
                        if fetch_single_statement.error_count < fetch_single_statement.MAX_ERROR_LOGS:
                            msg = data.get("message", "")
                            print(f"API error {data.get('status')} for {corp_code} {year} {fs_name}: {msg}")
                            fetch_single_statement.error_count += 1
            except Exception as e:
                if attempt == max_retries - 1:  # 마지막 시도에서만 로그
                    if fetch_single_statement.error_count < fetch_single_statement.MAX_ERROR_LOGS:
                        print(f"Request error for {corp_code} {year} {fs_name}: {e}")
                        fetch_single_statement.error_count += 1
                else:
                    await asyncio.sleep(1)  # 재시도 전 대기
    
    return pd.DataFrame()

fetch_single_statement.error_count = 0
fetch_single_statement.MAX_ERROR_LOGS = 5

async def fetch_bulk_statements(
    api_key: str,
    corp_codes: Iterable[str],
    years: Iterable[int],
    workers: int = 5,
    include_corp_names: bool = True,
    max_calls_per_minute: int = 600,
) -> pd.DataFrame:
    """Download statements for multiple companies in parallel."""
    rate_limiter = RateLimiter(max_calls_per_minute, 60.0)
    results: List[pd.DataFrame] = []
    sem = asyncio.Semaphore(workers)
    
    corp_list = list(corp_codes)
    year_list = list(years)
    total_requests = len(corp_list) * len(year_list)
    completed = 0
    
    # 기업명 매핑을 위한 기업코드 데이터 가져오기
    corp_name_map = {}
    if include_corp_names:
        try:
            corp_df = await fetch_corp_codes(api_key)
            corp_name_map = dict(zip(corp_df['corp_code'], corp_df['corp_name']))
            print(f"기업명 매핑: {len(corp_name_map)}개 기업")
        except Exception as e:
            print(f"기업명 매핑 실패: {e}, 기업명 없이 진행")
            include_corp_names = False

    async with aiohttp.ClientSession() as session:
        async def worker(corp: str, year: int) -> None:
            nonlocal completed
            async with sem:
                df = await fetch_single_statement(session, rate_limiter, api_key, corp, year)
                if not df.empty:
                    # 기업명 추가
                    if include_corp_names and corp in corp_name_map:
                        df['corp_name'] = corp_name_map[corp]
                    
                    results.append(df)
                
                completed += 1
                if completed % 50 == 0:  # 더 자주 진행률 표시
                    print(f"Progress: {completed}/{total_requests} ({completed/total_requests*100:.1f}%)")

        tasks = [worker(corp, year) for corp in corp_list for year in year_list]
        await asyncio.gather(*tasks)

    if results:
        final_df = pd.concat(results, ignore_index=True)
        
        # 컬럼 순서 정리 (기업 정보를 앞쪽으로)
        if include_corp_names and 'corp_name' in final_df.columns:
            cols = final_df.columns.tolist()
            # 기업 관련 컬럼들을 앞으로 이동
            priority_cols = ['corp_code', 'corp_name', 'stock_code', 'bsns_year']
            other_cols = [col for col in cols if col not in priority_cols]
            reordered_cols = [col for col in priority_cols if col in cols] + other_cols
            final_df = final_df[reordered_cols]
        
        return final_df
    return pd.DataFrame()

def save_to_excel(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(path, index=False, engine="openpyxl")
    print(f"Saved {len(df):,} rows to {path}")

__all__ = [
    "fetch_corp_codes",
    "filter_kospi_kosdaq_non_financial",
    "fetch_bulk_statements",
    "save_to_excel",
]

def main() -> None:
    parser = argparse.ArgumentParser(description="DART bulk download helper")
    parser.add_argument("--sample", type=int, default=5, help="number of companies for the test run")
    parser.add_argument("--start-year", type=int, default=2022)
    parser.add_argument("--end-year", type=int, default=2023)
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--output", type=Path, default=Path("dart_test.xlsx"))
    args = parser.parse_args()

    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        raise EnvironmentError("Set the DART_API_KEY environment variable")

    print("\n📥 DART 기업 코드 목록 수집 중...")
    corp_df = asyncio.run(fetch_corp_codes(api_key))
    print(f"\n📊 총 기업 수: {len(corp_df):,}개")

    target_df = filter_kospi_kosdaq_non_financial(corp_df)
    print(f"\n✅ 필터링된 상장 비금융 기업 수: {len(target_df):,}개")

    if len(target_df) == 0:
        print("❌ No valid corporations found")
        return

    corp_codes = target_df["corp_code"].unique()[: args.sample]
    names = [
        target_df.loc[target_df["corp_code"] == code, "corp_name"].iloc[0]
        for code in corp_codes
    ]

    years = range(args.start_year, args.end_year + 1)

    print(
        f"\n🧪 사업보고서 수집 테스트: 기업 {len(corp_codes)}개, 연도 [{args.start_year}, {args.end_year}]"
    )
    print("기업 리스트:", names)

    statements = asyncio.run(
        fetch_bulk_statements(api_key, corp_codes, years, workers=args.workers)
    )

    if not statements.empty:
        out_path = Path(__file__).resolve().parent.parent / "data" / args.output
        save_to_excel(statements, out_path)
        print("✅ Test completed successfully!")
    else:
        print("❌ No data retrieved in test")

if __name__ == "__main__":
    main()
