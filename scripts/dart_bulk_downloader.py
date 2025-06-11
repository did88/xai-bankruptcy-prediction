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
    """Download and parse DART corp code list."""
    url = f"{DART_CORPCODE_URL}?crtfc_key={api_key}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            data = await resp.read()

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        xml_data = zf.read("CORPCODE.xml").decode("utf-8")
    
    root = ET.fromstring(xml_data)
    data = []
    
    for corp in root.findall('.//list'):
        corp_dict = {}
        for child in corp:
            corp_dict[child.tag] = child.text
        data.append(corp_dict)
    
    df = pd.DataFrame(data)
    print(f"Available columns: {list(df.columns)}")
    return df

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
    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
        "bsns_year": year,
        "reprt_code": "11011",
        "fs_div": "CFS",
    }
    for attempt in range(max_retries):
        await rate_limiter.wait()
        
        try:
            async with session.get(DART_SINGLE_ACCOUNT_URL, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()

            if data.get("status") == "000":
                return pd.DataFrame(data.get("list", []))
            else:
                if attempt == max_retries - 1:  # 마지막 시도에서만 로그
                    if fetch_single_statement.error_count < fetch_single_statement.MAX_ERROR_LOGS:
                        msg = data.get("message", "")
                        print(f"API error {data.get('status')} for {corp_code} {year}: {msg}")
                        fetch_single_statement.error_count += 1
                return pd.DataFrame()
        except Exception as e:
            if attempt == max_retries - 1:  # 마지막 시도에서만 로그
                if fetch_single_statement.error_count < fetch_single_statement.MAX_ERROR_LOGS:
                    print(f"Request error for {corp_code} {year}: {e}")
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
) -> pd.DataFrame:
    """Download statements for multiple companies in parallel."""
    rate_limiter = RateLimiter(800, 60.0)  # 분당 800회로 안정적 제한
    results: List[pd.DataFrame] = []
    sem = asyncio.Semaphore(workers)
    
    corp_list = list(corp_codes)
    year_list = list(years)
    total_requests = len(corp_list) * len(year_list)
    completed = 0

    async with aiohttp.ClientSession() as session:
        async def worker(corp: str, year: int) -> None:
            nonlocal completed
            async with sem:
                df = await fetch_single_statement(session, rate_limiter, api_key, corp, year)
                if not df.empty:
                    df.insert(0, "corp_code", corp)
                    df.insert(1, "bsns_year", year)
                    results.append(df)
                
                completed += 1
                if completed % 50 == 0:  # 더 자주 진행률 표시
                    print(f"Progress: {completed}/{total_requests} ({completed/total_requests*100:.1f}%)")

        tasks = [worker(corp, year) for corp in corp_list for year in year_list]
        await asyncio.gather(*tasks)

    if results:
        return pd.concat(results, ignore_index=True)
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
