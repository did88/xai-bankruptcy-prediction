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
    
    # Parse XML manually for better control
    root = ET.fromstring(xml_data)
    data = []
    
    for corp in root.findall('.//list'):
        corp_dict = {}
        for child in corp:
            corp_dict[child.tag] = child.text
        data.append(corp_dict)
    
    df = pd.DataFrame(data)
    
    # Print column info for debugging
    print(f"Available columns: {list(df.columns)}")
    
    return df


def filter_kospi_kosdaq_non_financial(df: pd.DataFrame) -> pd.DataFrame:
    """Return only KOSPI/KOSDAQ non-financial firms."""
    # Basic filter: firms with valid stock codes (6-digit numbers are typical)
    if "stock_code" not in df.columns:
        print("Error: 'stock_code' column not found!")
        return pd.DataFrame()
    
    # Filter for firms with non-empty stock codes
    has_stock = df["stock_code"].notna() & (df["stock_code"].str.strip() != "")
    
    # Additional filter: stock code should be numeric and 6 digits (typical for listed companies)
    # This helps filter out non-listed companies that might have other formats
    valid_stock = has_stock & df["stock_code"].str.match(r'^\d{6}$', na=False)
    
    print(f"Total firms: {len(df):,}")
    print(f"Firms with stock codes: {has_stock.sum():,}")
    print(f"Firms with valid 6-digit codes: {valid_stock.sum():,}")
    
    # Filter out financial companies
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
) -> pd.DataFrame:
    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
        "bsns_year": year,
        "reprt_code": "11011",  # 사업보고서
    }
    await rate_limiter.wait()
    
    try:
        async with session.get(DART_SINGLE_ACCOUNT_URL, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()
            
        if data.get("status") == "000":  # Success
            return pd.DataFrame(data.get("list", []))
        else:
            # Don't print error for each request to avoid spam
            return pd.DataFrame()
    except Exception as e:
        # Don't print error for each request to avoid spam
        return pd.DataFrame()


async def fetch_bulk_statements(
    api_key: str,
    corp_codes: Iterable[str],
    years: Iterable[int],
    workers: int = 5,
) -> pd.DataFrame:
    """Download statements for multiple companies in parallel."""
    rate_limiter = RateLimiter(1000, 60.0)
    results: List[pd.DataFrame] = []
    sem = asyncio.Semaphore(workers)
    
    # Convert to list for progress tracking
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
                if completed % 100 == 0:
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


if __name__ == "__main__":
    import os

    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        raise EnvironmentError("Set the DART_API_KEY environment variable")

    # Test fetching corp codes
    print("Testing corp code fetch...")
    corp_df = asyncio.run(fetch_corp_codes(api_key))
    print(f"\nTotal corporations: {len(corp_df):,}")
    
    # Apply filters
    target_df = filter_kospi_kosdaq_non_financial(corp_df)
    print(f"\nFiltered corporations: {len(target_df):,}")
    
    if len(target_df) > 0:
        # Test with first 10 companies
        corp_codes = target_df["corp_code"].unique()[:10]
        years = range(2021, 2023)  # 2021-2022년 데이터 (더 안정적)
        
        print(f"\nTesting download for {len(corp_codes)} companies, {len(years)} years...")
        print(f"Test companies: {list(target_df.head(10)['corp_name'])}")
        
        statements = asyncio.run(
            fetch_bulk_statements(api_key, corp_codes, years, workers=5)
        )
        
        if not statements.empty:
            out_path = Path(__file__).resolve().parent.parent / "data" / "dart_test.xlsx"
            save_to_excel(statements, out_path)
            print(f"✅ Test completed successfully!")
        else:
            print("❌ No data retrieved in test")
