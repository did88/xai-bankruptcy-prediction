"""Utilities for bulk downloading DART financial statements."""

from __future__ import annotations

import asyncio
import io
import zipfile
from collections import deque
from pathlib import Path
from typing import Iterable, List

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
    root = pd.read_xml(xml_data)
    return root


def filter_kospi_kosdaq_non_financial(df: pd.DataFrame) -> pd.DataFrame:
    """Return only KOSPI/KOSDAQ non-financial firms."""
    mask = df["stock_code"].notna() & (df["corp_cls"].isin(["Y", "K"]))
    non_financial = df["corp_name"].str.contains("금융", na=False) == False
    return df[mask & non_financial]


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
        "reprt_code": "11011",
    }
    await rate_limiter.wait()
    async with session.get(DART_SINGLE_ACCOUNT_URL, params=params) as resp:
        if resp.status == 429:
            raise RuntimeError("API daily request limit reached (HTTP 429)")
        resp.raise_for_status()
        data = await resp.json()

    if data.get("status") != "000":
        msg = data.get("message", "")
        if any(k in msg for k in ["일일", "사용량", "제한", "초과", "limit", "트래픽"]):
            raise RuntimeError(f"API daily request limit reached: {msg}")
        return pd.DataFrame()

    return pd.DataFrame(data.get("list", []))


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

    async with aiohttp.ClientSession() as session:

        async def worker(corp: str, year: int) -> None:
            async with sem:
                df = await fetch_single_statement(
                    session, rate_limiter, api_key, corp, year
                )
                df.insert(0, "corp_code", corp)
                df.insert(1, "bsns_year", year)
                results.append(df)

        tasks = [worker(corp, year) for corp in corp_codes for year in years]
        await asyncio.gather(*tasks)

    if results:
        return pd.concat(results, ignore_index=True)
    return pd.DataFrame()


def save_to_excel(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(path, index=False, engine="openpyxl")


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

    corp_df = asyncio.run(fetch_corp_codes(api_key))
    target_df = filter_kospi_kosdaq_non_financial(corp_df)

    # 예시: 처음 100개 기업을 선택합니다. 팀별로 리스트를 분할해 사용하세요.
    corp_codes = target_df["corp_code"].unique()[:100]
    years = range(2015, 2025)

    statements = asyncio.run(
        fetch_bulk_statements(api_key, corp_codes, years, workers=10)
    )

    out_path = Path(__file__).resolve().parent.parent / "data" / "dart_statements.xlsx"
    save_to_excel(statements, out_path)
    print(f"✅ 엑셀 저장 완료: {out_path}")
