"""Download and split DART financial statements.

This script fetches fnlttSinglAcntAll data from the Open DART API for
KOSPI/KOSDAQ non-financial firms between 2015 and 2023. The results are
split into consolidated and separate balance sheets and income statements.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Iterable, List

import aiohttp
import pandas as pd
from dotenv import load_dotenv

# Reuse helpers from dart_bulk_downloader
try:
    from .dart_bulk_downloader import (
        RateLimiter,
        fetch_corp_codes,
        filter_kospi_kosdaq_non_financial,
    )
except ImportError:  # Fallback for running as a script without a package
    from dart_bulk_downloader import (
        RateLimiter,
        fetch_corp_codes,
        filter_kospi_kosdaq_non_financial,
    )

# Load environment variables from project root
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

DART_SINGLE_ACNT_ALL_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"


async def fetch_single_acnt_all(
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    api_key: str,
    corp_code: str,
    year: int,
    fs_div: str,
) -> pd.DataFrame:
    """Fetch a single corporation's statements for one year."""
    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
        "bsns_year": year,
        "reprt_code": "11011",
        "fs_div": fs_div,
    }
    await rate_limiter.wait()
    async with session.get(DART_SINGLE_ACNT_ALL_URL, params=params) as resp:
        resp.raise_for_status()
        data = await resp.json()

    if data.get("status") == "000":
        df = pd.DataFrame(data.get("list", []))
        if not df.empty:
            df["corp_code"] = corp_code
            df["bsns_year"] = year
            df["fs_div"] = fs_div
        return df
    return pd.DataFrame()


async def fetch_statements(
    api_key: str,
    corp_codes: Iterable[str],
    years: Iterable[int],
    workers: int = 5,
    max_calls_per_minute: int = 600,
) -> pd.DataFrame:
    """Download statements in parallel for many companies and years."""
    rate_limiter = RateLimiter(max_calls_per_minute, 60.0)
    results: List[pd.DataFrame] = []
    sem = asyncio.Semaphore(workers)

    async with aiohttp.ClientSession() as session:
        async def worker(corp: str, year: int, fs_div: str) -> None:
            async with sem:
                df = await fetch_single_acnt_all(
                    session, rate_limiter, api_key, corp, year, fs_div
                )
                if not df.empty:
                    results.append(df)

        tasks = [
            worker(corp, year, fs_div)
            for corp in corp_codes
            for year in years
            for fs_div in ("CFS", "OFS")
        ]
        await asyncio.gather(*tasks)

    if results:
        return pd.concat(results, ignore_index=True)
    return pd.DataFrame()


def split_and_save(df: pd.DataFrame, output_dir: Path) -> None:
    """Split DataFrame into four categories and save as CSV."""
    output_dir.mkdir(parents=True, exist_ok=True)

    mapping = {
        "cfs_bs": ("CFS", "BS", "연결재무제표_재무상태표.csv"),
        "cfs_is": ("CFS", "IS", "연결재무제표_손익계산서.csv"),
        "ofs_bs": ("OFS", "BS", "재무제표_재무상태표.csv"),
        "ofs_is": ("OFS", "IS", "재무제표_손익계산서.csv"),
    }

    for _, (fs_div, sj_div, filename) in mapping.items():
        subset = df[(df["fs_div"] == fs_div) & (df["sj_div"] == sj_div)]
        if not subset.empty:
            subset.to_csv(output_dir / filename, index=False, encoding="utf-8-sig")


async def main() -> None:
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        raise EnvironmentError("Set the DART_API_KEY environment variable")

    print("📥 Fetching corporation codes...")
    corp_df = await fetch_corp_codes(api_key)
    target_df = filter_kospi_kosdaq_non_financial(corp_df)
    corp_codes = target_df["corp_code"].tolist()
    years = range(2015, 2024)

    print(
        f"🚀 Downloading {len(corp_codes)} corps for years 2015-2023 (may take a while)"
    )
    df = await fetch_statements(api_key, corp_codes, years, workers=10)

    output_dir = Path(__file__).resolve().parent.parent / "data" / "raw"
    split_and_save(df, output_dir)
    print(f"✅ Saved split statements to {output_dir}")


if __name__ == "__main__":
    asyncio.run(main())