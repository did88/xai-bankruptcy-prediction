import os
import sys
from pathlib import Path
from typing import List, Dict
from xml.etree import ElementTree

import pandas as pd
import requests
from tqdm import tqdm

# Allow importing utility functions from src/
SRC_PATH = Path(__file__).resolve().parent.parent / "src"
sys.path.append(str(SRC_PATH))

from dart_bulk_downloader import (
    fetch_corp_codes,
    filter_kospi_kosdaq_non_financial,
)

DART_SINGLE_ALL_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.xml"


def parse_statement_xml(xml_text: str) -> List[Dict[str, str]]:
    """Parse XML returned by fnlttSinglAcntAll and return list of records."""
    root = ElementTree.fromstring(xml_text)
    status = root.findtext("status")
    if status != "000":
        return []
    records = []
    for item in root.iter("list"):
        record = {child.tag: (child.text or "") for child in item}
        records.append(record)
    return records


def fetch_statement(api_key: str, corp_code: str, year: int, fs_div: str) -> List[Dict[str, str]]:
    """Fetch all statements for a single company/year."""
    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
        "bsns_year": year,
        "reprt_code": "11011",  # 사업보고서
        "fs_div": fs_div,
    }
    resp = requests.get(DART_SINGLE_ALL_URL, params=params)
    resp.raise_for_status()
    return parse_statement_xml(resp.text)


def fetch_statements_range(
    api_key: str,
    corp_codes: List[str],
    years: List[int],
    progress_file: Path,
) -> pd.DataFrame:
    """Fetch statements for multiple companies and years with resume support."""

    progress_file.parent.mkdir(parents=True, exist_ok=True)

    if progress_file.exists():
        all_df = pd.read_csv(progress_file, dtype=str)
    else:
        all_df = pd.DataFrame()

    existing_keys = set()
    if not all_df.empty:
        existing_keys = set(
            zip(all_df["corp_code"], all_df["bsns_year"], all_df["fs_div"])
        )

    total = len(corp_codes) * len(years) * 2
    completed = len(existing_keys)

    header_exists = progress_file.exists() and progress_file.stat().st_size > 0

    with tqdm(total=total, desc="Downloading", initial=completed) as pbar:
        for corp in corp_codes:
            for year in years:
                for fs_div in ["CFS", "OFS"]:
                    key = (corp, str(year), fs_div)
                    if key in existing_keys:
                        pbar.update(1)
                        continue
                    rows = fetch_statement(api_key, corp, year, fs_div)
                    if rows:
                        df = pd.DataFrame(rows)
                        df["corp_code"] = corp
                        df["bsns_year"] = str(year)
                        df["fs_div"] = fs_div
                        df.to_csv(
                            progress_file,
                            mode="a",
                            header=not header_exists,
                            index=False,
                            encoding="utf-8-sig",
                        )
                        header_exists = True
                    existing_keys.add(key)
                    pbar.update(1)

    if progress_file.exists():
        return pd.read_csv(progress_file, dtype=str)
    return pd.DataFrame()


def save_csv(df: pd.DataFrame, filename: str) -> None:
    out_dir = Path(__file__).resolve().parent.parent / "data" / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / filename
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"Saved {len(df):,} rows -> {path}")


def main() -> None:
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        raise EnvironmentError("Set the DART_API_KEY environment variable")

    # Get corp codes and filter non-financial KOSPI/KOSDAQ firms
    print("Fetching corporation codes ...")
    corp_df = fetch_corp_codes(api_key)
    target_df = filter_kospi_kosdaq_non_financial(corp_df)
    corp_codes = target_df["corp_code"].unique().tolist()
    print(f"Target corporations: {len(corp_codes)}")

    years = list(range(2015, 2024))
    print(f"Fetching statements for years {years[0]}-{years[-1]} ...")

    progress_path = Path(__file__).resolve().parent.parent / "data" / "raw" / "financial_statements_progress.csv"
    statements = fetch_statements_range(api_key, corp_codes, years, progress_path)
    if statements.empty:
        print("No data fetched")
        return

    # Separate datasets
    cfs_bs = statements[(statements["fs_div"] == "CFS") & (statements["sj_div"] == "BS")]
    cfs_is = statements[(statements["fs_div"] == "CFS") & (statements["sj_div"] == "IS")]
    ofs_bs = statements[(statements["fs_div"] == "OFS") & (statements["sj_div"] == "BS")]
    ofs_is = statements[(statements["fs_div"] == "OFS") & (statements["sj_div"] == "IS")]

    save_csv(cfs_bs, "연결재무제표_재무상태표.csv")
    save_csv(cfs_is, "연결재무제표_손익계산서.csv")
    save_csv(ofs_bs, "재무제표_재무상태표.csv")
    save_csv(ofs_is, "재무제표_손익계산서.csv")


if __name__ == "__main__":
    main()
