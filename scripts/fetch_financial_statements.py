import os
import sys
from pathlib import Path
from typing import List, Dict
from xml.etree import ElementTree

import pandas as pd
import requests

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


def fetch_statements_range(api_key: str, corp_codes: List[str], years: List[int]) -> pd.DataFrame:
    """Fetch statements for multiple companies and years."""
    records: List[Dict[str, str]] = []
    for corp in corp_codes:
        for year in years:
            for fs_div in ["CFS", "OFS"]:
                rows = fetch_statement(api_key, corp, year, fs_div)
                for row in rows:
                    row["corp_code"] = corp
                    row["bsns_year"] = str(year)
                    row["fs_div"] = fs_div
                    records.append(row)
    if records:
        return pd.DataFrame(records)
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

    statements = fetch_statements_range(api_key, corp_codes, years)
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
