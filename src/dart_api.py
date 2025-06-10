"""DART API client for fetching Samsung Electronics data."""

from __future__ import annotations

import io
import os
import zipfile
from typing import Any, Dict
from pathlib import Path
from xml.etree import ElementTree

import pandas as pd
import requests
from dotenv import load_dotenv

# .env 파일 경로 설정 (src의 상위 디렉토리)
dotenv_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path)

def get_corp_code(api_key: str, corp_name: str = "삼성전자") -> str:
    """Return the DART corp_code for the given corporation name."""
    url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={api_key}"
    resp = requests.get(url)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        xml_data = zf.read("CORPCODE.xml").decode("utf-8")
    root = ElementTree.fromstring(xml_data)
    for item in root.iter("list"):
        name = item.findtext("corp_name")
        if name == corp_name:
            code = item.findtext("corp_code")
            if code:
                return code
    raise ValueError(f"{corp_name} not found in DART corp codes")


def fetch_samsung_statements(
    api_key: str,
    year: int,
    reprt_code: str = "11011",
) -> pd.DataFrame:
    """Fetch Samsung Electronics financial statements for the given year."""
    corp_code = get_corp_code(api_key, corp_name="삼성전자")
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
        "bsns_year": year,
        "reprt_code": reprt_code,
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data: Dict[str, Any] = resp.json()
    return pd.DataFrame(data.get("list", []))


if __name__ == "__main__":
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        raise EnvironmentError("Set the DART_API_KEY environment variable")

    df = fetch_samsung_statements(api_key, year=2023)
    print(df.head())

    # 🔽 엑셀로 저장
    output_path = Path(__file__).resolve().parent.parent / "samsung_statements_2023.xlsx"
    df.to_excel(output_path, index=False, engine="openpyxl")
    print(f"✅ 엑셀 저장 완료: {output_path}")
