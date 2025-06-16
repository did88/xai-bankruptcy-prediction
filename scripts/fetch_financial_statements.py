import os
import sys
import asyncio
from pathlib import Path
import pandas as pd

# 경로 설정
SRC_PATH = Path(__file__).resolve().parent.parent / "src"
sys.path.append(str(SRC_PATH))

from dart_bulk_downloader import (
    fetch_corp_codes,
    filter_kospi_kosdaq_non_financial,
    fetch_bulk_statements,
)

def save_csv(df: pd.DataFrame, filename: str) -> None:
    out_dir = Path(__file__).resolve().parent.parent / "data" / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / filename
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"📁 Saved {len(df):,} rows -> {path}")

async def main():
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        raise EnvironmentError("DART_API_KEY 환경변수가 설정되지 않았습니다.")

    print("📥 기업 코드 수집 중...")
    corp_df = await fetch_corp_codes(api_key)
    target_df = filter_kospi_kosdaq_non_financial(corp_df)

    corp_codes = target_df["corp_code"].unique().tolist()
    print(f"✅ 대상 비금융 상장 기업 수: {len(corp_codes)}")

    years = list(range(2015, 2024))
    print(f"📊 수집 대상 연도: {years[0]} ~ {years[-1]}")

    # 병렬로 재무제표 수집
    print("🚀 병렬로 재무제표 수집 중...")
    statements = await fetch_bulk_statements(
        api_key=api_key,
        corp_codes=corp_codes,
        years=years,
        workers=10,  # 동시에 처리할 최대 수
        include_corp_names=True,
        max_calls_per_minute=500,  # 너무 높이면 차단될 수 있음
    )

    if statements.empty:
        print("❌ 수집된 데이터가 없습니다.")
        return

    # 데이터 분리 및 저장
    cfs_bs = statements[(statements["fs_div"] == "CFS") & (statements["sj_div"] == "BS")]
    cfs_is = statements[(statements["fs_div"] == "CFS") & (statements["sj_div"] == "IS")]
    ofs_bs = statements[(statements["fs_div"] == "OFS") & (statements["sj_div"] == "BS")]
    ofs_is = statements[(statements["fs_div"] == "OFS") & (statements["sj_div"] == "IS")]

    save_csv(cfs_bs, "연결재무제표_재무상태표.csv")
    save_csv(cfs_is, "연결재무제표_손익계산서.csv")
    save_csv(ofs_bs, "재무제표_재무상태표.csv")
    save_csv(ofs_is, "재무제표_손익계산서.csv")

if __name__ == "__main__":
    asyncio.run(main())
