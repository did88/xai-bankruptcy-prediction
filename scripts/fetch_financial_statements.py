import os
import sys
import asyncio
import aiohttp
import pandas as pd
from pathlib import Path
from tqdm.asyncio import tqdm

SRC_PATH = Path(__file__).resolve().parent.parent / "src"
sys.path.append(str(SRC_PATH))

from dart_bulk_downloader import (
    fetch_corp_codes,
    filter_kospi_kosdaq_non_financial,
    fetch_single_statement,
    RateLimiter,
)

PROGRESS_PATH = Path(__file__).resolve().parent.parent / "data" / "raw" / "financial_statements_progress.csv"
PROGRESS_PATH.parent.mkdir(parents=True, exist_ok=True)

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
    corp_name_map = dict(zip(target_df["corp_code"], target_df["corp_name"]))
    print(f"✅ 비금융 상장기업 수: {len(corp_codes)}")

    years = list(range(2015, 2024))
    collected = []
    collected_keys = set()

    # 기존 수집된 데이터 불러오기
    if PROGRESS_PATH.exists():
        collected_df = pd.read_csv(PROGRESS_PATH, dtype=str)
        collected_keys = set(zip(collected_df["corp_code"], collected_df["bsns_year"], collected_df["fs_div"]))
        collected = [collected_df]

    sem = asyncio.Semaphore(10)
    rate_limiter = RateLimiter(max_calls=500, period=60)

    print("🚀 병렬 수집 시작...")

    async def worker(session, corp_code, year):
        for fs_div in ["CFS", "OFS"]:
            key = (corp_code, str(year), fs_div)
            if key in collected_keys:
                continue
            df = await fetch_single_statement(session, rate_limiter, api_key, corp_code, year)
            if not df.empty:
                df["corp_name"] = corp_name_map.get(corp_code, "")
                df["corp_code"] = corp_code
                df["bsns_year"] = year
                df["fs_div"] = fs_div
                collected.append(df)

                header = not PROGRESS_PATH.exists()
                df.to_csv(PROGRESS_PATH, mode="a", header=header, index=False, encoding="utf-8-sig")

            collected_keys.add(key)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for corp in corp_codes:
            for year in years:
                task = asyncio.create_task(worker(session, corp, year))  # 🔥 병렬 실행 예약
                tasks.append(task)

        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="진행률"):
            await f

    if not collected:
        print("❌ 수집된 데이터가 없습니다.")
        return

    final_df = pd.concat(collected, ignore_index=True)

    # 분리 및 저장
    cfs_bs = final_df[(final_df["fs_div"] == "CFS") & (final_df["sj_div"] == "BS")]
    cfs_is = final_df[(final_df["fs_div"] == "CFS") & (final_df["sj_div"] == "IS")]
    ofs_bs = final_df[(final_df["fs_div"] == "OFS") & (final_df["sj_div"] == "BS")]
    ofs_is = final_df[(final_df["fs_div"] == "OFS") & (final_df["sj_div"] == "IS")]

    save_csv(cfs_bs, "연결재무제표_재무상태표.csv")
    save_csv(cfs_is, "연결재무제표_손익계산서.csv")
    save_csv(ofs_bs, "재무제표_재무상태표.csv")
    save_csv(ofs_is, "재무제표_손익계산서.csv")

    print("✅ 전체 수집 및 저장 완료.")

if __name__ == "__main__":
    asyncio.run(main())
