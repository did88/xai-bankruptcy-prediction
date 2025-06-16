import os
import sys
import asyncio
import aiohttp
import pandas as pd
from pathlib import Path
from tqdm.asyncio import tqdm
import argparse
import pickle

SRC_PATH = Path(__file__).resolve().parent.parent / "src"
sys.path.append(str(SRC_PATH))

from dart_bulk_downloader import (
    fetch_corp_codes,
    filter_kospi_kosdaq_non_financial,
    fetch_single_statement,
    RateLimiter,
)

PROGRESS_PATH = Path(__file__).resolve().parent.parent / "data" / "raw" / "financial_statements_progress.csv"
CACHE_FILE = Path(__file__).resolve().parent.parent / "data" / "corp_codes_cache.pkl"
BATCH_SIZE = 100

async def get_corp_codes_with_cache(api_key: str, force_refresh: bool = False) -> pd.DataFrame:
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    if CACHE_FILE.exists() and not force_refresh:
        try:
            print("💾 캐시된 기업 코드 로드 중...")
            with open(CACHE_FILE, "rb") as f:
                corp_df = pickle.load(f)
            print(f"✅ 캐시에서 {len(corp_df)}개 기업 로드")
            return corp_df
        except Exception as e:
            print(f"⚠️ 캐시 로드 실패: {e}, API에서 다시 다운로드합니다")

    corp_df = await fetch_corp_codes(api_key)
    try:
        with open(CACHE_FILE, "wb") as f:
            pickle.dump(corp_df, f)
        print(f"💾 기업 코드 목록을 캐시에 저장: {CACHE_FILE}")
    except Exception as e:
        print(f"⚠️ 캐시 저장 실패: {e}")
    return corp_df

def save_csv(df: pd.DataFrame, filename: str) -> None:
    out_dir = Path(__file__).resolve().parent.parent / "data" / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / filename
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"📁 Saved {len(df):,} rows -> {path}")

async def main(reset: bool = False, use_cache: bool = True):
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        raise EnvironmentError("DART_API_KEY 환경변수가 설정되지 않았습니다.")

    if reset and PROGRESS_PATH.exists():
        PROGRESS_PATH.unlink()
        print("🗑️ 기존 진행 파일을 삭제했습니다. 새로 수집을 시작합니다.")

    print("📥 기업 코드 수집 중...")
    corp_df = await get_corp_codes_with_cache(api_key, force_refresh=not use_cache)
    target_df = filter_kospi_kosdaq_non_financial(corp_df)
    corp_codes = target_df["corp_code"].unique().tolist()
    corp_name_map = dict(zip(target_df["corp_code"], target_df["corp_name"]))
    print(f"✅ 비금융 상장기업 수: {len(corp_codes)}")

    years = list(range(2015, 2024))
    collected = []
    collected_keys = set()

    if PROGRESS_PATH.exists():
        collected_df = pd.read_csv(PROGRESS_PATH, dtype=str)
        collected_keys = set(zip(collected_df["corp_code"], collected_df["bsns_year"], collected_df["fs_div"]))
        collected = [collected_df]
        print(f"📋 기존 수집된 데이터: {len(collected_keys):,}개 작업 완료")
    else:
        print("📋 새로운 수집 시작")

    sem = asyncio.Semaphore(10)
    rate_limiter = RateLimiter(max_calls=500, period=60)
    stop_event = asyncio.Event()

    pending_tasks = []
    total_tasks = len(corp_codes) * len(years) * 2

    for corp_code in corp_codes:
        for year in years:
            for fs_div in ["CFS", "OFS"]:
                key = (corp_code, str(year), fs_div)
                if key not in collected_keys:
                    pending_tasks.append((corp_code, year, fs_div))

    completed_tasks = total_tasks - len(pending_tasks)
    print(f"📊 전체 작업: {total_tasks:,}개 / 완료: {completed_tasks:,}개 / 남은 작업: {len(pending_tasks):,}개")
    if not pending_tasks:
        print("✅ 모든 작업이 이미 완료되었습니다.")
    else:
        print("🚀 병렬 수집 시작...")

    async def worker(session, corp_code, year, fs_div):
        if stop_event.is_set():
            return
        async with sem:
            try:
                print(f"🔍 요청: {corp_code}, {year}, {fs_div}")
                df = await fetch_single_statement(session, rate_limiter, api_key, corp_code, year)
                print(f"📊 결과: {corp_code} {year} {fs_div} → df.empty={df.empty}, columns={list(df.columns)}")
            except RuntimeError as e:
                error_msg = str(e)
                if "조회된 데이타가 없습니다" in error_msg:
                    return
                else:
                    print(f"❌ 치명적 에러 발생: {e}")
                    stop_event.set()
                    return

        if stop_event.is_set() or df.empty:
            return

        print(f"✅ 데이터 수집됨: {corp_code} {year} {fs_div}, 행 수: {len(df)}")
        df["corp_name"] = corp_name_map.get(corp_code, "")
        df["corp_code"] = corp_code
        df["bsns_year"] = year
        df["fs_div"] = fs_div
        collected.append(df)

        header = not PROGRESS_PATH.exists()
        df.to_csv(PROGRESS_PATH, mode="a", header=header, index=False, encoding="utf-8-sig")
        collected_keys.add((corp_code, str(year), fs_div))

    if pending_tasks:
        async with aiohttp.ClientSession() as session:
            for start in range(0, len(pending_tasks), BATCH_SIZE):
                if stop_event.is_set():
                    print("📉 중단 조건 발생. 수집 종료.")
                    break

                batch = pending_tasks[start:start + BATCH_SIZE]
                tasks = [asyncio.create_task(worker(session, corp_code, year, fs_div)) for corp_code, year, fs_div in batch]
                progress_desc = f"진행률 ({completed_tasks:,}/{total_tasks:,} 완료)"

                for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=progress_desc):
                    try:
                        await f
                    except asyncio.CancelledError:
                        pass
                    if stop_event.is_set():
                        for t in tasks:
                            t.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
                        break

                if stop_event.is_set():
                    break
                completed_tasks += len(batch)

    if not collected:
        print("❌ 수집된 데이터가 없습니다.")
        return

    final_df = pd.concat(collected, ignore_index=True)
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
    parser = argparse.ArgumentParser(description="Download financial statements from DART")
    parser.add_argument("--reset", action="store_true", help="Ignore progress and download from scratch")
    parser.add_argument("--no-cache", action="store_true", help="Do not use cached corp codes")
    args = parser.parse_args()
    asyncio.run(main(reset=args.reset, use_cache=not args.no_cache))
