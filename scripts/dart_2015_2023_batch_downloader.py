import asyncio
import os
from pathlib import Path
import argparse
from typing import List

from dart_bulk_downloader import (
    fetch_corp_codes,
    filter_kospi_kosdaq_non_financial,
    fetch_bulk_statements,
    save_to_excel,
)

BATCH_SIZE = 100
SAFE_CALLS_PER_SECOND = 5  # 초당 5회 이하로 제한
CALL_INTERVAL = 1 / SAFE_CALLS_PER_SECOND  # = 0.2초
MAX_CONCURRENT_TASKS = 3  # 동시에 실행되는 작업 수 제한

semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)


async def download_batch(
    api_key: str,
    batch_num: int,
    corp_codes: List[str],
    years: range,
    output_dir: Path,
    workers: int,
) -> None:
    output_path = output_dir / f"dart_statements_2015_2023_batch_{batch_num:02d}.xlsx"
    if output_path.exists():
        print(f"[SKIP] Batch {batch_num} already downloaded. Skipping.")
        return

    try:
        async with semaphore:
            await asyncio.sleep(CALL_INTERVAL)  # 호출 간 간격 확보 (초당 5회 제한)
            df = await fetch_bulk_statements(
                api_key,
                corp_codes,
                years,
                workers=workers,
                include_corp_names=True,
                max_calls_per_minute=int(SAFE_CALLS_PER_SECOND * 60),  # = 300
            )
    except Exception as e:
        print(f"[ERROR] API server unresponsive during batch {batch_num}: {e}")
        raise SystemExit(1)

    if not df.empty:
        save_to_excel(df, output_path)
    else:
        print(f"[WARN] Batch {batch_num} returned no data")


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download DART statements (2015-2023) in batches"
    )
    parser.add_argument("--start", type=int, default=1, help="Start batch number")
    parser.add_argument("--end", type=int, help="End batch number (inclusive)")
    parser.add_argument(
        "--workers", type=int, default=MAX_CONCURRENT_TASKS, help="Number of concurrent workers"
    )
    args = parser.parse_args()

    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        raise EnvironmentError("Set DART_API_KEY in your environment or .env file")

    base_dir = Path(__file__).resolve().parent.parent
    output_dir = base_dir / "data" / "batches_2015_2023"
    output_dir.mkdir(parents=True, exist_ok=True)

    print("[INFO] Fetching corporation codes...")
    corp_df = await fetch_corp_codes(api_key)
    target_df = filter_kospi_kosdaq_non_financial(corp_df).sort_values("stock_code")
    corp_codes = target_df["corp_code"].tolist()

    batches = [
        corp_codes[i: i + BATCH_SIZE] for i in range(0, len(corp_codes), BATCH_SIZE)
    ]
    total_batches = len(batches)

    start = max(1, args.start)
    end = min(args.end if args.end else total_batches, total_batches)

    years = range(2015, 2024)

    for batch_num in range(start, end + 1):
        batch_codes = batches[batch_num - 1]
        print(f"[INFO] Starting batch {batch_num} ({len(batch_codes)} corps)...")
        await download_batch(
            api_key, batch_num, batch_codes, years, output_dir, args.workers
        )
        await asyncio.sleep(1.5)  # 각 배치 간 추가 딜레이로 안정성 확보


if __name__ == "__main__":
    asyncio.run(main())
