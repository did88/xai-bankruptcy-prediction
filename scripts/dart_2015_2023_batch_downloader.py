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
MAX_CALLS_PER_MINUTE = 1000
SAFE_CALLS_PER_MINUTE = 900  # 안정선으로 낮춤
SAFE_CALLS_PER_SECOND = SAFE_CALLS_PER_MINUTE / 60  # 15.0
CALL_INTERVAL = 1 / SAFE_CALLS_PER_SECOND  # 초당 호출 간격 (0.066초)

CONCURRENT_WORKERS = 5
semaphore = asyncio.Semaphore(CONCURRENT_WORKERS)


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
            await asyncio.sleep(CALL_INTERVAL)  # 호출 간 시간 간격 확보
            df = await fetch_bulk_statements(
                api_key,
                corp_codes,
                years,
                workers=workers,
                include_corp_names=True,
                max_calls_per_minute=int(SAFE_CALLS_PER_MINUTE),
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
        "--workers", type=int, default=CONCURRENT_WORKERS, help="Number of concurrent workers"
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
        corp_codes[i : i + BATCH_SIZE] for i in range(0, len(corp_codes), BATCH_SIZE)
    ]
    total_batches = len(batches)

    start = max(1, args.start)
    end = min(args.end if args.end else total_batches, total_batches)

    years = range(2015, 2024)

    for batch_num in range(start, end + 1):
        batch_codes = batches[batch_num - 1]
        await download_batch(
            api_key, batch_num, batch_codes, years, output_dir, args.workers
        )


if __name__ == "__main__":
    asyncio.run(main())
