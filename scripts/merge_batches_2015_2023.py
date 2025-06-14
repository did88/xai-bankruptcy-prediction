import argparse
from pathlib import Path
import pandas as pd

from dart_bulk_downloader import save_to_excel


def merge_batches(input_dir: Path, output_path: Path) -> None:
    """Merge all batch Excel files into a single dataset."""
    batch_files = sorted(input_dir.glob("dart_statements_2015_2023_batch_*.xlsx"))
    if not batch_files:
        print(f"No batch files found in {input_dir}")
        return

    all_data = []
    for f in batch_files:
        df = pd.read_excel(f, engine="openpyxl")
        all_data.append(df)
        print(f"Loaded {len(df):,} rows from {f.name}")

    merged_df = pd.concat(all_data, ignore_index=True)
    save_to_excel(merged_df, output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge 2015-2023 batch files")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "data" / "batches_2015_2023",
        help="Directory containing batch Excel files",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "data" / "dart_statements_2015_2023_merged.xlsx",
        help="Path to save merged Excel file",
    )
    args = parser.parse_args()

    merge_batches(args.input_dir, args.output)
