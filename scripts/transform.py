from __future__ import annotations

import csv
from pathlib import Path
import sys

def newest_csv(folder: Path) -> Path:
    files = sorted(folder.glob("orders_*.csv"))
    if not files:
        raise FileNotFoundError("No raw orders files found")
    return files[-1]


def main() -> None:

    
    raw_dir = Path("/opt/airflow/data/raw")
    clean_dir = Path("/opt/airflow/data/clean")
    clean_dir.mkdir(parents=True, exist_ok=True)

    src = newest_csv(raw_dir)
    out = clean_dir / f"clean_{src.name}"

    kept = 0

    with src.open("r", newline="", encoding="utf-8") as fin, out.open(
        "w", newline="", encoding="utf-8"
    ) as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            amount = int(row["amount"])
            if amount >= 100:
                writer.writerow(row)
                kept += 1

    print(f"Transformed: {src} to {out}")
    print(f"Rows kept: {kept}")


if __name__ == "__main__":
    main()
