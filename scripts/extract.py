from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path


def main() -> None:
    raw_dir = Path("/opt/airflow/data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = raw_dir / f"orders_{ts}.csv"

    rows = [
        {"order_id": 1, "amount": 120, "state": "NY"},
        {"order_id": 2, "amount": 50, "state": "CA"},
        {"order_id": 3, "amount": 999, "state": "TX"},
        {"order_id": 4, "amount": 10, "state": "NY"},
    ]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["order_id", "amount", "state"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote raw file: {out_path}")
    print(out_path)

if __name__ == "__main__":
    main()
    
