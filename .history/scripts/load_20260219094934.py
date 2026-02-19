from __future__ import annotations

import shutil
from pathlib import Path


def newest_csv(folder: Path) -> Path:
    files = sorted(folder.glob("clean_orders_*.csv"))
    if not files:
        raise FileNotFoundError("No clean files found")
    return files[-1]


def main() -> None:
    clean_dir = Path("/opt/airflow/data/clean")
    loaded_dir = Path("/opt/airflow/data/loaded")
    loaded_dir.mkdir(parents=True, exist_ok=True)

    src = newest_csv(clean_dir)
    dest = loaded_dir / src.name

    shutil.copy2(src, dest)

    line_count = sum(1 for _ in dest.open("r", encoding="utf-8"))

    print(f"Loaded file: {dest}")
    print(f"Total lines including header: {line_count}")


if __name__ == "__main__":
    main()
