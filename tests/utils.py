#!/usr/bin/env python3
"""Utils for tests"""

import csv
from pathlib import Path


def write_csv(path: Path, rows: list[dict]):
    """Utility: write CSV with DictWriter."""
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
