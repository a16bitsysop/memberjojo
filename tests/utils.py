#!/usr/bin/env python3
"""Utils for tests"""

import csv
import tempfile
from pathlib import Path
from typing import Generator

from memberjojo.mojo_common import MojoSkel


def write_csv(path: Path, rows: list[dict]):
    """Utility: write CSV with DictWriter."""
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def setup_initial_db(tmp_path: Path, db_name: str, password: str):
    """
    Common setup for diff tests:
    - Create a CSV with two initial rows
    - Load it into a MojoSkel instance
    Returns (MojoSkel instance, path to csv1)
    """
    csv1 = tmp_path / "members1.csv"
    original_rows = [
        {"id": "1", "name": "Alice", "age": "30"},
        {"id": "2", "name": "Bob", "age": "40"},
    ]
    write_csv(csv1, original_rows)

    db_path = tmp_path / db_name
    m = MojoSkel(str(db_path), password, "members")
    m.import_csv(csv1)
    return m, csv1


def get_db_path() -> Generator[Path, None, None]:
    """
    Generate a temporary database path that unlinks after use.
    Usage: db_path = next(get_db_path())
    """
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        path = Path(tmp.name)
    yield path
    if path.exists():
        path.unlink()


def get_sample_members() -> list[dict]:
    """Return a standard set of mock member data."""
    return [
        {
            "Member number": "1",
            "Title": "Mr",
            "First name": "John",
            "Last name": "Doe",
            "Membership": "Full",
            "membermojo ID": "1001",
            "Short URL": "http://short.url/johndoe",
            "Active Member": "yes",
            "Newsletter": "no",
        },
        {
            "Member number": "2",
            "Title": "Ms",
            "First name": "Jane",
            "Last name": "Smith",
            "Membership": "Full",
            "membermojo ID": "1002",
            "Short URL": "http://short.url/janesmith",
            "Active Member": "no",
            "Newsletter": "yes",
        },
    ]


def get_sample_transactions() -> list[dict]:
    """Return a standard set of mock transaction data."""
    return [
        {"id": "1", "amount": "100.5", "desc": "Deposit"},
        {"id": "2", "amount": "200", "desc": "Withdrawal"},
        {"id": "3", "amount": "150", "desc": "Refund"},
        {"id": "4", "amount": "175", "desc": None},
        {"id": "5", "amount": "345", "desc": ""},
    ]


def setup_mock_csv(tmp_path: Path, filename: str, rows: list[dict] = None) -> Path:
    """
    Create a mock CSV file in tmp_path.
    If rows is None, use get_sample_members().
    """
    if rows is None:
        rows = get_sample_members()
    csv_path = tmp_path / filename
    write_csv(csv_path, rows)
    return csv_path
