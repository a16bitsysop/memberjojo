#!/usr/bin/env python3
"""
CSV diff tests for the Member class
"""

import csv
from pathlib import Path

from memberjojo.mojo_common import MojoSkel


def write_csv(path: Path, rows: list[dict]):
    """Utility: write CSV with DictWriter."""
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def test_import_new_csv_generates_diff(tmp_path, capsys):
    """
    Test importing one CSV, then importing a changed CSV,
    and verify that the diff prints correctly.
    """

    # -----------------------
    # 1. Original CSV
    # -----------------------
    csv1 = tmp_path / "members1.csv"
    original_rows = [
        {"id": "1", "name": "Alice", "age": "30"},
        {"id": "2", "name": "Bob", "age": "40"},
    ]
    write_csv(csv1, original_rows)

    # Create DB
    db_path = tmp_path / "test_members.db"
    password = "Needs a Password"

    # Load initial CSV
    m = MojoSkel(str(db_path), password, "members")
    m.import_csv(csv1)

    assert m.count() == 2

    # -----------------------
    # 2. Modified CSV with one added, one deleted, one changed
    # -----------------------
    csv2 = tmp_path / "members2.csv"
    updated_rows = [
        {"id": "1", "name": "Alice", "age": "31"},  # changed: age 30 -> 31
        {"id": "3", "name": "Cara", "age": "22"},  # added
    ]
    write_csv(csv2, updated_rows)

    # Import second CSV → should trigger rename + diff + drop old table
    m.import_csv(csv2)

    # Capture printed diff output
    captured = capsys.readouterr().out

    # -----------------------
    # 3. Check diff lines
    # -----------------------
    # We expect:
    # - rowid 1: changed (age differs)
    # - rowid 2: deleted (Bob missing)
    # - rowid 3: added   (Cara new)
    #
    # Diff output rows look like:
    #   (1, 'changed')
    #   (2, 'deleted')
    #   (3, 'added')

    assert "changed" in captured
    assert "deleted" in captured
    assert "added" in captured

    # Count rows after second import
    assert m.count() == 2

    # Ensure old table is gone
    cur = m.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='members_old'"
    )
    assert cur.fetchone() is None
