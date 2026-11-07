#!/usr/bin/env python3
"""
Regression test for importing a CSV with an additional column.

Importing a new CSV whose headers have grown must succeed (a diff is
computed over the common columns) and must leave no stale `*_old` table.
"""

from .utils import write_csv, setup_initial_db


def test_add_column_to_csv_generates_diff(tmp_path):
    """
    Test importing one CSV, then importing a CSV with an additional column,
    and verify the import succeeds, keeps the data, and cleans up the old table.
    """
    # 1. Original CSV & DB
    m, _ = setup_initial_db(tmp_path, "repro.db", "password")
    assert m.count() == 2

    # 2. Modified CSV with an extra column
    csv2 = tmp_path / "members2.csv"
    updated_rows = [
        {"id": "1", "name": "Alice", "age": "30", "new_col": "val1"},
        {"id": "2", "name": "Bob", "age": "40", "new_col": "val2"},
    ]
    write_csv(csv2, updated_rows)

    # This must not raise: the diff runs over the common columns only.
    m.import_csv(csv2)

    assert m.count() == 2
    row = m.get_row_multi({"id": "1"})
    assert row is not None
    assert row.new_col == "val1"

    # The old table is renamed for the diff and then dropped.
    cur = m.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='members_old'"
    )
    assert cur.fetchone() is None
