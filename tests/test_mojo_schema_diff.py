#!/usr/bin/env python3
"""
Reproduction script for the sqlite3.OperationalError: no such column issue
when a new column is added to the CSV.
"""

from .utils import write_csv, setup_initial_db


def test_add_column_to_csv_generates_error(tmp_path):
    """
    Test importing one CSV, then importing a CSV with an additional column,
    and verify that it currently raises an OperationalError.
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

    # This should trigger the diff and the error
    m.import_csv(csv2)
