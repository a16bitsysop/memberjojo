#!/usr/bin/env python3
"""
Merge import tests for MojoSkel
"""

from memberjojo.mojo_common import MojoSkel

from .utils import write_csv


def test_import_merge_csv(tmp_path):
    """
    Test importing one CSV, then merging a second CSV,
    and verify that it appends instead of replacing.
    """

    # 1. Original CSV
    csv1 = tmp_path / "members1.csv"
    original_rows = [
        {"id": "1", "name": "Alice", "age": "30"},
    ]
    write_csv(csv1, original_rows)

    # Create DB
    db_path = tmp_path / "test_merge.db"
    password = "Needs a Password"

    # Load initial CSV
    m = MojoSkel(str(db_path), password, "members")
    m.import_csv(csv1)

    assert m.count() == 1

    # 2. Second CSV to merge
    csv2 = tmp_path / "members2.csv"
    new_rows = [
        {"id": "2", "name": "Bob", "age": "40"},
    ]
    write_csv(csv2, new_rows)

    # Import with merge=True
    m.import_csv(csv2, merge=True)

    # Should have 2 rows now
    assert m.count() == 2

    # Verify content
    # We iterate over m which yields row objects
    rows = list(m)
    assert len(rows) == 2
    names = {r.name for r in rows}
    assert "Alice" in names
    assert "Bob" in names


def test_import_merge_csv_no_existing(tmp_path):
    """
    Test merging into a non-existent table (should act like create).
    """
    csv1 = tmp_path / "members_new.csv"
    rows = [{"id": "1", "name": "Alice", "age": "30"}]
    write_csv(csv1, rows)

    db_path = tmp_path / "test_merge_new.db"
    password = "Needs a Password"

    m = MojoSkel(str(db_path), password, "members")
    # Table doesn't exist yet
    assert not m.table_exists()

    m.import_csv(csv1, merge=True)

    assert m.table_exists()
    assert m.count() == 1
    row = list(m)[0]
    assert row.name == "Alice"
