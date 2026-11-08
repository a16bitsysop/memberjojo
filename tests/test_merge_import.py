#!/usr/bin/env python3
"""
Merge import tests for MojoSkel
"""

from memberjojo.mojo_common import MojoSkel

from .utils import write_csv, setup_mock_csv


def test_import_merge_csv(tmp_path):
    """
    Test importing one CSV, then merging a second CSV,
    and verify that it appends instead of replacing.
    """

    # 1. Original CSV
    original_rows = [
        {"id": "1", "name": "Alice", "age": "30"},
    ]
    csv1 = setup_mock_csv(tmp_path, "members1.csv", original_rows)

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


def _merge_setup(tmp_path, first_rows):
    """Create a MojoSkel table from first_rows and return it"""
    csv1 = setup_mock_csv(tmp_path, "first.csv", first_rows)
    m = MojoSkel(str(tmp_path / "merge_key.db"), "Needs a Password", "members")
    m.import_csv(csv1)
    return m


def test_merge_skips_existing_membermojo_ids(tmp_path):
    """
    Merging a CSV skips rows whose membermojo_id is already in the table
    """
    m = _merge_setup(
        tmp_path,
        [
            {"membermojo ID": "1", "name": "Alice"},
            {"membermojo ID": "2", "name": "Bob"},
        ],
    )
    csv2 = tmp_path / "second.csv"
    write_csv(
        csv2,
        [
            {"membermojo ID": "2", "name": "Bob"},
            {"membermojo ID": "3", "name": "Cara"},
        ],
    )

    m.import_csv(csv2, merge=True)

    assert m.count() == 3
    assert sorted(r.membermojo_id for r in m) == [1, 2, 3]


def test_merge_skips_duplicates_within_csv(tmp_path):
    """
    Repeated membermojo_id values in the merged CSV are only imported once
    """
    m = _merge_setup(tmp_path, [{"membermojo ID": "1", "name": "Alice"}])
    csv2 = tmp_path / "second.csv"
    write_csv(
        csv2,
        [
            {"membermojo ID": "5", "name": "Dan"},
            {"membermojo ID": "5", "name": "Dan again"},
        ],
    )

    m.import_csv(csv2, merge=True)

    assert m.count() == 2
    assert sorted(r.membermojo_id for r in m) == [1, 5]


def test_merge_all_rows_existing_leaves_table_unchanged(tmp_path):
    """
    Merging rows that are all already present does not change the table
    """
    m = _merge_setup(tmp_path, [{"membermojo ID": "1", "name": "Alice"}])
    csv2 = tmp_path / "second.csv"
    write_csv(csv2, [{"membermojo ID": "1", "name": "Alice"}])

    m.import_csv(csv2, merge=True)

    assert m.count() == 1


def test_merge_without_key_column_appends_all_rows(tmp_path):
    """
    Without a membermojo_id column every merged row is appended
    """
    m = _merge_setup(tmp_path, [{"id": "1", "name": "Alice"}])
    csv2 = tmp_path / "second.csv"
    write_csv(csv2, [{"id": "1", "name": "Alice"}])

    m.import_csv(csv2, merge=True)

    assert m.count() == 2
