"""
Tests for the transaction module
"""

import tempfile
import os
import csv
from pathlib import Path

import pytest
from memberjojo import Transaction  # Update with your actual module name

# pylint: disable=redefined-outer-name
# or pylint thinks fixtures are redined as function variables
# --- Fixtures & Helpers ---


@pytest.fixture
def csv_file(tmp_path):
    """
    Temp csv file for testing
    """
    path = tmp_path / "test_data.csv"
    data = [
        {"id": "1", "amount": "100.5", "desc": "Deposit"},
        {"id": "2", "amount": "200", "desc": "Withdrawal"},
        {"id": "3", "amount": "150", "desc": "Refund"},
        {"id": "4", "amount": "175", "desc": None},
        {"id": "5", "amount": "345", "desc": ""},
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "amount", "desc"])
        writer.writeheader()
        writer.writerows(data)
    return str(path)


@pytest.fixture
def db_path():
    """
    Temp file for db connection
    """
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        path = tmp.name
    yield path
    os.remove(path)


@pytest.mark.parametrize(
    "input_value, expected",
    [
        (None, "TEXT"),
        ("", "TEXT"),
        ("abc", "TEXT"),
        ("123", "INTEGER"),
        ("123.45", "REAL"),
        ("   42   ", "INTEGER"),  # whitespace-trimmed input
    ],
)


# --- Tests ---


def test_guess_type_various(input_value, expected):
    """
    Test all the code paths in _guess_type
    """
    txn = Transaction(":memory:")
    assert txn._guess_type(input_value) == expected  # pylint: disable=protected-access


def test_empty_csv_import(tmp_path, db_path):
    """
    Test importing empty and just header csv
    """
    txn = Transaction(db_path)
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("", encoding="utf-8")  # Fully empty

    assert empty_csv.exists()
    assert empty_csv.stat().st_size == 0
    with pytest.raises(ValueError, match="CSV file is empty."):
        txn.import_csv(empty_csv)

    # OR with only headers
    empty_csv.write_text("id,amount,desc\n", encoding="utf-8")

    # Use it in your import
    with pytest.raises(ValueError, match="CSV file is empty."):
        txn.import_csv(empty_csv, pk_column="id")


def test_invalid_csv_path_message(tmp_path, db_path, capsys):
    """
    Test import non existing csv file
    """
    non_exist = Path(tmp_path, "non-exist.csv")
    txn = Transaction(db_path)
    txn.import_csv(non_exist)
    # Capture stdout
    captured = capsys.readouterr()
    assert "CSV file not found" in captured.out
    assert str(non_exist) in captured.out


def test_type_mismatch_in_second_import_raises(tmp_path, db_path):
    """
    Import valid CSV first. Then try a second CSV with invalid type, and ensure it raises.
    """
    txn = Transaction(db_path, table_name="payments")

    # First CSV: valid
    valid_csv = tmp_path / "valid.csv"
    valid_csv.write_text(
        ("id,amount,desc\n" + "1,100.0,Deposit\n" + "2,200.0,Withdrawal\n"),
        encoding="utf-8",
    )
    txn.import_csv(valid_csv, pk_column="id")

    # Second CSV: invalid amount type
    invalid_csv = tmp_path / "invalid.csv"
    invalid_csv.write_text(
        ("id,amount,desc\n" + "3,not_a_number,Invalid Amount\n"),
        encoding="utf-8",
    )

    # Reuse the same txn instance so the schema remains
    with pytest.raises(ValueError, match="Failed to import:"):
        txn.import_csv(invalid_csv, pk_column="id")

    # Ensure the invalid row was not inserted
    assert txn.count() == 2


def test_import_with_custom_primary_key(csv_file, db_path):
    """
    test_import_with_custom_primary_key
    """
    txn = Transaction(db_path, table_name="transactions")

    # Import using 'id' as the PK
    txn.import_csv(Path(csv_file), pk_column="id")

    # Check that 'id' is used as primary key
    txn.cursor.execute("PRAGMA table_info(transactions)")
    schema = txn.cursor.fetchall()
    pk_columns = [col["name"] for col in schema if col["pk"] == 1]
    assert pk_columns == ["id"], "Expected 'id' to be primary key"

    # Check row count
    assert txn.count() == 5

    # Retrieve row by primary key
    row = txn.get_row("id", "2")
    assert row is not None
    assert row["desc"] == "Withdrawal"
    assert row["amount"] == 200.0


def test_duplicate_primary_key_ignored(csv_file, db_path):
    """
    test_duplicate_primary_key_ignored
    """
    txn = Transaction(db_path)
    txn.import_csv(Path(csv_file), pk_column="id")

    # Re-import same CSV — should ignore duplicates due to OR IGNORE
    txn.import_csv(Path(csv_file), pk_column="id")

    assert txn.count() == 5  # No duplicates added


def test_invalid_primary_key_raises(csv_file, db_path):
    """
    test_invalid_primary_key_raises
    """
    txn = Transaction(db_path)

    with pytest.raises(ValueError, match="Primary key column 'uuid' not found in CSV"):
        txn.import_csv(Path(csv_file), pk_column="uuid")


def test_get_row_multi(csv_file, db_path):
    """
    Test retrieving a row using multiple column conditions
    """
    txn = Transaction(db_path, table_name="transactions")
    txn.import_csv(Path(csv_file), pk_column="id")

    # Exact match for id=2 and desc='Withdrawal'
    row = txn.get_row_multi({"id": "2", "desc": "Withdrawal"})
    assert row is not None
    assert row["id"] == 2
    assert row["desc"] == "Withdrawal"
    assert row["amount"] == 200.0

    # Match with numeric and empty string
    row = txn.get_row_multi({"id": "5", "desc": ""})
    assert row is not None
    assert row["id"] == 5
    assert row["desc"] is None
    assert row["amount"] == 345.0

    # No match
    row = txn.get_row_multi({"id": "3", "desc": "Not a match"})
    assert row is None
