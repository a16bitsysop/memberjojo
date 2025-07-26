"""
Tests for the transaction module
"""

import tempfile
import os
import csv
from pathlib import Path

import pytest
from memberjojo import Transaction  # Update with your actual module name

# --- Fixtures & Helpers ---
# pylint: disable=redefined-outer-name


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


# --- Tests ---


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
    assert txn.count() == 3

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

    assert txn.count() == 3  # No duplicates added


def test_invalid_primary_key_raises(csv_file, db_path):
    """
    test_invalid_primary_key_raises
    """
    txn = Transaction(db_path)

    with pytest.raises(ValueError, match="Primary key column 'uuid' not found in CSV"):
        txn.import_csv(Path(csv_file), pk_column="uuid")
