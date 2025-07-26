"""
Tests for the transaction module
"""

import sqlite3
import tempfile
import os
import csv
import pytest

from memberjojo import Transaction  # Update with your actual module name

# --- Fixtures & Helpers ---


@pytest.fixture
def csv_file():
    """
    Create a temporary CSV file for testing.
    """
    data = [
        {"id": "1", "amount": "100.5", "desc": "Deposit"},
        {"id": "2", "amount": "200", "desc": "Withdrawal"},
        {"id": "3", "amount": "150", "desc": "Refund"},
    ]
    with tempfile.NamedTemporaryFile(
        delete=False, mode="w", newline="", suffix=".csv"
    ) as tmp_file:
        writer = csv.DictWriter(tmp_file, fieldnames=["id", "amount", "desc"])
        writer.writeheader()
        for row in data:
            writer.writerow(row)
        yield tmp_file.name
        os.remove(tmp_file.name)


@pytest.fixture
def db_connection():
    """
    Create an in-memory SQLite database.
    """
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


# --- Tests ---


def test_import_with_custom_primary_key(test_csv_file, test_db_connection):
    """
    test_import_with_custom_primary_key
    """
    txn = Transaction(test_db_connection, table_name="transactions")

    # Import using 'id' as the PK
    txn.import_csv(test_csv_file, pk_column="id")

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


def test_duplicate_primary_key_ignored(test_csv_file, test_db_connection):
    """
    test_duplicate_primary_key_ignored
    """
    txn = Transaction(test_db_connection)
    txn.import_csv(test_csv_file, pk_column="id")

    # Re-import same CSV — should ignore duplicates due to OR IGNORE
    txn.import_csv(test_csv_file, pk_column="id")

    assert txn.count() == 3  # No duplicates added


def test_invalid_primary_key_raises(test_csv_file, test_db_connection):
    """
    test_invalid_primary_key_raises
    """
    txn = Transaction(test_db_connection)

    with pytest.raises(ValueError, match="Primary key column 'uuid' not found in CSV"):
        txn.import_csv(test_csv_file, pk_column="uuid")
