"""
Tests for the transaction module
"""

import tempfile
import csv
from pathlib import Path

import pytest
from memberjojo import Transaction  # Update with your actual module name
from memberjojo.mojo_loader import _guess_type as guess_type

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
    return Path(path)


@pytest.fixture
def db_path():
    """
    Temp file for db connection
    """
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        path = Path(tmp.name)
    yield path
    path.unlink()


@pytest.fixture
def payment_db(db_path, csv_file):
    """
    Test sqlite transaction database
    """
    test_db = Transaction(db_path, "Password1")
    test_db.import_csv(csv_file)
    return test_db


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
    assert guess_type(input_value) == expected  # pylint: disable=protected-access


def test_empty_csv_import(tmp_path, db_path):
    """
    Test importing empty and just header csv
    """
    txn = Transaction(db_path, "Fake Password")
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
        txn.import_csv(empty_csv)


def test_get_row_multi(payment_db):
    """
    Test retrieving a row using multiple column conditions
    """

    # Exact match for id=2 and desc='Withdrawal'
    row = payment_db.get_row_multi({"id": "2", "desc": "Withdrawal"})
    assert row is not None
    assert row["id"] == 2
    assert row["desc"] == "Withdrawal"
    assert row["amount"] == 200.0

    # Match with numeric and empty string (stored as NULL)
    row = payment_db.get_row_multi({"id": "5", "desc": None})
    assert row is not None
    assert row["id"] == 5
    assert row["desc"] is None
    assert row["amount"] == 345.0

    # Row with None description
    row = payment_db.get_row_multi({"id": "4", "desc": None})
    assert row is not None
    assert row["id"] == 4
    assert row["desc"] is None
    assert row["amount"] == 175.0

    # No match
    row = payment_db.get_row_multi({"id": "3", "desc": "Not a match"})
    assert row is None


def test_get_row(payment_db):
    """
    Test single row
    """
    row = payment_db.get_row("id", "5")
    assert row["id"] == 5
