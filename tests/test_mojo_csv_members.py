import sqlite3
import tempfile
import os
import csv
import pytest
from memberjojo import Member

"""
Test for the member module
"""

@pytest.fixture
def mock_csv_file():
    """
    Create a temporary mock CSV file for testing.
    Returns path to the CSV.
    """
    fieldnames = [
        "Member number",
        "Title",
        "First name",
        "Last name",
        "membermojo ID",
        "Short URL",
    ]
    rows = [
        {
            "Member number": "1",
            "Title": "Mr",
            "First name": "John",
            "Last name": "Doe",
            "membermojo ID": "1001",
            "Short URL": "http://short.url/johndoe",
        },
        {
            "Member number": "2",
            "Title": "Ms",
            "First name": "Jane",
            "Last name": "Smith",
            "membermojo ID": "1002",
            "Short URL": "http://short.url/janesmith",
        },
        {
            "Member number": "3",
            "Title": "Dr",
            "First name": "Emily",
            "Last name": "Stone",
            "membermojo ID": "1001",
            "Short URL": "http://short.url/emilystone",
        },  # duplicate ID
        {
            "Member number": "1",
            "Title": "Mrs",
            "First name": "Sara",
            "Last name": "Connor",
            "membermojo ID": "1003",
            "Short URL": "http://short.url/saraconnor",
        },  # duplicate number
        {
            "Member number": "4",
            "Title": "Sir",
            "First name": "Rick",
            "Last name": "Grimes",
            "membermojo ID": "1004",
            "Short URL": "http://short.url/rickgrimes",
        },  # invalid title
    ]

    with tempfile.NamedTemporaryFile(
        delete=False, suffix=".csv", mode="w", encoding="ISO-8859-1", newline=""
    ) as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        return f.name  # Return path to CSV


@pytest.fixture
def member_db():
    """
    Provides an in-memory Member database instance.
    """
    conn = sqlite3.connect(":memory:")
    return Member(conn)


def test_member_import_and_validation(test_member_db, test_mock_csv_file):
    """
    Test importing valid/invalid members from CSV.
    """
    test_member_db.import_csv(test_mock_csv_file)

    # Valid inserts
    assert test_member_db.get_number("john", "doe") == 1
    assert test_member_db.get_number("Jane", "Smith") == 2

    # Should not be inserted due to duplicate membermojo ID
    assert test_member_db.get_number("Emily", "Stone") is None

    # Should not be inserted due to duplicate member_number
    assert test_member_db.get_number("Sara", "Connor") is None

    # Should not be inserted due to invalid title
    assert test_member_db.get_number("Rick", "Grimes") is None

    os.remove(test_mock_csv_file)  # Clean up
