"""
Iteration tests for the Member class
"""

import csv
import pytest
from memberjojo import Member


# conftest.py or at top of test file
SAMPLE_MEMBERS = [
    {
        "Member number": "11",
        "Title": "Mr",
        "First name": "Johnny",
        "Last name": "Doe",
        "membermojo ID": "8001",
        "Short URL": "http://short.url/johnny",
    },
    {
        "Member number": "12",
        "Title": "Ms",
        "First name": "Janice",
        "Last name": "Smith",
        "membermojo ID": "8002",
        "Short URL": "http://short.url/janice",
    },
]


def test_member_iter(tmp_path):
    """
    Test for iterating over the member data.
    """
    # Prepare sample CSV data
    sample_csv = tmp_path / "members.csv"

    # Write CSV file
    with sample_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SAMPLE_MEMBERS[0].keys())
        writer.writeheader()
        writer.writerows(SAMPLE_MEMBERS)

    # Create DB path
    db_path = tmp_path / "test_members.db"

    # Instantiate Member and import CSV
    members = Member(db_path, "Needs a Password")
    members.import_csv(sample_csv)

    # Collect members from iterator
    iterated_members = list(members)

    # Check that iteration yields correct number of members
    assert len(iterated_members) == len(SAMPLE_MEMBERS)

    # Check that fields match for first member
    first = iterated_members[0]
    assert isinstance(first, members.row_class)
    assert first.member_number == int(SAMPLE_MEMBERS[0]["Member number"])
    assert first.title == SAMPLE_MEMBERS[0]["Title"]
    assert first.first_name == SAMPLE_MEMBERS[0]["First name"]
    assert first.last_name == SAMPLE_MEMBERS[0]["Last name"]
    assert first.membermojo_id == int(SAMPLE_MEMBERS[0]["membermojo ID"])
    assert first.short_url == SAMPLE_MEMBERS[0]["Short URL"]

    # Check second member also matches
    second = iterated_members[1]
    assert second.first_name == SAMPLE_MEMBERS[1]["First name"]
    assert second.last_name == SAMPLE_MEMBERS[1]["Last name"]


def test_iter_without_loading(tmp_path):
    """
    Iterating a MojoSkel instance without loading a table
    should raise RuntimeError.
    """
    db_path = tmp_path / "test.db"
    m = Member(str(db_path), "dummy_password", "members")

    with pytest.raises(RuntimeError, match="Table not loaded yet"):
        # Attempting iteration without loading CSV
        for _ in m:
            pass


def test_member_import_persist_and_reload(tmp_path):
    """
    Test importing a CSV, closing the DB, reopening it,
    and verifying that the data matches exactly.
    """

    # ------------------------
    # 1. Prepare CSV test data
    # ------------------------
    sample_csv = tmp_path / "members.csv"

    with sample_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SAMPLE_MEMBERS[0].keys())
        writer.writeheader()
        writer.writerows(SAMPLE_MEMBERS)

    # ----------------------
    # 2. Import into database
    # ----------------------
    db_path = tmp_path / "test_members.db"
    password = "Needs a Password"

    members = Member(db_path, password)
    members.import_csv(sample_csv)

    # Capture original rows
    rows_before = list(members)
    assert len(rows_before) == len(SAMPLE_MEMBERS)

    # Close DB
    members.conn.close()

    # ----------------------
    # 3. Re-open the database
    # ----------------------
    members2 = Member(db_path, password)

    rows_after = list(members2)
    assert len(rows_after) == len(SAMPLE_MEMBERS)

    # ---------------------------
    # 4. Compare row by row fields
    # ---------------------------
    for before, after in zip(rows_before, rows_after):
        # Same dataclass class?
        assert (
            before.__class__.__name__ == after.__class__.__name__
        ), f"Dataclass names differ: {before.__class__} vs {after.__class__}"

        # Compare all fields generically
        for field in before.__dataclass_fields__:
            assert getattr(before, field) == getattr(
                after, field
            ), f"Mismatch in field {field}"
