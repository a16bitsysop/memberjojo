"""
Iteration tests for the Member class
"""

import csv
from memberjojo import Member


def test_member_iter(tmp_path):
    """
    Test for iterating over the member data.
    """
    # Prepare sample CSV data
    sample_csv = tmp_path / "members.csv"
    sample_data = [
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

    # Write CSV file
    with sample_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=sample_data[0].keys())
        writer.writeheader()
        writer.writerows(sample_data)

    # Create DB path
    db_path = tmp_path / "test_members.db"

    # Instantiate Member and import CSV
    members = Member(db_path, "Needs a Password")
    members.import_csv(sample_csv)

    # Collect members from iterator
    iterated_members = list(members)

    # Check that iteration yields correct number of members
    assert len(iterated_members) == len(sample_data)

    # Check that fields match for first member
    first = iterated_members[0]
    assert isinstance(first, members.row_class)
    assert first.member_number == int(sample_data[0]["Member number"])
    assert first.title == sample_data[0]["Title"]
    assert first.first_name == sample_data[0]["First name"]
    assert first.last_name == sample_data[0]["Last name"]
    assert first.membermojo_id == int(sample_data[0]["membermojo ID"])
    assert first.short_url == sample_data[0]["Short URL"]

    # Check second member also matches
    second = iterated_members[1]
    assert second.first_name == sample_data[1]["First name"]
    assert second.last_name == sample_data[1]["Last name"]
