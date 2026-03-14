"""
Tests for date parsing during CSV import.
"""

from csv import DictWriter
from datetime import date

from memberjojo.mojo_member import Member


def test_date_parsing(tmp_path):
    """
    Test that dd/mm/yyyy dates are correctly parsed as date objects.
    """
    fieldnames = ["Member number", "First name", "Last name", "Date of birth"]
    rows = [
        {
            "Member number": "1",
            "First name": "John",
            "Last name": "Doe",
            "Date of birth": "01/01/1980",
        },
        {
            "Member number": "2",
            "First name": "Jane",
            "Last name": "Smith",
            "Date of birth": "15/05/1990",
        },
    ]

    csv_path = tmp_path / "test_members.csv"
    with csv_path.open(mode="w", encoding="utf-8", newline="") as f:
        writer = DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    db_path = tmp_path / "test.db"

    member_db = Member(db_path, "")
    member_db.import_csv(csv_path)

    members = list(member_db)
    assert len(members) == 2

    john = members[0]
    assert john.first_name == "John"
    assert isinstance(john.date_of_birth, date)
    assert john.date_of_birth == date(1980, 1, 1)
    assert john.date_of_birth.year == 1980

    jane = members[1]
    assert jane.first_name == "Jane"
    assert isinstance(jane.date_of_birth, date)
    assert jane.date_of_birth == date(1990, 5, 15)
    assert jane.date_of_birth.year == 1990
