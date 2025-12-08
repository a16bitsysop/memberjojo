"""
Tests for the member module
"""

from csv import DictWriter
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest
from memberjojo import Member

# pylint: disable=redefined-outer-name
# or pylint thinks fixtures are redined as function variables
# --- Fixtures & Helpers ---


@pytest.fixture
def mock_csv_file(tmp_path):
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
            "Member number": "4",
            "Title": "Mrs",
            "First name": "Sara",
            "Last name": "Connor",
            "membermojo ID": "1003",
            "Short URL": "http://short.url/saraconnor",
        },  # duplicate number
        {
            "Member number": "5",
            "Title": "Sir",
            "First name": "Rick",
            "Last name": "Grimes",
            "membermojo ID": "1004",
            "Short URL": "http://short.url/rickgrimes",
        },  # invalid title
    ]

    csv_path = tmp_path / "mock_data.csv"
    with csv_path.open(mode="w", encoding="ISO-8859-1", newline="") as f:
        writer = DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return Path(csv_path)


@pytest.fixture
def db_path():
    """
    Temp file for db connection
    """
    with NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        path = Path(tmp.name)
    yield path
    path.unlink()


@pytest.fixture
def member_db(db_path, mock_csv_file):
    """
    Test sqlite member database
    """
    test_db = Member(db_path, "A Password")
    test_db.import_csv(mock_csv_file)
    return test_db


# --- Tests ---


def test_empty_db(capsys):
    """
    Test empty db
    """
    with NamedTemporaryFile(suffix=".db") as tmp:
        empty_db = Member(Path(tmp.name), "Needs Password Now")
        # create tables so is empty database
        empty_db.show_table()
        captured = capsys.readouterr()
        assert "(No data)" in captured.out
        assert empty_db.count() == 0


def test_invalid_csv_path_message(tmp_path, db_path):
    """
    Test import non existing csv file
    """
    non_exist = tmp_path / "non-exist.csv"
    txn = Member(db_path, "Pass Protect")

    with pytest.raises(FileNotFoundError) as excinfo:
        txn.import_csv(non_exist)

    # assert message
    assert f"CSV file not found: {non_exist}" == str(excinfo.value)


def test_member_import_and_validation(member_db):
    """
    Test importing valid/invalid members from CSV.
    """
    # Valid inserts
    assert member_db.get_number_first_last("john", "doe") == 1
    assert member_db.get_number("Jane Smith") == 2
    assert member_db.get_name(2) == "Jane Smith"
    # Invalid member number
    assert member_db.get_name(888) is None

    # Should not be inserted due to duplicate not being present
    assert member_db.get_number_first_last("Emily", "Stave") is None

    # Should not be inserted due to not being present
    assert member_db.get_number("Sara Bonnor") is None

    # Should not be inserted due to invalid title
    assert member_db.get_number_first_last("Rick", "Dangerous") is None


def test_show_table(member_db):
    """
    Test the show table function
    """
    # Should be equal as default show_table is 5 entries and member_db is 2
    entries = member_db.count()
    assert entries == 5
    assert member_db.show_table() == member_db.show_table(100)
    assert member_db.show_table(entries) == member_db.show_table(100)


def test_get_number_first_last_not_found_raises(member_db):
    """
    Test found_error
    """
    with pytest.raises(
        ValueError, match=r"❌ Cannot find: John Snow in member database."
    ):
        member_db.get_number_first_last("John", "Snow", found_error=True)


def test_get_number_first_last_more_names(member_db):
    """
    Test logic for 3 names passed
    """
    assert member_db.get_number("Dr Jane Smith") == 2
    assert member_db.get_number("John Jojo Doe") == 1
    with pytest.raises(ValueError) as exc_info:
        member_db.get_number("Emily Sara", found_error=True)

    assert "Cannot find" in str(exc_info.value)


def test_single_word_name(member_db):
    """
    Test passing a one word name
    """
    assert member_db.get_mojo_name("TestName", found_error=False) is None

    with pytest.raises(ValueError, match=r"❌ Cannot extract name from: TestName"):
        member_db.get_mojo_name("TestName", found_error=True)


def test_initial_name(member_db):
    """
    Test matching initial to full name
    """
    assert member_db.get_mojo_name("J Doe", found_error=True) == ("John", "Doe")
    assert member_db.get_mojo_name("J A Doe", found_error=True) == ("John", "Doe")
    assert member_db.get_mojo_name("A J Doe", found_error=True) == ("John", "Doe")
    assert member_db.get_mojo_name("A Doe") is None
