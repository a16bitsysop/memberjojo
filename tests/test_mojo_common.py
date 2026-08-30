"""
Tests for the MojoSkel base-class methods
"""

from datetime import date
from decimal import Decimal
from unittest import mock

from memberjojo.mojo_common import MojoSkel
from memberjojo.sql_query import DateRange, Like

from .utils import setup_mock_csv

# pylint: disable=redefined-outer-name


def _mojo(tmp_path, db_name="test.db", table="members", password="pw"):
    return MojoSkel(str(tmp_path / db_name), password, table)


def test_set_table_switch_and_missing(tmp_path):
    """
    set_table should rebuild the row_class for an existing table and clear it otherwise
    """
    m = _mojo(tmp_path)
    m.import_csv(setup_mock_csv(tmp_path, "a.csv", [{"id": "1", "name": "Alice"}]))

    # Switch to a non-existent table -> row_class None
    m.set_table("nope")
    assert m.row_class is None

    # Switch back -> dataclass rebuilt
    m.set_table("members")
    assert m.row_class is not None
    row = list(m)[0]
    assert row.name == "Alice"


def test_create_view_joins_two_tables(tmp_path):
    """
    create_view should build a view joining on payment_id
    """
    m = _mojo(tmp_path, table="completed_payments")
    m.import_csv(
        setup_mock_csv(
            tmp_path,
            "payments.csv",
            [{"payment_id": "M1", "amount": "100.5"}],
        )
    )
    m.import_csv(
        setup_mock_csv(
            tmp_path,
            "items.csv",
            [{"payment_id": "M1", "item": "Fizzy Drink", "qty": "2"}],
        ),
        table_name="payment_items",
    )

    # Switch the base table back before building the join (as link_items does)
    m.set_table("completed_payments")
    m.create_view("linked_payments", "payment_items")

    m.set_table("linked_payments")
    row = m.get_row_multi({"payment_id": "M1"})
    assert row is not None
    assert row.item == "Fizzy Drink"
    assert row.qty == 2
    assert row.amount == 100.5


def test_get_row_multi_like(tmp_path):
    """
    get_row_multi should support Like conditions
    """
    m = _mojo(tmp_path)
    m.import_csv(
        setup_mock_csv(
            tmp_path,
            "likes.csv",
            [
                {"id": "1", "name": "Alice"},
                {"id": "2", "name": "Alligator"},
                {"id": "3", "name": "Bob"},
            ],
        )
    )
    rows = m.get_row_multi({"name": Like("al%")}, only_one=False)
    assert [r.name for r in rows] == ["Alice", "Alligator"]


def test_get_row_multi_daterange(tmp_path):
    """
    get_row_multi should support DateRange conditions on a DATE column
    """
    m = _mojo(tmp_path, table="events")
    m.import_csv(
        setup_mock_csv(
            tmp_path,
            "dates.csv",
            [
                {"id": "1", "when": "2023-01-15"},
                {"id": "2", "when": "2023-06-01"},
                {"id": "3", "when": "2024-02-20"},
            ],
        )
    )
    rows = m.get_row_multi(
        {"when": DateRange(date(2023, 1, 1), date(2023, 12, 31))}, only_one=False
    )
    assert {r.id for r in rows} == {1, 2}


def test_download_csv_populates_and_diffs(tmp_path):
    """
    download_csv should stream a CSV from a mocked session and import + diff it
    """
    m = _mojo(tmp_path)
    m.import_csv(
        setup_mock_csv(
            tmp_path,
            "base.csv",
            [{"id": "1", "name": "Alice", "age": "30"}],
        )
    )

    session = mock.Mock()

    # A response usable as a context manager that streams a CSV over the network
    resp = mock.MagicMock()
    resp.iter_content.return_value = iter(
        ["id,name,age\n", "1,Alice,31\n", "2,Bob,40\n"]
    )
    resp.__enter__.return_value = resp
    resp.raise_for_status.return_value = None
    session.get.return_value = resp

    m.download_csv(session, "https://example.com/members.csv")

    assert m.count() == 2


def test_row_to_obj_converts_decimal_and_date(tmp_path):
    """
    Iterating a table with REAL and DATE columns should yield Decimal/date types
    """
    m = _mojo(tmp_path, table="sales")
    m.import_csv(
        setup_mock_csv(
            tmp_path,
            "sales.csv",
            [{"id": "1", "amount": "10.50", "sold": "2023-04-01"}],
        )
    )
    row = list(m)[0]
    assert isinstance(row.amount, Decimal)
    assert row.amount == Decimal("10.50")
    assert isinstance(row.sold, date)
    assert row.sold == date(2023, 4, 1)
