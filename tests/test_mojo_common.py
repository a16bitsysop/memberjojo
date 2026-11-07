"""
Tests for the MojoSkel base-class methods
"""

from datetime import date
from decimal import Decimal
from unittest import mock

import pytest

from memberjojo.mojo_common import MojoSkel
from memberjojo.sql_query import DateRange, Like

from .utils import get_sample_members, setup_mock_csv

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


def test_import_csv_failure_preserves_existing_table(tmp_path):
    """
    A failed import must not destroy the previously imported data
    """
    m = _mojo(tmp_path)
    m.import_csv(setup_mock_csv(tmp_path, "good.csv", [{"id": "1", "name": "Alice"}]))
    assert m.count() == 1

    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="CSV file is empty."):
        m.import_csv(empty_csv)

    # Existing table untouched, and no *_{old,new} left behind
    assert m.count() == 1
    assert list(m)[0].name == "Alice"
    leftovers = {
        row[0]
        for row in m.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND (name='members' OR name='members_old' OR name='members_new')"
        )
    }
    assert leftovers == {"members"}


def test_download_csv_empty_preserves_existing_table(tmp_path):
    """
    A download that yields an empty CSV must leave any existing table untouched
    """
    m = _mojo(tmp_path)
    m.import_csv(setup_mock_csv(tmp_path, "base.csv", [{"id": "1", "name": "Alice"}]))
    assert m.count() == 1

    session = mock.Mock()
    resp = mock.MagicMock()
    # Headers only → no data rows → treated as empty
    resp.iter_content.return_value = iter(["id,name\n"])
    resp.__enter__.return_value = resp
    resp.raise_for_status.return_value = None
    session.get.return_value = resp

    m.download_csv(session, "https://example.com/members.csv")

    assert m.count() == 1
    assert list(m)[0].name == "Alice"
    leftovers = {
        row[0]
        for row in m.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND (name='members' OR name='members_old' OR name='members_new')"
        )
    }
    assert leftovers == {"members"}


def test_quote_identifier_validates_and_quotes(tmp_path):
    """
    quote_identifier should validate the column exists and return it quoted
    """
    m = _mojo(tmp_path)
    m.import_csv(setup_mock_csv(tmp_path, "q.csv", [{"id": "1", "name": "Alice"}]))

    assert m.quote_identifier("name") == '"name"'
    with pytest.raises(ValueError, match="Unknown column 'nope'"):
        m.quote_identifier("nope")


def test_quote_identifier_caches_columns_and_invalidates(tmp_path):
    """
    The column set should be cached per table and invalidated on import
    """
    # pylint: disable=protected-access
    m = _mojo(tmp_path)
    m.import_csv(setup_mock_csv(tmp_path, "q.csv", [{"id": "1", "name": "Alice"}]))
    assert not m._column_cache

    assert m.quote_identifier("name") == '"name"'
    assert m._column_cache["members"] == {"id", "name", "rowid"}

    # Re-import with a new column drops the stale cache entry
    m.import_csv(
        setup_mock_csv(tmp_path, "q2.csv", [{"id": "1", "name": "Bob", "age": "1"}])
    )
    assert not m._column_cache
    assert m.quote_identifier("age") == '"age"'


def test_import_creates_indexes_on_hot_columns(tmp_path):
    """
    Importing member data should create COLLATE NOCASE indexes on hot columns,
    and re-importing must recreate them under the canonical table name
    """
    m = _mojo(tmp_path)
    m.import_csv(setup_mock_csv(tmp_path, "members.csv", get_sample_members()))

    cur = m.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='members'"
    )
    index_names = {row[0] for row in cur.fetchall()}
    assert {"members__member_number", "members__first_name", "members__last_name"} <= (
        index_names
    )

    # Re-import (non-merge) recreates the indexes on the promoted table
    m.import_csv(setup_mock_csv(tmp_path, "members2.csv", get_sample_members()))
    cur = m.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='members'"
    )
    index_names = {row[0] for row in cur.fetchall()}
    assert {"members__member_number", "members__first_name", "members__last_name"} <= (
        index_names
    )
