"""
MojoSkel base class

This module provides a common base class (`MojoSkel`) for other `memberjojo` modules
It includes helper methods for working with SQLite databases
"""

from dataclasses import make_dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from pprint import pprint
from collections.abc import Iterator
from typing import Any

import requests

try:
    from sqlcipher3 import dbapi2 as sqlite3

    HAS_SQLCIPHER = True
except ImportError:
    import sqlite3  # stdlib

    HAS_SQLCIPHER = False

from . import mojo_loader
from .mojo_loader import _quote as _q
from .sql_query import Like, DateRange

# Rows are dynamic dataclasses built from the table schema at runtime, so
# their concrete type is only known once the table has been loaded.
type RowType = Any


class MojoSkel:
    # pylint: disable=too-many-instance-attributes
    """
    Establishes a connection to a SQLite database and provides helper methods
    for querying tables
    """

    def __init__(self, db_path: str | Path, db_key: str, table_name: str):
        """
        Initialize the MojoSkel class

        Connects to the SQLite database and sets the row factory for
        dictionary-style access to columns.

        :param db_path: Path to the SQLite database file
        :param db_key: key to unlock the encrypted sqlite database,
            unencrypted if sqlcipher3 not installed or unset
        :param table_name: Name of the table to operate on, or create when importing
        """
        self.db_path: str | Path = db_path
        self.table_name = table_name
        self.db_key = db_key
        self.debug = False
        self._column_cache: dict[str, set[str]] = {}

        # Open connection
        self.conn = sqlite3.connect(self.db_path)  # pylint: disable=no-member
        self.conn.row_factory = sqlite3.Row  # pylint: disable=no-member
        self.cursor = self.conn.cursor()

        self._apply_db_key(db_key)

        # After table exists (or after import), build the dataclass
        self.row_class: type[RowType] | None
        if self.table_exists():
            self.row_class = self._build_dataclass_from_table()
        else:
            self.row_class = None

    def _apply_db_key(self, db_key: str) -> None:
        """
        Apply the SQLCipher key when available, verifying it actually decrypts.

        · Without sqlcipher3 the database cannot be encrypted, so a provided
          db_key is ignored and the user is warned.
        · cipher_compatibility is only pinned for newly-created databases;
          applying it unconditionally could reject databases that were created
          with a different SQLCipher configuration.
        """
        if not HAS_SQLCIPHER:
            if db_key:
                print(
                    "⚠️  sqlcipher3 is not installed — database "
                    f"{self.db_path} will NOT be encrypted."
                )
            else:
                print(f"Unencrypted database {self.db_path} loaded securely.")
            return

        if not db_key:
            print(f"Unencrypted database {self.db_path} loaded securely.")
            return

        # Apply SQLCipher key (escape embedded single quotes to avoid SQL
        # injection). SQLCipher does not support bind parameters in PRAGMA.
        escaped_key = db_key.replace("'", "''")
        self.cursor.execute(f"PRAGMA key='{escaped_key}'")

        is_new = str(self.db_path) == ":memory:" or not Path(self.db_path).exists()
        if is_new:
            # Pin the SQLCipher 4 file format only for databases we create.
            self.cursor.execute("PRAGMA cipher_compatibility = 4")

        # Force page decryption so a wrong key fails fast instead of lazily.
        try:
            self.cursor.execute("SELECT count(*) FROM sqlite_master").fetchone()
        except sqlite3.DatabaseError as exc:  # pylint: disable=no-member
            raise ValueError(
                f"Unable to decrypt database {self.db_path} with the provided key."
            ) from exc

        print("Cipher:", self.cursor.execute("PRAGMA cipher_version;").fetchone()[0])
        print(f"Encrypted database {self.db_path} loaded securely.")

    def __iter__(self) -> Iterator[RowType]:
        """
        Allow iterating over the class, by outputing all members
        """
        if not self.row_class:
            raise RuntimeError("Table not loaded yet — no dataclass available")
        return self._iter_rows()

    def _row_to_obj(self, row: sqlite3.Row) -> RowType:
        """
        Convert an sqlite3 row into a dataclass object

        :param row: The sqlite3 row to convert

        :return: A dataclass object of the row
        """
        row_dict = dict(row)

        if self.row_class is None:
            raise RuntimeError("Table not loaded yet — no dataclass available")

        # Convert types based on dataclass field types
        for field in self.row_class.__dataclass_fields__.values():
            k = field.name
            v = row_dict.get(k)
            if v is None:
                continue

            if field.type == Decimal:
                if isinstance(v, float):
                    row_dict[k] = Decimal(str(v))
                elif isinstance(v, str):
                    try:
                        row_dict[k] = Decimal(v)
                    except InvalidOperation:
                        pass
            elif field.type == date:
                if isinstance(v, date):
                    continue

                dt = mojo_loader.parse_date(v)
                if dt:
                    row_dict[k] = dt.date()
                else:
                    # If we can't parse it, keep as original value
                    pass

        return self.row_class(**row_dict)

    def _iter_rows(self) -> Iterator[RowType]:
        """
        Iterate over table rows and yield dynamically-created dataclass objects
        Converts REAL columns to Decimal and DATE columns to date objects automatically

        :return: An interator of dataclass objects for rows
        """

        sql = f'SELECT * FROM "{self.table_name}"'

        cur = self.conn.cursor()
        cur.execute(sql)

        for row in cur:
            yield self._row_to_obj(row)

    def _build_dataclass_from_table(self) -> type[RowType]:
        """
        Dynamically create a dataclass from the table schema
        INTEGER → int
        REAL → Decimal
        DATE → date
        TEXT → str

        :return: A dataclass built from the table columns and types

        :raises ValueError: If no table
        """
        self.cursor.execute(f'PRAGMA table_info("{self.table_name}")')
        cols = self.cursor.fetchall()

        if not cols:
            raise ValueError(f"Table '{self.table_name}' does not exist")

        fields: list[tuple[str, type[Any]]] = []
        for _cid, name, col_type, _notnull, _dflt, _pk in cols:
            t = col_type.upper()

            py_type: type[Any]
            if t.startswith("INT"):
                py_type = int
            elif t.startswith("REAL") or t.startswith("NUM") or t.startswith("DEC"):
                py_type = Decimal
            elif t.startswith("DATE"):
                py_type = date
            else:
                py_type = str

            fields.append((name, py_type))

        return make_dataclass(f"{self.table_name}_Row", fields)

    def _staging_name(self) -> str:
        """Return a table name for staging an import before the final swap."""
        return f"{self.table_name}_new"

    def _drop_staging(self, staging: str) -> None:
        """Drop a staging table left behind by a failed import."""
        self.conn.execute(f'DROP TABLE IF EXISTS "{staging}"')

    def _drop_stale_old(self) -> None:
        """Drop any '*_old' table left over from an interrupted previous import."""
        self.conn.execute(f'DROP TABLE IF EXISTS "{self.table_name}_old"')

    def _swap_in_staging(self, staging: str) -> str:
        """
        Promote the staging table to the canonical name, preserving the
        previous table (if any) as '*_old'.

        :param staging: Name of the freshly-imported staging table

        :return: The old table name, or "" if there was no previous table
        """
        if not self.table_exists():
            self.conn.execute(f'ALTER TABLE "{staging}" RENAME TO "{self.table_name}"')
            return ""

        old_table = f"{self.table_name}_old"
        self.conn.execute("BEGIN")
        try:
            self.cursor.execute(
                "SELECT type FROM sqlite_master WHERE name=?", (self.table_name,)
            )
            result = self.cursor.fetchone()
            if result and result[0] == "view":
                # Views can't be renamed; the base tables still hold the data.
                self.conn.execute(f'DROP VIEW IF EXISTS "{self.table_name}"')
            else:
                self.conn.execute(
                    f'ALTER TABLE "{self.table_name}" RENAME TO "{old_table}"'
                )
                # The renamed table keeps its index names, which would collide
                # with the indexes the promoted table will need (e.g.
                # "members__member_number"). Drop them now; they are recreated
                # on the promoted table by _ensure_indexes().
                self._drop_table_indexes(old_table)
            self.conn.execute(f'ALTER TABLE "{staging}" RENAME TO "{self.table_name}"')
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        return old_table

    def _drop_table_indexes(self, table_name: str) -> None:
        """
        Drop all user indexes on a table (not the implicit sqlite_* indexes).
        """
        self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=? "
            "AND name NOT LIKE 'sqlite_autoindex%'",
            (table_name,),
        )
        for (index_name,) in self.cursor.fetchall():
            self.conn.execute(f'DROP INDEX "{index_name}"')

    def _ensure_indexes(self) -> None:
        """Create indexes for the current table if it exists."""
        if self.table_exists():
            mojo_loader.create_indexes(self.conn, self.table_name)

    def print_diff(self, old_table: str) -> None:
        """
        Print out diff between old and new db

        :param old_table: The name the existing table was renamed to
        """
        try:
            # Diff old vs new (SQLCipher → sqlite3 → dataclasses)
            diff_rows = mojo_loader.diff_cipher_tables(
                self.conn,
                new_table=self.table_name,
                old_table=old_table,
            )

            if diff_rows:
                for diff in diff_rows:
                    # diff is a DiffRow dataclass
                    print(diff.diff_type, diff.preview)

        finally:
            # Cleanup old table (always)
            self.conn.execute(f'DROP TABLE IF EXISTS "{old_table}"')

    def download_csv(
        self,
        session: requests.Session,
        url: str,
        merge: bool = False,
        table_name: str | None = None,
    ) -> None:
        """
        Download the CSV from url and import into the sqlite database
        If a previous table exists, generate a diff

        :param session: Requests session to use for download
        :param url: url of the csv to download
        :param merge: (optional) If True, merge into existing table. Defaults to False.
        :param table_name: (optional) Table name to use for import. Defaults to self.table_name.
        """
        if table_name:
            self.table_name = table_name

        self._invalidate_caches()
        self._drop_stale_old()
        if merge:
            ok = mojo_loader.download_csv_helper(
                self.conn, self.table_name, url, session, merge=True
            )
            if ok:
                self.row_class = self._build_dataclass_from_table()
                self._ensure_indexes()
            return

        staging = self._staging_name()
        try:
            ok = mojo_loader.download_csv_helper(self.conn, staging, url, session)
        except BaseException:
            self._drop_staging(staging)
            self.conn.rollback()
            raise
        if not ok:
            # Empty CSV — leave any existing table untouched.
            self._drop_staging(staging)
            return
        old_table = self._swap_in_staging(staging)
        self.row_class = self._build_dataclass_from_table()
        self._ensure_indexes()
        if old_table:
            self.print_diff(old_table)

    def import_csv(
        self, csv_path: Path, merge: bool = False, table_name: str | None = None
    ) -> None:
        """
        Import the passed CSV into the encrypted sqlite database

        :param csv_path: Path like path of csv file
        :param merge: (optional) If True, merge into existing table. Defaults to False.
            Form importing current, and expired members as headings are the same.
        :param table_name: (optional) Table name to use for import. Defaults to self.table_name.
        """
        if table_name:
            self.table_name = table_name

        self._invalidate_caches()
        self._drop_stale_old()
        if merge:
            mojo_loader.import_csv_helper(
                self.conn, self.table_name, csv_path, merge=True
            )
            self.row_class = self._build_dataclass_from_table()
            self._ensure_indexes()
            return

        staging = self._staging_name()
        try:
            mojo_loader.import_csv_helper(self.conn, staging, csv_path)
        except BaseException:
            self._drop_staging(staging)
            self.conn.rollback()
            raise
        old_table = self._swap_in_staging(staging)
        self.row_class = self._build_dataclass_from_table()
        self._ensure_indexes()
        if old_table:
            self.print_diff(old_table)

    def show_table(self, limit: int = 2) -> None:
        """
        Print the first few rows of the table as dictionaries

        :param limit: (optional) Number of rows to display. Defaults to 2
        """
        if self.table_exists():
            self.cursor.execute(f'SELECT * FROM "{self.table_name}" LIMIT ?', (limit,))
            rows = self.cursor.fetchall()

        else:
            print("(No data)")
            return

        for row in rows:
            print(dict(row))

    def count(self) -> int:
        """
        :return: count of the number of rows in the table, or 0 if no table
        """
        if self.table_exists():
            self.cursor.execute(f'SELECT COUNT(*) FROM "{self.table_name}"')
            result = self.cursor.fetchone()
            return int(result[0]) if result else 0

        return 0

    def get_row(
        self,
        entry_name: str,
        entry_value: str,
        only_one: bool = True,
        table_name: str | None = None,
    ) -> RowType | list[RowType] | None:
        """
        Retrieve a single or multiple rows matching column = value (case-insensitive)

        :param entry_name: Column name to filter by
        :param entry_value: Value to match
        :param only_one: If True (default), return the first matching row
                If False, return a list of all matching rows
        :param table_name: (optional) Table name to use for query. Defaults to self.table_name.

        :return:
            - If only_one=True → a single row object or None
            - If only_one=False → list of row objects (may be empty)
        """
        match_dict = {f"{entry_name}": entry_value}

        return self.get_row_multi(match_dict, only_one, table_name=table_name)

    def _build_query_conditions(
        self, match_dict: dict[str, Any]
    ) -> tuple[list[str], list[Any]]:
        """
        Build SQL WHERE conditions and values from match_dict
        """
        conditions: list[str] = []
        values: list[Any] = []

        for col, val in match_dict.items():
            quoted = self.quote_identifier(col)
            if val is None or val == "":
                conditions.append(f"{quoted} IS NULL")
            elif isinstance(val, Like):
                conditions.append(f"LOWER({quoted}) LIKE LOWER(?)")
                values.append(val.pattern)
            elif isinstance(val, DateRange):
                conditions.append(f"{quoted} BETWEEN ? AND ?")
                values.extend([val.start.isoformat(), val.end.isoformat()])
            elif isinstance(val, (tuple, list)) and len(val) == 2:
                self._add_range_condition(quoted, val, conditions, values)
            else:
                self._add_equality_condition(quoted, val, conditions, values)
        return conditions, values

    def _add_range_condition(
        self,
        col: str,
        val: tuple[Any, Any] | list[Any],
        conditions: list[str],
        values: list[Any],
    ) -> None:
        """Helper for range conditions (col is already quoted)"""
        lower, upper = val
        if lower is not None and upper is not None:
            conditions.append(f"{col} BETWEEN ? AND ?")
            values.extend(
                [x.isoformat() if isinstance(x, date) else x for x in (lower, upper)]
            )
        elif lower is not None:
            conditions.append(f"{col} >= ?")
            values.append(lower.isoformat() if isinstance(lower, date) else lower)
        elif upper is not None:
            conditions.append(f"{col} <= ?")
            values.append(upper.isoformat() if isinstance(upper, date) else upper)

    def _add_equality_condition(
        self,
        col: str,
        val: Any,
        conditions: list[str],
        values: list[Any],
    ) -> None:
        """Helper for equality conditions (col is already quoted)"""
        conditions.append(f"{col} = ?")
        if isinstance(val, date):
            values.append(val.isoformat())
        elif isinstance(val, Decimal):
            values.append(float(val.quantize(Decimal("0.01"))))
        else:
            values.append(val)

    def create_joined_table(
        self,
        table_name: str,
        join_table: str,
        join_col: str = "payment_id",
        is_view: bool = True,
    ) -> None:
        """
        Create a View or Table that joins the current table with another table

        :param table_name: Name of the view or table to create
        :param join_table: Name of the table to join with
        :param join_col: (optional) Column name to join on. Defaults to "payment_id"
        :param is_view: (optional) If True, create a VIEW, otherwise a TABLE.
            Defaults to True.
        """
        # Get column names for both tables
        self.cursor.execute(f"PRAGMA table_info({_q(self.table_name)})")
        cols1 = {row[1] for row in self.cursor.fetchall()}
        self.cursor.execute(f"PRAGMA table_info({_q(join_table)})")
        cols2 = [row[1] for row in self.cursor.fetchall()]

        # Skip redundant personal info columns that are likely identical
        # but preserve and alias other conflicting columns
        skip_cols = {
            "first_name",
            "last_name",
            "email",
            "member_number",
            "membermojo_id",
        }

        other_cols = []
        for c in cols2:
            if c == join_col or c in skip_cols:
                continue
            if c in cols1:
                other_cols.append(f"{_q(join_table)}.{_q(c)} AS {_q(f'item_{c}')}")
            else:
                other_cols.append(f"{_q(join_table)}.{_q(c)}")

        # Select all from table1, plus specific columns from table2
        select_clause = f"{_q(self.table_name)}.*"
        if other_cols:
            select_clause += ", " + ", ".join(other_cols)

        type_str = "VIEW" if is_view else "TABLE"

        sql = f"""
            CREATE {type_str} {_q(table_name)} AS
            SELECT {select_clause}
            FROM {_q(self.table_name)}
            LEFT JOIN {_q(join_table)}
            ON {_q(self.table_name)}.{_q(join_col)} = {_q(join_table)}.{_q(join_col)}
        """
        # Drop existing object based on its type to avoid OperationalError
        self.cursor.execute(
            "SELECT type FROM sqlite_master WHERE name=?", (table_name,)
        )
        result = self.cursor.fetchone()
        if result:
            if result[0] == "view":
                self.conn.execute(f'DROP VIEW IF EXISTS "{table_name}"')
            else:
                self.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')

        self.conn.execute(sql)
        self.conn.commit()

    def create_view(
        self, view_name: str, join_table: str, join_col: str = "payment_id"
    ) -> None:
        """
        Create a View that joins the current table with another table
        """
        self.create_joined_table(view_name, join_table, join_col, is_view=True)

    def _invalidate_caches(self) -> None:
        """
        Drop in-memory caches derived from the current tables.

        Subclasses may extend this to drop their own caches after the
        underlying data changes.
        """
        self._column_cache.clear()

    def set_table(self, table_name: str) -> None:
        """
        Switch to a different table and update the row_class
        """
        self.table_name = table_name
        self._invalidate_caches()
        if self.table_exists():
            self.row_class = self._build_dataclass_from_table()
        else:
            self.row_class = None

    def get_row_multi(
        self,
        match_dict: dict[str, Any],
        only_one: bool = True,
        table_name: str | None = None,
    ) -> RowType | list[RowType] | None:
        """
        Retrieve one or many rows matching multiple column=value pairs

        :param match_dict: Dictionary of column names and values to match
        :param only_one: If True (default), return the first matching row
                        If False, return a list of all matching rows
        :param table_name: (optional) Table name to use for query. Defaults to self.table_name.

        :return:
            - If only_one=True → a single row object or None
            - If only_one=False → list of row objects (may be empty)
        """
        if table_name:
            self.set_table(table_name)

        if not self.table_exists():
            return None if only_one else []

        conditions, values = self._build_query_conditions(match_dict)

        # Base query string
        base_query = (
            f'SELECT * FROM "{self.table_name}" WHERE {" AND ".join(conditions)}'
        )
        if self.debug:
            print("Sql:")
            pprint(base_query)

        if only_one:
            query = base_query + " LIMIT 1"
            self.cursor.execute(query, values)
            if row := self.cursor.fetchone():
                return self._row_to_obj(row)
            return None

        self.cursor.execute(base_query, values)
        return [self._row_to_obj(row) for row in self.cursor.fetchall()]

    def run_count_query(self, sql: str, params: tuple[Any, ...]) -> int:
        """
        Generate whole sql query for running on db table for counting and run

        :param sql: the sqlite query for matching rows
        :param params: the paramaters to use for the query

        :return: number of matching rows
        """
        sql_query = f'SELECT count(*) FROM "{self.table_name}" {sql}'
        cursor = self.cursor.execute(sql_query, params)
        row = cursor.fetchone()
        return int(row[0]) if row else 0

    def table_exists(self) -> bool:
        """
        Return True or False if a table exists
        """
        return mojo_loader.table_exists(self.cursor, self.table_name)

    def quote_identifier(self, identifier: str) -> str:
        """
        Validate that `identifier` is a real column in the current table and
        returns it safely quoted for interpolation into SQL.

        Raises ValueError if the identifier is not a known column, preventing
        SQL injection via quoted identifiers.

        :param identifier: The column name to validate and quote

        :return: The safely double-quoted column name

        :raises ValueError: If the column does not exist in the table
        """
        columns = self._column_cache.get(self.table_name)
        if columns is None:
            self.cursor.execute(f'PRAGMA table_info("{self.table_name}")')
            columns = {row[1] for row in self.cursor.fetchall()}
            self._column_cache[self.table_name] = columns

        if identifier not in columns:
            raise ValueError(
                f"Unknown column '{identifier}' for table '{self.table_name}'"
            )

        return _q(identifier)
