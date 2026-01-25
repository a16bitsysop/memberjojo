"""
MojoSkel base class

This module provides a common base class (`MojoSkel`) for other `memberjojo` modules
It includes helper methods for working with SQLite databases
"""

from dataclasses import make_dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from pprint import pprint
from typing import Any, Iterator, List, Type, Union

import requests

try:
    from sqlcipher3 import dbapi2 as sqlite3

    HAS_SQLCIPHER = True
except ImportError:
    import sqlite3  # stdlib

    HAS_SQLCIPHER = False

from . import mojo_loader
from .sql_query import Like


class MojoSkel:
    """
    Establishes a connection to a SQLite database and provides helper methods
    for querying tables
    """

    def __init__(self, db_path: str, db_key: str, table_name: str):
        """
        Initialize the MojoSkel class

        Connects to the SQLite database and sets the row factory for
        dictionary-style access to columns.

        :param db_path: Path to the SQLite database file
        :param db_key: key to unlock the encrypted sqlite database,
            unencrypted if sqlcipher3 not installed or unset
        :param table_name: Name of the table to operate on, or create when importing
        """
        self.db_path = db_path
        self.table_name = table_name
        self.db_key = db_key
        self.debug = False

        # Open connection
        self.conn = sqlite3.connect(self.db_path)  # pylint: disable=no-member
        self.conn.row_factory = sqlite3.Row  # pylint: disable=no-member
        self.cursor = self.conn.cursor()

        if HAS_SQLCIPHER and db_key:
            # Apply SQLCipher key
            self.cursor.execute(f"PRAGMA key='{db_key}'")
            self.cursor.execute("PRAGMA cipher_compatibility = 4")
            print(
                "Cipher:", self.cursor.execute("PRAGMA cipher_version;").fetchone()[0]
            )
            print(f"Encrypted database {self.db_path} loaded securely.")
        else:
            print(f"Unencrypted database {self.db_path} loaded securely.")

        # After table exists (or after import), build the dataclass
        if self.table_exists():
            self.row_class = self._build_dataclass_from_table()
        else:
            self.row_class = None

    def __iter__(self) -> Iterator[Any]:
        """
        Allow iterating over the class, by outputing all members
        """
        if not self.row_class:
            raise RuntimeError("Table not loaded yet — no dataclass available")
        return self._iter_rows()

    def _row_to_obj(self, row: sqlite3.Row) -> Type[Any]:
        """
        Convert an sqlite3 row into a dataclass object

        :param row: The sqlite3 row to convert

        :return: A dataclass object of the row
        """
        row_dict = dict(row)

        # Convert REAL → Decimal (including numeric strings)
        for k, v in row_dict.items():
            if isinstance(v, float):
                row_dict[k] = Decimal(str(v))
            elif isinstance(v, str):
                try:
                    row_dict[k] = Decimal(v)
                except InvalidOperation:
                    pass

        return self.row_class(**row_dict)

    def _iter_rows(self) -> Iterator[Any]:
        """
        Iterate over table rows and yield dynamically-created dataclass objects
        Converts REAL columns to Decimal automatically

        :return: An interator of dataclass objects for rows
        """

        sql = f'SELECT * FROM "{self.table_name}"'

        cur = self.conn.cursor()
        cur.execute(sql)

        for row in cur.fetchall():
            yield self._row_to_obj(row)

    def _build_dataclass_from_table(self) -> Type[Any]:
        """
        Dynamically create a dataclass from the table schema
        INTEGER → int
        REAL → Decimal
        TEXT → str

        :return: A dataclass built from the table columns and types

        :raises ValueError: If no table
        """
        self.cursor.execute(f'PRAGMA table_info("{self.table_name}")')
        cols = self.cursor.fetchall()

        if not cols:
            raise ValueError(f"Table '{self.table_name}' does not exist")

        fields = []
        for _cid, name, col_type, _notnull, _dflt, _pk in cols:
            t = col_type.upper()

            if t.startswith("INT"):
                py_type = int
            elif t.startswith("REAL") or t.startswith("NUM") or t.startswith("DEC"):
                py_type = Decimal
            else:
                py_type = str

            fields.append((name, py_type))

        return make_dataclass(f"{self.table_name}_Row", fields)

    def rename_old_table(self, existing: bool) -> str:
        """
        If there was an exising table rename for comparison

        :param existing: bool for table exists

        :return: the old table name
        """
        old_table = f"{self.table_name}_old"
        self.conn.execute(f"DROP TABLE IF EXISTS {old_table}")
        # Preserve existing table
        if existing:
            self.conn.execute(f"ALTER TABLE {self.table_name} RENAME TO {old_table}")
        return old_table

    def print_diff(self, old_table: str):
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
            self.conn.execute(f"DROP TABLE {old_table}")

    def download_csv(self, session: requests.Session, url: str, merge: bool = False):
        """
        Download the CSV from url and import into the sqlite database
        If a previous table exists, generate a diff

        :param session: Requests session to use for download
        :param url: url of the csv to download
        :param merge: (optional) If True, merge into existing table. Defaults to False.
        """
        had_existing = False
        old_table = ""

        if not merge:
            had_existing = self.table_exists()
            old_table = self.rename_old_table(had_existing)

        # Download CSV as new table
        mojo_loader.download_csv_helper(
            self.conn, self.table_name, url, session, merge=merge
        )
        self.row_class = self._build_dataclass_from_table()

        if merge:
            return

        if not had_existing:
            return

        self.print_diff(old_table)

    def import_csv(self, csv_path: Path, merge: bool = False):
        """
        Import the passed CSV into the encrypted sqlite database

        :param csv_path: Path like path of csv file
        :param merge: (optional) If True, merge into existing table. Defaults to False.
            Form importing current, and expired members as headings are the same.
        """
        had_existing = False
        old_table = ""

        if not merge:
            had_existing = self.table_exists()
            old_table = self.rename_old_table(had_existing)

        # Import CSV as new table
        mojo_loader.import_csv_helper(self.conn, self.table_name, csv_path, merge=merge)
        self.row_class = self._build_dataclass_from_table()

        if merge:
            return

        if not had_existing:
            return

        self.print_diff(old_table)

    def show_table(self, limit: int = 2):
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
            return result[0] if result else 0

        return 0

    def get_row(
        self, entry_name: str, entry_value: str, only_one: bool = True
    ) -> Union[sqlite3.Row, List[sqlite3.Row], None]:
        """
        Retrieve a single or multiple rows matching column = value (case-insensitive)

        :param entry_name: Column name to filter by
        :param entry_value: Value to match
        :param only_one: If True (default), return the first matching row
                If False, return a list of all matching rows

        :return:
            - If only_one=True → a single sqlite3.Row or None
            - If only_one=False → list of sqlite3.Row (may be empty)
        """
        match_dict = {f"{entry_name}": entry_value}

        return self.get_row_multi(match_dict, only_one)

    def get_row_multi(
        self, match_dict: dict, only_one: bool = True
    ) -> Union[sqlite3.Row, List[sqlite3.Row], None]:
        """
        Retrieve one or many rows matching multiple column=value pairs

        :param match_dict: Dictionary of column names and values to match
        :param only_one: If True (default), return the first matching row
                        If False, return a list of all matching rows

        :return:
            - If only_one=True → a single sqlite3.Row or None
            - If only_one=False → list of sqlite3.Row (may be empty)
        """
        conditions = []
        values = []

        for col, val in match_dict.items():
            if val is None or val == "":
                conditions.append(f'"{col}" IS NULL')
            elif isinstance(val, Like):
                conditions.append(f'LOWER("{col}") LIKE LOWER(?)')
                values.append(val.pattern)
            elif isinstance(val, (tuple, list)) and len(val) == 2:
                lower, upper = val
                if lower is not None and upper is not None:
                    conditions.append(f'"{col}" BETWEEN ? AND ?')
                    values.extend([lower, upper])
                elif lower is not None:
                    conditions.append(f'"{col}" >= ?')
                    values.append(lower)
                elif upper is not None:
                    conditions.append(f'"{col}" <= ?')
                    values.append(upper)
                else:
                    # Both are None, effectively no condition on this column
                    pass
            else:
                conditions.append(f'"{col}" = ?')
                values.append(
                    float(val.quantize(Decimal("0.01")))
                    if isinstance(val, Decimal)
                    else val
                )

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

    def run_count_query(self, sql: str, params: tuple):
        """
        Generate whole sql query for running on db table for counting and run

        :param sql: the sqlite query for matching rows
        :param params: the paramaters to use for the query
        """
        sql_query = f"SELECT count (*) FROM {self.table_name} {sql}"
        cursor = self.cursor.execute(sql_query, params)
        return cursor.fetchone()[0]

    def member_type_count(self, membership_type: str):
        """
        Count members by membership type string

        :param membership_type: the string to match, can use percent to match
            remaining or preceeding text
                - Full (match only Full)
                - Full% (match Full and any words after)
                - %Full% ( match Full in the middle)
        """
        query = "WHERE membership LIKE ?"
        return self.run_count_query(query, (f"{membership_type}",))

    def table_exists(self) -> bool:
        """
        Return True or False if a table exists
        """
        return mojo_loader.table_exists(self.cursor, self.table_name)
