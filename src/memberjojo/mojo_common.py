"""
MojoSkel base class

This module provides a common base class (`MojoSkel`) for other `memberjojo` modules.
It includes helper methods for working with SQLite databases.
"""

from csv import DictReader
from decimal import Decimal
from pathlib import Path
from typing import Union, List
import re

from sqlcipher3 import dbapi2 as sqlite3

from . import mojo_loader


class MojoSkel:
    """
    Establishes a connection to a SQLite database and provides helper methods
    for querying tables.
    """

    def __init__(self, db_path: str, db_key: str, table_name: str):
        """
        Initialize the MojoSkel class.

        Connects to the SQLite database and sets the row factory for
        dictionary-style access to columns.

        :param db_path: Path to the SQLite database file.
        :param table_name: Name of the table to operate on, or create when importing.
        :param db_key: (optional) key to unlock the encrypted sqlite database, unencrypted if unset.
        """
        self.db_path = db_path
        self.table_name = table_name
        self.columns = {}
        self.conn = None
        self.db_key = db_key

        # Open connection
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

        # Apply SQLCipher key
        self.cursor.execute(f"PRAGMA key='{db_key}'")
        self.cursor.execute("PRAGMA cipher_compatibility = 4")
        print("Cipher:", self.cursor.execute("PRAGMA cipher_version;").fetchone()[0])

        if self.db_path == ":memory:":
            db_name = self.db_path
        else:
            db_name = self.db_path.name
        print(f"Encrypted database {db_name} loaded securely.")

    def import_csv(self, csv_path: Path):
        mojo_loader.import_csv_into_encrypted_db(csv_path)

    def show_table(self, limit: int = 2):
        """
        Print the first few rows of the table as dictionaries.

        :param limit: (optional) Number of rows to display. Defaults to 2.
        """
        self.cursor.execute(f'SELECT * FROM "{self.table_name}" LIMIT ?', (limit,))
        rows = self.cursor.fetchall()

        if not rows:
            print("(No data)")
            return

        for row in rows:
            print(dict(row))

    def count(self) -> int:
        """
        Returns count of the number of rows in the table.
        Safe: returns 0 if the table doesn't exist.
        """
        if self.table_exists(self.table_name):
            self.cursor.execute(f'SELECT COUNT(*) FROM "{self.table_name}"')
            result = self.cursor.fetchone()
            return result[0] if result else 0
        else:
            return 0

    def get_row(self, entry_name: str, entry_value: str) -> dict:
        """
        Retrieve a single row matching column = value (case-insensitive).

        :param entry_name: Column name to filter by.
        :param entry_value: Value to match.

        :return: The matching row as a dictionary, or None if not found.
        """
        if not entry_value:
            return None
        query = (
            f'SELECT * FROM "{self.table_name}" WHERE LOWER("{entry_name}") = LOWER(?)'
        )
        self.cursor.execute(query, (entry_value,))
        row = self.cursor.fetchone()
        return dict(row) if row else None

    def get_row_multi(
        self, match_dict: dict, only_one: bool = True
    ) -> Union[sqlite3.Row, List[sqlite3.Row], None]:
        """
        Retrieve one or many rows matching multiple column=value pairs.

        :param match_dict: Dictionary of column names and values to match.
        :param only_one: If True (default), return the first matching row.
                        If False, return a list of all matching rows.

        :return:
            - If only_one=True → a single sqlite3.Row or None
            - If only_one=False → list of sqlite3.Row (may be empty)
        """
        conditions = []
        values = []

        for col, val in match_dict.items():
            if val is None or val == "":
                conditions.append(f'"{col}" IS NULL')
            else:
                conditions.append(f'"{col}" = ?')
                values.append(
                    float(val.quantize(Decimal("0.01")))
                    if isinstance(val, Decimal)
                    else val
                )

        base_query = (
            f'SELECT * FROM "{self.table_name}" WHERE {" AND ".join(conditions)}'
        )

        if only_one:
            query = base_query + " LIMIT 1"
            self.cursor.execute(query, values)
            return self.cursor.fetchone()

        # Return *all* rows
        self.cursor.execute(base_query, values)
        return self.cursor.fetchall()

    def table_exists(self, table_name: str) -> bool:
        self.cursor.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;",
            (table_name,),
        )
        return self.cursor.fetchone() is not None
