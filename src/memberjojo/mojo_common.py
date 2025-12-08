"""
MojoSkel base class

This module provides a common base class (`MojoSkel`) for other `memberjojo` modules.
It includes helper methods for working with SQLite databases.
"""

# pylint: disable=no-member

from dataclasses import make_dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Union, List

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
        :param db_key: key to unlock the encrypted sqlite database, or encrypt new one.
        :param table_name: Name of the table to operate on, or create when importing.
        """
        self.db_path = db_path
        self.table_name = table_name
        self.db_key = db_key

        # Open connection
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

        # Apply SQLCipher key
        self.cursor.execute(f"PRAGMA key='{db_key}'")
        self.cursor.execute("PRAGMA cipher_compatibility = 4")
        print("Cipher:", self.cursor.execute("PRAGMA cipher_version;").fetchone()[0])
        print(f"Encrypted database {self.db_path} loaded securely.")

        # After table exists (or after import), build the dataclass
        if self.table_exists(table_name):
            self.row_class = self._build_dataclass_from_table()
        else:
            self.row_class = None

    def __iter__(self):
        """
        Allow iterating over the class, by outputing all members.
        """
        if not self.row_class:
            raise RuntimeError("Table not loaded yet — no dataclass available")
        return self._iter_rows()

    def _iter_rows(self):
        """
        Iterate over table rows and yield dynamically-created dataclass objects.
        Converts REAL columns to Decimal automatically.
        """

        sql = f'SELECT * FROM "{self.table_name}"'

        cur = self.conn.cursor()
        cur.execute(sql)

        for row in cur.fetchall():
            row_dict = dict(row)

            # Convert REAL → Decimal
            for k, v in row_dict.items():
                if isinstance(v, float):
                    row_dict[k] = Decimal(str(v))
                elif isinstance(v, str):
                    # Try converting numeric strings
                    try:
                        row_dict[k] = Decimal(v)
                    except InvalidOperation:
                        pass

            yield self.row_class(**row_dict)

    def _build_dataclass_from_table(self):
        """
        Dynamically create a dataclass from the table schema.
        INTEGER → int
        REAL → Decimal
        TEXT → str

        :return: A dataclass built from the table columns and types.
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

    def import_csv(self, csv_path: Path):
        """
        import the passed CSV into the sqlite database

        :param csv_path: Path like path of csv file.
        """
        mojo_loader.import_csv_helper(self.conn, self.table_name, csv_path)
        self.row_class = self._build_dataclass_from_table()

    def show_table(self, limit: int = 2):
        """
        Print the first few rows of the table as dictionaries.

        :param limit: (optional) Number of rows to display. Defaults to 2.
        """
        if self.table_exists(self.table_name):
            self.cursor.execute(f'SELECT * FROM "{self.table_name}" LIMIT ?', (limit,))
            rows = self.cursor.fetchall()

        else:
            print("(No data)")
            return

        for row in rows:
            print(dict(row))

    def count(self) -> int:
        """
        :return: count of the number of rows in the table, or 0 if no table.
        """
        if self.table_exists(self.table_name):
            self.cursor.execute(f'SELECT COUNT(*) FROM "{self.table_name}"')
            result = self.cursor.fetchone()
            return result[0] if result else 0

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
        """
        Return true or false if a table exists
        """
        self.cursor.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;",
            (table_name,),
        )
        return self.cursor.fetchone() is not None
