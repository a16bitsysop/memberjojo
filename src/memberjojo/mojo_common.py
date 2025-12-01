"""
MojoSkel base class

This module provides a common base class (`MojoSkel`) for other `memberjojo` modules.
It includes helper methods for working with SQLite databases.
"""

from csv import DictReader
from decimal import Decimal
from pathlib import Path
from typing import Union, List

from sqlcipher3 import dbapi2 as sqlite


class MojoSkel:
    """
    Establishes a connection to a SQLite database and provides helper methods
    for querying tables.
    """

    def __init__(self, db_path: str, table_name: str, db_key: str):
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

        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        if db_key is None:
            if db_path == ":memory:":
                db_name = db_path
            else:
                db_name = db_path.name
            print(f"Unencrypted database {db_name} loaded.")
        else:
            self.conn.execute(f"PRAGMA key='{db_key}'")
            self.conn.execute("PRAGMA cipher_compatibility = 3")
            print(f"Encrypted database {self.db_path.name} loaded securely.")

    def import_csv_into_encrypted_db(self, csv_path: Path, key: str, table_name: str):
        """Import a CSV file into an encrypted SQLCipher database."""
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        # Count rows before import
        count_before = self.count()

        create_table_sql = self._create_table_sql_from_csv(csv_path, table_name)
        lines = [
            f"PRAGMA key='{key}';",
            "PRAGMA cipher_compatibility = 4;",
            "",
            # Drop table if it exists to ensure clean import
            f"DROP TABLE IF EXISTS {table_name};",
            "",
            # Table creation SQL
            create_table_sql.strip() + ";",
            "",
            # Dot commands MUST be raw, no indentation!
            # Skip first row as it is headers
            ".mode csv",
            f'.import --skip 1 "{csv_path}" {table_name}',
        ]

        print(f"  Importing {csv_path.name} into {self.db_path.name}...")
        self._run_sqlcipher(lines)
        # Load the encrypted database into memory
        self._load_encrypted_db(key)
        print(
            f"Inserted {self.count() - count_before} new rows into '{self.table_name}'."
        )

    def _get_column_type_map(self):
        """Return a mapping of column names to their SQL types."""
        return {
            # Integer columns
            "Member Number": "INTEGER",
            "Member number": "INTEGER",
            "membermojo ID": "INTEGER",
            # Real/Float columns
            "Cost": "REAL",
            "Paid": "REAL",
            # All other columns default to TEXT
        }

    def _create_table_from_columns(self, table_name: str, columns: list[str]) -> str:
        """Generate CREATE TABLE SQL with proper types for given columns."""
        type_map = self._get_column_type_map()

        column_defs = []
        for col in columns:
            col_type = type_map.get(col, "TEXT")  # Default to TEXT
            column_defs.append(f'    "{col}" {col_type}')

        return (
            f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
            + ",\n".join(column_defs)
            + "\n)"
        )

    def _create_table_sql_from_csv(self, csv_path: Path, table_name: str) -> str:
        """
        Generate CREATE TABLE SQL dynamically from the CSV header
        using csv.DictReader. All columns are TEXT.
        """
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = DictReader(f)
            columns = reader.fieldnames

        if not columns:
            raise ValueError(f"CSV file '{csv_path}' has no header row.")

        return self._create_table_from_columns(table_name, columns)

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
        Safe: returns 0 if the table doesn't exist or the DB is unreadable.
        """
        try:
            self.cursor.execute(f'SELECT COUNT(*) FROM "{self.table_name}"')
            result = self.cursor.fetchone()
            return result[0] if result else 0
        except (sqlite3.OperationalError, sqlite3.DatabaseError, AttributeError):
            # Table missing, DB unreadable (e.g. encrypted file), or cursor not set
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
