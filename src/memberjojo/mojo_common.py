"""
MojoSkel base class

This module provides a common base class (`MojoSkel`) for other `memberjojo` modules.
It includes helper methods for working with SQLite databases.
"""

import subprocess
import sqlite3
from pathlib import Path
from csv import DictReader
from decimal import Decimal


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

        if db_key is None:
            self.conn = sqlite3.connect(db_path)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()

            if db_path == ":memory:":
                db_name = db_path
            else:
                db_name = db_path.name
            print(f"Unencrypted database {db_name} loaded.")
        else:
            self._load_encrypted_db(db_key)

    def _load_encrypted_db(self, db_key):
        # Dump decrypted SQL from SQLCipher
        sql_commands = [
            f"PRAGMA key='{db_key}';",
            ".output stdout",
            ".dump",
        ]
        dump_sql = self._run_sqlcipher(sql_commands)

        # SQLCipher may output "ok" after PRAGMA key, which is not valid SQL.
        # Filter out lines that are not SQL statements.
        filtered_dump_sql = "\n".join(
            line for line in dump_sql.splitlines() if line.strip() != "ok"
        )

        # Load into in-memory SQLite
        self.conn = sqlite3.connect(":memory:")
        self.conn.executescript(filtered_dump_sql)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

        print(f"Encrypted database {self.db_path.name} loaded securely.")

    def _run_sqlcipher(self, sql_lines: list[str]):
        """
        Run SQLCipher with a list of raw lines.
        Dot-commands must not be indented or mixed with semicolon SQL.
        Uses a context manager for cleaner subprocess handling.
        """

        script = "".join(
            line if line.endswith("\n") else line + "\n" for line in sql_lines
        )

        # Use context manager for automatic resource cleanup
        with subprocess.Popen(
            ["sqlcipher", str(self.db_path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ) as proc:

            out, err = proc.communicate(script)

            if proc.returncode != 0:
                raise RuntimeError(f"SQLCipher failed:\n{err}")

            return out

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

    def get_row_multi(self, match_dict: dict) -> dict:
        """
        Retrieve the first row matching multiple column = value pairs.

        :param match_dict: Dictionary of column names and values to match.

        :return: The first matching row, or None if not found.
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

        query = f'SELECT * FROM "{self.table_name}" WHERE {" AND ".join(conditions)} LIMIT 1'
        self.cursor.execute(query, values)
        return self.cursor.fetchone()
