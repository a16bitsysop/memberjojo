"""
MojoSkel base class

This module provides a common base class (`MojoSkel`) for other `memberjojo` modules.
It includes helper methods for working with SQLite databases.
"""

import subprocess
import sqlite3
from pathlib import Path
from csv import DictReader
from collections import defaultdict, Counter
from decimal import Decimal
from sqlite3 import IntegrityError, OperationalError, DatabaseError

from .config import CSV_ENCODING  # import encoding from config.py


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
        if db_key is None:
            self.conn = sqlite3.connect(db_path)
            print("Unencrypted database loaded.")
        else:
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

            print("Decrypted database loaded in-memory securely.")

        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.table_name = table_name
        self.columns = {}
        self.db_path = db_path

    def _guess_type(self, value: any) -> str:
        """
        Guess the SQLite data type of a CSV field value.

        :param value: The value from a CSV field.

        :return: One of 'INTEGER', 'REAL', or 'TEXT'.
        """
        if value is None:
            return "TEXT"

        if isinstance(value, str):
            value = value.strip()
            if value == "":
                return "TEXT"

        try:
            int(value)
            return "INTEGER"
        except (ValueError, TypeError):
            try:
                float(value)
                return "REAL"
            except (ValueError, TypeError):
                return "TEXT"

    def _infer_columns_from_rows(self, rows: list[dict]):
        """
        Infer SQLite column types based on sample CSV data.

        :param rows: Sample rows from CSV to analyze.
        """
        type_counters = defaultdict(Counter)

        for row in rows:
            for key, value in row.items():
                guessed_type = self._guess_type(value)
                type_counters[key][guessed_type] += 1

        self.columns = {}
        for col, counter in type_counters.items():
            if counter["TEXT"] == 0:
                if counter["REAL"] > 0:
                    self.columns[col] = "REAL"
                else:
                    self.columns[col] = "INTEGER"
            else:
                self.columns[col] = "TEXT"

        print("Inferred columns:", self.columns)

    def _create_full_tables(self, table: str, primary_col: str):
        """
        Create the table if it doesn't exist, using inferred schema.

        :param table: Table name.
        :param primary_col: Column to use as primary key, or None for default.
        """
        col_names = list(self.columns.keys())
        if primary_col is None:
            primary_col = col_names[0]
        elif primary_col not in self.columns:
            raise ValueError(f"Primary key column '{primary_col}' not found in CSV.")

        cols_def = ", ".join(
            f'"{col}" {col_type}{" PRIMARY KEY" if col == primary_col else ""}'
            for col, col_type in self.columns.items()
        )

        self.cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({cols_def})')
        self.conn.commit()

    def _parse_row_values(self, row: dict, column_types: dict) -> tuple:
        """
        Convert CSV row string values to types suitable for SQLite.

        :param row: A dictionary from the CSV row.
        :param column_types: Mapping of column names to SQLite types.

        :return: Parsed values.
        """
        values = []
        for col, col_type in column_types.items():
            val = row.get(col, "")
            if val is None or val.strip() == "":
                values.append(None)
            elif col_type == "REAL":
                values.append(float(val))
            elif col_type == "INTEGER":
                values.append(int(val))
            else:
                values.append(val)
        return tuple(values)

    def import_csv(self, csv_path: Path, pk_column: str = None, sample_size: int = 100):
        """
        Import a csv file into SQLite.

        Infers column types and logs failed rows. Creates the table if needed.

        :param csv_path: Path to the CSV file.
        :param pk_column: (optional) Primary key column name. Defaults to the first column.
        :param sample_size: (optional) Number of rows to sample for type inference. Defaults to 100.

        :raises ValueError: If the CSV is empty or contains failed insertions.
        """
        try:
            # First pass: infer schema from sample
            with csv_path.open(newline="", encoding=CSV_ENCODING) as csvfile:
                reader = DictReader(csvfile)
                sample_rows = [row for _, row in zip(range(sample_size), reader)]
                if not sample_rows:
                    raise ValueError("CSV file is empty.")

                # Only infer columns if not already set (i.e., first import)
                if not self.columns:
                    self._infer_columns_from_rows(sample_rows)

        except FileNotFoundError:
            print(f"CSV file not found: {csv_path}")
            return

        # Create table
        self._create_full_tables(self.table_name, pk_column)

        # Count rows before import
        count_before = self.count()

        # Second pass: insert all rows
        failed_rows = []
        with csv_path.open(newline="", encoding=CSV_ENCODING) as csvfile:
            reader = DictReader(csvfile)
            column_list = ", ".join(f'"{col}"' for col in self.columns)
            placeholders = ", ".join(["?"] * len(self.columns))
            insert_stmt = f'INSERT OR IGNORE INTO "{self.table_name}" ({column_list}) \
                VALUES ({placeholders})'

            for row in reader:
                try:
                    self.cursor.execute(
                        insert_stmt, self._parse_row_values(row, self.columns)
                    )

                except (
                    IntegrityError,
                    OperationalError,
                    DatabaseError,
                    ValueError,
                ) as e:
                    failed_rows.append((row.copy(), str(e)))

            self.conn.commit()

        print(
            f"Inserted {self.count() - count_before} new rows into '{self.table_name}'."
        )

        if failed_rows:
            print(f"{len(failed_rows)} rows failed to insert:")
            for row, error in failed_rows[:5]:
                print(f"Failed: {error} | Data: {row}")
            if len(failed_rows) > 5:
                print(f"... and {len(failed_rows) - 5} more")
            raise ValueError(f"Failed to import: {csv_path}")

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
        return self._run_sqlcipher(lines)

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
        """
        self.cursor.execute(f'SELECT COUNT(*) FROM "{self.table_name}"')
        result = self.cursor.fetchone()
        return result[0] if result else 0

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
