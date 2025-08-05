"""
Module to import and interact with Membermojo completed_payments.csv data in SQLite.

Provides automatic column type inference, robust CSV importing, and
helper methods for querying the database.
"""

from collections import Counter, defaultdict
from csv import DictReader
from pathlib import Path
from sqlite3 import IntegrityError, OperationalError, DatabaseError

from .config import CSV_ENCODING  # import encoding from config.py
from .mojo_common import MojoSkel


class Transaction(MojoSkel):
    """
    Handles importing and querying completed payment data.

    Extends:
        MojoSkel: Base class with transaction database operations.

    :param payment_db_path: Path to the SQLite database.
    :param table_name: (optional) Name of the table. Defaults to "payments".
    """

    def __init__(self, payment_db_path: str, table_name: str = "payments"):
        """
        Initialize the Transaction object.
        """
        super().__init__(payment_db_path, table_name)
        self.columns = {}

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

    def _create_tables(self, table: str, primary_col: str):
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
        Import a completed_payments.csv file into SQLite.

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
        self._create_tables(self.table_name, pk_column)

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
                values.append(val)

        query = f'SELECT * FROM "{self.table_name}" WHERE {" AND ".join(conditions)} LIMIT 1'
        self.cursor.execute(query, values)
        return self.cursor.fetchone()
