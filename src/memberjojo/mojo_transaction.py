"""
Module to import and interact with Membermojo completed_payments.csv data in SQLite.

Provides automatic column type inference, robust CSV importing, and
helper methods for querying the database.
"""

from collections import defaultdict, Counter
from csv import DictReader
from pathlib import Path
from sqlite3 import IntegrityError, OperationalError, DatabaseError

from .mojo_common import MojoSkel
from .config import CSV_ENCODING  # import encoding from config.py


class Transaction(MojoSkel):
    """
    Handles importing and querying completed payment data.

    Extends:
        MojoSkel: Base class with transaction database operations.

    :param payment_db_path: Path to the SQLite database.
    :param table_name: (optional) Name of the table. Defaults to "payments".
    :param db_key: (optional) key to unlock the encrypted sqlite database, unencrypted if unset.
    """

    def __init__(
        self,
        payment_db_path: str,
        db_key: str,
        table_name: str = "payments",
    ):
        """
        Initialize the Transaction object.
        """
        super().__init__(payment_db_path, table_name, db_key)

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
        Column names are normalized using _normalize().
        """
        type_counters = defaultdict(Counter)

        # Accumulate type guesses keyed by NORMALIZED names
        for row in rows:
            for key, value in row.items():
                norm_key = self._normalize(key)
                guessed_type = self._guess_type(value)
                type_counters[norm_key][guessed_type] += 1

        # Decide final type for each normalized column
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
        Column names are normalized for SQL-safe identifiers.
        """

        # Original CSV column names:
        original_cols = list(self.columns.keys())

        # Map original → normalized
        norm_map = {c: self._normalize(c) for c in original_cols}

        # Normalize primary key name
        if primary_col is None:
            primary_col = original_cols[0]
        elif primary_col not in self.columns:
            raise ValueError(f"Primary key column '{primary_col}' not found in CSV.")

        norm_pk = norm_map[primary_col]

        # Build CREATE TABLE SQL using normalized names
        cols_def = ", ".join(
            f'"{norm_map[col]}" {self.columns[col]}'
            + (" PRIMARY KEY" if norm_map[col] == norm_pk else "")
            for col in original_cols
        )

        sql = f'CREATE TABLE IF NOT EXISTS "{table}" ({cols_def})'
        self.cursor.execute(sql)
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
