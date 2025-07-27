"""
Module to create an sqlite databse from a downloaded membermojo completed_payments.csv
Provides functions to interacte with it as well
"""

from collections import Counter, defaultdict
from csv import DictReader
from sqlite3 import IntegrityError, OperationalError, DatabaseError

from .config import CSV_ENCODING  # import encoding from config.py
from .mojo_common import MojoSkel


class Transaction(MojoSkel):
    """
    The Transaction class is used to contain these funcitons
    """

    def __init__(self, payment_db_path, table_name="payments"):
        super().__init__(payment_db_path, table_name)
        self.columns = {}

    def _guess_type(self, value):
        """
        Determine SQLite type for a value: INTEGER, REAL, or TEXT.
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

    def _infer_columns_from_rows(self, rows):
        """
        Create dict of column headers and types from csv rows.
        Promote to REAL if any float is present, but only if all values are numeric.
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

    def _create_tables(self, table, primary_col):
        """
        Create SQLite tables if do not exist, with guessed columns and types.
        Using passed Primary Key Column or first column
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
        Parse a row of CSV data according to expected column types.

        Parameters:
            row (dict): A dictionary of column names to string values from CSV.
            column_types (dict): A dictionary of column names to expected SQLite types
                                ('REAL', 'INTEGER', or 'TEXT').

        Returns:
            tuple: Parsed values suitable for SQLite insertion.
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

    def import_csv(self, csv_path, pk_column=None, sample_size=100):
        """
        Memory-efficient CSV import with type inference, accurate row count,
        and failed row logging.
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

    def get_row(self, entry_name, entry_value):
        """
        Return row matching EntryName=EntryValue.
        """
        if not entry_value:
            return None
        query = (
            f'SELECT * FROM "{self.table_name}" WHERE LOWER("{entry_name}") = LOWER(?)'
        )
        self.cursor.execute(query, (entry_value,))
        row = self.cursor.fetchone()
        return dict(row) if row else None

    def get_row_multi(self, match_dict):
        """
        Return the first row matching multiple column = value pairs (case-insensitive).
        Accepts a dictionary: {column: value}
        """
        if not match_dict:
            return None

        keys = list(match_dict.keys())
        values = list(match_dict.values())

        conditions = [f'LOWER("{key}") = LOWER(?)' for key in keys]
        where_clause = " AND ".join(conditions)

        query = f"SELECT * FROM {self.table_name} WHERE {where_clause}"
        self.cursor.execute(query, values)
        row = self.cursor.fetchone()

        return dict(row) if row else None
