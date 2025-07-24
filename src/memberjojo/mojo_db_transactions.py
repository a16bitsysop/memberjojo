from collections import Counter, defaultdict
from csv import DictReader
import sqlite3
from .config import CSV_ENCODING  # import encoding from config.py
from .mojo_common import MojoSkel


class Transaction(MojoSkel):
    def __init__(self, PaymentDBpath, TableName="payments"):
        super().__init__(PaymentDBpath, TableName)
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

    def _create_tables(self, Table, PrimaryCol):
        """
        Create SQLite tables if do not exist, with guessed columns and types.
        Using passed Primary Key Column or first column
        """
        col_names = list(self.columns.keys())
        if PrimaryCol is None:
            PrimaryCol = col_names[0]
        elif PrimaryCol not in self.columns:
            raise ValueError(f"Primary key column '{PrimaryCol}' not found in CSV.")

        cols_def = ", ".join(
            f'"{col}" {self.columns[col]}{" PRIMARY KEY" if col == PrimaryCol else ""}'
            for col in self.columns
        )
        self.cursor.execute(f'CREATE TABLE IF NOT EXISTS "{Table}" ({cols_def})')
        self.conn.commit()

    def import_csv(self, CSVpath, PKcolumn=None, SampleSize=100):
        """
        Memory-efficient CSV import with type inference, accurate row count,
        and failed row logging.
        """
        try:
            # First pass: infer schema from sample
            with CSVpath.open(newline="", encoding=CSV_ENCODING) as csvfile:
                reader = DictReader(csvfile)
                sample_rows = [row for _, row in zip(range(SampleSize), reader)]
                if not sample_rows:
                    raise ValueError("CSV file is empty.")
                self._infer_columns_from_rows(sample_rows)
        except FileNotFoundError:
            print(f"CSV file not found: {CSVpath}")
            return

        # Create table
        self._create_tables(self.table_name, PKcolumn)

        # Count rows before import
        count_before = self.count()

        # Second pass: insert all rows
        failed_rows = []
        with CSVpath.open(newline="", encoding=CSV_ENCODING) as csvfile:
            reader = DictReader(csvfile)
            column_list = ", ".join(f'"{col}"' for col in self.columns)
            placeholders = ", ".join(["?"] * len(self.columns))
            insert_stmt = f'INSERT OR IGNORE INTO "{self.table_name}" ({column_list}) VALUES ({placeholders})'

            for row in reader:
                try:
                    values = tuple(
                        (
                            float(row.get(col, ""))
                            if self.columns[col] == "REAL"
                            and row.get(col, "").strip() != ""
                            else (
                                int(row.get(col, ""))
                                if self.columns[col] == "INTEGER"
                                and row.get(col, "").strip() != ""
                                else (
                                    None
                                    if row.get(col) is None
                                    or row.get(col, "").strip() == ""
                                    else row.get(col)
                                )
                            )
                        )
                        for col in self.columns
                    )
                    self.cursor.execute(insert_stmt, values)

                except Exception as e:
                    failed_rows.append((row.copy(), str(e)))

            self.conn.commit()

        # Count new inserts
        inserted_count = self.count() - count_before
        print(f"Inserted {inserted_count} new rows into '{self.table_name}'.")

        if failed_rows:
            print(f"{len(failed_rows)} rows failed to insert:")
            for row_data, error in failed_rows[:5]:
                print(f"Failed: {error} | Data: {row_data}")
            if len(failed_rows) > 5:
                print(f"... and {len(failed_rows) - 5} more")
            raise ValueError(f"Failed to import: {CSVpath}")

    def get_row(self, EntryName, EntryValue):
        """
        Return row matching EntryName=EntryValue.
        """
        if not EntryValue:
            return None
        query = (
            f'SELECT * FROM "{self.table_name}" WHERE LOWER("{EntryName}") = LOWER(?)'
        )
        self.cursor.execute(query, (EntryValue,))
        row = self.cursor.fetchone()
        return dict(row) if row else None

    def get_row_multi(self, MatchDict):
        """
        Return the first row matching multiple column = value pairs (case-insensitive).
        Accepts a dictionary: {column: value}
        """
        if not MatchDict:
            return None

        keys = list(MatchDict.keys())
        values = list(MatchDict.values())

        conditions = [f'LOWER("{key}") = LOWER(?)' for key in keys]
        where_clause = " AND ".join(conditions)

        query = f"SELECT * FROM {self.table_name} WHERE {where_clause}"
        self.cursor.execute(query, values)
        row = self.cursor.fetchone()

        return dict(row) if row else None
