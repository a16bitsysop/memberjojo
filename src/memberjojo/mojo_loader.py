#!/usr/bin/env python3

from csv import DictReader
from pathlib import Path
import re


def import_csv_into_encrypted_db(csv_path: Path):
    """Import CSV into an encrypted SQLCipher database using sqlcipher3."""
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    count_before = self.count()

    # Drop existing table
    self.cursor.execute(f'DROP TABLE IF EXISTS "{self.table_name}";')

    # Create table
    create_sql = _create_table_sql_from_csv(csv_path, self.table_name)
    self.cursor.execute(create_sql)

    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = DictReader(f)
        cols = reader.fieldnames

        # Normalise mappings
        norm_map = {c: _normalize(c) for c in cols}

        # Build INSERT
        colnames = ",".join(f'"{norm_map[c]}"' for c in cols)
        placeholders = ",".join("?" for _ in cols)

        insert_sql = (
            f'INSERT INTO "{self.table_name}" ({colnames}) VALUES ({placeholders})'
        )

        # Insert rows
        for row in reader:
            self.cursor.execute(insert_sql, [row[c] for c in cols])

    self.conn.commit()
    print(f"Inserted {self.count() - count_before} new rows.")


def _get_column_type_map():
    """Return a mapping of column names to their SQL types."""
    return {
        # Integer columns
        "member_number": "INTEGER",
        "membermojo_id": "INTEGER",
        # Real/Float columns
        "cost": "REAL",
        "paid": "REAL",
        # All other columns default to TEXT
    }


def _normalize(name: str) -> str:
    """Normalize a column name: lowercase, remove symbols, convert to snake case."""
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")


def _create_table_from_columns(table_name: str, columns: list[str]) -> str:
    """Generate CREATE TABLE SQL with proper types for given columns."""
    type_map = _get_column_type_map()

    column_defs = []
    for col in columns:
        norm_col = _normalize(col)
        col_type = type_map.get(norm_col, "TEXT")  # Default to TEXT

        column_defs.append(f'    "{norm_col}" {col_type}')

    return (
        f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n'
        + ",\n".join(column_defs)
        + "\n)"
    )


def _create_table_sql_from_csv(csv_path: Path, table_name: str) -> str:
    """
    Generate CREATE TABLE SQL dynamically from the CSV header
    using csv.DictReader. All columns are TEXT.
    """
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = DictReader(f)
        columns = reader.fieldnames

    if not columns:
        raise ValueError(f"CSV file '{csv_path}' has no header row.")

    return _create_table_from_columns(table_name, columns)
