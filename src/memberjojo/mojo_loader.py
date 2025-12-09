#!/usr/bin/env python3
"""
Helper module for importing a CSV into a SQLite database.
"""

from csv import DictReader
from pathlib import Path
import re
from collections import defaultdict, Counter

from sqlcipher3 import dbapi2 as sqlite3

# -----------------------
# Normalization & Type Guessing
# -----------------------


def _normalize(name: str) -> str:
    """
    Normalize a column name: lowercase, remove symbols, convert to snake case.

    :param name: Raw name to normalize.

    :return: Normalized lowercase string in snake case with no symbols.
    """
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")


def _guess_type(value: any) -> str:
    """
    Guess SQLite data type of a CSV value: 'INTEGER', 'REAL', or 'TEXT'.
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


def infer_columns_from_rows(rows: list[dict]) -> dict[str, str]:
    """
    Infer column types from CSV rows.
    Returns mapping: normalized column name -> SQLite type.
    """
    type_counters = defaultdict(Counter)

    for row in rows:
        for key, value in row.items():
            norm_key = _normalize(key)
            type_counters[norm_key][_guess_type(value)] += 1

    inferred_cols = {}
    for col, counter in type_counters.items():
        if counter["TEXT"] == 0:
            if counter["REAL"] > 0:
                inferred_cols[col] = "REAL"
            else:
                inferred_cols[col] = "INTEGER"
        else:
            inferred_cols[col] = "TEXT"
    return inferred_cols


# -----------------------
# Table Creation
# -----------------------


def _create_table_from_columns(table_name: str, columns: dict[str, str]) -> str:
    """
    Generate CREATE TABLE SQL from column type mapping.

    :param table_name: Table to use when creating columns.
    :param columns: dict of columns to create.

    :return: SQL commands to create the table.
    """
    column_defs = [f'"{col}" {col_type}' for col, col_type in columns.items()]
    return (
        f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n'
        + ",\n".join(column_defs)
        + "\n)"
    )


# -----------------------
# CSV Import
# -----------------------


def import_csv_helper(conn, table_name: str, csv_path: Path):
    """
    Import CSV into database using given cursor.
    Column types inferred automatically.

    :param conn: SQLite database connection to use.
    :param table_name: Table to import the CSV into.
    :param csv_path: Path like path of the CSV file to import.
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    # Read CSV rows
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = list(DictReader(f))
        if not reader:
            raise ValueError("CSV file is empty.")
        inferred_cols = infer_columns_from_rows(reader)

        cursor = conn.cursor()
        # Drop existing table
        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}";')

        # Create table
        create_sql = _create_table_from_columns(table_name, inferred_cols)
        cursor.execute(create_sql)

        # Insert rows
        cols = list(reader[0].keys())
        norm_map = {c: _normalize(c) for c in cols}
        colnames = ",".join(f'"{norm_map[c]}"' for c in cols)
        placeholders = ",".join("?" for _ in cols)
        insert_sql = f'INSERT INTO "{table_name}" ({colnames}) VALUES ({placeholders})'

        for row in reader:
            values = [row[c] if row[c] != "" else None for c in cols]
            cursor.execute(insert_sql, values)

    cursor.close()
    conn.commit()


def generate_sql_diff(conn, new_table: str, old_table: str) -> list[sqlite3.Row]:
    """
    Returns a single SQL statement that produces a diff between
    <new_table> and <old_table> using only SQLite features available in SQLCipher.

    :param conn: SQLite database connection
    :param new_table: the newly imported table name
    :param old_table: the old table table to compare with

    :return: SQL diff rows
    """

    # Get columns from PRAGMA
    cur = conn.execute(f"PRAGMA table_info({new_table})")
    cols = [row[1] for row in cur.fetchall()]  # column name is index 1

    # Build column comparison: (n.col IS NOT o.col)
    comparisons = " OR ".join([f"(n.{c} IS NOT o.{c})" for c in cols])

    sql = f"""
WITH
    new_ids AS (SELECT rowid FROM {new_table}),
    old_ids AS (SELECT rowid FROM {old_table}),

    added AS (
        SELECT n.rowid AS rowid, 'added' AS diff_type
        FROM new_ids n
        LEFT JOIN old_ids o USING(rowid)
        WHERE o.rowid IS NULL
    ),

    deleted AS (
        SELECT o.rowid AS rowid, 'deleted' AS diff_type
        FROM old_ids o
        LEFT JOIN new_ids n USING(rowid)
        WHERE n.rowid IS NULL
    ),

    changed AS (
        SELECT n.rowid AS rowid, 'changed' AS diff_type
        FROM {new_table} n
        JOIN {old_table} o USING(rowid)
        WHERE {comparisons}
    )

SELECT * FROM added
UNION ALL SELECT * FROM deleted
UNION ALL SELECT * FROM changed
ORDER BY rowid;
"""
    return list(conn.execute(sql))
