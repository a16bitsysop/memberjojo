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
    col_defs = []
    first = True

    for col, col_type in columns.items():
        if first:
            col_defs.append(f'"{col}" {col_type} PRIMARY KEY')
            first = False
        else:
            col_defs.append(f'"{col}" {col_type}')

    return (
        f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n' + ",\n".join(col_defs) + "\n)"
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

    # 1. Get columns (preserve the schema order)
    cur = conn.execute(f"PRAGMA table_info({new_table})")
    cols_info = cur.fetchall()
    cols = [row[1] for row in cols_info]  # column names

    if not cols:
        raise RuntimeError(f"Table {new_table!r} has no columns")

    # 2. Detect primary key column from PRAGMA (pk field at index 5)
    pk_col = next((row[1] for row in cols_info if row[5] == 1), None)
    # Fallback: use 'id' if present, else use the first column
    if pk_col is None:
        if "id" in cols:
            pk_col = "id"
        else:
            pk_col = cols[0]

    key = pk_col

    # 3. First few preview columns (excluding the key), but ensure key appears in preview
    preview_cols = [c for c in cols if c != key][:5]
    # Guarantee the primary key is included at the LEFT of the preview (not duplicated)
    # We'll select the key separately as "k" and also include it in preview for readability.
    preview_cols_for_select = [key] + [c for c in preview_cols if c != key]

    new_col_list = (
        ", ".join(f"n.{c}" for c in preview_cols_for_select)
        if preview_cols_for_select
        else ""
    )
    old_col_list = (
        ", ".join(f"o.{c}" for c in preview_cols_for_select)
        if preview_cols_for_select
        else ""
    )

    # 4. NULL-safe comparisons for all columns
    comparisons = " OR ".join(
        [f"NOT ((n.{c} = o.{c}) OR (n.{c} IS NULL AND o.{c} IS NULL))" for c in cols]
    )

    sql = f"""
    WITH
        added AS (
            SELECT n.{key} AS k, 'added' AS diff_type{', ' + new_col_list if new_col_list else ''}
            FROM {new_table} n
            LEFT JOIN {old_table} o ON n.{key} = o.{key}
            WHERE o.{key} IS NULL
        ),
        deleted AS (
            SELECT o.{key} AS k, 'deleted' AS diff_type{', ' + old_col_list if old_col_list else ''}
            FROM {old_table} o
            LEFT JOIN {new_table} n ON n.{key} = o.{key}
            WHERE n.{key} IS NULL
        ),
        changed AS (
            SELECT n.{key} AS k, 'changed' AS diff_type{', ' + new_col_list if new_col_list else ''}
            FROM {new_table} n
            JOIN {old_table} o ON n.{key} = o.{key}
            WHERE {comparisons}
        )
    SELECT * FROM added
    UNION ALL SELECT * FROM deleted
    UNION ALL SELECT * FROM changed
    ORDER BY k;
    """
    return list(conn.execute(sql))
