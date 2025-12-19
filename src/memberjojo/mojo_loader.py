#!/usr/bin/env python3
"""
Helper module for importing a CSV into a SQLite database.
"""

from collections import defaultdict, Counter
from csv import DictReader
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Tuple

import re
import sqlite3 as sqlite3_builtin


@dataclass(frozen=True)
class DiffRow:
    """
    Represents a single diff result.

    - diff_type: 'added' | 'deleted' | 'changed'
    - preview: tuple of values, with preview[0] == key
    """

    diff_type: str
    preview: Tuple[Any, ...]


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

    :param value: entry from sqlite database to guess the type of

    :return: string of the type, TEXT, INTEGER, REAL
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

    :param rows: list of rows to use for inference

    :return: dict of name, type for columns
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


# -----------------------
# diff generation
# -----------------------


def _diffrow_from_sql_row(row: sqlite3_builtin.Row) -> DiffRow:
    """
    Convert a sqlite3.Row from generate_sql_diff into DiffRow.
    Row shape:
        (diff_type, col1, col2, col3, ...)

    :param row: Row from sqlite3 database to create a dataclass entry from

    :return: A dataclass of the row
    """
    return DiffRow(
        diff_type=row[0],
        preview=tuple(row[1:]),
    )


def diff_cipher_tables(
    cipher_conn,
    *,
    new_table: str,
    old_table: str,
) -> list[DiffRow]:
    """
    Copy old and new tables from SQLCipher into a single
    in-memory sqlite3 database and diff them there.

    :param cipher_conn: sqlite connection to the encrypted db
    :param new_table: name of the new table for comparison
    :param old_table: name of the old table for comparison

    :return: a list of DiffRow entries of the changed rows
    """

    plain = sqlite3_builtin.connect(":memory:")
    plain.row_factory = sqlite3_builtin.Row

    try:
        for table in (old_table, new_table):
            # 1. Clone schema using SQLite itself
            schema_sql = cipher_conn.execute(
                """
                SELECT sql
                FROM sqlite_master
                WHERE type='table' AND name=?
                """,
                (table,),
            ).fetchone()

            if schema_sql is None:
                raise RuntimeError(f"Table {table!r} not found in cipher DB")

            plain.execute(schema_sql[0])

            # 2. Copy data
            rows = cipher_conn.execute(f"SELECT * FROM {table}")
            cols = [d[0] for d in rows.description]

            col_list = ", ".join(cols)
            placeholders = ", ".join("?" for _ in cols)

            plain.executemany(
                f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})",
                rows.fetchall(),
            )

        # 3. Run sqlite-only diff
        rows = _generate_sql_diff(
            plain,
            new_table=new_table,
            old_table=old_table,
        )

        return [_diffrow_from_sql_row(r) for r in rows]

    finally:
        plain.close()


def _generate_sql_diff(
    conn: sqlite3_builtin.Connection,
    *,
    new_table: str,
    old_table: str,
) -> list[sqlite3_builtin.Row]:
    """
    Generate a diff between two tables using standard SQLite features.

    - The FIRST column is the primary key.
    - Returned row shape:
            (diff_type, preview_col1, preview_col2, preview_col3, ...)

    :param conn: sqlite connection to the db, using python builtin sqlite
    :param new_table: name of the new table to use for comparison
    :param old_table: name of the old table to use for comparison

    :return: list of sqlite rows that are changed
    """

    # 1. Introspect schema (order-preserving)
    cols_info = conn.execute(f"PRAGMA table_info({new_table})").fetchall()

    if not cols_info:
        raise RuntimeError(f"Table {new_table!r} has no columns")

    cols = [row[1] for row in cols_info]

    key = cols[0]
    non_key_cols = cols[1:]

    # 2. Preview columns (key first, limit for readability)
    preview_cols = [key] + non_key_cols[:5]

    new_preview = ", ".join(f"n.{c}" for c in preview_cols)
    old_preview = ", ".join(f"o.{c}" for c in preview_cols)

    # 3. Row-value comparison (NULL-safe)
    if non_key_cols:
        changed_predicate = (
            f"({', '.join(f'n.{c}' for c in non_key_cols)}) "
            f"IS NOT "
            f"({', '.join(f'o.{c}' for c in non_key_cols)})"
        )
    else:
        # Key-only table
        changed_predicate = "0"

    sql = f"""
        WITH
            added AS (
                SELECT 'added' AS diff_type, {new_preview}
                FROM {new_table} n
                LEFT JOIN {old_table} o USING ({key})
                WHERE o.{key} IS NULL
            ),
            deleted AS (
                SELECT 'deleted' AS diff_type, {old_preview}
                FROM {old_table} o
                LEFT JOIN {new_table} n USING ({key})
                WHERE n.{key} IS NULL
            ),
            changed AS (
                SELECT 'changed' AS diff_type, {new_preview}
                FROM {new_table} n
                JOIN {old_table} o USING ({key})
                WHERE {changed_predicate}
            )
        SELECT * FROM added
        UNION ALL
        SELECT * FROM deleted
        UNION ALL
        SELECT * FROM changed
        ORDER BY {key};
        """

    return list(conn.execute(sql))
