#!/usr/bin/env python3
"""
Helper module for importing a CSV into a SQLite database
"""

from collections import defaultdict, Counter
from csv import DictReader
from dataclasses import dataclass
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any, IO, Tuple

import re
import sqlite3 as sqlite3_builtin

import requests


@dataclass(frozen=True)
class DiffRow:
    """
    Represents a single diff result

    - diff_type: 'added' | 'deleted' | 'changed'
    - preview: tuple of values, with preview[0] == key
    """

    diff_type: str
    preview: Tuple[Any, ...]


# -----------------------
# Normalization & Type Guessing
# -----------------------


def normalize(name: str) -> str:
    """
    Normalize a column name: lowercase, remove symbols, convert to snake case

    :param name: Raw name to normalize

    :return: Normalized lowercase string in snake case with no symbols
    """
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")


def parse_date(value: Any) -> datetime | None:
    """
    Try to parse a date string into a datetime object using various formats.
    """
    if not value or not isinstance(value, str):
        return None
    val_str = value.strip()
    if not val_str:
        return None

    # Try ISO format first
    try:
        return datetime.fromisoformat(val_str)
    except ValueError:
        pass

    # Try other common formats
    for fmt in (
        "%d/%m/%Y",
        "%d.%m.%Y",
        "%d-%m-%Y",
        "%d %m %Y",
        "%d/%m/%y",
        "%d.%m.%y",
        "%d-%m-%y",
        "%d %m %y",
    ):
        try:
            return datetime.strptime(val_str, fmt)
        except ValueError:
            pass
    return None


def _guess_type(value: any) -> str:
    """
    Guess SQLite data type of a CSV value: 'INTEGER', 'REAL', 'DATE', or 'TEXT'

    :param value: entry from sqlite database to guess the type of

    :return: string of the type, TEXT, INTEGER, REAL, DATE, or EMPTY
    """
    if value is None:
        return "EMPTY"
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return "EMPTY"

        if parse_date(value):
            return "DATE"

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
    Infer column types from CSV rows
    Returns mapping: normalized column name -> SQLite type

    :param rows: list of rows to use for inference

    :return: dict of name, type for columns
    """
    type_counters = defaultdict(Counter)

    for row in rows:
        for key, value in row.items():
            norm_key = normalize(key)
            type_counters[norm_key][_guess_type(value)] += 1

    inferred_cols = {}
    for col, counter in type_counters.items():
        # If any value is TEXT, the whole column is TEXT
        if counter["TEXT"] > 0:
            inferred_cols[col] = "TEXT"
        elif counter["REAL"] > 0:
            inferred_cols[col] = "REAL"
        elif counter["DATE"] > 0:
            inferred_cols[col] = "DATE"
        elif counter["INTEGER"] > 0:
            inferred_cols[col] = "INTEGER"
        else:
            # All values are EMPTY or there were no rows
            inferred_cols[col] = "TEXT"
    return inferred_cols


# -----------------------
# Table Creation
# -----------------------


def _create_table_from_columns(table_name: str, columns: dict[str, str]) -> str:
    """
    Generate CREATE TABLE SQL from column type mapping
    Adds an auto-incrementing rowid as the primary key

    :param table_name: Table to use when creating columns
    :param columns: dict of columns to create

    :return: SQL commands to create the table
    """
    col_defs = ["rowid INTEGER PRIMARY KEY AUTOINCREMENT"]

    for col, col_type in columns.items():
        col_defs.append(f'"{col}" {col_type}')

    return (
        f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n' + ",\n".join(col_defs) + "\n)"
    )


def table_exists(cursor, table_name: str) -> bool:
    """
    Return True or False if a table exists

    :param cursor: SQLite cursor of db to find table in
    :param table_name: name of the table to check existance of

    :return: bool of existence
    """
    cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;",
        (table_name,),
    )
    return cursor.fetchone() is not None


# -----------------------
# CSV Import
# -----------------------


def _process_row(row: dict, cols: list, norm_map: dict, inferred_cols: dict) -> list:
    """
    Process a CSV row, converting dates and handling empty values.
    """
    values = []
    for c in cols:
        val = row[c]
        if val == "" or val is None:
            values.append(None)
        elif inferred_cols.get(norm_map[c]) == "DATE":
            dt = parse_date(val)
            if dt:
                values.append(dt.strftime("%Y-%m-%d"))
            else:
                values.append(val)
        else:
            values.append(val)
    return values


def import_data(conn, table_name: str, reader: list[dict], merge: bool = False):
    """
    Import data in the list of dicts into the SQLite3 database at conn

    :param conn: SQLite database connection to use
    :param table_name: Name of the table to import into
    :param reader: A list of dict objects to import from
    :param merge: (optional) If True, merge into existing table. Defaults to False.
    """

    cursor = conn.cursor()
    inferred_cols = infer_columns_from_rows(reader)
    if not merge or not table_exists(cursor, table_name):
        # Drop existing table
        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}";')
        # Create table
        create_sql = _create_table_from_columns(table_name, inferred_cols)
        cursor.execute(create_sql)

    # Insert rows
    cols = list(reader[0].keys())
    norm_map = {c: normalize(c) for c in cols}
    colnames = ",".join(f'"{norm_map[c]}"' for c in cols)
    placeholders = ",".join("?" for _ in cols)
    insert_sql = f'INSERT INTO "{table_name}" ({colnames}) VALUES ({placeholders})'

    for row in reader:
        values = _process_row(row, cols, norm_map, inferred_cols)
        cursor.execute(insert_sql, values)

    cursor.close()
    conn.commit()


def import_csv_helper(conn, table_name: str, csv_path: Path, merge: bool = False):
    """
    Import CSV into database using given cursor
    Column types inferred automatically

    :param conn: SQLite database connection to use
    :param table_name: Table to import the CSV into
    :param csv_path: Path like path of the CSV file to import
    :param merge: (optional) If True, merge into existing table. Defaults to False.
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    # Read CSV rows
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = list(DictReader(f))
        if not reader:
            raise ValueError("CSV file is empty.")
        import_data(conn, table_name, reader, merge=merge)


# -----------------------
# diff generation
# -----------------------


def _diffrow_from_sql_row(row: sqlite3_builtin.Row) -> DiffRow:
    """
    Convert a sqlite3.Row from generate_sql_diff into DiffRow
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
    conn,
    *,
    new_table: str,
    old_table: str,
) -> list[DiffRow]:
    """
    Copy old and new tables from SQLCipher into a single
    in-memory sqlite3 database and diff them there.

    :param conn: sqlite connection to the db
    :param new_table: name of the new table for comparison
    :param old_table: name of the old table for comparison

    :return: a list of DiffRow entries of the changed rows
    """

    plain = sqlite3_builtin.connect(":memory:")
    plain.row_factory = sqlite3_builtin.Row

    try:
        for table in (old_table, new_table):
            # Clone schema using SQLite itself
            schema_sql = conn.execute(
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
            rows = conn.execute(f"SELECT * FROM {table}")
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
    Generate a diff between two tables using standard SQLite features

    - Uses rowid as the primary key for joining
    - Returned row shape:
            (diff_type, preview_col1, preview_col2, preview_col3, ...)

    :param conn: sqlite connection to the db, using python builtin sqlite
    :param new_table: name of the new table to use for comparison
    :param old_table: name of the old table to use for comparison

    :return: list of sqlite rows that are changed
    """

    # Introspect schemas (order-preserving)
    cols_new = [
        row[1] for row in conn.execute(f"PRAGMA table_info({new_table})").fetchall()
    ]
    cols_old = [
        row[1] for row in conn.execute(f"PRAGMA table_info({old_table})").fetchall()
    ]

    if not cols_new:
        raise RuntimeError(f"Table {new_table!r} has no columns")
    if not cols_old:
        raise RuntimeError(f"Table {old_table!r} has no columns")

    # Use intersection for comparison and preview, preserving new_table order
    common_cols = [c for c in cols_new if c in cols_old and c != "rowid"]

    if not common_cols:
        raise RuntimeError(f"No common columns between {new_table} and {old_table}")

    # First column of common columns is key, others are for comparison
    key = common_cols[0]
    non_key_cols = common_cols[1:]

    # Preview columns (key first, then others for readability)
    preview_cols = [key] + non_key_cols[:5]

    new_preview = ", ".join(f"n.{c}" for c in preview_cols)
    old_preview = ", ".join(f"o.{c}" for c in preview_cols)

    # Row-value comparison (NULL-safe)
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


def download_csv_helper(
    conn, table_name: str, url: str, session: requests.Session, merge: bool = False
) -> IO[str]:
    """
    Download url into a StringIO file object using streaming
    and import into database

    :param conn: The SQLite3 database connection to use
    :param table_name: The name of the table to import it into
    :param url: URL of the csv to download
    :param session: A requests session to use for the download
    :param merge: (optional) If True, merge into existing table. Defaults to False.
    """

    print(f"☁️ Downloading from: {url}")

    # Enable streaming
    with session.get(url, stream=True) as resp:
        resp.raise_for_status()

        # Initialize the string buffer
        string_buffer = StringIO()

        # Stream decoded text
        # decode_unicode=True uses the encoding from the response headers
        for chunk in resp.iter_content(chunk_size=8192, decode_unicode=True):
            if chunk:
                string_buffer.write(chunk)

        # Reset pointer to the beginning for DictReader
        string_buffer.seek(0)

        reader = list(DictReader(string_buffer))
        if reader:
            print(f"✅ Downloaded with encoding {resp.encoding}.")
            import_data(conn, table_name, reader, merge=merge)
            return True

    print("   ⚠️ CSV is empty ⚠️")
    return False
