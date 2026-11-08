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
from typing import Any, cast

import logging
import re
import sqlite3 as sqlite3_builtin

import requests

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DiffRow:
    """
    Represents a single diff result

    - diff_type: 'added' | 'deleted' | 'changed'
    - preview: tuple of values, with preview[0] == key
    """

    diff_type: str
    preview: tuple[Any, ...]


# Columns that benefit from an index when the column is present.
# Tables are recreated on each non-merge import, so indexes are recreated
# against the canonical table name afterwards (see MojoSkel._ensure_indexes).
INDEXABLE_COLUMNS = (
    "member_number",
    "first_name",
    "last_name",
    "membership",
    "payment_id",
)


def quote_name(name: str) -> str:
    """Return an identifier safely double-quoted for interpolation in SQL."""
    return '"' + name.replace('"', '""') + '"'


# -----------------------
# Normalization & Type Guessing
# -----------------------


def _normalize(name: str) -> str:
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


def _guess_type(value: Any) -> str:
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


def infer_columns_from_rows(rows: list[dict[str, Any]]) -> dict[str, str]:
    """
    Infer column types from CSV rows
    Returns mapping: normalized column name -> SQLite type

    :param rows: list of rows to use for inference

    :return: dict of name, type for columns
    """
    type_counters: dict[str, Counter[str]] = defaultdict(Counter[str])

    for row in rows:
        for key, value in row.items():
            norm_key = _normalize(key)
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
        col_defs.append(f"{quote_name(col)} {col_type}")

    return (
        f"CREATE TABLE IF NOT EXISTS {quote_name(table_name)} (\n"
        + ",\n".join(col_defs)
        + "\n)"
    )


def table_exists(cursor: sqlite3_builtin.Cursor, table_name: str) -> bool:
    """
    Return True or False if a table exists

    :param cursor: SQLite cursor of db to find table in
    :param table_name: name of the table to check existance of

    :return: bool of existence
    """
    cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE (type='table' OR type='view') AND name=? LIMIT 1;",
        (table_name,),
    )
    return cursor.fetchone() is not None


def create_indexes(conn: sqlite3_builtin.Connection, table_name: str) -> None:
    """
    Create COLLATE NOCASE indexes on present hot lookup columns.

    Indexes are created for the canonical table name so they survive a
    staging-table rename and match the case-insensitive lookups in
    `mojo_member`.

    :param conn: SQLite database connection to use
    :param table_name: Name of the table to index
    """
    cursor = conn.cursor()
    try:
        cursor.execute(f"PRAGMA table_info({quote_name(table_name)})")
        existing = {row[1] for row in cursor.fetchall()}
        for col in INDEXABLE_COLUMNS:
            if col in existing:
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS {quote_name(table_name + '__' + col)} "
                    f"ON {quote_name(table_name)} ({quote_name(col)} COLLATE NOCASE)"
                )
    finally:
        cursor.close()
        conn.commit()


# -----------------------
# CSV Import
# -----------------------


def _process_row(
    row: dict[str, Any],
    cols: list[str],
    norm_map: dict[str, str],
    inferred_cols: dict[str, str],
) -> list[str | None]:
    """
    Process a CSV row, converting dates and handling empty values.

    :param row: A single CSV row
    :param cols: The (raw header) column names in insertion order
    :param norm_map: Mapping of raw header to normalized column name
    :param inferred_cols: Normalized column name to SQLite type mapping

    :return: The processed values, with dates normalised to YYYY-MM-DD
    """
    values: list[Any] = []
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


MERGE_KEY = "membermojo_id"


def _skip_existing_rows(
    cursor: sqlite3_builtin.Cursor,
    table_name: str,
    reader: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Drop rows whose membermojo_id is already in the table, or repeated in reader

    Rows without a membermojo_id are kept. If either the table or the CSV has no
    membermojo_id column, every row is kept.

    :param cursor: SQLite cursor of the database holding table_name
    :param table_name: Name of the table being merged into
    :param reader: A list of dict objects to filter

    :return: The rows that are not already present
    """
    key_col = next((c for c in reader[0] if _normalize(c) == MERGE_KEY), None)
    cursor.execute(f"PRAGMA table_info({quote_name(table_name)})")
    table_cols = {row[1] for row in cursor.fetchall()}
    if key_col is None or MERGE_KEY not in table_cols:
        return reader

    cursor.execute(f"SELECT {quote_name(MERGE_KEY)} FROM {quote_name(table_name)}")
    seen = {str(row[0]).strip() for row in cursor.fetchall() if row[0] is not None}

    fresh: list[dict[str, Any]] = []
    for row in reader:
        key = str(row[key_col] or "").strip()
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        fresh.append(row)

    skipped = len(reader) - len(fresh)
    if skipped:
        logger.info("Skipped %d rows already in %s", skipped, table_name)
    return fresh


def import_data(
    conn: sqlite3_builtin.Connection,
    table_name: str,
    reader: list[dict[str, Any]],
    merge: bool = False,
) -> None:
    """
    Import data in the list of dicts into the SQLite3 database at conn

    When merging into an existing table, rows whose membermojo_id is already
    present are skipped.

    :param conn: SQLite database connection to use
    :param table_name: Name of the table to import into
    :param reader: A list of dict objects to import from
    :param merge: (optional) If True, merge into existing table. Defaults to False.
    """

    cursor = conn.cursor()
    inferred_cols = infer_columns_from_rows(reader)
    if not merge or not table_exists(cursor, table_name):
        # Drop existing table
        cursor.execute(f"DROP TABLE IF EXISTS {quote_name(table_name)};")
        # Create table
        create_sql = _create_table_from_columns(table_name, inferred_cols)
        cursor.execute(create_sql)
    else:
        reader = _skip_existing_rows(cursor, table_name, reader)

    if reader:
        _insert_rows(cursor, table_name, reader, inferred_cols)

    cursor.close()
    conn.commit()


def _insert_rows(
    cursor: sqlite3_builtin.Cursor,
    table_name: str,
    reader: list[dict[str, Any]],
    inferred_cols: dict[str, str],
) -> None:
    """
    Insert rows into table_name, with values processed by _process_row

    :param cursor: SQLite cursor of the database holding table_name
    :param table_name: Name of the table to insert into
    :param reader: A non-empty list of dict objects to insert
    :param inferred_cols: Normalized column name to SQLite type mapping
    """
    cols = list(reader[0].keys())
    norm_map = {c: _normalize(c) for c in cols}
    colnames = ",".join(quote_name(norm_map[c]) for c in cols)
    placeholders = ",".join("?" for _ in cols)
    insert_sql = (
        f"INSERT INTO {quote_name(table_name)} ({colnames}) VALUES ({placeholders})"
    )

    rows = [_process_row(row, cols, norm_map, inferred_cols) for row in reader]
    cursor.executemany(insert_sql, rows)


def import_csv_helper(
    conn: sqlite3_builtin.Connection,
    table_name: str,
    csv_path: Path,
    merge: bool = False,
) -> None:
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


def diff_tables(
    conn: sqlite3_builtin.Connection,
    *,
    new_table: str,
    old_table: str,
    key: str | None = None,
) -> list[DiffRow]:
    """
    Diff the old and new tables directly on the given connection.

    Both tables already live in the same database (SQLCipher or plain
    SQLite), so the diff is computed there without materialising either
    table into memory.

    :param conn: sqlite connection (SQLCipher or plain) holding both tables
    :param new_table: name of the new table for comparison
    :param old_table: name of the old table for comparison
    :param key: (optional) column to use as the join key for the diff.
        Defaults to the first common column.

    :return: a list of DiffRow entries of the changed rows
    """
    return [
        _diffrow_from_sql_row(r)
        for r in _generate_sql_diff(
            conn, new_table=new_table, old_table=old_table, key=key
        )
    ]


def _generate_sql_diff(
    conn: sqlite3_builtin.Connection,
    *,
    new_table: str,
    old_table: str,
    key: str | None = None,
) -> list[sqlite3_builtin.Row]:
    """
    Generate a diff between two tables using standard SQLite features

    - Uses rowid as the primary key for joining
    - Returned row shape:
            (diff_type, preview_col1, preview_col2, preview_col3, ...)

    :param conn: sqlite connection to the db, SQLCipher or plain
    :param new_table: name of the new table to use for comparison
    :param old_table: name of the old table to use for comparison
    :param key: (optional) column to use as the join key for the diff.
        Defaults to the first common column (excluding rowid).

    :return: list of sqlite rows that are changed
    """
    # pylint: disable=too-many-locals

    new_q = quote_name(new_table)
    old_q = quote_name(old_table)

    # Introspect schemas (order-preserving)
    cols_new = [
        row[1] for row in conn.execute(f"PRAGMA table_info({new_q})").fetchall()
    ]
    cols_old = [
        row[1] for row in conn.execute(f"PRAGMA table_info({old_q})").fetchall()
    ]

    if not cols_new:
        raise RuntimeError(f"Table {new_table!r} has no columns")
    if not cols_old:
        raise RuntimeError(f"Table {old_table!r} has no columns")

    # Use intersection for comparison and preview, preserving new_table order
    common_cols = [c for c in cols_new if c in cols_old and c != "rowid"]

    if not common_cols:
        raise RuntimeError(f"No common columns between {new_table} and {old_table}")

    # Explicit key if provided, otherwise default to the first common column
    if key is not None:
        if key not in common_cols:
            raise RuntimeError(
                f"Diff key {key!r} is not a common column between "
                f"{new_table} and {old_table}"
            )
    else:
        key = common_cols[0]

    key_q = quote_name(key)
    non_key_cols = [c for c in common_cols if c != key]

    # Preview columns (key first, then others for readability)
    preview_cols = [key] + non_key_cols[:5]

    new_preview = ", ".join(f"n.{quote_name(c)}" for c in preview_cols)
    old_preview = ", ".join(f"o.{quote_name(c)}" for c in preview_cols)

    # Row-value comparison (NULL-safe)
    if non_key_cols:
        changed_predicate = (
            f"({', '.join(f'n.{quote_name(c)}' for c in non_key_cols)}) "
            f"IS NOT "
            f"({', '.join(f'o.{quote_name(c)}' for c in non_key_cols)})"
        )
    else:
        # Key-only table
        changed_predicate = "0"

    sql = f"""
        WITH
            added AS (
                SELECT 'added' AS diff_type, {new_preview}
                FROM {new_q} n
                LEFT JOIN {old_q} o USING ({key_q})
                WHERE o.{key_q} IS NULL
            ),
            deleted AS (
                SELECT 'deleted' AS diff_type, {old_preview}
                FROM {old_q} o
                LEFT JOIN {new_q} n USING ({key_q})
                WHERE n.{key_q} IS NULL
            ),
            changed AS (
                SELECT 'changed' AS diff_type, {new_preview}
                FROM {new_q} n
                JOIN {old_q} o USING ({key_q})
                WHERE {changed_predicate}
            )
        SELECT * FROM added
        UNION ALL
        SELECT * FROM deleted
        UNION ALL
        SELECT * FROM changed
        ORDER BY {key_q};
        """

    return list(conn.execute(sql))


def download_csv_helper(
    conn: sqlite3_builtin.Connection,
    table_name: str,
    url: str,
    session: requests.Session,
    merge: bool = False,
) -> bool:
    """
    Download url into a StringIO file object using streaming
    and import into database

    :param conn: The SQLite3 database connection to use
    :param table_name: The name of the table to import it into
    :param url: URL of the csv to download
    :param session: A requests session to use for the download
    :param merge: (optional) If True, merge into existing table. Defaults to False.

    :return: True if the CSV was downloaded and imported, False if empty
    """

    logger.info("☁️ Downloading from: %s", url)

    # Enable streaming
    with session.get(url, stream=True) as resp:
        resp.raise_for_status()

        # Initialize the string buffer
        string_buffer = StringIO()

        # Stream decoded text
        # decode_unicode=True uses the encoding from the response headers
        for chunk in resp.iter_content(chunk_size=8192, decode_unicode=True):
            if chunk:
                # decode_unicode=True yields str chunks despite requests' typing
                string_buffer.write(cast(str, chunk))

        # Reset pointer to the beginning for DictReader
        string_buffer.seek(0)

        reader = list(DictReader(string_buffer))
        if reader:
            logger.info("✅ Downloaded with encoding %s.", resp.encoding)
            import_data(conn, table_name, reader, merge=merge)
            return True

    logger.warning("⚠️ CSV is empty ⚠️")
    return False
