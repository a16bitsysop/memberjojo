"""
Member module for creating and interacting with a SQLite database.

This module loads data from a `members.csv` file downloaded from Membermojo,
stores it in SQLite, and provides helper functions for member lookups.
"""

from csv import DictReader
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import sqlite3
from .config import CSV_ENCODING  # import encoding from config.py
from .mojo_common import MojoSkel


@dataclass
class MemberData:
    """
    A dataclass to represent a single member's data for database operations.

    Attributes:
        member_num (int): Unique member number (primary key).
        title (str): Title (e.g., Mr, Mrs, Ms).
        first_name (str): Member's first name.
        last_name (str): Member's last name.
        membermojo_id (int): Unique Membermojo ID.
        short_url (str): Short URL to Membermojo profile.
    """

    member_num: int
    title: str
    first_name: str
    last_name: str
    membermojo_id: int
    short_url: str


class Member(MojoSkel):
    """
    Subclass of MojoSkel providing member-specific database functions.

    This class connects to a SQLite database and supports importing member data
    from CSV and performing queries like lookup by name or member number.

    :param member_db_path (Path): Path to the SQLite database file.
    :param table_name (str): (optional) Table name to use. Defaults to "members".
    """

    def __init__(self, member_db_path: Path, table_name: str = "members"):
        """
        Initialize the Member database handler.
        """
        super().__init__(member_db_path, table_name)

    def __iter__(self):
        """
        Allow iterating over the class, by outputing all members.
        """
        sql = (
            f"SELECT member_number, "
            f"title, "
            f"first_name, "
            f"last_name, "
            f"membermojo_id, "
            f"short_url "
            f'FROM "{self.table_name}"'
        )
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        for row in rows:
            yield MemberData(*row)

    def _create_tables(self):
        """
        Create the members table in the database if it doesn't exist.

        The table includes member number, title, first/last names,
        Membermojo ID, and a short profile URL.
        """
        sql_statements = [
            f"""CREATE TABLE IF NOT EXISTS "{self.table_name}" (
                member_number INTEGER PRIMARY KEY,
                title TEXT NOT NULL CHECK(title IN ('Dr', 'Mr', 'Mrs', 'Miss', 'Ms')),
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                membermojo_id INTEGER UNIQUE NOT NULL,
                short_url TEXT NOT NULL
            );"""
        ]

        for statement in sql_statements:
            self.cursor.execute(statement)
        self.conn.commit()

    def get_number_first_last(
        self, first_name: str, last_name: str, found_error: bool = False
    ) -> Optional[int]:
        """
        Find a member number based on first and last name (case-insensitive).

        :param first_name: First name of the member.
        :param last_name: Last name of the member.
        :param found_error: (optional): If True, raises ValueError if not found.

        :return: The member number if found, otherwise None.

        :raises ValueError: If not found and `found_error` is True.
        """
        sql = f"""
            SELECT member_number
            FROM "{self.table_name}"
            WHERE LOWER(first_name) = LOWER(?) AND LOWER(last_name) = LOWER(?)
        """
        self.cursor.execute(sql, (first_name, last_name))
        result = self.cursor.fetchone()

        if not result and found_error:
            raise ValueError(
                f"❌ Cannot find: {first_name} {last_name} in member database."
            )

        return result[0] if result else None

    def get_number(self, full_name: str, found_error: bool = False) -> Optional[int]:
        """
        Find a member number by full name (tries first and last, and then middle last if 3 words).

        :param full_name: Full name of the member.
        :param found_error: (optional) Raise ValueError if not found.

        :return: Member number if found, else None.

        :raises ValueError: If not found and `found_error` is True.
        """
        member_num = None
        try_names = []
        parts = full_name.strip().split()
        # Try first + last
        if len(parts) >= 2:
            first_name = parts[0]
            last_name = parts[-1]
            try_names.append(f"<{first_name} {last_name}>")
            member_num = self.get_number_first_last(first_name, last_name)

        # Try middle + last if 3 parts and first+last failed
        if not member_num and len(parts) == 3:
            first_name = parts[1]
            last_name = parts[2]
            try_names.append(f"<{first_name} {last_name}>")
            member_num = self.get_number_first_last(first_name, last_name)

        if not member_num and found_error:
            tried = ", ".join(try_names) if try_names else "No valid names to find"
            raise ValueError(
                f"❌ Cannot find {full_name} in member database. Tried: {tried}"
            )
        return member_num

    def get_name(self, member_number: int) -> Optional[str]:
        """
        Get full name for a given member number.

        :param member_number: Member number to look up.

        :return: Full name as "First Last", or None if not found.
        """
        sql = f"""
            SELECT first_name, last_name
            FROM "{self.table_name}"
            WHERE member_number = ?
            """
        self.cursor.execute(sql, (member_number,))
        result = self.cursor.fetchone()

        if result:
            first_name, last_name = result
            return f"{first_name} {last_name}"
        return None

    def _add(self, member: MemberData):
        """
        Insert a member into the database if not already present.

        :param member: The member to add.
        """
        sql = f"""INSERT OR ABORT INTO "{self.table_name}"
            (member_number, title, first_name, last_name, membermojo_id, short_url)
            VALUES (?, ?, ?, ?, ?, ?)"""

        try:
            self.cursor.execute(
                sql,
                (
                    member.member_num,
                    member.title,
                    member.first_name,
                    member.last_name,
                    member.membermojo_id,
                    member.short_url,
                ),
            )
            self.conn.commit()
            print(
                f"Created user {member.member_num}: {member.first_name} {member.last_name}"
            )
        except sqlite3.IntegrityError:
            pass

    def import_csv(self, csv_path: Path):
        """
        Load members from a Membermojo CSV file and insert into the database.

        :param csv_path: Path to the CSV file.

        Notes:
            Only adds members not already in the database (INSERT OR ABORT).
        """
        print(f"Using SQLite database version {sqlite3.sqlite_version}")
        self._create_tables()

        try:
            with csv_path.open(newline="", encoding=CSV_ENCODING) as csvfile:
                mojo_reader = DictReader(csvfile)

                for row in mojo_reader:
                    member = MemberData(
                        member_num=int(row["Member number"]),
                        title=row["Title"].strip(),
                        first_name=row["First name"].strip(),
                        last_name=row["Last name"].strip(),
                        membermojo_id=int(row["membermojo ID"]),
                        short_url=row["Short URL"].strip(),
                    )
                    self._add(member)
        except FileNotFoundError:
            print(f"CSV file not found: {csv_path}")
