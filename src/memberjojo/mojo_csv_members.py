"""
Module to create an sqlite databse from a downloaded membermojo members.csv
Provides functions to interacte with it as well
"""

from csv import DictReader
from dataclasses import dataclass
import sqlite3
from .config import CSV_ENCODING  # import encoding from config.py
from .mojo_common import MojoSkel


@dataclass
class MemberData:
    """
    class to hold the member data used in sqlite calls
    """

    member_num: int
    title: str
    first_name: str
    last_name: str
    membermojo_id: int
    short_url: str


class Member(MojoSkel):
    """
    The Member class is used to contain these funcitons
    """

    def __init__(self, member_db_path, table_name=None):
        super().__init__(member_db_path, table_name or "members")

    def _create_tables(self):
        """
        Create minimal tables for the user database.
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

    def get_number_first_last(self, first_name, last_name, found_error=False):
        """
        Returns member number for a first last name case-insensitive match.
        Returns None if no match is found, or raises error if FoundError=True
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

    def get_name(self, member_number):
        """
        Returns (first_name, last_name) tuple for a given member number.
        Returns None if no match is found.
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

    def get_number(self, full_name, found_error=False):
        """
        Find the member number for given full_name
        """
        parts = full_name.split()
        first_name = parts[0]
        last_name = " ".join(parts[1:])  # Handles middle names too
        return self.get_number_first_last(first_name, last_name, found_error)

    def _add(self, member: MemberData):
        """
        Add member into sqlite database with passed values if not already in database.
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

    def import_csv(self, csv_path):
        """
        Import CSV into SQL database and create tables if needed.
        Only adds non-existing members.
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
