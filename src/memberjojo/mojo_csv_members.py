from csv import DictReader
import sqlite3
from .config import CSV_ENCODING  # import encoding from config.py
from .mojo_common import MojoSkel


class Member(MojoSkel):
    def __init__(self, MemberDBpath, TableName="members"):
        super().__init__(MemberDBpath, TableName)

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

    def _add(self, MemberNum, Title, FirstName, LastName, MembermojoID, ShortURL):
        """
        Add member into sqlite database with passed values if not already in database.
        """
        sql = f"""INSERT OR ABORT INTO "{self.table_name}" 
            (member_number, title, first_name, last_name, membermojo_id, short_url)
            VALUES (?, ?, ?, ?, ?, ?)"""

        try:
            self.cursor.execute(
                sql, (MemberNum, Title, FirstName, LastName, MembermojoID, ShortURL)
            )
            self.conn.commit()
            print(f"Created user {MemberNum}: {FirstName} {LastName}")
        except sqlite3.IntegrityError:
            pass

    def get_number_first_last(self, FirstName, LastName, FoundError=False):
        """
        Returns member number for a first last name case-insensitive match.
        Returns None if no match is found, or raises error if FoundError=True
        """
        sql = f"""
            SELECT member_number
            FROM "{self.table_name}"
            WHERE LOWER(first_name) = LOWER(?) AND LOWER(last_name) = LOWER(?)
        """
        self.cursor.execute(sql, (FirstName, LastName))
        result = self.cursor.fetchone()

        if not result and FoundError:
            raise ValueError(
                f"❌ Cannot find: {FirstName} {LastName} in member database."
            )

        return result[0] if result else None

    def get_name(self, MemberNumber):
        """
        Returns (first_name, last_name) tuple for a given member number.
        Returns None if no match is found.
        """
        sql = f"""
            SELECT first_name, last_name
            FROM "{self.table_name}"
            WHERE member_number = ?
            """
        self.cursor.execute(sql, (MemberNumber,))
        result = self.cursor.fetchone()

        if result:
            first_name, last_name = result
            return f"{first_name} {last_name}"
        return None

    def get_number(self, FullName, FoundError=False):
        parts = FullName.split()
        first_name = parts[0]
        last_name = " ".join(parts[1:])  # Handles middle names too
        return self.get_number_first_last(first_name, last_name, FoundError)

    def import_csv(self, CSVpath):
        """
        Import CSV into SQL database and create tables if needed.
        Only adds non-existing members.
        """
        print(f"Using SQLite database version {sqlite3.sqlite_version}")
        self._create_tables()

        try:
            with CSVpath.open(newline="", encoding=CSV_ENCODING) as csvfile:
                MOJOreader = DictReader(csvfile)

                for row in MOJOreader:
                    self._add(
                        int(row["Member number"]),
                        row["Title"].strip(),
                        row["First name"].strip(),
                        row["Last name"].strip(),
                        int(row["membermojo ID"]),
                        row["Short URL"].strip(),
                    )

        except FileNotFoundError:
            print(f"CSV file not found: {CSVpath}")
