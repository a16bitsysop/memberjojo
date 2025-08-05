"""
MojoSkel base class

This module provides a common base class (`MojoSkel`) for other `memberjojo` modules.
It includes helper methods for working with SQLite databases.
"""

import sqlite3


class MojoSkel:
    """
    Establishes a connection to a SQLite database and provides helper methods
    for querying tables.
    """

    def __init__(self, db_path: str, table_name: str):
        """
        Initialize the MojoSkel class.

        Connects to the SQLite database and sets the row factory for
        dictionary-style access to columns.

        :param db_path: Path to the SQLite database file.
        :param table_name: Name of the table to operate on, or create when importing.
        """
        self.conn = sqlite3.connect(db_path)

        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.table_name = table_name

    def show_table(self, limit: int = 2):
        """
        Print the first few rows of the table as dictionaries.

        :param limit: (optional) Number of rows to display. Defaults to 2.
        """
        self.cursor.execute(f'SELECT * FROM "{self.table_name}" LIMIT ?', (limit,))
        rows = self.cursor.fetchall()

        if not rows:
            print("(No data)")
            return

        for row in rows:
            print(dict(row))

    def count(self) -> int:
        """
        Returns count of the number of rows in the table.
        """
        self.cursor.execute(f'SELECT COUNT(*) FROM "{self.table_name}"')
        result = self.cursor.fetchone()
        return result[0] if result else 0
