"""
Common class to base other modules from
includes some helper functions
"""

import sqlite3


class MojoSkel:
    """
    Base class for the other memberjojo modules
    """

    def __init__(self, db_path, table_name):
        """
        connect to database and set factory for name based access to columns
        """
        try:
            self.conn = sqlite3.connect(db_path)
        except sqlite3.Error as e:
            raise RuntimeError(f"❌ SQLite init error: {e}") from e

        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.table_name = table_name

    def show_table(self, limit=2):
        """
        Print first `limit` entries as dicts.
        """
        self.cursor.execute(f'SELECT * FROM "{self.table_name}" LIMIT ?', (limit,))
        rows = self.cursor.fetchall()

        if not rows:
            print("(No data)")
            return

        for row in rows:
            print(dict(row))

    def count(self):
        """
        Return number of rows.
        """
        self.cursor.execute(f'SELECT COUNT(*) FROM "{self.table_name}"')
        result = self.cursor.fetchone()
        return result[0] if result else 0
