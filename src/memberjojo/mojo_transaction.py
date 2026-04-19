"""
Module to import and interact with Membermojo completed_payments.csv data in SQLite

Provides automatic column type inference, robust CSV importing, and
helper methods for querying the database
"""

from .mojo_common import MojoSkel
from . import mojo_loader


class Transaction(MojoSkel):
    """
    Handles importing and querying completed payment data

    Extends:
        MojoSkel: Base class with transaction database operations

    :param payment_db_path: Path to the SQLite database
    :param db_key: key to unlock the encrypted sqlite database,
        unencrypted if sqlcipher3 not installed or unset
    :param table_name: (optional) Name of the table. Defaults to "completed_payments"
    """

    def __init__(
        self,
        payment_db_path: str,
        db_key: str,
        table_name: str = "completed_payments",
    ):
        """
        Initialize the Transaction object
        """
        super().__init__(payment_db_path, db_key, table_name)
        # Automatically try to link if we are loading an existing DB
        self.link_items()

    def link_items(self, view_name: str = "linked_payments"):
        """
        Link completed_payments with payment_items through a SQL view

        :param view_name: (optional) Name of the view. Defaults to "linked_payments"
        """
        join_col = "payment_id"
        if not mojo_loader.table_exists(self.cursor, "completed_payments") or \
           not mojo_loader.table_exists(self.cursor, "payment_items"):
            return

        # Ensure join_col exists in both tables
        for table in ["completed_payments", "payment_items"]:
            self.cursor.execute(f'PRAGMA table_info("{table}")')
            cols = {row[1] for row in self.cursor.fetchall()}
            if join_col not in cols:
                return

        self.set_table("completed_payments")
        self.create_view(view_name, "payment_items", join_col=join_col)
        self.set_table(view_name)
