"""
Module to import and interact with Membermojo completed_payments.csv data in SQLite

Provides automatic column type inference, robust CSV importing, and
helper methods for querying the database
"""

from pathlib import Path
from typing import override

import requests

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
    ) -> None:
        """
        Initialize the Transaction object
        """
        super().__init__(payment_db_path, db_key, table_name)
        # Automatically try to link if we are loading an existing DB
        self.link_items()

    @override
    def import_csv(
        self,
        csv_path: Path,
        merge: bool = False,
        table_name: str | None = None,
    ) -> None:
        """
        Import CSV and relink items

        :param csv_path: Path like path of csv file
        :param merge: (optional) If True, merge into existing table. Defaults to False.
        :param table_name: (optional) Table name to use for import.
            Defaults to self.table_name.
        """
        super().import_csv(csv_path, merge=merge, table_name=table_name)
        self.link_items(force=True)
        if table_name:
            self.set_table(table_name)

    @override
    def download_csv(
        self,
        session: requests.Session,
        url: str,
        merge: bool = False,
        table_name: str | None = None,
    ) -> None:
        """
        Download CSV and relink items

        :param session: Requests session to use for download
        :param url: url of the csv to download
        :param merge: (optional) If True, merge into existing table. Defaults to False.
        :param table_name: (optional) Table name to use for import.
            Defaults to self.table_name.
        """
        super().download_csv(session, url, merge=merge, table_name=table_name)
        self.link_items(force=True)
        if table_name:
            self.set_table(table_name)

    def link_items(
        self, table_name: str = "linked_payments", force: bool = False
    ) -> None:
        """
        Link completed_payments with payment_items through a SQL table

        :param table_name: (optional) Name of the table. Defaults to "linked_payments"
        :param force: (optional) If True, recreate the table even if it exists.
        """
        join_col = "payment_id"
        if not mojo_loader.table_exists(
            self.cursor, "completed_payments"
        ) or not mojo_loader.table_exists(self.cursor, "payment_items"):
            return

        if not force and mojo_loader.table_exists(self.cursor, table_name):
            self.set_table(table_name)
            return

        self.set_table("completed_payments")
        self.create_joined_table(
            table_name, "payment_items", join_col=join_col, is_view=True
        )
        self.set_table(table_name)
