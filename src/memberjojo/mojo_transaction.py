"""
Module to import and interact with Membermojo completed_payments.csv data in SQLite.

Provides automatic column type inference, robust CSV importing, and
helper methods for querying the database.
"""

from .mojo_common import MojoSkel


class Transaction(MojoSkel):
    """
    Handles importing and querying completed payment data.

    Extends:
        MojoSkel: Base class with transaction database operations.

    :param payment_db_path: Path to the SQLite database.
    :param table_name: (optional) Name of the table. Defaults to "payments".
    :param db_key: (optional) key to unlock the encrypted sqlite database, unencrypted if unset.
    """

    def __init__(
        self,
        payment_db_path: str,
        db_key: str,
        table_name: str = "payments",
    ):
        """
        Initialize the Transaction object.
        """
        super().__init__(payment_db_path, db_key, table_name)
