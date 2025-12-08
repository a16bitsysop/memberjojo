"""
Member module for creating and interacting with a SQLite database.

This module loads data from a `members.csv` file downloaded from Membermojo,
stores it in SQLite, and provides helper functions for member lookups.
"""

from pathlib import Path
from typing import Optional
from .mojo_common import MojoSkel


class Member(MojoSkel):
    """
    Subclass of MojoSkel providing member-specific database functions.

    This class connects to a SQLite database and supports importing member data
    from CSV and performing queries like lookup by name or member number.

    :param member_db_path (Path): Path to the SQLite database file.
    :param table_name (str): (optional) Table name to use. Defaults to "members".
    :param db_key: (optional) key to unlock the encrypted sqlite database, unencrypted if unset.
    """

    def __init__(
        self,
        member_db_path: Path,
        db_key: str,
        table_name: str = "members",
    ):
        """
        Initialize the Member database handler.
        """
        super().__init__(member_db_path, db_key, table_name)

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
            SELECT "member_number"
            FROM "{self.table_name}"
            WHERE LOWER("first_name") = LOWER(?) AND LOWER("last_name") = LOWER(?)
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
        Find a member number by passed full_name.
        Tries first and last, and then middle last if 3 words,
        Then initial of first name if initials passed.

        :param full_name: Full name of the member.
        :param found_error: (optional) Raise ValueError if not found.

        :return: Member number if found, else None.

        :raises ValueError: If not found and `found_error` is True.
        """
        result = self.get_mojo_name(full_name, found_error)
        if result:
            return self.get_number_first_last(result[0], result[1])
        return None

    def _lookup_exact(self, first_name: str, last_name: str) -> Optional[tuple]:
        """
        Lookup first_name and last_name in the member database, return found name or none

        :param first_name: First name to lookup
        :param last_name: Last name to lookup

        :return: Name on membermojo or None
        """
        sql = f"""
                SELECT "first_name", "last_name"
                FROM "{self.table_name}"
                WHERE LOWER("first_name") = LOWER(?)
                    AND LOWER("last_name")  = LOWER(?)
        """
        self.cursor.execute(sql, (first_name, last_name))
        row = self.cursor.fetchone()
        return (row["first_name"], row["last_name"]) if row else None

    def _lookup_initial(self, letter: str, last_name: str) -> Optional[tuple]:
        """
        Lookup Initial and last_name in the member database, return found name or none

        :param letter: initial to search for
        :param last_name: last name to search for

        :return: Name on membermojo or None
        """
        sql = f"""
                SELECT "first_name", "last_name"
                FROM "{self.table_name}"
                WHERE LOWER("first_name") LIKE LOWER(?) || '%'
                    AND LOWER("last_name") = LOWER(?)
                LIMIT 1
        """
        self.cursor.execute(sql, (letter, last_name))
        row = self.cursor.fetchone()
        return (row["first_name"], row["last_name"]) if row else None

    def get_mojo_name(
        self, full_name: str, found_error: bool = False
    ) -> Optional[tuple]:
        """
        Resolve a member name from a free-text full name.
        Search order:
            1. first + last
            2. middle + last (if three parts)
            3. initial 1st letter + last
            4. initial 2nd letter + last (for two-letter initials)
        Returns (first_name, last_name) or None.

        :param full_name: Full name of the member to find.
        :param found_error: (optional) Raise ValueError if not found.

        :return: Membermojo name if found, else None.

        :raises ValueError: If not found and `found_error` is True.
        """

        parts = full_name.strip().split()
        tried = []

        # If only one one word passed, fail early
        if len(parts) < 2:
            if found_error:
                raise ValueError(f"❌ Cannot extract name from: {full_name}")
            return None

        # ----------------------------
        # 1. Try direct first + last
        # ----------------------------
        tried.append(f"{parts[0]} {parts[-1]}")

        result = self._lookup_exact(parts[0], parts[-1])
        if result:
            return result

        # ----------------------------
        # 2. Try middle + last and build initials if no match
        # ----------------------------
        if len(parts) == 3:
            tried.append(f"{parts[1]} {parts[2]}")

            result = self._lookup_exact(parts[1], parts[2])
            if result:
                return result

            # First letter of first + first letter of middle
            initials = parts[0][0].upper() + parts[1][0].upper()
        else:
            # Only first letter of first name
            initials = parts[0][0].upper()

        # ------------------------------------------------
        # Initial fallback lookups
        # ------------------------------------------------

        # 3. Try first initial + last name
        first_initial = initials[0]
        tried.append(f"{first_initial} {parts[-1]}")
        result = self._lookup_initial(first_initial, parts[-1])
        if result:
            return result

        # 4. Try second initial + last name (e.g., for JA or AM)
        if len(initials) > 1:
            second_initial = initials[1]
            tried.append(f"{second_initial} {parts[-1]}")
            result = self._lookup_initial(second_initial, parts[-1])
            if result:
                return result

        # ----------------------------
        # 5. No match
        # ----------------------------
        if found_error:
            raise ValueError(
                f"❌ Cannot find {full_name} in member database. Tried: {tried}"
            )

        return None

    def get_first_last_name(self, member_number: int) -> Optional[str]:
        """
        Get full name for a given member number.

        :param member_number: Member number to look up.

        :return: Full name as tuple, or None if not found.
        """
        sql = f"""
            SELECT "first_name", "last_name"
            FROM "{self.table_name}"
            WHERE "member_number" = ?
            """
        self.cursor.execute(sql, (member_number,))
        result = self.cursor.fetchone()

        return result if result else None

    def get_name(self, member_number: int) -> Optional[str]:
        """
        Get full name for a given member number.

        :param member_number: Member number to look up.

        :return: Full name as "First Last", or None if not found.
        """

        result = self.get_first_last_name(member_number)

        if result:
            first_name, last_name = result
            return f"{first_name} {last_name}"
        return None
