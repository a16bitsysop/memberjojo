"""
Member module for creating and interacting with a SQLite database

This module loads data from a `members.csv` file downloaded from Membermojo,
stores it in SQLite, and provides helper functions for member lookups
"""

import heapq
import logging
from difflib import SequenceMatcher
from pathlib import Path
from typing import override

from .mojo_common import MojoSkel
from .mojo_loader import quote_name
from .sql_query import Like

logger = logging.getLogger(__name__)

# Fuzzy-match acceptance thresholds (mirror difflib.get_close_matches
# defaults plus the per-field guards below).
NAME_RATIO_CUTOFF = 0.7
LAST_NAME_RATIO_CUTOFF = 0.7
FIRST_NAME_RATIO_CUTOFF = 0.6
MAX_CANDIDATES = 10


def _passes_name_guards(
    input_first: str, input_last: str, cand_first: str, cand_last: str
) -> bool:
    """
    Check a fuzzy candidate's first and last name against the per-field cutoffs

    The input names are expected to be lowercase; candidate names are
    lowercased here.

    :param input_first: first name typed by the user
    :param input_last: last name typed by the user
    :param cand_first: first name of the candidate member
    :param cand_last: last name of the candidate member

    :return: True if the candidate is close enough on both names
    """
    first_lower = cand_first.lower()
    last_lower = cand_last.lower()

    last_ratio = SequenceMatcher(None, input_last, last_lower).ratio()

    # Check prefix ratio for extra text appended to last name
    # (e.g. EMILY STONMEMBERSHIP)
    short_len = min(len(input_last), len(last_lower))
    prefix_ratio = (
        SequenceMatcher(None, input_last[:short_len], last_lower[:short_len]).ratio()
        if short_len > 0
        else 0
    )
    if last_ratio < LAST_NAME_RATIO_CUTOFF and prefix_ratio < LAST_NAME_RATIO_CUTOFF:
        return False

    first_ratio = SequenceMatcher(None, input_first, first_lower).ratio()
    first_initial_match = (
        bool(input_first) and bool(first_lower) and input_first[0] == first_lower[0]
    )
    return first_ratio >= FIRST_NAME_RATIO_CUTOFF or first_initial_match


class Member(MojoSkel):
    """
    Subclass of MojoSkel providing member-specific database functions

    This class connects to a SQLite database and supports importing member data
    from CSV and performing queries like lookup by name or member number

    :param member_db_path (Path): Path to the SQLite database file
    :param db_key: key to unlock the encrypted sqlite database,
        unencrypted if sqlcipher3 not installed or unset
    :param table_name (str): (optional) Table name to use. Defaults to "members"
    """

    def __init__(
        self,
        member_db_path: str | Path,
        db_key: str,
        table_name: str = "members",
    ) -> None:
        """
        Initialize the Member database handler
        """
        super().__init__(member_db_path, db_key, table_name)
        self._name_index: list[tuple[str, str, str]] | None = None

    @override
    def _invalidate_caches(self) -> None:
        """
        Drop the cached name index when the underlying table changes.
        """
        super()._invalidate_caches()
        self._name_index = None

    def _get_name_index(self) -> list[tuple[str, str, str]]:
        """
        Build and cache an in-memory index of distinct member names.

        Each entry is a (lowercase full name, first_name, last_name) tuple,
        deduplicated on the lowercase full name.

        The index is only refreshed when the table is re-imported or
        ``set_table`` is called; changes made directly via ``self.cursor``
        are not picked up.

        :return: The cached name index
        """
        if self._name_index is not None:
            return self._name_index

        names: list[tuple[str, str, str]] = []
        seen: set[str] = set()
        self.cursor.execute(
            f"SELECT first_name, last_name FROM {quote_name(self.table_name)}"
        )
        for row in self.cursor:
            first_name = row["first_name"]
            last_name = row["last_name"]
            full = f"{first_name} {last_name}".lower()
            if full in seen:
                continue
            seen.add(full)
            names.append((full, first_name, last_name))

        self._name_index = names
        return names

    def get_bool(self, entry_name: str, member_number: int) -> bool:
        """
        Return a bool for a member entry that is a tick box on membermojo

        :param entry_name: The entry name to return as a bool
        :param member_number: The member number to check value of entry_name

        :return: True is entry is yes otherwise False

        :raises ValueError: If entry name not found
        """
        sql = f"""
            SELECT {self.quote_identifier(entry_name)}
            FROM {quote_name(self.table_name)}
            WHERE "member_number" = ?
        """
        self.cursor.execute(sql, (member_number,))

        row = self.cursor.fetchone()
        if row is None:
            raise ValueError(
                f"❌ Cannot find: {entry_name} for member {member_number}."
            )

        value = row[0]
        return str(value).lower() == "yes"

    def member_type_count(self, membership_type: str) -> int:
        """
        Count members by membership type string

        :param membership_type: the string to match, can use percent to match
            remaining or preceeding text

                - Full (match only Full)
                - Full% (match Full and any words after)
                - %Full% ( match Full in the middle)

        :return: Number of members that are `membership_type`
        """
        return self.count_rows({"membership": Like(membership_type)})

    def get_number_first_last(
        self, first_name: str, last_name: str, found_error: bool = False
    ) -> int | None:
        """
        Find a member number based on first and last name (case-insensitive)

        :param first_name: First name of the member
        :param last_name: Last name of the member
        :param found_error: (optional): If True, raises ValueError if not found

        :return: The member number if found, otherwise None

        :raises ValueError: If not found and `found_error` is True
        """
        sql = f"""
            SELECT "member_number"
            FROM {quote_name(self.table_name)}
            WHERE "first_name" = ? COLLATE NOCASE
              AND "last_name" = ? COLLATE NOCASE
        """
        self.cursor.execute(sql, (first_name, last_name))
        result = self.cursor.fetchone()
        if self.debug:
            logger.debug("Sql: %r", sql)
            logger.debug("First Name: %s Last Name: %s", first_name, last_name)
            logger.debug("Result: %r", result[0] if result is not None else None)

        if not result and found_error:
            raise ValueError(
                f"❌ Cannot find: {first_name} {last_name} in member database."
            )

        if result is None:
            return None
        return int(result[0])

    def get_number(self, full_name: str, found_error: bool = False) -> int | None:
        """
        Find a member number by passed full_name
        Tries first and last, and then middle last if 3 words,
        Then initial of first name if initials passed
        Finnaly a fuzzy lookup is tried

        :param full_name: Full name of the member
        :param found_error: (optional) Raise ValueError if not found

        :return: Member number if found, else None

        :raises ValueError: If not found and `found_error` is True
        """
        result = self.get_mojo_name(full_name)
        if not result:
            result = self.get_fuzz_name(full_name, found_error)

        if result:
            return self.get_number_first_last(result[0], result[1])
        return None

    def _lookup_exact(self, first_name: str, last_name: str) -> tuple[str, str] | None:
        """
        Lookup first_name and last_name in the member database, return found name or none

        :param first_name: First name to lookup
        :param last_name: Last name to lookup

        :return: Name on membermojo or None
        """
        sql = f"""
                SELECT "first_name", "last_name"
                FROM {quote_name(self.table_name)}
                WHERE "first_name" = ? COLLATE NOCASE
                    AND "last_name"  = ? COLLATE NOCASE
        """
        self.cursor.execute(sql, (first_name, last_name))
        row = self.cursor.fetchone()
        return (row["first_name"], row["last_name"]) if row else None

    def _lookup_initial(self, letter: str, last_name: str) -> tuple[str, str] | None:
        """
        Lookup Initial and last_name in the member database, return found name or none

        :param letter: initial to search for
        :param last_name: last name to search for

        :return: Name on membermojo or None
        """
        sql = f"""
                SELECT "first_name", "last_name"
                FROM {quote_name(self.table_name)}
                WHERE "first_name" LIKE ? || '%' COLLATE NOCASE
                    AND "last_name" = ? COLLATE NOCASE
                LIMIT 1
        """
        self.cursor.execute(sql, (letter, last_name))
        row = self.cursor.fetchone()
        return (row["first_name"], row["last_name"]) if row else None

    def get_mojo_name(
        self, full_name: str, found_error: bool = False
    ) -> tuple[str, str] | None:
        """
        Resolve a member name from a free-text full name

        **Search order**

        1. first + last
        2. middle + last (if three parts)
        3. initial 1st letter + last
        4. initial 2nd letter + last (for two-letter initials)

        Returns (first_name, last_name) or None

        :param full_name: Full name of the member to find
        :param found_error: (optional) Raise ValueError if not found

        :return: Membermojo name if found, else None

        :raises ValueError: If not found and `found_error` is True
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

    def get_first_last_name(self, member_number: int) -> tuple[str, str] | None:
        """
        Get first and last name for a given member number

        :param member_number: Member number to look up

        :return: (first_name, last_name) tuple, or None if not found
        """
        sql = f"""
            SELECT "first_name", "last_name"
            FROM {quote_name(self.table_name)}
            WHERE "member_number" = ?
            """
        self.cursor.execute(sql, (member_number,))
        result = self.cursor.fetchone()

        if result is None:
            return None
        return (result["first_name"], result["last_name"])

    def get_name(self, member_number: int) -> str | None:
        """
        Get full name for a given member number

        :param member_number: Member number to look up

        :return: Full name as "First Last", or None if not found
        """

        result = self.get_first_last_name(member_number)

        if result:
            first_name, last_name = result
            return f"{first_name} {last_name}"
        return None

    def get_fuzz_name(
        self, name: str, found_error: bool = False
    ) -> tuple[str, str] | None:
        # pylint: disable=too-many-locals
        """
        Fuzzy search for members by name using partial matching
        Searches across first_name and last_name fields

        :param name: Free text name to search for (partial match)

        :return: Tuple of (first_name, last_name) or None

        :raises ValueError: If not found and `found_error` is True
        """

        original = name
        name = name.strip().lower()
        parts = name.split()

        # Iterate the cached member name index, keeping only the best
        # candidates in memory. This replicates difflib.get_close_matches /
        # heapq.nlargest exactly (real_quick_ratio/quick_ratio pre-filters,
        # ratio cutoff, and ordering by the (ratio, full_name) tuple) without
        # re-scanning the whole table on every lookup.
        cutoff = NAME_RATIO_CUTOFF
        s = SequenceMatcher()
        s.set_seq2(name)
        # Min-heap holding the MAX_CANDIDATES best entries. Full names are
        # unique in the index, so ties are broken on full_name as in
        # heapq.nlargest and the name fields are never compared.
        best: list[tuple[float, str, str, str]] = []

        for full, first_name, last_name in self._get_name_index():
            s.set_seq1(full)
            # Same acceptance chain as get_close_matches
            if not (
                s.real_quick_ratio() >= cutoff
                and s.quick_ratio() >= cutoff
                and s.ratio() >= cutoff
            ):
                continue

            entry = (s.ratio(), full, first_name, last_name)
            if len(best) < MAX_CANDIDATES:
                heapq.heappush(best, entry)
            else:
                heapq.heappushpop(best, entry)

        # Process candidates best-first (mirrors get_close_matches ordering)
        ranked = sorted(best, reverse=True)
        scores = [(ratio, full) for ratio, full, _, _ in ranked]
        for ratio, _full, cand_first, cand_last in ranked:
            if len(parts) >= 2 and not _passes_name_guards(
                parts[0], parts[-1], cand_first, cand_last
            ):
                continue

            matched = (cand_first, cand_last)
            self._report_fuzzy_match(original, matched, ratio, scores)
            return matched

        if self.debug:
            logger.debug("No fuzzy name match found for '%s'.", original)
            self._print_fuzzy_candidates(scores)
        if found_error:
            raise ValueError(
                f"❌ Cannot find {original} in member database with fuzzy match."
            )
        return None

    def _report_fuzzy_match(
        self,
        original: str,
        matched: tuple[str, str],
        ratio: float,
        candidates: list[tuple[float, str]],
    ) -> None:
        """
        Log the outcome of a fuzzy name lookup at debug level when debug mode is on.

        The match ratio and every candidate considered are logged for context.

        :param original: Name passed to the fuzzy lookup
        :param matched: The matched (first_name, last_name) tuple
        :param ratio: Ratio score of the matched candidate
        :param candidates: All candidate (ratio, full_name) tuples considered
        """
        if not self.debug:
            return
        logger.debug("Fuzzy name match: '%s' -> '%s'", original, " ".join(matched))
        logger.debug("  Match ratio: %.3f", ratio)
        self._print_fuzzy_candidates(candidates)

    def _print_fuzzy_candidates(self, candidates: list[tuple[float, str]]) -> None:
        """
        Log the candidate names and their ratio scores at debug level.

        :param candidates: All candidate (ratio, full_name) tuples considered
        """
        if not candidates:
            return
        logger.debug("  Candidates considered:")
        for cand_ratio, cand_name in candidates:
            logger.debug("    %s: %.3f", cand_name, cand_ratio)
