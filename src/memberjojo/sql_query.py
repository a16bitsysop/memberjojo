"""
Classes for use in sqlite row matching
"""

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Like:
    """Marker type for SQL LIKE comparisons"""

    pattern: str


@dataclass(frozen=True)
class DateRange:
    """Marker type for SQL BETWEEN comparisons for dates"""

    start: date
    end: date
