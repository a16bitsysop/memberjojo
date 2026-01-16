"""
Classes for use in sqlite row matching
"""

from dataclasses import dataclass

@dataclass(frozen=True)
class Like:
    """Marker type for SQL LIKE comparisons."""
    pattern: str