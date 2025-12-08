"""
memberjojo - tools for working with members.
"""

try:
    from ._version import version as __version__
except ModuleNotFoundError:
    # _version.py is written when building dist
    __version__ = "0.0.0+local"

from .mojo_member import Member
from .mojo_transaction import Transaction
