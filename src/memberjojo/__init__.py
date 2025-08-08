"""
memberjojo - tools for working with members.
"""

try:
    from ._version import version as __version__
except ModuleNotFoundError:
    # This happens during flit build before _version.py is written
    __version__ = "0.0.0+local"

from .mojo_member import Member, MemberData
from .mojo_transaction import Transaction
