"""
Test for _version.py not present
"""

import importlib
import sys
from unittest import mock


def test_version_fallback():
    """
    Remove _version to test for missing _version.py
    """

    # Remove the _version module from sys.modules if present
    sys.modules.pop("memberjojo._version", None)

    # Patch sys.modules so importing memberjojo._version raises ImportError
    with mock.patch.dict("sys.modules", {"memberjojo._version": None}):
        # Remove the main package module so reload triggers fresh import logic
        sys.modules.pop("memberjojo", None)

        import memberjojo  # pylint: disable=import-outside-toplevel

        importlib.reload(memberjojo)

        assert memberjojo.__version__ == "0.0.0+local"
