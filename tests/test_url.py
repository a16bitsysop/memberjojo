"""
Tests for the URL module
"""

import pytest

from memberjojo import URL

# pylint: disable=redefined-outer-name


@pytest.fixture
def url():
    """
    A URL helper for a known shortname
    """
    return URL("myshortname")


def test_init(url):
    """
    __init__ should store the shortname and default base url
    """
    assert url.shortname == "myshortname"
    assert url.base_url == "https://membermojo.co.uk"


def test_make_url(url):
    """
    make_url should compose a full url from an endpoint
    """
    assert url.make_url("membersx") == "https://membermojo.co.uk/myshortname/membersx"


def test_login_property(url):
    """
    login should point at the password sign-in endpoint
    """
    assert url.login == "https://membermojo.co.uk/myshortname/signin_password"


def test_membership_property(url):
    """
    membership should point at the membership endpoint
    """
    assert url.membership == "https://membermojo.co.uk/myshortname/membership"


def test_completed_payments_property(url):
    """
    completed_payments should point at the completed payments download endpoint
    """
    assert (
        url.completed_payments
        == "https://membermojo.co.uk/myshortname/membership/download_completed_payments?state=CO"
    )


def test_payment_items_property(url):
    """
    payment_items should point at the payment items download endpoint
    """
    assert (
        url.payment_items
        == "https://membermojo.co.uk/myshortname/membership/download_payment_items"
    )


def test_pending_properties(url):
    """
    Each pending download endpoint should be exposed as a property
    """
    assert (
        url.pending_approval
        == "https://membermojo.co.uk/myshortname/membership/download_pending_approval_members"
    )
    assert (
        url.pending_completion
        == "https://membermojo.co.uk/myshortname/membership/download_pending_completion_members"
    )
    assert (
        url.pending_payments
        == "https://membermojo.co.uk/myshortname/membership/download_pending_payments"
    )


def test_pending_aproval_alias(url):
    """
    The old misspelled pending_aproval property is kept as a deprecated alias
    """
    assert url.pending_aproval == url.pending_approval


def test_test_property(url):
    """
    test should point at the membership page used for login verification
    """
    assert url.test == url.membership


@pytest.mark.parametrize(
    "state,expected",
    [("", "download_members"), ("expired", "download_expired_members")],
)
def test_members_states(url, state, expected):
    """
    members() should build the right download url for the member state
    """
    assert (
        url.members(state)
        == f"https://membermojo.co.uk/myshortname/membership/{expected}"
    )


def test_members_active_state(url):
    """
    members("active") should resolve to the active (default) download
    """
    assert url.members("active") == url.members()


def test_members_invalid_state(url):
    """
    members() with an unknown state should raise ValueError
    """
    with pytest.raises(ValueError, match="Invalid member state"):
        url.members("bogus")
