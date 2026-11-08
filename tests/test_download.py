"""
Tests for the Download module, mocking network and interactive input.
"""

import logging
from http.cookiejar import Cookie, MozillaCookieJar
from unittest import mock

import pytest

from memberjojo import Download

# pylint: disable=redefined-outer-name


def _set_session_cookie(download):
    """
    Add a session cookie to the download's cookie jar directly
    """
    cookie = Cookie(
        version=0,
        name="session",
        value="abcdef",
        port=None,
        port_specified=False,
        domain="membermojo.co.uk",
        domain_specified=True,
        domain_initial_dot=False,
        path="/",
        path_specified=True,
        secure=False,
        expires=None,
        discard=True,
        comment=None,
        comment_url=None,
        rest={},
        rfc2109=False,
    )
    download.session.cookies.set_cookie(cookie)


@pytest.fixture
def cookie_jar():
    """
    An empty MozillaCookieJar backed by a temp file
    """
    jar = MozillaCookieJar()
    jar.filename = "unused.cookies.txt"
    return jar


@pytest.fixture
def download(cookie_jar):
    """
    A Download instance with the real session but mocked network calls
    """
    return Download("myshortname", cookie_jar)


def test_init_sets_url_and_cookies(download, cookie_jar):
    """
    __init__ should build the URL helper and assign the cookie jar to the session
    """
    assert download.url.shortname == "myshortname"
    assert download.url.base_url == "https://membermojo.co.uk"
    assert download.session.cookies is cookie_jar


@mock.patch("memberjojo.download.getpass.getpass", return_value="secret")
@mock.patch("builtins.input", return_value="foo@example.com")
def test_fill_login(
    mock_input, mock_getpass, download
):  # pylint: disable=unused-argument
    """
    fill_login should return a dict of email and password
    """
    result = download.fill_login()
    assert result == {"email": "foo@example.com", "password": "secret"}


def test_mojo_login_success(download):
    """
    mojo_login should post login data and verify against the test page
    """
    _set_session_cookie(download)
    with (
        mock.patch.object(download.session, "post") as mock_post,
        mock.patch.object(download.session, "get") as mock_get,
    ):
        mock_get.return_value = mock.Mock(text="<html>dashboard</html>")
        download.mojo_login({"email": "e", "password": "p"})
        mock_post.assert_called_once_with(
            download.url.login, data={"email": "e", "password": "p"}
        )
        mock_get.assert_called_once_with(download.url.test)


def test_mojo_login_no_cookies_no_verify(download):
    """
    mojo_login with no cookies and email_verify=False should raise
    """
    with pytest.raises(ValueError, match="No cookies found"):
        download.mojo_login({"email": "e", "password": "p"})


def test_mojo_login_no_cookies_with_verify(download):
    """
    mojo_login with no cookies and email_verify=True should trigger the email flow
    """
    with (
        mock.patch.object(download, "trigger_email") as mock_trigger,
        mock.patch.object(download.session, "post") as mock_post,
        mock.patch.object(download.session, "get") as mock_get,
    ):
        mock_get.return_value = mock.Mock(text="<html>dashboard</html>")
        download.mojo_login({"email": "e", "password": "p"}, email_verify=True)
        mock_trigger.assert_called_once_with({"email": "e", "password": "p"})
        mock_post.assert_not_called()


def test_mojo_login_login_page_no_verify(download):
    """
    mojo_login seeing the login page with email_verify=False should raise
    """
    _set_session_cookie(download)
    with (
        mock.patch.object(download.session, "post"),
        mock.patch.object(download.session, "get") as mock_get,
    ):
        mock_get.return_value = mock.Mock(text="<mm2-loginpage>")
        with pytest.raises(ValueError, match="Authentication Failed"):
            download.mojo_login({"email": "e", "password": "p"})


def test_mojo_login_login_page_with_verify(download):
    """
    mojo_login seeing the login page with email_verify=True should trigger email flow
    """
    _set_session_cookie(download)
    with (
        mock.patch.object(download.session, "post") as mock_post,
        mock.patch.object(download.session, "get") as mock_get,
        mock.patch.object(download, "trigger_email") as mock_trigger,
    ):
        mock_get.return_value = mock.Mock(text="<mm2-loginpage>")
        download.mojo_login({"email": "e", "password": "p"}, email_verify=True)
        mock_post.assert_called_once()
        mock_trigger.assert_called_once_with({"email": "e", "password": "p"})


def test_trigger_email_success(download):
    """
    trigger_email success path: obtains a CSRF token and submits verification
    """
    download.session.cookies.clear()
    login_response = mock.Mock()
    login_response.text = "check your email"
    homepage = mock.Mock(text='{"csrf_token":"tok123"}')
    verify_response = mock.Mock(ok=True)

    posts = {
        download.url.login: login_response,
        "https://verify.example/link": verify_response,
    }

    def fake_post(url, data=None, **kwargs):  # pylint: disable=unused-argument
        return posts[url]

    with (
        mock.patch.object(download.session, "post", side_effect=fake_post),
        mock.patch.object(download.session, "get", return_value=homepage),
        mock.patch("builtins.input", return_value="https://verify.example/link"),
    ):
        download.trigger_email({"email": "e"})


@pytest.mark.parametrize("debug", [False, True])
def test_trigger_email_logs_sensitive_output_only_in_debug(download, caplog, debug):
    """
    The CSRF token is only logged when debug is on
    """
    caplog.set_level(logging.DEBUG, logger="memberjojo.download")
    download.debug = debug
    download.session.cookies.clear()
    login_response = mock.Mock()
    login_response.text = "check your email"
    homepage = mock.Mock(text='{"csrf_token":"tok123"}')
    verify_response = mock.Mock(ok=True)
    posts = {
        download.url.login: login_response,
        "https://verify.example/link": verify_response,
    }

    def fake_post(url, data=None, **kwargs):  # pylint: disable=unused-argument
        return posts[url]

    with (
        mock.patch.object(download.session, "post", side_effect=fake_post),
        mock.patch.object(download.session, "get", return_value=homepage),
        mock.patch("builtins.input", return_value="https://verify.example/link"),
    ):
        download.trigger_email({"email": "e"})

    assert ("tok123" in caplog.text) is debug


@pytest.mark.parametrize("debug", [False, True])
def test_trigger_email_login_failed_body_only_in_debug(download, caplog, debug):
    """
    The failed login page body is only logged when debug is on
    """
    caplog.set_level(logging.DEBUG, logger="memberjojo.download")
    download.debug = debug
    download.session.cookies.clear()
    login_response = mock.Mock()
    login_response.text = "secret-page-body"
    login_response.ok = False

    with (
        mock.patch.object(download.session, "post", return_value=login_response),
        mock.patch.object(download.session, "get"),
        pytest.raises(ValueError, match="Failed to submit login form"),
    ):
        download.trigger_email({"email": "e"})

    assert ("secret-page-body" in caplog.text) is debug


def test_trigger_email_missing_csrf(download):
    """
    trigger_email should raise if the CSRF token cannot be found
    """
    download.session.cookies.clear()
    login_response = mock.Mock()
    login_response.text = "check your email"
    homepage = mock.Mock(text="no token here")

    with (
        mock.patch.object(download.session, "post", return_value=login_response),
        mock.patch.object(download.session, "get", return_value=homepage),
    ):
        with pytest.raises(ValueError, match="Could not find CSRF token"):
            download.trigger_email({"email": "e"})


def test_trigger_email_login_failed(download):
    """
    trigger_email should raise if the initial login form submission fails
    """
    download.session.cookies.clear()
    login_response = mock.Mock()
    login_response.text = "something else"
    login_response.ok = False

    with (
        mock.patch.object(download.session, "post", return_value=login_response),
        mock.patch.object(download.session, "get"),
    ):
        with pytest.raises(ValueError, match="Failed to submit login form"):
            download.trigger_email({"email": "e"})


def test_trigger_email_verify_failed_reports_only(download, tmp_path, monkeypatch):
    """
    trigger_email should report the failure without writing a file
    """
    download.session.cookies.clear()
    login_response = mock.Mock()
    login_response.text = "check your email"
    homepage = mock.Mock(text='{"csrf_token":"tok123"}')
    verify_response = mock.Mock(ok=False, status_code=500, text="<html>fail</html>")

    posts = {
        download.url.login: login_response,
        "https://verify.example/link": verify_response,
    }

    def fake_post(url, data=None, **kwargs):  # pylint: disable=unused-argument
        return posts[url]

    # Ensure any stray file write would land in the tmp dir, then assert none
    monkeypatch.chdir(tmp_path)

    with (
        mock.patch.object(download.session, "post", side_effect=fake_post),
        mock.patch.object(download.session, "get", return_value=homepage),
        mock.patch("builtins.input", return_value="https://verify.example/link"),
    ):
        download.trigger_email({"email": "e"})

    assert not (tmp_path / "verify.html").exists()


def test_download_membermojo(download):
    """
    download_membermojo should download members and payments and link items
    """
    mock_member = mock.Mock()
    mock_payments = mock.Mock()
    mock_member.count.return_value = 12
    mock_payments.count.return_value = 30

    with (
        mock.patch(
            "memberjojo.download.Member", return_value=mock_member
        ) as mock_member_cls,
        mock.patch(
            "memberjojo.download.Transaction", return_value=mock_payments
        ) as mock_payments_cls,
    ):
        member_db, payments_db = download.download_membermojo("mem.db", "pay.db", "key")

        assert member_db is mock_member
        assert payments_db is mock_payments

        mock_member_cls.assert_called_once_with("mem.db", "key")
        mock_payments_cls.assert_called_once_with("pay.db", "key")

        # member download
        mock_member.download_csv.assert_called_once_with(
            download.session, download.url.members()
        )
        # payment downloads for both tables
        assert mock_payments.download_csv.call_count == 2
        mock_payments.download_csv.assert_any_call(
            download.session,
            download.url.completed_payments,
            table_name="completed_payments",
        )
        mock_payments.download_csv.assert_any_call(
            download.session, download.url.payment_items, table_name="payment_items"
        )
        mock_payments.link_items.assert_called_once()
