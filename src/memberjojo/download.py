"""
A class for downloading from membermojo
"""

import getpass
import re
from http.cookiejar import MozillaCookieJar
from pathlib import Path
import requests

from .url import URL
from .mojo_member import Member
from .mojo_transaction import Transaction


class Download:
    """A class for managing Membermojo downloads"""

    def __init__(self, shortname: str, cookie_jar: MozillaCookieJar):
        """
        Initialise the class

        :param shortname: the membermojo shortname
        :param cookie_jar: a MozillaCookieJar with the session cookie, or empty to get one
        """
        self.url = URL(shortname)
        self.cookie_jar = cookie_jar
        self.session = requests.Session()
        self.session.cookies = self.cookie_jar

    def fill_login(self):
        """
        Prompt for email and password to get login data

        :return: a dict of the login data
        """
        email = input("📧 Enter your Membermojo email: ").strip()
        password = getpass.getpass("🔐 Enter your password: ").strip()

        # Submit login form (this triggers verification email if needed)
        return {"email": email, "password": password}

    def mojo_login(self, login_data: dict, email_verify: bool = False):
        """
        Login to membermojo, cookie jar should be saved afterwards with updated cookie

        :param login_data: a dict containing email and password for requests
        :param email_verify: if True membermojo email verification will be triggered
            if login fails, or no cookie found to create a new session cookie

        :raises ValueError: If authentication fails and email_verify is False
        """
        if not self.session.cookies:
            if email_verify:
                print("🍪 No cookies saved, triggering email verification.")
                self.trigger_email(login_data)
            else:
                raise ValueError("⚠️ No cookies found — email verification required.")
        self.session.post(self.url.login, data=login_data)

        # Attempt to access a protected page to verify login worked
        print(f"Verifying login with: {self.url.test}")
        verify_response = self.session.get(self.url.test)
        if "<mm2-loginpage" in verify_response.text:
            if email_verify:
                print("📧 Authentication failed, triggering email verification")
                self.trigger_email(login_data)
            else:
                raise ValueError(
                    "⚠️ Authentication Failed — email verification required."
                )

    def trigger_email(self, login_data: dict):
        """
        Triggers a login verification email, prompts the user for the verification URL,
        and then submits it to complete the login process.

        :param login_data: A dictionary containing login credentials (e.g., email)

        :raises: ValueError: If a CSRF token cannot be found or if the login form submission fails
        """
        self.session.cookies.clear()
        response = self.session.post(self.url.login, data=login_data)

        if "check your email" in response.text.lower() or response.ok:
            print("✅ Login submitted — check your inbox for a verification link.")

            # Get CSRF token from homepage
            homepage = self.session.get(self.url.base_url)
            match = re.search(r'"csrf_token":"([^"]+)"', homepage.text)
            if not match:
                raise ValueError("❌ Could not find CSRF token.")

            csrf_token = match.group(1)
            print(f"✅ CSRF token: {csrf_token}")

            # Ask user for the verification link
            verification_url = input(
                "🔗 Paste the verification URL from the email: "
            ).strip()

            # Submit the verification request
            verify_response = self.session.post(
                verification_url, data={"csrf_token": csrf_token}
            )

            # Output
            if verify_response.ok:
                print("✅ Verification successful. You're now logged in.")
            else:
                print("⚠️ Verification may have failed.")
                verify_html = Path("verify.html")
                with verify_html.open("w", encoding="UTF-8") as f:
                    f.write(verify_response.text)

        else:
            print(response.text)
            raise ValueError("❌ Failed to submit login form.")

    def download_membermojo(
        self,
        member_db_path: str,
        payments_db_path: str,
        db_key: str,
    ):
        """
        Download and link Membermojo data (current members and payments).

        :param member_db_path: Path to the member SQLite database (can be :memory:)
        :param payments_db_path: Path to the payments SQLite database (can be :memory:)
        :param db_key: Key for encrypted databases
        :return: A tuple of (Member instance, Transaction instance)
        """

        # Download Current Member Data
        member_db = Member(member_db_path, db_key)
        member_start = member_db.count()
        member_db.download_csv(self.session, self.url.members())
        member_added = member_db.count() - member_start
        print(f"Member Database: Total: {member_db.count()}, Added: {member_added}")

        # 2. Download Payment Data
        payments_db = Transaction(payments_db_path, db_key)
        # Drop linked view if it exists to avoid rename errors when underlying tables change
        payments_db.conn.execute('DROP VIEW IF EXISTS "linked_payments"')
        payment_start = payments_db.count()

        # Download both tables into the same DB
        payments_db.download_csv(
            self.session, self.url.completed_payments, table_name="completed_payments"
        )
        payments_db.download_csv(
            self.session, self.url.payment_items, table_name="payment_items"
        )

        payment_added = payments_db.count() - payment_start
        print(f"Payment Database: Total: {payments_db.count()}, Added: {payment_added}")

        # Link Payment Items to Completed Payments
        payments_db.link_items()
        print("Linked completed_payments and payment_items via SQL View.")

        return member_db, payments_db
