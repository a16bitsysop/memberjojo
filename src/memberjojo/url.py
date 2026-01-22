"""
A class for managing Membermojo URLs
"""


class URL:
    """A class for managing Membermojo URLs"""

    def __init__(self, shortname: str):
        """
        Ininitalise the class

        :param shortname: the shortname setting on membermojo
        """
        self.shortname = shortname
        self.base_url = "https://membermojo.co.uk"

    def make_url(self, endpoint: str) -> str:
        """
        return a whole url for endpoint

        :param endpoint: the endpoint to make url for

        :return: a complete url
        """
        return f"{self.base_url}/{self.shortname}/{endpoint}"

    def members(self, state: str = "") -> str:
        """
        return the active, expired, or archived urls

        :param state: membership state to return

        :return: url for the state

        state:
            "active" or "" -> active members
            "expired" -> expired members
            "archived" -> archived members
        """
        if state not in {"", "expired", "archived"}:
            raise ValueError(f"Invalid member state: {state}")

        if state == "active":
            state = ""
        suffix = f"_{state}" if state else ""
        return f"{self.membership}/download{suffix}_members"

    @property
    def login(self):
        """Returns the membermojo login URL"""
        return self.make_url("signin_password")

    @property
    def membership(self):
        """Returns the URL for membership"""
        return self.make_url("membership")

    @property
    def completed_payments(self):
        """Returns the completed payments download URL"""
        return f"{self.membership}/download_completed_payments?state=CO"

    @property
    def pending_aproval(self):
        """Returns the members pending approval URL"""
        return f"{self.membership}/download_pending_approval_members"

    @property
    def pending_completion(self):
        """Returns the members pending completion URL"""
        return f"{self.membership}/download_pending_completion_members"

    @property
    def pending_payments(self):
        """Returns the members pending payments URL"""
        return f"{self.membership}/download_pending_payments"

    @property
    def test(self):
        """Returns the test URL for login verification"""
        return self.membership
