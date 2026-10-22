"""Bounded, authenticated access to the documented companies differential API."""

import time
from datetime import datetime
from email.utils import parsedate_to_datetime

import httpx

from .common import IngestionError, Paused, timestamp, utc_now

API_BASE = "https://registre-national-entreprises.inpi.fr/api"


class RneApi:
    def __init__(
        self,
        username: str,
        password: str,
        *,
        client: httpx.Client,
        page_size: int = 10,
        max_requests: int = 5000,
        attempts: int = 5,
        max_response_bytes: int = 64 * 1024 * 1024,
        sleep=time.sleep,
    ):
        if not 1 <= page_size <= 100 or max_requests < 1 or attempts < 1:
            raise IngestionError("Invalid API limits")
        self._username = username
        self._password = password
        self._token = None
        self.client = client
        self.page_size = page_size
        self.max_requests = max_requests
        self.attempts = attempts
        self.max_response_bytes = max_response_bytes
        self.sleep = sleep
        self.requests = 0

    def _login(self) -> None:
        body, _ = self._request(
            "POST",
            "/sso/login",
            authenticated=False,
            json={"username": self._username, "password": self._password},
        )
        value = self._json(body)
        if not isinstance(value, dict) or not isinstance(value.get("token"), str):
            raise IngestionError("INPI login did not return a token")
        self._token = value["token"]

    def _request(self, method, path, *, authenticated=True, **kwargs):
        if authenticated and self._token is None:
            self._login()
        renewed = False
        failures = 0
        while failures < self.attempts:
            if self.requests >= self.max_requests:
                raise Paused("API request budget reached; resume the same checkpoint")
            self.sleep(0.25)  # Deliberately modest request rate, not a measured INPI quota.
            self.requests += 1
            headers = {"Accept": "application/json"}
            if authenticated:
                headers["Authorization"] = f"Bearer {self._token}"
            try:
                with self.client.stream(
                    method, API_BASE + path, headers=headers, **kwargs
                ) as response:
                    status = response.status_code
                    if status == 401 and authenticated and not renewed:
                        renewed = True
                        self._token = None
                        # Close the response before opening the login request.
                        needs_login = True
                    else:
                        needs_login = False
                        if status == 429 or 500 <= status < 600:
                            delay = self._retry_delay(response.headers.get("Retry-After"), failures)
                        elif status != 200:
                            raise IngestionError(f"INPI HTTP {status}; response body omitted")
                        else:
                            body = bytearray()
                            for chunk in response.iter_bytes(chunk_size=64 * 1024):
                                body.extend(chunk)
                                if len(body) > self.max_response_bytes:
                                    raise IngestionError("API response too large; reduce page size")
                            return bytes(body), response.headers
                if needs_login:
                    self._login()
                    continue
            except httpx.TransportError:
                delay = min(2**failures, 60)
            failures += 1
            if failures < self.attempts:
                self.sleep(delay)
        raise IngestionError("INPI transient error: bounded retries exhausted")

    @staticmethod
    def _retry_delay(value: str | None, failures: int) -> float:
        if value is None:
            return min(2**failures, 60)
        try:
            delay = float(value)
        except ValueError:
            try:
                delay = (parsedate_to_datetime(value) - utc_now()).total_seconds()
            except (ValueError, TypeError):
                raise IngestionError("Invalid Retry-After header") from None
        if not 0 <= delay <= 300:
            # Do not retry earlier than instructed, or sleep past a job's budget.
            raise Paused("Retry-After exceeds the wait budget; retry this run later")
        return delay

    @staticmethod
    def _json(body: bytes):
        import json

        try:
            return json.loads(body)
        except (ValueError, UnicodeError):
            raise IngestionError("INPI returned invalid JSON") from None

    def count_over_limit(self, start: datetime, end: datetime) -> bool:
        body, _ = self._request(
            "GET", "/companies/diff/count", params={"from": timestamp(start), "to": timestamp(end)}
        )
        value = self._json(body)
        if not isinstance(value, dict) or type(value.get("isNbResultsOver10000")) is not bool:
            raise IngestionError("Invalid differential count response")
        if not value["isNbResultsOver10000"]:
            count = value.get("nbResults")
            if type(count) is not int or not 0 <= count <= 10000:
                raise IngestionError("Invalid differential count")
        return value["isNbResultsOver10000"]

    def page(self, start: datetime, end: datetime, cursor: str | None):
        params = {"from": timestamp(start), "to": timestamp(end), "pageSize": self.page_size}
        if cursor is not None:
            params["searchAfter"] = cursor
        body, headers = self._request("GET", "/companies/diff", params=params)
        items = self._json(body)
        if not isinstance(items, list) or len(items) > self.page_size:
            raise IngestionError("Invalid differential page")
        return body, items, headers.get("pagination-search-after")
