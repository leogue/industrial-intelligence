"""INPI source clients: authenticated HTTP API and the separate stock FTP."""

import ftplib
import hashlib
import json
import logging
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import BinaryIO

import httpx

from .common import IngestionError, Paused, json_bytes, timestamp, utc_now
from .log import log_event

API_BASE = "https://registre-national-entreprises.inpi.fr/api"
logger = logging.getLogger(__name__)


class RneApi:
    def __init__(
        self,
        username: str,
        password: str,
        *,
        client: httpx.Client,
        page_size: int = 100,
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
        throttled = False
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
                        log_event(logger, "api_token_renewed", path=path)
                        # Close the response before opening the login request.
                        needs_login = True
                    else:
                        needs_login = False
                        throttled = status == 429
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
            except httpx.TransportError as error:
                throttled = False
                status = type(error).__name__
                delay = min(2**failures, 60)
            failures += 1
            log_event(
                logger,
                "api_retry",
                logging.WARNING,
                path=path,
                status=status,
                attempt=failures,
                max_attempts=self.attempts,
                delay_seconds=delay if failures < self.attempts else None,
            )
            if failures < self.attempts:
                self.sleep(delay)
        if throttled:
            # Documented daily quota (HTTP 429): stop cleanly, the next run resumes.
            raise Paused("INPI quota or rate limit reached; resume the same checkpoint")
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
        try:
            return json.loads(body)
        except (ValueError, UnicodeError):
            raise IngestionError("INPI returned invalid JSON") from None

    def count(self, start: datetime, end: datetime) -> int | None:
        """Return the exact window count, or None above the provider's 10,000 ceiling."""
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
            return count
        return None

    def page(self, start: datetime, end: datetime, cursor: str | None):
        params = {"from": timestamp(start), "to": timestamp(end), "pageSize": self.page_size}
        if cursor is not None:
            params["searchAfter"] = cursor
        body, headers = self._request("GET", "/companies/diff", params=params)
        items = self._json(body)
        if not isinstance(items, list) or len(items) > self.page_size:
            raise IngestionError("Invalid differential page")
        return body, items, headers.get("pagination-search-after")


@dataclass(frozen=True)
class Stock:
    filename: str
    size: int
    modified: str

    @property
    def revision(self) -> str:
        # Remote revision hint, NOT the content checksum or the business date.
        return hashlib.sha256(json_bytes(asdict(self))).hexdigest()


class StockFtp:
    def __init__(self, host: str, port: int, username: str, password: str, *, sleep=time.sleep):
        self.host = host
        self.port = port
        self._username = username
        self._password = password
        self.sleep = sleep
        self.ftp = None

    def __enter__(self):
        # Retry connection establishment only; a failed large transfer restarts on rerun.
        for attempt in range(3):
            ftp = ftplib.FTP(timeout=60)
            try:
                ftp.connect(self.host, self.port)
                ftp.login(self._username, self._password)
                ftp.voidcmd("TYPE I")
                self.ftp = ftp
                return self
            except ftplib.error_perm:
                ftp.close()
                raise IngestionError("FTP connection/authentication refused") from None
            except (OSError, EOFError, ftplib.error_temp):
                ftp.close()
                if attempt == 2:
                    raise IngestionError("FTP connection failed after bounded retries") from None
                self.sleep(2**attempt)

    def __exit__(self, *_):
        if self.ftp is not None:
            self.ftp.close()

    def stat(self, filename: str) -> Stock:
        if not re.fullmatch(r"stock_RNE_formalites_[a-zA-Z0-9_.-]+\.zip", filename):
            raise IngestionError("Select an explicit RNE formalities ZIP basename")
        try:
            size = self.ftp.size(filename)
            modified = self.ftp.sendcmd("MDTM " + filename)
        except (ftplib.Error, OSError, EOFError):
            raise IngestionError("Cannot read stock SIZE/MDTM") from None
        if type(size) is not int or size <= 0 or not re.fullmatch(r"213 [0-9]{14}", modified):
            raise IngestionError("Invalid stock SIZE/MDTM")
        return Stock(filename, size, modified[4:])

    def transfer(self, stock: Stock, writer: BinaryIO) -> dict:
        total = 0
        checksum = hashlib.sha256()
        signature = bytearray()
        started = last_report_at = time.monotonic()
        last_report_bytes = 0
        log_event(logger, "stock_transfer_started", expected_bytes=stock.size)

        def consume(block: bytes):
            nonlocal total, last_report_at, last_report_bytes
            total += len(block)
            if total > stock.size:
                raise IngestionError("FTP stock grew during transfer")
            if len(signature) < 4:
                signature.extend(block[: 4 - len(signature)])
            checksum.update(block)
            writer.write(block)
            now = time.monotonic()
            if total - last_report_bytes >= 256 * 1024**2 or now - last_report_at >= 30:
                log_event(
                    logger,
                    "stock_transfer_progress",
                    bytes=total,
                    expected_bytes=stock.size,
                    elapsed_seconds=round(now - started, 1),
                )
                last_report_at, last_report_bytes = now, total

        try:
            self.ftp.retrbinary("RETR " + stock.filename, consume, blocksize=1024 * 1024)
        except (ftplib.Error, OSError, EOFError):
            raise IngestionError("FTP transfer interrupted; no checkpoint advanced") from None
        if total != stock.size or bytes(signature) != b"PK\x03\x04":
            raise IngestionError("Incomplete stock or invalid ZIP signature")
        if self.stat(stock.filename) != stock:
            raise IngestionError("Stock SIZE/MDTM changed during transfer; refusing to validate")
        log_event(logger, "stock_source_verified", bytes=total)
        return {"size": total, "sha256": checksum.hexdigest()}
