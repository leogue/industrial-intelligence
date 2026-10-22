"""The stock FTP is distinct from the documentary SFTP and personal API login."""

import ftplib
import hashlib
import re
import time
from dataclasses import asdict, dataclass
from typing import BinaryIO

from .common import IngestionError, json_bytes


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

        def consume(block: bytes):
            nonlocal total
            total += len(block)
            if total > stock.size:
                raise IngestionError("FTP stock grew during transfer")
            if len(signature) < 4:
                signature.extend(block[: 4 - len(signature)])
            checksum.update(block)
            writer.write(block)

        try:
            self.ftp.retrbinary("RETR " + stock.filename, consume, blocksize=1024 * 1024)
        except (ftplib.Error, OSError, EOFError):
            raise IngestionError("FTP transfer interrupted; no checkpoint advanced") from None
        if total != stock.size or bytes(signature) != b"PK\x03\x04":
            raise IngestionError("Incomplete stock or invalid ZIP signature")
        if self.stat(stock.filename) != stock:
            raise IngestionError("Stock SIZE/MDTM changed during transfer; refusing to validate")
        return {"size": total, "sha256": checksum.hexdigest()}
