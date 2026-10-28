import hashlib
import io
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from rne_fakes import MemoryStore

from industrial_intelligence.ingestion.rne.bootstrap import archive_stock
from industrial_intelligence.ingestion.rne.common import IngestionError
from industrial_intelligence.ingestion.rne.sources import Stock, StockFtp

PREFIX = "inpi/rne"
FILENAME = "stock_RNE_formalites_NIVEAU1_20200101_1400.zip"


class Source:
    host = "ftp.example.invalid"
    body = b"PK\x03\x04synthetic-test-only"

    def stat(self, filename):
        return Stock(filename, len(self.body), "20200101000000")

    def transfer(self, stock, writer):
        writer.write(self.body)
        return {"size": len(self.body), "sha256": hashlib.sha256(self.body).hexdigest()}


def archives(store):
    return [name for name in store.objects if name.endswith(".zip")]


def test_bootstrap_archives_the_original_with_a_receipt():
    store = MemoryStore()
    result = archive_stock(store, PREFIX, Source(), FILENAME)
    assert result["status"] == "archived" and result["bytes"] == len(Source.body)
    assert result["sha256"] == hashlib.sha256(Source.body).hexdigest()
    receipt = store.read_json(f"control/{PREFIX}/stocks/{Source().stat(FILENAME).revision}.json")[0]
    assert store.objects[receipt["object"]][0] == Source.body
    assert store.read_json(receipt["object"] + ".manifest.json")[0] == receipt


def test_bootstrap_reuses_a_verified_receipt_without_redownloading():
    store = MemoryStore()
    archive_stock(store, PREFIX, Source(), FILENAME)

    class MustReuse(Source):
        def transfer(self, stock, writer):
            raise AssertionError("The validated archive must not be downloaded twice")

    assert archive_stock(store, PREFIX, MustReuse(), FILENAME)["reused_archive"] is True
    assert len(archives(store)) == 1


def test_failed_transfer_writes_no_receipt_and_no_archive():
    class BrokenSource(Source):
        def transfer(self, stock, writer):
            writer.write(b"partial")
            raise IngestionError("interrupted")

    store = MemoryStore()
    with pytest.raises(IngestionError):
        archive_stock(store, PREFIX, BrokenSource(), FILENAME)
    assert store.objects == {}


def test_absolute_size_limit_and_large_download_opt_in():
    class LargeSource(Source):
        def __init__(self, size):
            self.size = size

        def stat(self, filename):
            return Stock(filename, self.size, "20200101000000")

    with pytest.raises(IngestionError, match="byte limit"):
        archive_stock(
            MemoryStore(),
            PREFIX,
            LargeSource(20 * 1024**3 + 1),
            FILENAME,
            allow_large_download=True,
        )
    with pytest.raises(IngestionError, match="allow-large-download"):
        archive_stock(MemoryStore(), PREFIX, LargeSource(101 * 1024**2), FILENAME)


def test_failed_receipt_never_forces_an_overwrite_of_the_orphaned_archive(monkeypatch):
    store = MemoryStore()
    original = store.write_json

    def fail_receipt(name, value, **kwargs):
        if name.startswith(f"control/{PREFIX}/stocks/"):
            raise IngestionError("simulated receipt failure")
        return original(name, value, **kwargs)

    with monkeypatch.context() as context:
        context.setattr(store, "write_json", fail_receipt)
        with pytest.raises(IngestionError, match="receipt failure"):
            archive_stock(store, PREFIX, Source(), FILENAME)
    archive_stock(store, PREFIX, Source(), FILENAME)
    assert len(archives(store)) == 2  # A new unique path, never an overwrite.


@pytest.mark.parametrize("changed, truncated", [(False, False), (True, False), (False, True)])
def test_ftp_validates_size_signature_and_remote_revision(changed, truncated):
    source = StockFtp("example.invalid", 21, "user", "secret")
    original = b"PK\x03\x04sample"

    class FakeFtp:
        transferred = False

        def size(self, filename):
            return len(original)

        def sendcmd(self, cmd):
            return "213 " + ("20200102000000" if changed and self.transferred else "20200101000000")

        def retrbinary(self, cmd, consume, blocksize):
            data = original[:-1] if truncated else original
            for pos in range(0, len(data), 2):
                consume(data[pos : pos + 2])
            self.transferred = True

    source.ftp = FakeFtp()
    stock = source.stat(FILENAME)
    output = io.BytesIO()
    if changed or truncated:
        with pytest.raises(IngestionError):
            source.transfer(stock, output)
    else:
        assert source.transfer(stock, output)["sha256"] == hashlib.sha256(original).hexdigest()


def test_ftp_progress_is_a_structured_event_without_credentials(monkeypatch, caplog):
    source = StockFtp("example.invalid", 21, "private-user", "private-password")
    body = b"PK\x03\x04test"
    source.ftp = Mock()
    source.ftp.size.return_value = len(body)
    source.ftp.sendcmd.return_value = "213 20200101000000"
    source.ftp.retrbinary.side_effect = lambda cmd, consume, blocksize: consume(body)
    clock = iter([0, 31])
    monkeypatch.setattr(
        "industrial_intelligence.ingestion.rne.sources.time",
        SimpleNamespace(monotonic=lambda: next(clock)),
    )
    caplog.set_level("INFO", logger="industrial_intelligence.ingestion.rne.sources")
    source.transfer(source.stat(FILENAME), io.BytesIO())
    progress = [r for r in caplog.records if r.getMessage() == "stock_transfer_progress"]
    assert progress and progress[0].event_fields["bytes"] == len(body)
    assert "private-user" not in caplog.text and "private-password" not in caplog.text


def test_stock_path_rejects_command_injection():
    with pytest.raises(IngestionError):
        StockFtp("example.invalid", 21, "user", "secret").stat(FILENAME + "\r\nDELE x")
