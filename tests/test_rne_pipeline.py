import copy
import hashlib
import io
from datetime import timedelta

import httpx
import pytest

from industrial_intelligence.ingestion.rne.api import RneApi
from industrial_intelligence.ingestion.rne.common import (
    Conflict,
    IngestionError,
    Paused,
    json_bytes,
    parse_time,
    timestamp,
)
from industrial_intelligence.ingestion.rne.ftp import Stock, StockFtp
from industrial_intelligence.ingestion.rne.pipeline import RnePipeline

START = parse_time("2020-01-01T00:00:00Z")
END = START + timedelta(hours=1)
FILENAME = "stock_RNE_formalites_NIVEAU1_20200101_1400.zip"


class MemoryStore:
    def __init__(self):
        self.objects = {}
        self.counter = 0
        self.fail_raw = False
        self.fail_checkpoint = False

    def uri(self, name):
        return "gs://test/" + name

    def read_json(self, name):
        value = self.objects.get(name)
        return (copy.deepcopy(value[0]), value[1]) if value else (None, 0)

    def write_json(self, name, value, *, generation=0):
        if self.fail_checkpoint and name.endswith("checkpoint.json"):
            raise IngestionError("simulated checkpoint failure")
        current = self.objects.get(name, (None, 0))[1]
        if current != generation:
            raise Conflict("generation conflict")
        self.counter += 1
        self.objects[name] = (copy.deepcopy(value), self.counter)
        return self.counter

    def create_bytes(self, name, value, content_type):
        if self.fail_raw:
            raise IngestionError("simulated upload failure")
        if name not in self.objects:
            self.counter += 1
            self.objects[name] = (value, self.counter)

    def upload_stream(self, name, produce):
        output = io.BytesIO()
        result = produce(output)
        self.create_bytes(name, output.getvalue(), "application/zip")
        return {**result, "generation": self.objects[name][1]}

    def stat(self, name):
        value = self.objects.get(name)
        return {"size": len(value[0]), "generation": value[1]} if value else None

    def delete(self, name, *, generation):
        if self.objects[name][1] != generation:
            raise Conflict("generation conflict")
        del self.objects[name]


def initialized():
    store = MemoryStore()
    pipeline = RnePipeline(store)
    store.write_json(
        pipeline.checkpoint,
        {
            "schema_version": 1,
            "initial_from": timestamp(START),
            "completed_until": timestamp(START),
            "bootstrap_receipt": "control/inpi/rne/stocks/example.json",
            "batch": None,
        },
    )
    return store, pipeline


def company(updated=END, siren="000000001"):
    return {"siren": siren, "updatedAt": timestamp(updated), "formality": {"siren": siren}}


class FakeApi:
    page_size = 10

    def __init__(self):
        self.calls = []
        self.fail_cursor = False

    def count_over_limit(self, start, end):
        return False

    def page(self, start, end, cursor):
        self.calls.append((start, end, cursor))
        if cursor is None:
            items = [{"company": company(end)}]
            return json_bytes(items), items, "opaque-cursor"
        if self.fail_cursor:
            raise IngestionError("simulated API failure")
        return b"[]", [], None


def state(store, pipeline):
    return store.read_json(pipeline.checkpoint)[0]


def test_http_api_and_pipeline_preserve_original_response_bytes():
    store, pipeline = initialized()
    body = b"[\n  " + json_bytes({"company": company()}) + b"\n]"

    def handler(request):
        if request.url.path.endswith("login"):
            return httpx.Response(200, json={"token": "synthetic-test-token"})
        if request.url.path.endswith("count"):
            return httpx.Response(200, json={"nbResults": 1, "isNbResultsOver10000": False})
        if "searchAfter" in request.url.params:
            return httpx.Response(200, content=b"[]")
        return httpx.Response(
            200, content=body, headers={"Pagination-Search-After": "opaque-cursor"}
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        api = RneApi("test-user", "test-password", client=client, sleep=lambda _: None)
        pipeline.refresh(api, END)
    digest = hashlib.sha256(body).hexdigest()
    assert any(
        key.endswith(digest + ".json") and value[0] == body for key, value in store.objects.items()
    )


def test_refresh_completes_only_after_empty_page():
    store, pipeline = initialized()
    result = pipeline.refresh(FakeApi(), END, window_hours=1)
    assert result["records"] == 1 and result["pages"] == 2
    assert state(store, pipeline)["completed_until"] == timestamp(END)
    assert state(store, pipeline)["batch"] is None
    manifests = [value[0] for key, value in store.objects.items() if "/manifest-" in key]
    assert len(manifests) == 1 and len(manifests[0]["pages"]) == 2
    assert not any(key.endswith("lock.json") for key in store.objects)


def test_page_budget_resumes_cursor_without_advancing_watermark():
    store, pipeline = initialized()
    api = FakeApi()
    with pytest.raises(Paused):
        pipeline.refresh(api, END, window_hours=1, max_pages=1)
    checkpoint = state(store, pipeline)
    assert checkpoint["completed_until"] == timestamp(START)
    assert checkpoint["batch"]["active"]["cursor"] == "opaque-cursor"
    resumed = FakeApi()
    pipeline.refresh(resumed, END, window_hours=1)
    assert resumed.calls[0][2] == "opaque-cursor"
    assert state(store, pipeline)["completed_until"] == timestamp(END)


def test_resume_keeps_original_horizon_even_when_a_newer_day_is_available():
    store, pipeline = initialized()
    with pytest.raises(Paused):
        pipeline.refresh(FakeApi(), END, max_pages=1)
    result = pipeline.refresh(FakeApi(), END + timedelta(days=1))
    assert result["completed_until"] == timestamp(END)
    assert state(store, pipeline)["batch"] is None


def test_failure_mid_pagination_keeps_last_successful_page():
    store, pipeline = initialized()
    api = FakeApi()
    api.fail_cursor = True
    with pytest.raises(IngestionError):
        pipeline.refresh(api, END)
    assert state(store, pipeline)["batch"]["active"]["records"] == 1
    assert state(store, pipeline)["completed_until"] == timestamp(START)
    pipeline.refresh(FakeApi(), END)
    assert state(store, pipeline)["completed_until"] == timestamp(END)


def test_failed_raw_write_never_acknowledges_page():
    store, pipeline = initialized()
    store.fail_raw = True
    with pytest.raises(IngestionError):
        pipeline.refresh(FakeApi(), END)
    active = state(store, pipeline)["batch"]["active"]
    assert active["cursor"] is None and active["pages"] == []


def test_failed_checkpoint_replays_content_without_duplicate_raw_objects():
    store, pipeline = initialized()

    class FailOnceApi(FakeApi):
        def page(self, start, end, cursor):
            result = super().page(start, end, cursor)
            store.fail_checkpoint = True
            return result

    with pytest.raises(IngestionError):
        pipeline.refresh(FailOnceApi(), END)
    store.fail_checkpoint = False
    assert state(store, pipeline)["batch"]["active"]["cursor"] is None
    pipeline.refresh(FakeApi(), END)
    bodies = [key for key in store.objects if "/companies/" in key and "/manifest-" not in key]
    assert len(bodies) == 2  # One nonempty body + one terminal empty body.


@pytest.mark.parametrize("cursor", [None, "opaque-cursor"])
def test_missing_or_repeated_cursor_fails_closed(cursor):
    store, pipeline = initialized()

    class InvalidCursor(FakeApi):
        def page(self, start, end, incoming):
            items = [{"company": company(end)}]
            return json_bytes(items), items, cursor

    with pytest.raises(IngestionError, match="cursor"):
        pipeline.refresh(InvalidCursor(), END)
    assert state(store, pipeline)["completed_until"] == timestamp(START)


@pytest.mark.parametrize(
    "item",
    [
        {"unexpected": {}},
        {"company": company(siren="123")},
        {"company": company(START)},  # Lower bound is exclusive.
        {"company": company(END + timedelta(seconds=1))},
        {"company": {"siren": "000000001"}},
    ],
)
def test_invalid_data_does_not_advance(item):
    store, pipeline = initialized()

    class InvalidData(FakeApi):
        def page(self, start, end, cursor):
            return json_bytes([item]), [item], "cursor"

    with pytest.raises(IngestionError):
        pipeline.refresh(InvalidData(), END)
    assert state(store, pipeline)["batch"]["active"]["cursor"] is None


def test_count_ceiling_splits_windows():
    store, pipeline = initialized()

    class SplitApi(FakeApi):
        def count_over_limit(self, start, end):
            return end - start > timedelta(minutes=30)

    api = SplitApi()
    pipeline.refresh(api, END, window_hours=1)
    assert len(api.calls) == 4
    assert all(end - start <= timedelta(minutes=30) for start, end, _ in api.calls)


def test_overlap_replays_once_and_deduplicates_identical_raw_pages():
    store, pipeline = initialized()
    pipeline.refresh(FakeApi(), END, window_hours=1)
    api = FakeApi()
    pipeline.refresh(api, END + timedelta(hours=1), window_hours=1, overlap_hours=1)
    assert api.calls[0][0] == START
    assert state(store, pipeline)["completed_until"] == timestamp(END + timedelta(hours=1))
    bodies = [key for key in store.objects if "/companies/" in key and "/manifest-" not in key]
    assert len(bodies) == 4  # Two bodies for each window, not six after replay.


def test_no_bootstrap_no_refresh():
    with pytest.raises(IngestionError, match="bootstrap"):
        RnePipeline(MemoryStore()).refresh(FakeApi(), END)


def test_existing_lock_is_not_stolen_or_deleted():
    store, pipeline = initialized()
    store.write_json("control/inpi/rne/lock.json", {"run_id": "other"})
    with pytest.raises(IngestionError, match="locked"):
        pipeline.refresh(FakeApi(), END)
    assert store.read_json("control/inpi/rne/lock.json")[0]["run_id"] == "other"


def test_cas_rejects_a_stale_generation():
    store = MemoryStore()
    first = store.write_json("state", {"value": 1})
    store.write_json("state", {"value": 2}, generation=first)
    with pytest.raises(Conflict):
        store.write_json("state", {"value": 3}, generation=first)


class Source:
    host = "ftp.example.invalid"
    body = b"PK\x03\x04synthetic-test-only"

    def stat(self, filename):
        return Stock(filename, len(self.body), "20200101000000")

    def transfer(self, stock, writer):
        writer.write(self.body)
        return {"size": len(self.body), "sha256": hashlib.sha256(self.body).hexdigest()}


def test_bootstrap_stores_original_and_initializes_explicit_boundary():
    store = MemoryStore()
    pipeline = RnePipeline(store)
    result = pipeline.bootstrap(Source(), FILENAME, START)
    assert result["bytes"] == len(Source.body)
    checkpoint = state(store, pipeline)
    assert checkpoint["initial_from"] == timestamp(START)
    receipt = store.read_json(checkpoint["bootstrap_receipt"])[0]
    assert store.objects[receipt["object"]][0] == Source.body
    with pytest.raises(IngestionError, match="already initialized"):
        pipeline.bootstrap(Source(), FILENAME, START)


def test_bootstrap_failure_does_not_initialize():
    class BrokenSource(Source):
        def transfer(self, stock, writer):
            writer.write(b"partial")
            raise IngestionError("interrupted")

    store = MemoryStore()
    pipeline = RnePipeline(store)
    with pytest.raises(IngestionError):
        pipeline.bootstrap(BrokenSource(), FILENAME, START)
    assert state(store, pipeline) is None
    assert not any(key.endswith(".zip") for key in store.objects)


def test_large_download_requires_explicit_opt_in():
    class LargeSource(Source):
        def stat(self, filename):
            return Stock(filename, 101 * 1024**2, "20200101000000")

    store = MemoryStore()
    pipeline = RnePipeline(store)
    with pytest.raises(IngestionError, match="allow-large-download"):
        pipeline.bootstrap(LargeSource(), FILENAME, START)
    assert state(store, pipeline) is None


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


def test_stock_path_rejects_command_injection():
    with pytest.raises(IngestionError):
        StockFtp("example.invalid", 21, "user", "secret").stat(FILENAME + "\r\nDELE x")


def test_bootstrap_can_reuse_receipt_after_checkpoint_failure():
    store = MemoryStore()
    pipeline = RnePipeline(store)
    store.fail_checkpoint = True
    with pytest.raises(IngestionError):
        pipeline.bootstrap(Source(), FILENAME, START)
    store.fail_checkpoint = False

    class MustReuse(Source):
        def transfer(self, stock, writer):
            raise AssertionError("A complete archive should not be downloaded twice")

    pipeline.bootstrap(MustReuse(), FILENAME, START)
    assert state(store, pipeline)["initial_from"] == timestamp(START)
    assert len([key for key in store.objects if key.endswith(".zip")]) == 1


def test_company_tombstone_is_preserved_in_raw_not_silently_dropped():
    store, pipeline = initialized()

    class TombstoneApi(FakeApi):
        def page(self, start, end, cursor):
            if cursor is None:
                item = {
                    "company": {"siren": "000000001", "updatedAt": timestamp(end), "deleted": True}
                }
                return json_bytes([item]), [item], "tombstone-cursor"
            return b"[]", [], None

    pipeline.refresh(TombstoneApi(), END)
    bodies = [value[0] for value in store.objects.values() if isinstance(value[0], bytes)]
    assert any(b'"deleted":true' in body for body in bodies)


def test_explicit_restart_replays_incomplete_window_with_new_page_size():
    store, pipeline = initialized()
    with pytest.raises(Paused):
        pipeline.refresh(FakeApi(), END, max_pages=1)
    api = FakeApi()
    api.page_size = 5
    pipeline.refresh(api, END, restart_window=True)
    assert api.calls[0][2] is None
    assert state(store, pipeline)["completed_until"] == timestamp(END)


def test_resume_rejects_a_different_page_size():
    store, pipeline = initialized()
    with pytest.raises(Paused):
        pipeline.refresh(FakeApi(), END, max_pages=1)
    api = FakeApi()
    api.page_size = 20
    with pytest.raises(IngestionError, match="page size"):
        pipeline.refresh(api, END)
