from datetime import timedelta
from decimal import Decimal

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import simplejson
from rne_fakes import MemoryStore

from industrial_intelligence.ingestion.rne.common import (
    IngestionError,
    Paused,
    json_bytes,
    parse_time,
    timestamp,
)
from industrial_intelligence.ingestion.rne.diff import DiffSync
from industrial_intelligence.ingestion.rne.sources import RneApi

START = parse_time("2026-03-01T00:00:00Z")
START_TS = timestamp(START)
UNTIL = START + timedelta(days=3)
CHECKPOINT = "control/inpi/rne/diff/checkpoint.json"


def company(siren, updated, **extra):
    return {"siren": siren, "updatedAt": timestamp(updated), "formality": {"siren": siren}, **extra}


class FakeApi:
    """In-memory /companies/diff: counts and cursors over a fixed list of changes."""

    def __init__(self, changes, *, page_size=2, max_requests=1000, count_limit=10000):
        self.changes = sorted(changes, key=lambda change: change[0])
        self.page_size = page_size
        self.max_requests = max_requests
        self.count_limit = count_limit
        self.requests = 0
        self.pages = []

    def _call(self):
        if self.requests >= self.max_requests:
            raise Paused("API request budget reached; resume the same checkpoint")
        self.requests += 1

    def _window(self, start, end):
        return [item for at, item in self.changes if start < at <= end]

    def count(self, start, end):
        self._call()
        total = len(self._window(start, end))
        return None if total > self.count_limit else total

    def page(self, start, end, cursor):
        self._call()
        offset = int(cursor or 0)
        items = self._window(start, end)[offset : offset + self.page_size]
        self.pages.append((start, end, cursor))
        next_cursor = str(offset + len(items)) if items else None
        body = simplejson.dumps(items, use_decimal=True).encode()
        return body, items, next_cursor


def changes(count, *, every=timedelta(hours=1)):
    return [
        (START + every * (number + 1), {"company": company(f"{number:09d}", START + every)})
        for number in range(count)
    ]


def bronze(store):
    return {
        name: pq.read_table(pa.BufferReader(value))
        for name, (value, _) in store.objects.items()
        if name.startswith("bronze/")
    }


def raw_pages(store):
    return [name for name in store.objects if name.startswith("raw/")]


def checkpoint(store):
    return store.read_json(CHECKPOINT)[0]


def test_first_run_starts_at_the_stock_boundary_and_catches_up():
    store = MemoryStore()
    result = DiffSync(store).run(FakeApi(changes(5)), START, UNTIL)
    assert result["status"] == "up_to_date" and result["records"] == 5
    assert result["days_behind"] == 0
    state = checkpoint(store)
    assert state["start"] == timestamp(START)
    assert state["completed_until"] == timestamp(UNTIL) and state["window"] is None
    tables = bronze(store)
    assert sum(table.num_rows for table in tables.values()) == 5
    assert all(name.endswith("/companies.parquet") for name in tables)
    assert all("/companies/diff/from=" in name for name in [*tables, *raw_pages(store)])


def test_next_run_resumes_and_a_different_start_is_refused():
    store = MemoryStore()
    DiffSync(store).run(FakeApi(changes(2)), START, UNTIL)
    api = FakeApi(changes(2))
    assert DiffSync(store).run(api, START, UNTIL)["windows"] == 0 and api.requests == 0
    with pytest.raises(IngestionError, match="another --start"):
        DiffSync(store).run(FakeApi([]), START + timedelta(days=1), UNTIL)


def test_windows_grow_after_success_so_a_long_gap_needs_few_calls():
    store = MemoryStore()
    long_until = START + timedelta(days=30)
    api = FakeApi([])
    result = DiffSync(store).run(api, START, long_until)
    # 6 h, 12 h, then 1-day windows (+ a 6 h remainder): one count + one empty page each.
    assert result["windows"] == 2 + 29 + 1 and api.requests == 2 * result["windows"]


def test_window_is_halved_until_the_count_is_exact():
    store = MemoryStore()
    api = FakeApi(changes(4), count_limit=1)
    result = DiffSync(store).run(api, START, START + timedelta(hours=6))
    assert result["records"] == 4 and checkpoint(store)["window"] is None
    assert all(len(api._window(start, end)) <= 1 for start, end, _ in api.pages)


def test_request_budget_is_a_clean_partial_run_and_the_cursor_resumes():
    store = MemoryStore()
    data = changes(5)
    result = DiffSync(store).run(FakeApi(data, max_requests=3), START, UNTIL)
    assert result["status"] == "partial" and "budget" in result["reason"]
    window = checkpoint(store)["window"]
    assert window["cursor"] == "4" and window["records"] == 4
    assert checkpoint(store)["completed_until"] == timestamp(START)

    api = FakeApi(data)
    assert DiffSync(store).run(api, START, UNTIL)["status"] == "up_to_date"
    assert api.pages[0][2] == "4"  # Resumed from the saved cursor, not from scratch.
    rows = sum(table.num_rows for table in bronze(store).values())
    assert rows == 5  # Pages from the first run were re-read from raw GCS objects.


def test_time_budget_is_a_clean_partial_run(monkeypatch):
    clock = iter([0, 0, 10**6])
    monkeypatch.setattr(
        "industrial_intelligence.ingestion.rne.diff.time.monotonic", lambda: next(clock)
    )
    store = MemoryStore()
    result = DiffSync(store).run(FakeApi(changes(5)), START, UNTIL, max_seconds=10)
    assert result["status"] == "partial" and "Time budget" in result["reason"]


def test_count_mismatch_restarts_the_window_loudly():
    store = MemoryStore()

    class MovingCount(FakeApi):
        def count(self, start, end):
            return super().count(start, end) + 1

    with pytest.raises(IngestionError, match="restarts from scratch"):
        DiffSync(store).run(MovingCount(changes(1)), START, UNTIL)
    assert checkpoint(store)["window"] is None
    assert checkpoint(store)["completed_until"] == timestamp(START)
    assert not bronze(store)
    assert DiffSync(store).run(FakeApi(changes(1)), START, UNTIL)["records"] == 1


@pytest.mark.parametrize("cursor, acknowledged", [(None, 0), ("", 0), ("stuck", 1)])
def test_missing_or_repeated_cursor_never_acknowledges_the_page(cursor, acknowledged):
    store = MemoryStore()

    class BadCursor(FakeApi):
        def page(self, start, end, previous):
            body, items, _ = super().page(start, end, None)  # Always the first page.
            return body, items, cursor

    with pytest.raises(IngestionError, match="cursor"):
        DiffSync(store).run(BadCursor(changes(3)), START, UNTIL)
    assert len(checkpoint(store)["window"]["pages"]) == acknowledged


@pytest.mark.parametrize(
    "item",
    [{"siren": "12"}, {"company": {"siren": "000000001"}}, {"company": "not-an-object"}],
)
def test_invalid_rows_block_the_page_and_keep_the_cursor(item):
    store = MemoryStore()
    api = FakeApi([(START + timedelta(hours=1), item)])
    with pytest.raises(IngestionError):
        DiffSync(store).run(api, START, UNTIL)
    window = checkpoint(store)["window"]
    assert window["cursor"] is None and window["records"] == 0


def test_parquet_already_written_before_a_lost_checkpoint_is_reused(monkeypatch):
    store = MemoryStore()
    original = store.write_json
    failed = []

    def lose_completion(name, value, **kwargs):
        completing = value.get("window") is None and value.get("completed_until") != START_TS
        if name == CHECKPOINT and completing and not failed:
            failed.append(True)
            raise IngestionError("simulated checkpoint failure")
        return original(name, value, **kwargs)

    monkeypatch.setattr(store, "write_json", lose_completion)
    with pytest.raises(IngestionError, match="simulated"):
        DiffSync(store).run(FakeApi(changes(1)), START, UNTIL)
    uploaded = list(store.uploaded)
    assert DiffSync(store).run(FakeApi(changes(1)), START, UNTIL)["status"] == "up_to_date"
    assert store.uploaded == uploaded  # Same window, same Parquet: not rewritten.
    assert len(raw_pages(store)) == 1  # Content-addressed page: not duplicated.


def test_empty_windows_do_not_create_parquet_files():
    store = MemoryStore()
    assert DiffSync(store).run(FakeApi([]), START, UNTIL)["status"] == "up_to_date"
    assert not bronze(store) and not raw_pages(store)


def test_bronze_keeps_exact_decimals_tombstones_and_raw_provenance():
    store = MemoryStore()
    tombstone = company("000000002", START, deleted=True)
    valued = company("000000001", START, capital=Decimal("1234567890.123456789"))
    data = [
        (START + timedelta(hours=1), {"company": valued}),
        (START + timedelta(hours=2), {"company": tombstone}),
    ]
    DiffSync(store).run(FakeApi(data), START, UNTIL)
    (table,) = bronze(store).values()
    rows = table.to_pylist()
    assert rows[0]["source_kind"] == "diff" and rows[1]["deleted"] is True
    payload = simplejson.loads(rows[0]["company_json"], use_decimal=True)
    assert payload["capital"] == Decimal("1234567890.123456789")
    raw_name = rows[0]["source_uri"].removeprefix("gs://test/")
    assert rows[0]["source_generation"] == str(store.objects[raw_name][1])


def test_real_http_client_bytes_are_archived_unchanged():
    store = MemoryStore()
    item = {"company": company("000000001", START)}
    body = b"[\n  " + json_bytes(item) + b"\n]"
    served = []

    def handler(request):
        if request.url.path.endswith("login"):
            return httpx.Response(200, json={"token": "synthetic-test-token"})
        if request.url.path.endswith("count"):
            return httpx.Response(200, json={"isNbResultsOver10000": False, "nbResults": 1})
        if "searchAfter" in request.url.params or served:
            return httpx.Response(200, content=b"[]")
        served.append(True)
        return httpx.Response(
            200, content=body, headers={"pagination-search-after": "opaque-cursor"}
        )

    api = RneApi(
        "unit-test-user",
        "never-log-this-password",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=lambda _: None,
    )
    DiffSync(store).run(api, START, START + timedelta(hours=6))
    (name,) = raw_pages(store)
    assert store.objects[name][0] == body
    assert "never-log-this-password" not in str(store.objects)
