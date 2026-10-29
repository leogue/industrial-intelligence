import io
import re
from datetime import date, timedelta

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from rne_fakes import MemoryStore

from industrial_intelligence.ingestion.bodacc.archive import COLUMNS, DATASET_URL, archive
from industrial_intelligence.ingestion.rne.common import IngestionError

PREFIX = "dila/bodacc"
DAY1, DAY2, DAY3 = date(2008, 1, 1), date(2008, 1, 2), date(2008, 1, 3)
CHECKPOINT = f"control/{PREFIX}/checkpoint.json"


def export(ids, day, *, dates=None, drop=()):
    """Synthetic export with the published column names; only id and date carry values."""
    columns = {name: pa.array([None] * len(ids), pa.string()) for name in COLUMNS}
    columns["id"] = pa.array(ids, pa.string())
    columns["dateparution"] = pa.array(dates or [day] * len(ids), pa.date32())
    buffer = io.BytesIO()
    pq.write_table(pa.table({k: v for k, v in columns.items() if k not in drop}), buffer)
    return buffer.getvalue()


def client_for(days, *, counts=None, calls=None, responses=None):
    """``days`` maps a day to its notice ids; absent days have no notices."""

    def handler(request):
        if calls is not None:
            calls.append(request.url.path)
        if responses:
            return responses.pop(0)
        day = date.fromisoformat(re.search(r"date'(.+)'", request.url.params["where"])[1])
        ids = days.get(day, [])
        if request.url.path.endswith("/records"):
            return httpx.Response(200, json={"total_count": (counts or {}).get(day, len(ids))})
        assert str(request.url).startswith(DATASET_URL + "/exports/parquet")
        assert request.url.params["order_by"] == "id"
        body = ids if isinstance(ids, bytes) else export(ids, day)
        return httpx.Response(200, content=body)

    return httpx.Client(transport=httpx.MockTransport(handler))


def run(store, client, until=DAY3, **kwargs):
    return archive(store, PREFIX, client, until, sleep=lambda _: None, **kwargs)


def manifests(store, day):
    root = f"raw/{PREFIX}/annonces/dateparution={day.isoformat()}/"
    return [store.read_json(n)[0] for n in store.objects if n.startswith(root) and "manifest" in n]


def test_first_run_archives_every_day_since_2008_including_empty_days():
    store = MemoryStore()
    result = run(store, client_for({DAY2: ["A1", "B2"], DAY3: ["C3"]}))
    assert result == {
        "status": "up_to_date",
        "completed_until": "2008-01-03",
        "target": "2008-01-03",
        "days_checked": 3,
        "new_versions": 3,
        "changed_days": 0,
        "rows": 3,
    }
    [manifest] = manifests(store, DAY2)
    assert manifest["rows"] == 2
    assert manifest["object"] == (
        f"raw/{PREFIX}/annonces/dateparution=2008-01-02/{manifest['sha256']}.parquet"
    )
    assert pq.read_table(io.BytesIO(store.objects[manifest["object"]][0])).num_rows == 2
    assert manifest["retrieved_at"].endswith("Z")
    assert manifests(store, DAY1)[0]["rows"] == 0
    assert store.read_json(CHECKPOINT)[0]["completed_until"] == "2008-01-03"


def test_rerun_rechecks_recent_days_without_new_objects():
    store = MemoryStore()
    days = {DAY2: ["A1"], DAY3: ["C3"]}
    run(store, client_for(days))
    before = dict(store.objects)
    calls = []
    result = run(store, client_for(days, calls=calls), recheck_days=2)
    assert (result["days_checked"], result["new_versions"]) == (2, 0)
    assert len(calls) == 4  # count + export for DAY2 and DAY3, not DAY1
    assert store.objects.keys() == before.keys()


def test_republished_day_is_added_beside_the_previous_version():
    store = MemoryStore()
    run(store, client_for({DAY2: ["A1"]}))
    result = run(store, client_for({DAY2: ["A1", "A2"]}), recheck_days=30)
    assert (result["new_versions"], result["changed_days"]) == (1, 1)
    assert sorted(m["rows"] for m in manifests(store, DAY2)) == [1, 2]


def test_new_days_after_the_checkpoint_are_not_counted_as_changes():
    store = MemoryStore()
    run(store, client_for({DAY2: ["A1"]}), until=DAY2)
    result = run(store, client_for({DAY3: ["C3"]}), recheck_days=0)
    assert (result["days_checked"], result["new_versions"], result["changed_days"]) == (1, 1, 0)
    assert store.read_json(CHECKPOINT)[0]["completed_until"] == "2008-01-03"


@pytest.mark.parametrize(
    ("body", "count", "message"),
    [
        (export(["A1", "B2"], DAY2), 3, "count of 3"),
        (export(["A1", "A1"], DAY2), 2, "duplicate"),
        (export(["A1"], DAY2, dates=[DAY3]), 1, "another publication date"),
        (export(["A1"], DAY2, drop=("registre",)), 1, "registre"),
        (b"<html>maintenance</html>", 1, "readable Parquet"),
    ],
)
def test_invalid_export_stores_nothing_and_keeps_the_checkpoint(body, count, message):
    store = MemoryStore()
    run(store, client_for({}), until=DAY1)
    with pytest.raises(IngestionError, match=message):
        run(store, client_for({DAY2: body}, counts={DAY2: count}), recheck_days=0)
    assert manifests(store, DAY2) == []
    assert store.read_json(CHECKPOINT)[0]["completed_until"] == "2008-01-01"


def test_time_budget_saves_progress_and_the_next_run_resumes():
    store = MemoryStore()
    ticks = iter([0, 0, 0, 99])  # start, DAY1, DAY2, then the budget is spent before DAY3
    result = run(store, client_for({}), max_seconds=10, clock=lambda: next(ticks))
    assert (result["status"], result["completed_until"]) == ("partial", "2008-01-02")
    assert run(store, client_for({}), recheck_days=0)["completed_until"] == "2008-01-03"


def test_retries_throttling_then_succeeds():
    responses = [httpx.Response(429, headers={"Retry-After": "1"}), httpx.Response(503)]
    result = run(MemoryStore(), client_for({}, responses=responses), until=DAY1)
    assert result["completed_until"] == "2008-01-01"


def test_client_errors_fail_without_retry():
    calls = []
    client = client_for({}, calls=calls, responses=[httpx.Response(400)])
    with pytest.raises(IngestionError, match="HTTP 400"):
        run(MemoryStore(), client, until=DAY1)
    assert len(calls) == 1


def test_until_before_2008_is_rejected():
    with pytest.raises(IngestionError, match="Invalid bounds"):
        run(MemoryStore(), client_for({}), until=DAY1 - timedelta(days=1))
