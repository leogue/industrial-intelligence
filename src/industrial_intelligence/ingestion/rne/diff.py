"""Differential RNE sync: fill the gap after the stock, then stay up to date daily.

One loop, one checkpoint (``control/{prefix}/diff/checkpoint.json``):

1. Open a window ``[completed_until, to]`` sized so ``/companies/diff/count`` <= 10,000.
2. Page through ``/companies/diff``; store each raw JSON page, then save the cursor.
3. On the empty final page, check the total against the count, write one Bronze
   Parquet file for the window, then advance ``completed_until``.

The first run starts at ``start`` (assumed stock boundary, e.g. 2026-03-01). Later
runs resume the checkpoint, so the daily job catches up within its request budget
and then only collects the previous day. Budget exhaustion is not a failure.
"""

import hashlib
import logging
import time
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

from .common import IngestionError, Paused, parse_time, timestamp, utc_now, validate_prefix
from .companies import COMPANY_SCHEMA, diff_rows
from .log import log_event
from .storage import DigestWriter

FIRST_WINDOW = timedelta(hours=6)
MAX_WINDOW = timedelta(days=1)
logger = logging.getLogger(__name__)


class DiffSync:
    def __init__(self, store, prefix: str = "inpi/rne"):
        self.store = store
        self.prefix = validate_prefix(prefix)
        self.checkpoint = f"control/{prefix}/diff/checkpoint.json"
        self._rows = {}  # Parsed rows of pages fetched by this run, keyed by raw object.

    def run(self, api, start: datetime, until: datetime, *, max_seconds: int = 20000) -> dict:
        if start >= until or until > utc_now() or max_seconds < 1:
            raise IngestionError("Invalid sync bounds: expected start < until <= now")
        deadline = time.monotonic() + max_seconds
        state, generation = self.store.read_json(self.checkpoint)
        if state is None:
            state = {
                "schema_version": 1,
                "start": timestamp(start),
                "completed_until": timestamp(start),
                "window": None,
            }
            generation = self.store.write_json(self.checkpoint, state)
        elif state.get("schema_version") != 1 or state.get("start") != timestamp(start):
            raise IngestionError("Checkpoint was started from another --start; keep the same one")

        def save():
            nonlocal generation
            generation = self.store.write_json(self.checkpoint, state, generation=generation)

        log_event(
            logger,
            "sync_started",
            start=state["start"],
            completed_until=state["completed_until"],
            target=timestamp(until),
            resumes_window=state["window"] is not None,
        )

        span, windows, records, paused = FIRST_WINDOW, 0, 0, None
        try:
            while state["window"] is not None or parse_time(state["completed_until"]) < until:
                if time.monotonic() >= deadline:
                    raise Paused("Time budget reached; the next run resumes the checkpoint")
                if state["window"] is None:
                    begin = parse_time(state["completed_until"])
                    state["window"] = self._open_window(api, begin, min(begin + span, until))
                    save()
                window = state["window"]
                begin, end = parse_time(window["from"]), parse_time(window["to"])
                body, items, cursor = api.page(begin, end, window["cursor"])
                if items:
                    if not cursor or cursor == window["cursor"]:
                        raise IngestionError("Pagination cursor missing or not advancing")
                    window["pages"].append(self._store_page(window, body))
                    window["records"] += len(items)
                    window["cursor"] = cursor
                    if window["records"] > window["expected"]:
                        self._restart(state, save, window)
                    save()
                    continue
                if window["records"] != window["expected"]:
                    self._restart(state, save, window)
                self._write_bronze(window)
                state["completed_until"], state["window"] = window["to"], None
                save()
                windows += 1
                records += window["records"]
                span = min(2 * (end - begin), MAX_WINDOW)
                log_event(
                    logger,
                    "window_completed",
                    window_from=window["from"],
                    window_to=window["to"],
                    records=window["records"],
                    pages=len(window["pages"]),
                    api_requests=api.requests,
                )
        except Paused as error:
            paused = str(error)  # Budget reached: progress is saved, the job still succeeds.
        behind = until - parse_time(state["completed_until"])
        return {
            "status": "partial" if paused else "up_to_date",
            "reason": paused,
            "completed_until": state["completed_until"],
            "target": timestamp(until),
            "days_behind": round(max(behind.total_seconds(), 0) / 86400, 2),
            "windows": windows,
            "records": records,
            "api_requests": api.requests,
        }

    @staticmethod
    def _open_window(api, begin: datetime, end: datetime) -> dict:
        """Halve the window until the provider returns an exact count (<= 10,000)."""
        expected = api.count(begin, end)
        while expected is None:
            if end - begin <= timedelta(seconds=1):
                raise IngestionError("More than 10,000 changes in a one-second window")
            end = begin + (end - begin) / 2
            expected = api.count(begin, end)
        return {
            "from": timestamp(begin),
            "to": timestamp(end),
            "expected": expected,
            "records": 0,
            "cursor": None,
            "pages": [],
        }

    @staticmethod
    def _restart(state, save, window):
        # The count moved during pagination: collect the whole window again, loudly.
        log_event(
            logger,
            "window_restarted",
            logging.WARNING,
            window_from=window["from"],
            window_to=window["to"],
            records=window["records"],
            expected=window["expected"],
        )
        state["window"] = None
        save()
        raise IngestionError(
            f"Window {window['from']}..{window['to']}: {window['records']} records for a "
            f"count of {window['expected']}; it restarts from scratch on the next run"
        )

    def _directory(self, window) -> str:
        return f"companies/diff/from={window['from']}/to={window['to']}"

    def _store_page(self, window, body: bytes) -> dict:
        """Content-addressed raw page: a replay never overwrites or duplicates it."""
        digest = hashlib.sha256(body).hexdigest()
        name = f"raw/{self.prefix}/{self._directory(window)}/{digest}.json"
        self.store.create_bytes(name, body, "application/json")
        stored = self.store.stat(name)
        if stored is None or stored["size"] != len(body):
            raise IngestionError("Raw page missing or size changed after upload")
        page = {
            "object": name,
            "sha256": digest,
            "size": len(body),
            "generation": stored["generation"],
            "retrieved_at": timestamp(utc_now()),
        }
        # Validate before acknowledging the cursor: a bad page blocks the window.
        self._rows[name] = diff_rows(body, self._provenance(page))
        return page

    def _provenance(self, page) -> dict:
        return {**page, "uri": self.store.uri(page["object"])}

    def _write_bronze(self, window) -> None:
        name = f"bronze/{self.prefix}/{self._directory(window)}/companies.parquet"
        if not window["records"] or self.store.stat(name) is not None:
            return  # Empty window, or already written before an interrupted checkpoint save.
        rows = []
        for page in window["pages"]:
            if page["object"] not in self._rows:  # Page fetched by an earlier run.
                body = self.store.read_bytes(page["object"], generation=page["generation"])
                self._rows[page["object"]] = diff_rows(body, self._provenance(page))
            rows.extend(self._rows.pop(page["object"]))

        def produce(stream):
            writer = DigestWriter(stream)
            table = pa.Table.from_pylist(rows, schema=COMPANY_SCHEMA)
            pq.write_table(table, writer, compression="zstd")
            return writer.metadata()

        self.store.upload_stream(name, produce, content_type="application/vnd.apache.parquet")
