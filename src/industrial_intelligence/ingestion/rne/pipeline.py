"""Two batch operations. Raw ingestion only: no ETL, PDF downloads or OCR."""

import hashlib
import uuid
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timedelta

from .common import (
    Conflict,
    IngestionError,
    Paused,
    company_siren,
    parse_time,
    timestamp,
    utc_now,
    validate_prefix,
)


class RnePipeline:
    def __init__(self, store, prefix: str = "inpi/rne"):
        prefix = validate_prefix(prefix)
        self.store = store
        self.raw = f"raw/{prefix}"
        self.control = f"control/{prefix}"
        self.checkpoint = f"{self.control}/checkpoint.json"
        self.run_id = uuid.uuid4().hex

    @contextmanager
    def exclusive(self):
        """No automatic lock expiry: a killed worker must not overlap its replacement."""
        self.run_id = uuid.uuid4().hex
        name = f"{self.control}/lock.json"
        try:
            generation = self.store.write_json(
                name, {"run_id": self.run_id, "started_at": timestamp(utc_now())}
            )
        except Conflict:
            raise IngestionError(
                "RNE is locked; inspect running jobs before manual recovery"
            ) from None
        try:
            yield
        finally:
            self.store.delete(name, generation=generation)

    def bootstrap(
        self,
        source,
        filename: str,
        delta_from: datetime,
        *,
        allow_large_download: bool = False,
        max_stock_bytes: int = 20 * 1024**3,
    ) -> dict:
        """Archive FTP -> GCS, then explicitly initialize the differential boundary."""
        if delta_from >= utc_now():
            raise IngestionError("Initial differential boundary must be in the past")
        with self.exclusive():
            state, _ = self.store.read_json(self.checkpoint)
            if state is not None:
                raise IngestionError("Checkpoint already initialized; bootstrap cannot reset it")
            stock = source.stat(filename)
            if stock.size > max_stock_bytes:
                raise IngestionError("Stock exceeds configured byte limit")
            if stock.size > 100 * 1024**2 and not allow_large_download:
                raise IngestionError(
                    "Stock exceeds 100 MiB; explicit --allow-large-download required"
                )
            receipt_name = f"{self.control}/stocks/{stock.revision}.json"
            receipt, _ = self.store.read_json(receipt_name)
            if receipt is not None:
                if receipt.get("source_host") != source.host:
                    raise IngestionError("Stock receipt belongs to a different FTP host")
                actual = self.store.stat(receipt["object"])
                if actual != {"size": receipt["size"], "generation": receipt["generation"]}:
                    raise IngestionError("Previously validated archive is missing or changed")
            else:
                # Run ID prevents a crash between archive finalization and receipt creation
                # from ever forcing an overwrite. Unreferenced objects can be reviewed later.
                name = f"{self.raw}/stocks/{stock.revision}/{self.run_id}/{stock.filename}"
                result = self.store.upload_stream(
                    name, lambda writer: source.transfer(stock, writer)
                )
                if result["size"] != stock.size:
                    raise IngestionError("Archive size verification failed")
                receipt = {
                    "schema_version": 1,
                    "source": "inpi_rne",
                    "resource_type": "companies_stock",
                    "run_id": self.run_id,
                    "source_host": source.host,
                    "source_protocol": "ftp",
                    "remote": asdict(stock),
                    "remote_revision": stock.revision,
                    "object": name,
                    "uri": self.store.uri(name),
                    "retrieved_at": timestamp(utc_now()),
                    "content_type": "application/zip",
                    **result,
                }
                self.store.write_json(name + ".manifest.json", receipt)
                self.store.write_json(receipt_name, receipt)
            # A declared conservative boundary, NEVER inferred from filename or MDTM.
            state = {
                "schema_version": 1,
                "bootstrap_receipt": receipt_name,
                "initial_from": timestamp(delta_from),
                "completed_until": timestamp(delta_from),
                "batch": None,
            }
            self.store.write_json(self.checkpoint, state)
            return {
                "status": "initialized",
                "archive_uri": receipt["uri"],
                "bytes": receipt["size"],
            }

    def refresh(
        self,
        api,
        until: datetime,
        *,
        window_hours: int = 6,
        overlap_hours: int = 24,
        max_pages: int = 4500,
        restart_window: bool = False,
    ) -> dict:
        if until > utc_now() or window_hours < 1 or overlap_hours < 0 or max_pages < 1:
            raise IngestionError("Invalid refresh bounds or limits")
        with self.exclusive():
            state, generation = self.store.read_json(self.checkpoint)
            if state is None:
                raise IngestionError(
                    "No checkpoint: complete bootstrap with an explicit delta-from first"
                )
            if state.get("schema_version") != 1:
                raise IngestionError("Unsupported checkpoint version")

            # Small closure makes every checkpoint a compare-and-swap, not a blind overwrite.
            def save():
                nonlocal generation
                generation = self.store.write_json(self.checkpoint, state, generation=generation)

            batch = state["batch"]
            if batch is None:
                completed = parse_time(state["completed_until"])
                if until <= completed:
                    return {"status": "up_to_date", "pages": 0, "records": 0}
                start = max(
                    parse_time(state["initial_from"]), completed - timedelta(hours=overlap_hours)
                )
                batch = {"until": timestamp(until), "next_from": timestamp(start), "active": None}
                state["batch"] = batch
                save()
            # A resumed execution finishes the original frozen batch, not a moving horizon.
            target = parse_time(batch["until"])
            if until < target:
                raise IngestionError(
                    "Requested end precedes the pending batch; resume its original bounds"
                )
            if restart_window and batch["active"] is not None:
                # Replay from the last COMPLETE window, without deleting any original.
                # Useful after a page-size change or an invalidated source cursor.
                batch["active"] = None
                save()
            pages = records = 0
            while parse_time(batch["next_from"]) < target:
                if pages >= max_pages:
                    raise Paused("Page budget reached; checkpoint saved for the next execution")
                if batch["active"] is None:
                    start = parse_time(batch["next_from"])
                    end = min(start + timedelta(hours=window_hours), target)
                    # Keep each query below the documented 10,000-result ceiling.
                    while api.count_over_limit(start, end):
                        if end - start <= timedelta(seconds=1):
                            raise IngestionError("More than 10,000 changes in a one-second window")
                        end = start + (end - start) / 2
                    batch["active"] = {
                        "from": timestamp(start),
                        "to": timestamp(end),
                        "cursor": None,
                        "seen_cursors": [],
                        "page_size": api.page_size,
                        "pages": [],
                        "records": 0,
                    }
                    save()
                active = batch["active"]
                if api.page_size != active["page_size"]:
                    raise IngestionError("Resume with the original page size")
                start, end = parse_time(active["from"]), parse_time(active["to"])
                body, items, next_cursor = api.page(start, end, active["cursor"])
                self._validate_page(items, start, end)
                if active["records"] + len(items) > 10000:
                    raise IngestionError(
                        "Window exceeded the count ceiling; restart with narrower bounds"
                    )
                if items and (
                    not next_cursor
                    or next_cursor == active["cursor"]
                    or next_cursor in active["seen_cursors"]
                ):
                    raise IngestionError(
                        "Missing or repeated differential cursor; window not completed"
                    )
                digest = hashlib.sha256(body).hexdigest()
                directory = f"{self.raw}/companies/from={active['from']}/to={active['to']}"
                name = f"{directory}/{digest}.json"
                self.store.create_bytes(name, body, "application/json")
                page = {
                    "uri": self.store.uri(name),
                    "sha256": digest,
                    "size": len(body),
                    "retrieved_at": timestamp(utc_now()),
                    "cursor_in": active["cursor"],
                    "cursor_out": next_cursor,
                    "records": len(items),
                }
                active["pages"].append(page)
                active["records"] += len(items)
                pages += 1
                records += len(items)
                if items:
                    active["seen_cursors"].append(next_cursor)
                    active["cursor"] = next_cursor
                else:
                    # Only an explicit empty page closes a window, never just a short page.
                    manifest = {
                        "schema_version": 1,
                        "source": "inpi_rne",
                        "resource_type": "companies_diff",
                        "endpoint": "/api/companies/diff",
                        "run_id": self.run_id,
                        "completed_at": timestamp(utc_now()),
                        "from_exclusive": active["from"],
                        "to_inclusive": active["to"],
                        "page_size": api.page_size,
                        "pages": active["pages"],
                        "records": active["records"],
                    }
                    self.store.write_json(f"{directory}/manifest-{self.run_id}.json", manifest)
                    batch["next_from"] = active["to"]
                    state["completed_until"] = timestamp(
                        max(parse_time(state["completed_until"]), end)
                    )
                    batch["active"] = None
                save()  # Raw body + completion manifest (if any) must exist first.
            state["batch"] = None
            save()
            return {
                "status": "completed",
                "pages": pages,
                "records": records,
                "completed_until": state["completed_until"],
            }

    @staticmethod
    def _validate_page(items: list, start: datetime, end: datetime) -> None:
        seen = set()
        for item in items:
            if not isinstance(item, dict) or "company" not in item:
                raise IngestionError("Unexpected companies differential envelope")
            company = item["company"]
            siren = company_siren(company)
            if siren in seen:
                raise IngestionError("Duplicate SIREN within a differential page")
            seen.add(siren)
            if not start < parse_time(company["updatedAt"]) <= end:
                # Do not silently discard changes or move the cursor on an unqualified filter.
                raise IngestionError(
                    "Company updatedAt outside requested window; investigate source filters"
                )
