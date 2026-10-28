"""Stream stock ZIP entries from GCS into resumable, fixed-schema Parquet parts.

A member manifest is the commit point. An API differential checkpoint is never
created/modified here. A snapshot manifest is published only after ALL members.
"""

import gzip
import hashlib
import logging
import re
import time
import uuid
import zipfile

import ijson
import pyarrow as pa
import pyarrow.parquet as pq

from .common import (
    IngestionError,
    Paused,
    json_bytes,
    parse_time,
    timestamp,
    utc_now,
    validate_prefix,
)
from .companies import COMPANY_SCHEMA, SCHEMA_VERSION, company_row
from .log import log_event
from .storage import DigestWriter

COUNT_FIELDS = ("input_rows", "valid_rows", "rejected_rows", "duplicate_sirens")


def _rows(source):
    # ijson.items(..., 'item') alone would silently accept a root object as zero rows.
    events = ijson.parse(source, use_float=False)
    if next(events, None) != ("", "start_array", None):
        raise IngestionError("A stock member must contain a root JSON array")
    yield from ijson.items(events, "item")


def write_member(
    archive,
    member,
    receipt,
    data_stream,
    rejects_stream,
    *,
    batch_rows=500,
    max_batch_bytes=8 * 1024**2,
):
    """Consume the entire member (including ZIP CRC validation), bounded by batches."""
    data = DigestWriter(data_stream)
    rejects = DigestWriter(rejects_stream)
    counts = dict.fromkeys(COUNT_FIELDS, 0)
    seen_sirens = set()
    min_updated = max_updated = None
    batch, batch_bytes = [], 0
    with gzip.GzipFile(fileobj=rejects, mode="wb", mtime=0) as rejected:
        with pq.ParquetWriter(data, COMPANY_SCHEMA, compression="zstd") as writer:
            try:
                with archive.open(member) as source:
                    for number, company in enumerate(_rows(source), 1):
                        counts["input_rows"] += 1
                        try:
                            row = company_row(company, receipt, member.filename, number)
                        except (
                            IngestionError,
                            ValueError,
                            TypeError,
                            OverflowError,
                        ) as error:
                            # Keep source values and arbitrary exception messages out of rejects.
                            reason = (
                                str(error)
                                if isinstance(error, IngestionError)
                                else type(error).__name__
                            )
                            rejected.write(
                                json_bytes(
                                    {
                                        "archive_sha256": receipt["sha256"],
                                        "zip_member": member.filename,
                                        "zip_row_number": number,
                                        "reason": reason,
                                    }
                                )
                                + b"\n"
                            )
                            counts["rejected_rows"] += 1
                            continue
                        updated = row["source_updated_at"]
                        min_updated = min(min_updated, updated) if min_updated else updated
                        max_updated = max(max_updated, updated) if max_updated else updated
                        if row["siren"] in seen_sirens:
                            counts["duplicate_sirens"] += 1
                        seen_sirens.add(row["siren"])
                        # Preserve duplicates, do not choose an arbitrary legal version here.
                        batch.append(row)
                        batch_bytes += len(row["company_json"].encode("utf-8"))
                        counts["valid_rows"] += 1
                        if len(batch) >= batch_rows or batch_bytes >= max_batch_bytes:
                            writer.write_table(pa.Table.from_pylist(batch, schema=COMPANY_SCHEMA))
                            batch, batch_bytes = [], 0
                    # Iterating to EOF is mandatory: ZipExtFile checks its CRC at EOF.
                    if source.tell() != member.file_size:
                        raise IngestionError("Uncompressed member size mismatch")
                if batch:
                    writer.write_table(pa.Table.from_pylist(batch, schema=COMPANY_SCHEMA))
            except (ijson.JSONError, zipfile.BadZipFile, EOFError):
                raise IngestionError("Invalid/truncated JSON or failed ZIP member CRC") from None
    assert counts["input_rows"] == counts["valid_rows"] + counts["rejected_rows"]
    return {
        **counts,
        "min_updated_at": timestamp(min_updated) if min_updated else None,
        "max_updated_at": timestamp(max_updated) if max_updated else None,
        "parquet": data.metadata(),
        "rejects": rejects.metadata(),
    }


class StockMaterializer:
    def __init__(self, store, prefix: str = "inpi/rne"):
        self.store = store
        self.prefix = validate_prefix(prefix)
        self.logger = logging.getLogger(__name__)

    def _source(self, revision):
        if not re.fullmatch(r"[a-f0-9]{64}", revision):
            raise IngestionError("An explicit 64-character stock revision is required")
        receipt, _ = self.store.read_json(f"control/{self.prefix}/stocks/{revision}.json")
        if receipt is None:
            raise IngestionError("No validated archive receipt for this stock revision")
        if (
            receipt.get("schema_version") != 1
            or receipt.get("resource_type") != "companies_stock"
            or not re.fullmatch(r"[a-f0-9]{64}", receipt.get("sha256", ""))
            or not receipt.get("object", "").startswith(f"raw/{self.prefix}/stocks/")
            or receipt.get("uri") != self.store.uri(receipt["object"])
            or receipt.get("remote_revision") != revision
            or type(receipt.get("generation")) is not int
            or receipt["generation"] <= 0
            or type(receipt.get("size")) is not int
            or receipt["size"] <= 0
        ):
            raise IngestionError("Invalid or out-of-namespace stock receipt")
        if self.store.stat(receipt["object"]) != {
            "size": receipt["size"],
            "generation": receipt["generation"],
        }:
            raise IngestionError("Archive missing or generation/size changed")
        parse_time(receipt.get("retrieved_at"))
        original, _ = self.store.read_json(receipt["object"] + ".manifest.json")
        if original != receipt:
            raise IngestionError("Archive receipt differs from the immutable source manifest")
        return receipt

    def _member_manifest(self, root, index):
        return f"{root}/manifests/part-{index + 1:06d}.json"

    def _validate_part(self, part, source, member):
        if (
            part.get("schema_version") != SCHEMA_VERSION
            or part.get("archive_sha256") != source["sha256"]
            or part.get("member") != member.filename
            or part.get("member_crc32") != member.CRC
            or part.get("member_uncompressed_bytes") != member.file_size
        ):
            raise IngestionError("Member manifest does not match this source/schema")
        for kind in ("parquet", "rejects"):
            artifact = part[kind]
            if self.store.stat(artifact["object"]) != {
                "size": artifact["size"],
                "generation": artifact["generation"],
            }:
                raise IngestionError("Validated output missing or generation/size changed")

    def _extract(self, archive, member, source, root, index, batch_rows, max_batch_bytes):
        attempt = uuid.uuid4().hex
        part_id = f"part-{index + 1:06d}"
        data_name = f"{root}/parts/{part_id}/{attempt}.parquet"
        rejects_name = f"{root}/rejects/{part_id}/{attempt}.jsonl.gz"
        outcome = {}

        def produce_data(data_stream):
            def produce_rejects(rejects_stream):
                outcome.update(
                    write_member(
                        archive,
                        member,
                        source,
                        data_stream,
                        rejects_stream,
                        batch_rows=batch_rows,
                        max_batch_bytes=max_batch_bytes,
                    )
                )
                return outcome["rejects"]

            outcome["rejects"] = self.store.upload_stream(
                rejects_name, produce_rejects, content_type="application/gzip"
            )
            return outcome["parquet"]

        # Unique attempt paths: a crash cannot require overwriting a finished original.
        outcome["parquet"] = self.store.upload_stream(
            data_name, produce_data, content_type="application/vnd.apache.parquet"
        )
        for kind, name in [("parquet", data_name), ("rejects", rejects_name)]:
            outcome[kind].update({"object": name, "uri": self.store.uri(name)})
        return {
            "schema_version": SCHEMA_VERSION,
            "source": "inpi_rne",
            "archive_sha256": source["sha256"],
            "archive_uri": source["uri"],
            "archive_generation": source["generation"],
            "member": member.filename,
            "member_crc32": member.CRC,
            "member_uncompressed_bytes": member.file_size,
            "member_compressed_bytes": member.compress_size,
            "completed_at": timestamp(utc_now()),
            **outcome,
        }

    def _publish_snapshot(self, root, source, members, state):
        manifest_name = root + "/manifest.json"
        manifest, _ = self.store.read_json(manifest_name)
        if manifest is not None:
            if any(manifest.get(key) != value for key, value in state.items()):
                raise IngestionError("Final snapshot manifest differs from completed state")
            return

        # Independently check all acknowledged parts before publishing the snapshot.
        totals = dict.fromkeys(COUNT_FIELDS, 0)
        for index, member in enumerate(members):
            part, _ = self.store.read_json(self._member_manifest(root, index))
            if part is None:
                raise IngestionError("Cannot publish a snapshot with missing member manifests")
            self._validate_part(part, source, member)
            for key in COUNT_FIELDS:
                totals[key] += part[key]
        if any(state[key] != value for key, value in totals.items()):
            raise IngestionError("Snapshot row counts differ from member manifests")
        self.store.write_json(
            manifest_name,
            {
                "status": "complete",
                "schema_version": SCHEMA_VERSION,
                "archive_uri": source["uri"],
                "completed_at": timestamp(utc_now()),
                "global_siren_uniqueness_checked": False,
                "quality_status": "member_checks_passed"
                if (
                    state["valid_rows"]
                    and not state["rejected_rows"]
                    and not state["duplicate_sirens"]
                )
                else "requires_review",
                "columns": [
                    {"name": f.name, "type": str(f.type), "nullable": f.nullable}
                    for f in COMPANY_SCHEMA
                ],
                "parts": [
                    self.store.uri(self._member_manifest(root, i)) for i in range(len(members))
                ],
                **state,
            },
        )

    @staticmethod
    def _members(archive):
        """Validate the ZIP directory before any member is materialized."""
        members = [member for member in archive.infolist() if not member.is_dir()]
        names = [member.filename for member in members]
        if (
            not members
            or len(set(names)) != len(names)
            or any(not re.fullmatch(r"stock_[0-9]{6}\.json", name) for name in names)
            or any(member.flag_bits & 1 for member in members)
        ):
            raise IngestionError("Unexpected/duplicate/encrypted stock ZIP members")
        return sorted(members, key=lambda member: member.filename)

    def _load_checkpoint(self, name, source, members):
        """Create or resume state only if it matches this exact archive and directory."""
        identity = hashlib.sha256(
            json_bytes([[m.filename, m.file_size, m.compress_size, m.CRC] for m in members])
        ).hexdigest()
        state, generation = self.store.read_json(name)
        if state is None:
            state = {
                "schema_version": SCHEMA_VERSION,
                "archive_sha256": source["sha256"],
                "archive_generation": source["generation"],
                "members_digest": identity,
                "members_total": len(members),
                "total_uncompressed_bytes": sum(m.file_size for m in members),
                "next_index": 0,
                **dict.fromkeys(COUNT_FIELDS, 0),
            }
            generation = self.store.write_json(name, state)
        if (
            state.get("schema_version") != SCHEMA_VERSION
            or state.get("archive_sha256") != source["sha256"]
            or state.get("archive_generation") != source["generation"]
            or state.get("members_digest") != identity
            or not 0 <= state["next_index"] <= len(members)
        ):
            raise IngestionError("Materialization checkpoint does not match the source")
        return state, generation

    def run(
        self,
        revision,
        *,
        max_members=1,
        max_input_bytes=128 * 1024**2,
        max_member_bytes=128 * 1024**2,
        batch_rows=500,
        max_batch_bytes=8 * 1024**2,
        max_seconds=1800,
    ):
        if (
            (max_members is not None and max_members < 1)
            or max_input_bytes < 1
            or max_member_bytes < 1
            or batch_rows < 1
            or max_batch_bytes < 1
            or max_seconds < 1
        ):
            raise IngestionError("Invalid materialization limits")
        started = time.monotonic()
        source = self._source(revision)
        root = (
            f"bronze/{self.prefix}/companies/snapshot_id={source['sha256']}"
            f"/schema_version={SCHEMA_VERSION}"
        )
        checkpoint = (
            f"control/{self.prefix}/materialize/{source['sha256']}"
            f"/v{SCHEMA_VERSION}/checkpoint.json"
        )
        with (
            self.store.open_reader(source["object"], generation=source["generation"]) as reader,
            zipfile.ZipFile(reader) as archive,
        ):
            members = self._members(archive)
            state, generation = self._load_checkpoint(checkpoint, source, members)
            processed = input_bytes = 0
            while state["next_index"] < len(members):
                if time.monotonic() - started >= max_seconds:
                    break  # Stop cleanly before the Cloud Run hard timeout.
                if max_members is not None and processed >= max_members:
                    break
                index = state["next_index"]
                member = members[index]
                if member.file_size > max_member_bytes:
                    raise IngestionError("ZIP member exceeds the configured uncompressed limit")
                manifest_name = self._member_manifest(root, index)
                part, _ = self.store.read_json(manifest_name)
                if part is None:
                    if input_bytes + member.file_size > max_input_bytes:
                        raise Paused("Materialization byte budget reached; completed members saved")
                    part = self._extract(
                        archive, member, source, root, index, batch_rows, max_batch_bytes
                    )
                    self.store.write_json(manifest_name, part)
                    input_bytes += member.file_size
                else:
                    self._validate_part(part, source, member)
                # Commit only AFTER both outputs and the member manifest are durable.
                for field in COUNT_FIELDS:
                    state[field] += part[field]
                state["next_index"] = index + 1
                generation = self.store.write_json(checkpoint, state, generation=generation)
                processed += 1
                log_event(
                    self.logger,
                    "stock_member_materialized",
                    member=member.filename,
                    valid_rows=part["valid_rows"],
                    rejected_rows=part["rejected_rows"],
                    members_done=state["next_index"],
                    members_total=len(members),
                )
            complete = state["next_index"] == len(members)
            if complete:
                self._publish_snapshot(root, source, members, state)
            return {
                "status": "complete" if complete else "partial",
                "snapshot_uri": self.store.uri(root),
                "members_processed_this_run": processed,
                "members_done": state["next_index"],
                "members_total": len(members),
                "valid_rows": state["valid_rows"],
                "rejected_rows": state["rejected_rows"],
                "duplicate_sirens_within_members": state["duplicate_sirens"],
                "input_bytes_this_run": input_bytes,
                "elapsed_seconds": round(time.monotonic() - started, 3),
            }
