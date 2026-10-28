import gzip
import hashlib
import io
import json
import zipfile
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import simplejson
from rne_fakes import MemoryStore

from industrial_intelligence.ingestion.rne.common import (
    Conflict,
    IngestionError,
    Paused,
    parse_time,
    validate_company,
)
from industrial_intelligence.ingestion.rne.companies import COMPANY_SCHEMA, company_row
from industrial_intelligence.ingestion.rne.materialize import StockMaterializer

REVISION = "b" * 64
DATE = "2026-01-01T01:00:00+01:00"


def company(siren="000000001", **extra):
    return {
        "siren": siren,
        "id": "synthetic-id",
        "updatedAt": DATE,
        "formality": {
            "siren": siren,
            "typePersonne": "M",
            "diffusionINSEE": "O",
            "diffusionCommerciale": False,
            "content": {
                "personneMorale": {
                    "identite": {
                        "entreprise": {
                            "denomination": "EXEMPLE",
                            "formeJuridique": "5710",
                            "codeApe": "6201Z",
                        }
                    }
                }
            },
        },
        **extra,
    }


def stock_zip(entries, compression=zipfile.ZIP_DEFLATED):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=compression, allowZip64=True) as archive:
        for filename, payload in entries.items():
            content = (
                payload
                if isinstance(payload, bytes)
                else simplejson.dumps(payload, use_decimal=True, separators=(",", ":")).encode()
            )
            archive.writestr(filename, content)
    return output.getvalue()


def prepared(entries=None, *, raw=None):
    if raw is None:
        raw = stock_zip(entries or {"stock_000001.json": [company()]})
    store = MemoryStore()
    name = f"raw/inpi/rne/stocks/{REVISION}/attempt/stock.zip"
    generation = store.put(name, raw)
    receipt = {
        "schema_version": 1,
        "source": "inpi_rne",
        "resource_type": "companies_stock",
        "remote_revision": REVISION,
        "object": name,
        "uri": store.uri(name),
        "generation": generation,
        "size": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "retrieved_at": DATE,
        "remote": {"filename": "stock_RNE_20260304.zip"},
    }
    store.write_json(name + ".manifest.json", receipt)
    store.write_json(f"control/inpi/rne/stocks/{REVISION}.json", receipt)
    materializer = StockMaterializer(store)
    root = "bronze/inpi/rne/companies/stock/stock_RNE_20260304"
    checkpoint = f"control/inpi/rne/materialize/{receipt['sha256']}/checkpoint.json"
    return store, materializer, root, checkpoint


def part(store, root, number=1):
    return store.read_json(f"{root}/_manifests/part-{number:06d}.json")[0]


def table(store, manifest):
    return pq.read_table(pa.BufferReader(store.objects[manifest["parquet"]["object"]][0]))


def test_parquet_has_fixed_schema_exact_payload_and_provenance():
    original = company(
        amount=Decimal("0.12345678901234567890123456789"), unknown={"future": [1, 2]}
    )
    store, materializer, root, _ = prepared({"stock_000001.json": [original]})
    result = materializer.run(REVISION)
    assert result["status"] == "complete" and result["valid_rows"] == 1
    output = table(store, part(store, root))
    assert output.schema.equals(COMPANY_SCHEMA)
    row = output.to_pylist()[0]
    assert row["siren"] == "000000001"  # Initial zeros preserved.
    assert row["denomination"] == "EXEMPLE" and row["forme_juridique"] == "5710"
    assert row["diffusion_commerciale"] is False
    assert row["source_updated_at"].hour == 0 and str(row["source_updated_at"].tzinfo) == "UTC"
    assert simplejson.loads(row["company_json"], use_decimal=True) == original
    assert row["source_member"] == "stock_000001.json" and row["source_row_number"] == 1
    assert row["source_kind"] == "stock"
    assert hashlib.sha256(row["company_json"].encode()).hexdigest() == row["company_sha256"]
    assert store.read_json("control/inpi/rne/checkpoint.json") == (None, 0)


def test_company_validation_returns_siren_and_update_time():
    original = company()
    assert validate_company(original) == ("000000001", parse_time(DATE))
    original["updatedAt"] = "invalid"
    with pytest.raises(IngestionError, match="ISO 8601"):
        validate_company(original)


def test_company_row_parses_each_timestamp_only_once(monkeypatch):
    parse = Mock(wraps=parse_time)
    monkeypatch.setattr("industrial_intelligence.ingestion.rne.common.parse_time", parse)
    monkeypatch.setattr("industrial_intelligence.ingestion.rne.companies.parse_time", parse)
    receipt = {
        "sha256": "c" * 64,
        "uri": "gs://test/raw.zip",
        "generation": 42,
        "retrieved_at": DATE,
    }
    row = company_row(company(), receipt, "stock_000001.json", 1)
    assert parse.call_count == 2  # Company updatedAt and source retrieved_at, once each.
    assert row["source_updated_at"] == row["source_retrieved_at"] == parse_time(DATE)


@pytest.mark.parametrize("first_branch", [None, {}])
def test_person_branch_selection_preserves_empty_mapping_precedence(first_branch):
    original = company()
    content = original["formality"]["content"]
    content["personnePhysique"] = content["personneMorale"]
    content["personneMorale"] = first_branch
    receipt = {
        "sha256": "c" * 64,
        "uri": "gs://test/raw.zip",
        "generation": 42,
        "retrieved_at": DATE,
    }
    row = company_row(original, receipt, "stock_000001.json", 1)
    assert row["denomination"] == (None if first_branch == {} else "EXEMPLE")


def test_partial_snapshot_is_not_published_as_complete_and_resumes_next_member():
    store, materializer, root, checkpoint = prepared(
        {"stock_000001.json": [company()], "stock_000002.json": [company("000000002")]}
    )
    first = materializer.run(REVISION, max_members=1)
    assert first["status"] == "partial" and first["members_done"] == 1
    assert store.read_json(root + "/_manifest.json")[0] is None
    assert store.read_json(checkpoint)[0]["next_index"] == 1
    second = materializer.run(REVISION, max_members=1)
    assert second["status"] == "complete" and second["valid_rows"] == 2
    snapshot = store.read_json(root + "/_manifest.json")[0]
    assert len(snapshot["parts"]) == 2 and snapshot["members_total"] == 2
    assert snapshot["global_siren_uniqueness_checked"] is False
    before = list(store.uploaded)
    assert materializer.run(REVISION)["members_processed_this_run"] == 0
    assert store.uploaded == before


def test_rejections_have_trace_and_reason_but_no_personal_values():
    invalid = company(siren="bad-id", private_value="do-not-copy-this-personal-value")
    store, materializer, root, _ = prepared({"stock_000001.json": [company(), invalid]})
    result = materializer.run(REVISION, batch_rows=1)
    assert result["valid_rows"] == 1 and result["rejected_rows"] == 1
    manifest = part(store, root)
    rejected = gzip.decompress(store.objects[manifest["rejects"]["object"]][0])
    row = json.loads(rejected)
    assert row["zip_row_number"] == 2 and row["zip_member"] == "stock_000001.json"
    assert "personal-value" not in rejected.decode() and "bad-id" not in rejected.decode()
    assert store.read_json(root + "/_manifest.json")[0]["quality_status"] == "requires_review"


@pytest.mark.parametrize(
    "error",
    [
        ValueError("private-message"),
        TypeError("private-message"),
        OverflowError("private-message"),
        UnicodeEncodeError("utf-8", "\ud800", 0, 1, "private-message"),
    ],
)
def test_normalization_errors_are_rejected_without_exposing_their_messages(monkeypatch, error):
    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr("industrial_intelligence.ingestion.rne.materialize.company_row", fail)
    store, materializer, root, _ = prepared()
    result = materializer.run(REVISION)
    assert result["valid_rows"] == 0 and result["rejected_rows"] == 1
    manifest = part(store, root)
    rejected = gzip.decompress(store.objects[manifest["rejects"]["object"]][0])
    assert json.loads(rejected)["reason"] == type(error).__name__
    assert b"private-message" not in rejected


def test_duplicates_are_counted_and_preserved_not_arbitrarily_dropped():
    store, materializer, root, _ = prepared({"stock_000001.json": [company(), company()]})
    result = materializer.run(REVISION, batch_rows=1)
    assert result["valid_rows"] == 2 and result["duplicate_sirens_within_members"] == 1
    assert table(store, part(store, root)).num_rows == 2
    assert store.read_json(root + "/_manifest.json")[0]["quality_status"] == "requires_review"


@pytest.mark.parametrize("payload", [b"{}", b"null", b"[", b"[{},", b"[] trailing"])
def test_invalid_root_or_truncated_json_does_not_commit_a_member(payload):
    store, materializer, root, checkpoint = prepared({"stock_000001.json": payload})
    with pytest.raises(IngestionError):
        materializer.run(REVISION)
    assert store.read_json(checkpoint)[0]["next_index"] == 0
    assert part(store, root) is None
    assert not store.uploaded
    assert store.read_json("control/inpi/rne/checkpoint.json")[0] is None


def test_zip_crc_failure_does_not_commit_outputs():
    raw = stock_zip({"stock_000001.json": [company()]}, compression=zipfile.ZIP_STORED)
    position = raw.index(b"000000001")
    corrupted = raw[:position] + b"1" + raw[position + 1 :]
    store, materializer, root, checkpoint = prepared(raw=corrupted)
    with pytest.raises(IngestionError):
        materializer.run(REVISION)
    assert store.read_json(checkpoint)[0]["next_index"] == 0
    assert part(store, root) is None and not store.uploaded


def test_manifest_commit_then_failed_checkpoint_reuses_outputs():
    store, materializer, root, checkpoint = prepared()
    store.fail_checkpoint_once = True
    with pytest.raises(IngestionError):
        materializer.run(REVISION)
    assert part(store, root) and store.read_json(checkpoint)[0]["next_index"] == 0
    uploaded = list(store.uploaded)
    assert materializer.run(REVISION)["status"] == "complete"
    assert store.uploaded == uploaded


def test_final_manifest_failure_is_recoverable_without_reprocessing():
    store, materializer, root, _ = prepared()
    store.fail_snapshot_once = True
    with pytest.raises(IngestionError):
        materializer.run(REVISION)
    assert store.read_json(root + "/_manifest.json")[0] is None
    uploaded = list(store.uploaded)
    assert materializer.run(REVISION)["status"] == "complete"
    assert store.uploaded == uploaded


def test_unacknowledged_output_from_a_crash_is_never_overwritten():
    store, materializer, root, checkpoint = prepared()
    store.put(root + "/_rejects/part-000001.jsonl.gz", b"left-by-a-crash")
    with pytest.raises(Conflict):
        materializer.run(REVISION)
    assert store.objects[root + "/_rejects/part-000001.jsonl.gz"][0] == b"left-by-a-crash"
    assert store.read_json(checkpoint)[0]["next_index"] == 0 and part(store, root) is None


def test_completed_member_with_missing_output_is_not_reused():
    store, materializer, root, _ = prepared()
    store.fail_checkpoint_once = True
    with pytest.raises(IngestionError):
        materializer.run(REVISION)
    manifest = part(store, root)
    del store.objects[manifest["parquet"]["object"]]
    with pytest.raises(IngestionError, match="missing"):
        materializer.run(REVISION)


def test_empty_array_is_valid_empty_parquet_not_a_silent_bad_root():
    store, materializer, root, _ = prepared({"stock_000001.json": []})
    assert materializer.run(REVISION)["valid_rows"] == 0
    assert table(store, part(store, root)).schema.equals(COMPANY_SCHEMA)
    assert table(store, part(store, root)).num_rows == 0


def test_optional_fields_can_be_absent_in_personne_physique():
    value = {"siren": "000000003", "updatedAt": DATE, "formality": {"typePersonne": "P"}}
    store, materializer, root, _ = prepared({"stock_000001.json": [value]})
    materializer.run(REVISION)
    row = table(store, part(store, root)).to_pylist()[0]
    assert row["type_personne"] == "P" and row["denomination"] is None
    assert row["diffusion_commerciale"] is None and row["deleted"] is None


def test_common_row_schema_can_represent_a_future_api_page_without_a_zip_member():
    receipt = {
        "sha256": "c" * 64,
        "uri": "gs://test/raw/page.json",
        "generation": 42,
        "retrieved_at": DATE,
    }
    row = company_row(company(), receipt, None, 1, source_kind="diff")
    assert row["source_kind"] == "diff" and row["source_member"] is None
    assert pa.Table.from_pylist([row], schema=COMPANY_SCHEMA).num_rows == 1


def test_unexpected_optional_type_is_rejected_not_silently_cast():
    value = company()
    value["formality"]["diffusionCommerciale"] = "false"
    store, materializer, root, _ = prepared({"stock_000001.json": [value]})
    assert materializer.run(REVISION)["rejected_rows"] == 1
    assert table(store, part(store, root)).num_rows == 0


def test_declared_size_and_execution_byte_limits_fail_closed():
    store, materializer, root, checkpoint = prepared()
    with pytest.raises(IngestionError, match="uncompressed limit"):
        materializer.run(REVISION, max_member_bytes=1)
    assert not store.uploaded
    with pytest.raises(Paused, match="byte budget"):
        materializer.run(REVISION, max_input_bytes=1)
    assert store.read_json(checkpoint)[0]["next_index"] == 0
    assert part(store, root) is None


@pytest.mark.parametrize("name", ["../stock_000001.json", "company.json"])
def test_member_names_are_validated_not_extracted_as_filesystem_paths(name):
    store, materializer, _, _ = prepared({name: [company()]})
    with pytest.raises(IngestionError, match="ZIP members"):
        materializer.run(REVISION)
    assert not store.uploaded


def test_archive_generation_change_refuses_materialization():
    store, materializer, _, _ = prepared()
    receipt = store.read_json(f"control/inpi/rne/stocks/{REVISION}.json")[0]
    store.put(receipt["object"], b"modified")
    with pytest.raises(IngestionError, match="generation/size"):
        materializer.run(REVISION)


def test_soft_time_budget_returns_partial_without_starting_a_member(monkeypatch):
    store, materializer, root, checkpoint = prepared()
    values = iter([0, 2000, 2000])
    monkeypatch.setattr(
        "industrial_intelligence.ingestion.rne.materialize.time",
        SimpleNamespace(monotonic=lambda: next(values)),
    )
    result = materializer.run(REVISION, max_seconds=1800)
    assert result["status"] == "partial" and result["members_done"] == 0
    assert store.read_json(checkpoint)[0]["next_index"] == 0
    assert part(store, root) is None and not store.uploaded


def test_byte_budget_preserves_completed_members_before_pausing():
    store, materializer, root, checkpoint = prepared(
        {"stock_000001.json": [company()], "stock_000002.json": [company("000000002")]}
    )
    receipt = store.read_json(f"control/inpi/rne/stocks/{REVISION}.json")[0]
    with store.open_reader(receipt["object"], generation=receipt["generation"]) as reader:
        with zipfile.ZipFile(reader) as archive:
            first_size = archive.infolist()[0].file_size
    with pytest.raises(Paused):
        materializer.run(REVISION, max_members=None, max_input_bytes=first_size)
    assert store.read_json(checkpoint)[0]["next_index"] == 1 and part(store, root)
    assert materializer.run(REVISION)["status"] == "complete"


def test_final_publication_detects_a_missing_previously_acknowledged_manifest():
    store, materializer, root, _ = prepared(
        {"stock_000001.json": [company()], "stock_000002.json": [company("000000002")]}
    )
    materializer.run(REVISION)
    del store.objects[root + "/_manifests/part-000001.json"]
    with pytest.raises(IngestionError, match="missing member manifests"):
        materializer.run(REVISION)
    assert store.read_json(root + "/_manifest.json")[0] is None


def test_final_publication_checks_totals_against_independent_member_manifests():
    store, materializer, root, checkpoint = prepared(
        {"stock_000001.json": [company()], "stock_000002.json": [company("000000002")]}
    )
    materializer.run(REVISION)
    manifest = part(store, root)
    manifest["valid_rows"] += 1
    store.put(root + "/_manifests/part-000001.json", manifest)
    with pytest.raises(IngestionError, match="row counts differ"):
        materializer.run(REVISION)
    assert store.read_json(root + "/_manifest.json")[0] is None
    assert store.read_json(checkpoint)[0]["next_index"] == 2


def test_completed_snapshot_must_still_match_the_checkpoint():
    store, materializer, root, _ = prepared()
    materializer.run(REVISION)
    manifest_name = root + "/_manifest.json"
    manifest = store.read_json(manifest_name)[0]
    manifest["valid_rows"] += 1
    store.put(manifest_name, manifest)
    before = list(store.uploaded)
    with pytest.raises(IngestionError, match="Final snapshot manifest differs"):
        materializer.run(REVISION)
    assert store.uploaded == before
    assert store.read_json(manifest_name)[0] == manifest


def test_large_company_above_old_16_mib_limit_is_preserved():
    original = company(large_field="x" * (17 * 1024**2))
    receipt = {
        "sha256": "c" * 64,
        "uri": "gs://test/raw.json",
        "generation": 1,
        "retrieved_at": DATE,
    }
    row = company_row(original, receipt, None, 1, source_kind="diff")
    assert len(row["company_json"].encode()) > 16 * 1024**2
    assert row["company_sha256"] == hashlib.sha256(row["company_json"].encode()).hexdigest()


def test_no_receipt_no_materialization():
    with pytest.raises(IngestionError, match="validated archive receipt"):
        StockMaterializer(MemoryStore()).run(REVISION)
