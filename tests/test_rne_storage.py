import hashlib
import io
from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import NotFound, PreconditionFailed

from industrial_intelligence.ingestion.rne.common import Conflict, IngestionError
from industrial_intelligence.ingestion.rne.storage import DigestWriter, GcsStore


def test_digest_writer_counts_all_writes_without_closing_the_outer_stream():
    stream = io.BytesIO()
    writer = DigestWriter(stream)
    assert writer.write(b"first") == 5
    assert writer.write(b"second") == 6
    writer.flush()
    writer.close()
    assert stream.getvalue() == b"firstsecond"
    assert writer.tell() == 11 and not writer.closed
    assert writer.metadata() == {
        "size": 11,
        "sha256": hashlib.sha256(b"firstsecond").hexdigest(),
    }


def test_digest_writer_refuses_to_acknowledge_a_short_write():
    class ShortStream(io.BytesIO):
        def write(self, data):
            return super().write(data[:-1])

    writer = DigestWriter(ShortStream())
    with pytest.raises(IngestionError, match="Incomplete output write"):
        writer.write(b"test")
    assert writer.tell() == 0


def store_and_blob():
    client = MagicMock()
    blob = client.bucket.return_value.blob.return_value
    blob.generation = 42
    blob.size = 8
    return GcsStore("test", client), blob


def test_control_read_pins_the_download_to_the_read_generation():
    store, blob = store_and_blob()
    blob.download_as_bytes.return_value = b'{"schema_version":1}'
    assert store.read_json("checkpoint.json") == ({"schema_version": 1}, 42)
    blob.download_as_bytes.assert_called_once_with(if_generation_match=42)


def test_absent_control_is_distinct_from_an_invalid_read():
    store, blob = store_and_blob()
    blob.reload.side_effect = NotFound("not found")
    assert store.read_json("checkpoint.json") == (None, 0)


def test_control_write_uses_compare_and_swap():
    store, blob = store_and_blob()
    assert store.write_json("checkpoint.json", {"value": 1}, generation=7) == 42
    assert blob.upload_from_string.call_args.kwargs["if_generation_match"] == 7
    blob.upload_from_string.side_effect = PreconditionFailed("conflict")
    with pytest.raises(Conflict):
        store.write_json("checkpoint.json", {}, generation=7)


def test_existing_content_addressed_object_is_not_overwritten():
    store, blob = store_and_blob()
    blob.upload_from_string.side_effect = PreconditionFailed("exists")
    store.create_bytes("hash.json", b"[]", "application/json")
    assert blob.upload_from_string.call_args.kwargs["if_generation_match"] == 0


def test_stream_uses_bounded_chunks_and_create_only_upload():
    store, blob = store_and_blob()
    result = store.upload_stream("stock.zip", lambda writer: {"size": 8, "sha256": "test"})
    assert result["generation"] == 42
    assert blob.open.call_args.kwargs["if_generation_match"] == 0
    assert blob.open.call_args.kwargs["chunk_size"] == 8 * 1024**2


def test_stream_loads_generation_only_after_finalization():
    store, blob = store_and_blob()
    blob.generation = None
    blob.size = None

    def metadata():
        blob.generation = 123
        blob.size = 8

    blob.reload.side_effect = metadata
    result = store.upload_stream("stock.zip", lambda writer: {"size": 8, "sha256": "test"})
    assert result["generation"] == 123
    blob.reload.assert_called_once()


def test_stream_does_not_validate_a_server_size_mismatch():
    store, blob = store_and_blob()
    blob.size = 9
    with pytest.raises(IngestionError, match="size mismatch"):
        store.upload_stream("stock.zip", lambda writer: {"size": 8, "sha256": "test"})


def test_stream_error_is_propagated_to_the_writer_context():
    store, blob = store_and_blob()

    def fail(writer):
        raise RuntimeError("transfer interrupted")

    with pytest.raises(RuntimeError):
        store.upload_stream("stock.zip", fail)
    # BlobWriter.__exit__ terminates the resumable upload on a producer error.
    context = blob.open.return_value
    assert context.__exit__.call_args.args[0] is RuntimeError


def test_ranged_reader_is_pinned_to_the_receipt_generation():
    store, _ = store_and_blob()
    store.open_reader("raw/stock.zip", generation=42)
    store.bucket.blob.assert_called_with("raw/stock.zip", generation=42)
    store.bucket.blob.return_value.open.assert_called_once_with(
        "rb", chunk_size=1024 * 1024, if_generation_match=42
    )


def test_stream_accepts_parquet_content_type_without_weakening_preconditions():
    store, blob = store_and_blob()
    store.upload_stream(
        "bronze/part.parquet",
        lambda writer: {"size": 8},
        content_type="application/vnd.apache.parquet",
    )
    assert blob.content_type == "application/vnd.apache.parquet"
    assert blob.open.call_args.kwargs["if_generation_match"] == 0


def test_raw_read_is_pinned_to_the_acknowledged_generation():
    store, blob = store_and_blob()
    blob.download_as_bytes.return_value = b"[]"
    assert store.read_bytes("raw/page.json", generation=7) == b"[]"
    store.bucket.blob.assert_called_with("raw/page.json", generation=7)
    blob.download_as_bytes.assert_called_once_with(if_generation_match=7)


def test_changed_raw_generation_is_not_silently_read():
    store, blob = store_and_blob()
    blob.download_as_bytes.side_effect = PreconditionFailed("changed")
    with pytest.raises(IngestionError, match="generation changed"):
        store.read_bytes("raw/page.json", generation=7)
