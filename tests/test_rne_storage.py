from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import NotFound, PreconditionFailed

from industrial_intelligence.ingestion.rne.common import Conflict
from industrial_intelligence.ingestion.rne.storage import GcsStore


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


def test_stream_error_is_propagated_to_the_writer_context():
    store, blob = store_and_blob()

    def fail(writer):
        raise RuntimeError("transfer interrupted")

    with pytest.raises(RuntimeError):
        store.upload_stream("stock.zip", fail)
    # BlobWriter.__exit__ terminates the resumable upload on a producer error.
    context = blob.open.return_value
    assert context.__exit__.call_args.args[0] is RuntimeError


def test_lock_delete_is_generation_guarded():
    store, blob = store_and_blob()
    store.delete("lock.json", generation=42)
    blob.delete.assert_called_once_with(if_generation_match=42)
