"""GCS objects and generation-guarded control files (ADC, no service-account keys)."""

import hashlib
import json
from collections.abc import Callable
from typing import BinaryIO

from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import storage

from .common import Conflict, IngestionError, json_bytes

CHUNK_SIZE = 8 * 1024 * 1024  # Multiple of the GCS resumable-upload quantum.


class DigestWriter:
    """Hash/count output without closing or flushing the enclosing upload."""

    def __init__(self, stream):
        self.stream = stream
        self.size = 0
        self.checksum = hashlib.sha256()

    @property
    def closed(self):
        return self.stream.closed

    def writable(self):
        return True

    def seekable(self):
        return False

    def write(self, data):
        written = self.stream.write(data)
        if written != len(data):
            raise IngestionError("Incomplete output write")
        self.size += written
        self.checksum.update(data)
        return written

    def tell(self):
        return self.size

    def flush(self):
        pass  # BlobWriter owns chunking and finalization, not Arrow/gzip.

    def close(self):
        pass  # Nested writers must not close the enclosing resumable upload.

    def metadata(self):
        return {"size": self.size, "sha256": self.checksum.hexdigest()}


class GcsStore:
    def __init__(self, bucket: str, client: storage.Client | None = None):
        self.bucket_name = bucket
        self.bucket = (client or storage.Client()).bucket(bucket)

    def uri(self, name: str) -> str:
        return f"gs://{self.bucket_name}/{name}"

    def read_json(self, name: str) -> tuple[dict | None, int]:
        blob = self.bucket.blob(name)
        try:
            blob.reload()
            generation = int(blob.generation)
            body = blob.download_as_bytes(if_generation_match=generation)
        except NotFound:
            return None, 0
        except PreconditionFailed:
            raise Conflict("Control object changed during read") from None
        try:
            value = json.loads(body)
        except ValueError:
            raise IngestionError("Invalid control JSON") from None
        if not isinstance(value, dict):
            raise IngestionError("Control object must be a JSON object")
        return value, generation

    def write_json(self, name: str, value: dict, *, generation: int = 0) -> int:
        blob = self.bucket.blob(name)
        try:
            blob.upload_from_string(
                json_bytes(value), content_type="application/json", if_generation_match=generation
            )
        except PreconditionFailed:
            raise Conflict("Control write conflict; no state was overwritten") from None
        return int(blob.generation)

    def read_bytes(self, name: str, *, generation: int) -> bytes:
        """Read one immutable object version, never a newer replacement."""
        try:
            return self.bucket.blob(name, generation=generation).download_as_bytes(
                if_generation_match=generation
            )
        except (NotFound, PreconditionFailed):
            raise IngestionError("Raw object missing or generation changed") from None

    def create_bytes(self, name: str, value: bytes, content_type: str) -> None:
        """For content-addressed objects only: never overwrite an existing original."""
        try:
            self.bucket.blob(name).upload_from_string(
                value, content_type=content_type, if_generation_match=0
            )
        except PreconditionFailed:
            pass  # The SHA-256-named body is already present from an earlier attempt.

    def open_reader(self, name: str, *, generation: int):
        """Seekable ranged reader pinned to one immutable GCS generation."""
        return self.bucket.blob(name, generation=generation).open(
            "rb", chunk_size=1024 * 1024, if_generation_match=generation
        )

    def upload_stream(
        self,
        name: str,
        produce: Callable[[BinaryIO], dict],
        *,
        content_type: str = "application/zip",
    ) -> dict:
        """Finalize only if the producer has validated the complete transfer."""
        blob = self.bucket.blob(name)
        blob.content_type = content_type
        try:
            with blob.open(
                "wb", chunk_size=CHUNK_SIZE, ignore_flush=True, if_generation_match=0
            ) as writer:
                result = produce(writer)
        except PreconditionFailed:
            raise Conflict("Output object already exists") from None
        # BlobWriter finalizes the upload but does not populate Blob.generation/size.
        # Read server metadata before writing a receipt or declaring the archive valid.
        blob.reload()
        if int(blob.size) != result["size"]:
            raise IngestionError("Stored object size mismatch; no receipt will be written")
        return {**result, "generation": int(blob.generation)}

    def stat(self, name: str) -> dict | None:
        blob = self.bucket.blob(name)
        try:
            blob.reload()
        except NotFound:
            return None
        return {"size": int(blob.size), "generation": int(blob.generation)}
