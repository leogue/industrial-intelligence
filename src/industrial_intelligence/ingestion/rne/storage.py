"""GCS objects and generation-guarded control files (ADC, no service-account keys)."""

import json
from collections.abc import Callable
from typing import BinaryIO

from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import storage

from .common import Conflict, IngestionError, json_bytes

CHUNK_SIZE = 8 * 1024 * 1024  # Multiple of the GCS resumable-upload quantum.


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

    def create_bytes(self, name: str, value: bytes, content_type: str) -> None:
        """For content-addressed objects only: never overwrite an existing original."""
        try:
            self.bucket.blob(name).upload_from_string(
                value, content_type=content_type, if_generation_match=0
            )
        except PreconditionFailed:
            pass  # The SHA-256-named body is already present from an earlier attempt.

    def upload_stream(self, name: str, produce: Callable[[BinaryIO], dict]) -> dict:
        """Finalize only if the producer has validated the complete transfer."""
        blob = self.bucket.blob(name)
        blob.content_type = "application/zip"
        try:
            with blob.open(
                "wb", chunk_size=CHUNK_SIZE, ignore_flush=True, if_generation_match=0
            ) as writer:
                result = produce(writer)
        except PreconditionFailed:
            raise Conflict("Archive object already exists") from None
        return {**result, "generation": int(blob.generation)}

    def stat(self, name: str) -> dict | None:
        blob = self.bucket.blob(name)
        try:
            blob.reload()
        except NotFound:
            return None
        return {"size": int(blob.size), "generation": int(blob.generation)}

    def delete(self, name: str, *, generation: int) -> None:
        try:
            self.bucket.blob(name).delete(if_generation_match=generation)
        except PreconditionFailed:
            raise Conflict("Lock changed; refusing to delete another owner's lock") from None
