"""Shared in-memory object store for offline RNE ingestion and materialization tests."""

import copy
import hashlib
import io

from industrial_intelligence.ingestion.rne.common import Conflict, IngestionError


class MemoryStore:
    def __init__(self):
        self.objects = {}
        self.serial = 0
        self.fail_checkpoint_once = False
        self.fail_snapshot_once = False
        self.uploaded = []

    def uri(self, name):
        return "gs://test/" + name

    def put(self, name, value):
        self.serial += 1
        self.objects[name] = (copy.deepcopy(value), self.serial)
        return self.serial

    def read_json(self, name):
        value = self.objects.get(name)
        return (copy.deepcopy(value[0]), value[1]) if value else (None, 0)

    def write_json(self, name, value, *, generation=0):
        if self.fail_checkpoint_once and "/materialize/" in name and value.get("next_index", 0):
            self.fail_checkpoint_once = False
            raise IngestionError("simulated checkpoint failure")
        if (
            self.fail_snapshot_once
            and name.endswith("/_manifest.json")
            and name.startswith("bronze/")
        ):
            self.fail_snapshot_once = False
            raise IngestionError("simulated snapshot manifest failure")
        if self.objects.get(name, (None, 0))[1] != generation:
            raise Conflict("generation conflict")
        return self.put(name, value)

    def read_bytes(self, name, *, generation):
        value, actual = self.objects.get(name, (None, 0))
        if value is None or actual != generation:
            raise IngestionError("Raw object missing or generation changed")
        return value

    def create_bytes(self, name, value, content_type):
        if name not in self.objects:
            self.put(name, value)

    def stat(self, name):
        item = self.objects.get(name)
        return {"size": len(item[0]), "generation": item[1]} if item else None

    def open_reader(self, name, *, generation):
        value, actual = self.objects[name]
        assert generation == actual
        return io.BytesIO(value)

    def upload_stream(self, name, produce, *, content_type="application/zip"):
        if name in self.objects:
            raise Conflict("Output object already exists")
        output = io.BytesIO()
        result = produce(output)
        payload = output.getvalue()
        assert len(payload) == result["size"]
        assert hashlib.sha256(payload).hexdigest() == result["sha256"]
        self.uploaded.append(name)
        return {**result, "generation": self.put(name, payload)}
