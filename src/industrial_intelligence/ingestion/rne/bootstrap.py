"""Archive one explicit FTP stock ZIP in GCS, streamed, with a validated receipt."""

import uuid
from dataclasses import asdict

from .common import IngestionError, timestamp, utc_now, validate_prefix


def archive_stock(
    store, prefix, source, filename, *, allow_large_download=False, max_stock_bytes=20 * 1024**3
):
    """Copy the ZIP once; a valid receipt makes every later run a no-op."""
    validate_prefix(prefix)
    stock = source.stat(filename)
    if stock.size > max_stock_bytes:
        raise IngestionError("Stock exceeds configured byte limit")
    if stock.size > 100 * 1024**2 and not allow_large_download:
        raise IngestionError("Stock exceeds 100 MiB; explicit --allow-large-download required")
    receipt_name = f"control/{prefix}/stocks/{stock.revision}.json"
    receipt, _ = store.read_json(receipt_name)
    reused_archive = receipt is not None
    if receipt is not None:
        if receipt.get("source_host") != source.host:
            raise IngestionError("Stock receipt belongs to a different FTP host")
        if store.stat(receipt["object"]) != {
            "size": receipt["size"],
            "generation": receipt["generation"],
        }:
            raise IngestionError("Previously validated archive is missing or changed")
    else:
        # Unique paths leave an orphan, not an overwrite, if receipt creation fails.
        run_id = uuid.uuid4().hex
        name = f"raw/{prefix}/stocks/{stock.revision}/{run_id}/{stock.filename}"
        result = store.upload_stream(name, lambda writer: source.transfer(stock, writer))
        if result["size"] != stock.size:
            raise IngestionError("Archive size verification failed")
        receipt = {
            "schema_version": 1,
            "source": "inpi_rne",
            "resource_type": "companies_stock",
            "run_id": run_id,
            "source_host": source.host,
            "source_protocol": "ftp",
            "remote": asdict(stock),
            "remote_revision": stock.revision,
            "object": name,
            "uri": store.uri(name),
            "retrieved_at": timestamp(utc_now()),
            "content_type": "application/zip",
            **result,
        }
        store.write_json(name + ".manifest.json", receipt)
        store.write_json(receipt_name, receipt)
    return {
        "status": "archived",
        "archive_uri": receipt["uri"],
        "bytes": receipt["size"],
        "sha256": receipt["sha256"],
        "reused_archive": reused_archive,
    }
