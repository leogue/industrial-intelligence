"""Publish the latest complete Silver run as Iceberg tables in the BigLake catalog.

Each Silver table replaces its Iceberg table in one atomic transaction. Iceberg keeps the
previous snapshots, so older runs stay readable by time travel. The run id written in the
snapshot summary makes reruns idempotent without a separate checkpoint.
"""

import logging
import re

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.transforms import IdentityTransform

from ..ingestion.rne.common import IngestionError
from ..ingestion.rne.log import log_event

logger = logging.getLogger(__name__)

RUN_KEY = "silver-run"
BATCH_ROWS = 500_000  # Bounds memory on national tables; samples fit in one batch.

# Rows are sorted on the usual join/filter key so Parquet min/max statistics prune files.
# No partitioning yet: PyIceberg needs the native pyiceberg-core extra to write partitions,
# and sample-sized tables gain nothing from it.
SORT_KEYS = {
    "annonces_bodacc": ["dateparution", "id"],
    "doublons_sirene": ["siren"],
    "entreprises_rne": ["siren"],
    "entreprises_sirene": ["siren"],
    "etablissements": ["siret"],
    "historique_etablissements": ["siret", "dateDebut"],
    "historique_unites_legales": ["siren", "dateDebut"],
    "liens_annonce_entreprise": ["siren"],
    "successions_etablissements": ["siretEtablissementPredecesseur"],
}


def latest_complete_run(store, prefix: str) -> tuple[str, dict]:
    depth = prefix.count("/") + 2
    names = [
        name
        for name in store.list_names(prefix + "/")
        if name.endswith("/_manifest.json") and name.count("/") == depth
    ]
    # Run folders start with a UTC timestamp: lexical order is chronological.
    for name in sorted(names, reverse=True):
        manifest, _ = store.read_json(name)
        if manifest.get("status") == "complete":
            run = name.split("/")[-2]
            validate_manifest(manifest, f"{prefix}/{run}/")
            return run, manifest
    raise IngestionError("No complete Silver run under the prefix")


def validate_manifest(manifest: dict, root: str) -> None:
    if manifest.get("schema_version") != 1:
        raise IngestionError("Unsupported Silver manifest schema_version")
    tables = manifest.get("tables")
    if not isinstance(tables, dict) or not tables:
        raise IngestionError("Silver manifest has no tables")
    for table, info in tables.items():
        if not re.fullmatch(r"[a-z0-9_]+", table):
            raise IngestionError("Invalid Silver table name")
        parts = info.get("parts")
        if not isinstance(parts, list) or not parts:
            raise IngestionError("Silver table has no parts")
        for part in parts:
            if not str(part.get("object", "")).startswith(f"{root}{table}/"):
                raise IngestionError("Silver part outside its run folder")
            if not isinstance(part.get("generation"), int) or not isinstance(part.get("rows"), int):
                raise IngestionError("Silver part without generation or row count")
        if sum(part["rows"] for part in parts) != info.get("rows"):
            raise IngestionError("Silver table row count differs from its parts")


def publish_table(catalog, store, namespace: str, run: str, fingerprint: str, table, info):
    parts = info["parts"]
    files = [
        pq.ParquetFile(store.open_reader(p["object"], generation=p["generation"])) for p in parts
    ]
    schema = files[0].schema_arrow
    identifier = (namespace, table)
    if not catalog.table_exists(identifier):
        with catalog.create_table_transaction(identifier, schema=schema) as tx:
            if keys := SORT_KEYS.get(table):
                with tx.update_sort_order() as order:
                    for key in keys:
                        order.asc(key, IdentityTransform())
    iceberg = catalog.load_table(identifier)
    current = iceberg.current_snapshot()
    if current and current.summary and current.summary.get(RUN_KEY) == run:
        return "skipped", 0
    sort = [(name, "ascending") for name in SORT_KEYS.get(table, [])]
    properties = {RUN_KEY: run, "silver-input-fingerprint": fingerprint}
    rows = 0
    # Any exception below leaves the transaction uncommitted: readers keep the previous run.
    with iceberg.transaction() as tx:
        with tx.update_schema() as update:
            update.union_by_name(schema)
        if current:
            tx.delete(AlwaysTrue(), snapshot_properties=properties)
        for file in files:
            for batch in file.iter_batches(batch_size=BATCH_ROWS):
                chunk = pa.Table.from_batches([batch])
                tx.append(chunk.sort_by(sort) if sort else chunk, snapshot_properties=properties)
                rows += chunk.num_rows
        if rows != info["rows"]:
            raise IngestionError("Rows read differ from the Silver manifest")
    return "published", rows


def publish(catalog, store, prefix: str, namespace: str) -> dict:
    run, manifest = latest_complete_run(store, prefix)
    catalog.create_namespace_if_not_exists(namespace)
    counts = {"published": 0, "skipped": 0}
    rows = 0
    for table, info in sorted(manifest["tables"].items()):
        status, written = publish_table(
            catalog, store, namespace, run, manifest.get("input_fingerprint", ""), table, info
        )
        counts[status] += 1
        rows += written
        log_event(logger, "table_" + status, table=table, rows=written)
    return {
        "run": run,
        "tables_published": counts["published"],
        "tables_skipped": counts["skipped"],
        "rows": rows,
    }
