import io
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from rne_fakes import MemoryStore

from industrial_intelligence.ingestion.rne.common import IngestionError
from industrial_intelligence.lakehouse.load import RUN_KEY, publish

PREFIX = "silver/samples"
OLD = "20261118T000000Z-aaaaaaaaaaaa"
NEW = "20261119T000000Z-bbbbbbbbbbbb"


def parquet(table: pa.Table) -> bytes:
    output = io.BytesIO()
    pq.write_table(table, output)
    return output.getvalue()


def companies(*sirens):
    return pa.table({"siren": pa.array(sirens, pa.string()), "n": list(range(len(sirens)))})


def annonces(*days):
    return pa.table(
        {
            "id": [f"A{i}" for i in range(len(days))],
            "dateparution": pa.array(days, pa.date32()),
        }
    )


def add_run(store, run, tables, status="complete", rows_override=None):
    manifest = {"schema_version": 1, "status": status, "input_fingerprint": "f", "tables": {}}
    for name, table in tables.items():
        obj = f"{PREFIX}/{run}/{name}/data_0.parquet"
        generation = store.put(obj, parquet(table))
        rows = table.num_rows if rows_override is None else rows_override
        manifest["tables"][name] = {
            "rows": rows,
            "parts": [{"object": obj, "generation": generation, "rows": rows}],
        }
    store.put(f"{PREFIX}/{run}/_manifest.json", manifest)


@pytest.fixture
def catalog(tmp_path):
    return SqlCatalog(
        "test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}"
    )


def test_publishes_the_latest_complete_run_sorted_on_its_key(catalog):
    store = MemoryStore()
    add_run(store, OLD, {"entreprises_sirene": companies("000000002", "000000001")})
    add_run(
        store,
        NEW,
        {
            "entreprises_sirene": companies("000000009"),
            "annonces_bodacc": annonces(date(2026, 11, 18)),
        },
    )
    add_run(store, "20261120T000000Z-cccccccccccc", {"entreprises_sirene": companies()}, "running")

    result = publish(catalog, store, PREFIX, "silver")

    assert result == {"run": NEW, "tables_published": 2, "tables_skipped": 0, "rows": 2}
    table = catalog.load_table(("silver", "entreprises_sirene"))
    assert table.scan().to_arrow()["siren"].to_pylist() == ["000000009"]
    assert table.current_snapshot().summary[RUN_KEY] == NEW
    assert table.schema().find_column_name(table.sort_order().fields[0].source_id) == "siren"
    bodacc = catalog.load_table(("silver", "annonces_bodacc"))
    assert bodacc.scan().to_arrow().num_rows == 1


def test_rerun_is_a_no_op_and_a_new_run_replaces_rows(catalog):
    store = MemoryStore()
    add_run(store, OLD, {"entreprises_sirene": companies("000000002", "000000001")})
    publish(catalog, store, PREFIX, "silver")
    table = catalog.load_table(("silver", "entreprises_sirene"))
    first = table.current_snapshot().snapshot_id

    assert publish(catalog, store, PREFIX, "silver")["tables_skipped"] == 1
    assert (
        catalog.load_table(("silver", "entreprises_sirene")).current_snapshot().snapshot_id == first
    )

    add_run(store, NEW, {"entreprises_sirene": companies("000000003")})
    publish(catalog, store, PREFIX, "silver")
    table = catalog.load_table(("silver", "entreprises_sirene"))
    assert table.scan().to_arrow()["siren"].to_pylist() == ["000000003"]
    # Time travel still reads the previous run, sorted on siren.
    assert table.scan(snapshot_id=first).to_arrow()["siren"].to_pylist() == [
        "000000001",
        "000000002",
    ]


def test_row_count_mismatch_keeps_the_previous_run(catalog):
    store = MemoryStore()
    add_run(store, OLD, {"entreprises_sirene": companies("000000001")})
    publish(catalog, store, PREFIX, "silver")
    add_run(store, NEW, {"entreprises_sirene": companies("000000002")}, rows_override=5)

    with pytest.raises(IngestionError, match="Rows read differ"):
        publish(catalog, store, PREFIX, "silver")

    table = catalog.load_table(("silver", "entreprises_sirene"))
    assert table.current_snapshot().summary[RUN_KEY] == OLD
    assert table.scan().to_arrow()["siren"].to_pylist() == ["000000001"]


def test_rejects_parts_outside_the_run_folder(catalog):
    store = MemoryStore()
    add_run(store, NEW, {"entreprises_sirene": companies("000000001")})
    manifest = store.read_json(f"{PREFIX}/{NEW}/_manifest.json")[0]
    manifest["tables"]["entreprises_sirene"]["parts"][0]["object"] = "bronze/elsewhere.parquet"
    store.put(f"{PREFIX}/{NEW}/_manifest.json", manifest)

    with pytest.raises(IngestionError, match="outside its run folder"):
        publish(catalog, store, PREFIX, "silver")


def test_fails_without_a_complete_run(catalog):
    store = MemoryStore()
    add_run(store, NEW, {"entreprises_sirene": companies()}, status="running")
    with pytest.raises(IngestionError, match="No complete Silver run"):
        publish(catalog, store, PREFIX, "silver")
