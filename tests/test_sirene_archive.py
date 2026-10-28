import hashlib

import httpx
import pytest
from rne_fakes import MemoryStore

from industrial_intelligence.ingestion.rne.common import IngestionError
from industrial_intelligence.ingestion.sirene.archive import DATASET_URL, TABLES, archive

PREFIX = "insee/sirene"
BODY = b"PAR1synthetic-test-onlyPAR1"
URL = (
    "https://static.data.gouv.fr/resources/base-sirene/20261001-073650/"
    "stock-stockdoublons-parquet.parquet"
)
ROOT = f"bronze/{PREFIX}/doublons/date=2026-10-01"


def resource(body=BODY, **overrides):
    return {
        "id": TABLES["doublons"],
        "title": "Sirene : Fichier StockDoublons (format parquet)",
        "format": "parquet",
        "url": URL,
        "filesize": len(body),
        "last_modified": "2026-10-01T07:36:51.719000+00:00",
        "checksum": {"type": "sha1", "value": hashlib.sha1(body).hexdigest()},
    } | overrides


def client_for(resources, body=BODY, calls=None):
    def handler(request):
        if calls is not None:
            calls.append(str(request.url))
        if str(request.url) == DATASET_URL:
            return httpx.Response(200, json={"resources": resources})
        assert str(request.url) == URL
        return httpx.Response(200, content=body)

    return httpx.Client(transport=httpx.MockTransport(handler))


def run(store, client, **kwargs):
    return archive(store, PREFIX, client, ["doublons"], sleep=lambda _: None, **kwargs)


def test_archives_the_original_parquet_with_a_manifest():
    store = MemoryStore()
    result = run(store, client_for([resource()]))
    assert result == {
        "status": "completed",
        "archived": 1,
        "skipped": 0,
        "downloaded_bytes": len(BODY),
    }
    manifest = store.read_json(f"{ROOT}/_manifest.json")[0]
    assert manifest["object"] == f"{ROOT}/stock-stockdoublons-parquet.parquet"
    assert store.objects[manifest["object"]][0] == BODY
    assert manifest["sha256"] == hashlib.sha256(BODY).hexdigest()
    assert manifest["sha1"] == manifest["remote"]["sha1"]
    assert manifest["remote"]["resource_id"] == TABLES["doublons"]
    assert manifest["retrieved_at"].endswith("Z")


def test_rerun_skips_an_archived_publication_without_downloading():
    store = MemoryStore()
    run(store, client_for([resource()]))
    calls = []
    assert run(store, client_for([resource()], calls=calls))["skipped"] == 1
    assert calls == [DATASET_URL]


def test_checksum_mismatch_stores_nothing():
    store = MemoryStore()
    bad = resource(checksum={"type": "sha1", "value": "0" * 40})
    with pytest.raises(IngestionError, match="SHA-1"):
        run(store, client_for([bad]))
    assert store.objects == {}


def test_never_overwrites_a_different_file_for_the_same_date():
    store = MemoryStore()
    run(store, client_for([resource()]))
    other = b"PAR1republished-same-dayPAR1"
    with pytest.raises(IngestionError, match="already archived"):
        run(store, client_for([resource(other)], body=other))


@pytest.mark.parametrize(
    "resources",
    [
        [],
        [resource(url="https://example.invalid/stock.parquet")],
        [resource(format="csv")],
        [resource(checksum={"type": "md5", "value": "0" * 32})],
    ],
)
def test_rejects_unexpected_catalog_metadata_before_downloading(resources):
    calls = []
    with pytest.raises(IngestionError):
        run(MemoryStore(), client_for(resources, calls=calls))
    assert calls == [DATASET_URL]


def test_large_files_require_explicit_opt_in():
    with pytest.raises(IngestionError, match="allow-large-download"):
        run(MemoryStore(), client_for([resource(filesize=100 * 1024**2 + 1)]))


def test_catalog_retries_server_errors():
    responses = iter([httpx.Response(503), httpx.Response(200, json={"resources": [resource()]})])

    def handler(request):
        return (
            next(responses)
            if str(request.url) == DATASET_URL
            else httpx.Response(200, content=BODY)
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    assert run(MemoryStore(), client)["archived"] == 1
