"""Copy the BODACC commercial notices to raw GCS, one Parquet export per publication day.

One checkpoint (``control/{prefix}/checkpoint.json``) holds the last archived day. Each run
exports the days after it, plus the last ``recheck_days`` already archived: the publisher
reprocesses its data, so a past day can change. Exports are sorted by ``id``, which makes an
unchanged day byte-identical; files are named by SHA-256, so a changed day is added beside
the previous version and nothing is ever overwritten.
"""

import hashlib
import io
import logging
import time
from datetime import date, timedelta

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from ..rne.common import IngestionError, timestamp, utc_now, validate_prefix
from ..rne.log import log_event

DATASET_URL = (
    "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
    "annonces-commerciales"
)
FIRST_DAY = date(2008, 1, 1)  # The API starts on 2008-01-02.
PARQUET = "application/vnd.apache.parquet"
# Columns observed on 2026-10-29; a missing one fails the run rather than silently thinning raw.
COLUMNS = (
    "id",
    "publicationavis",
    "parution",
    "dateparution",
    "numeroannonce",
    "typeavis",
    "typeavis_lib",
    "familleavis",
    "familleavis_lib",
    "numerodepartement",
    "departement_nom_officiel",
    "region_code",
    "region_nom_officiel",
    "tribunal",
    "commercant",
    "ville",
    "registre",
    "cp",
    "pdf_parution_subfolder",
    "ispdf_unitaire",
    "listepersonnes",
    "listeetablissements",
    "jugement",
    "acte",
    "modificationsgenerales",
    "radiationaurcs",
    "depot",
    "listeprecedentexploitant",
    "listeprecedentproprietaire",
    "divers",
    "parutionavisprecedent",
    "url_complete",
)
logger = logging.getLogger(__name__)


def get(client: httpx.Client, path: str, params: dict, *, attempts=5, sleep=time.sleep):
    """GET with retries on throttling, server and network errors, honouring Retry-After."""
    for attempt in range(1, attempts + 1):
        try:
            response = client.get(DATASET_URL + path, params=params)
        except httpx.TransportError as error:
            status, delay = type(error).__name__, min(2**attempt, 60)
        else:
            status = response.status_code
            if status == 200:
                return response
            if status != 429 and status < 500:
                raise IngestionError(f"BODACC API HTTP {status}")
            retry_after = response.headers.get("Retry-After", "")
            delay = int(retry_after) if retry_after.isdigit() else min(2**attempt, 60)
        log_event(logger, "http_retry", logging.WARNING, status=status, attempt=attempt)
        if attempt < attempts:
            sleep(delay)
    raise IngestionError("BODACC API unavailable after retries")


def fetch_day(client: httpx.Client, day: date, *, sleep=time.sleep) -> tuple[bytes, int]:
    """Export one publication day and check it against the API count before storing it."""
    where = f"dateparution=date'{day.isoformat()}'"
    try:
        expected = get(client, "/records", {"where": where, "limit": 0}, sleep=sleep).json()
        expected = expected["total_count"]
    except (ValueError, KeyError, TypeError):
        raise IngestionError("Unexpected BODACC count JSON") from None
    if type(expected) is not int:
        raise IngestionError("Unexpected BODACC count JSON")
    params = {"where": where, "order_by": "id"}
    body = get(client, "/exports/parquet", params, sleep=sleep).content
    try:
        parquet = pq.ParquetFile(io.BytesIO(body))
    except pa.ArrowException:
        raise IngestionError(f"{day}: export is not a readable Parquet file") from None
    missing = set(COLUMNS) - set(parquet.schema_arrow.names)
    if missing:
        raise IngestionError(f"{day}: export lacks columns {sorted(missing)}")
    table = parquet.read(columns=["id", "dateparution"])
    ids = table.column("id").to_pylist()
    if len(ids) != expected:
        raise IngestionError(f"{day}: export has {len(ids)} rows for a count of {expected}")
    if None in ids or len(set(ids)) != len(ids):
        raise IngestionError(f"{day}: missing or duplicate notice ids")
    if set(table.column("dateparution").to_pylist()) - {day}:
        raise IngestionError(f"{day}: export contains another publication date")
    return body, len(ids)


def store_day(store, prefix: str, day: date, body: bytes, rows: int) -> bool:
    """Return False if this exact export is already archived for the day."""
    digest = hashlib.sha256(body).hexdigest()
    name = f"raw/{prefix}/annonces/dateparution={day.isoformat()}/{digest}.parquet"
    manifest_name = f"{name}.manifest.json"
    if store.read_json(manifest_name)[0] is not None:
        return False
    store.create_bytes(name, body, PARQUET)
    stored = store.stat(name)
    if stored is None or stored["size"] != len(body):
        raise IngestionError("Raw export missing or size changed after upload")
    store.write_json(
        manifest_name,
        {
            "schema_version": 1,
            "source": "dila_bodacc",
            "dataset": "annonces-commerciales",
            "url": f"{DATASET_URL}/exports/parquet",
            "where": f"dateparution=date'{day.isoformat()}'",
            "order_by": "id",
            "dateparution": day.isoformat(),
            "rows": rows,
            "object": name,
            "uri": store.uri(name),
            "size": len(body),
            "sha256": digest,
            "generation": stored["generation"],
            "retrieved_at": timestamp(utc_now()),
            "content_type": PARQUET,
        },
    )
    return True


def archive(
    store,
    prefix,
    client,
    until: date,
    *,
    recheck_days=30,
    max_seconds=20000,
    sleep=time.sleep,
    clock=time.monotonic,
):
    validate_prefix(prefix)
    if until < FIRST_DAY or recheck_days < 0 or max_seconds < 1:
        raise IngestionError("Invalid bounds: --until, --recheck-days or --max-seconds")
    deadline = clock() + max_seconds
    checkpoint = f"control/{prefix}/checkpoint.json"
    state, generation = store.read_json(checkpoint)
    if state is None:
        state, completed = {"schema_version": 1, "completed_until": None}, None
        day = FIRST_DAY
    else:
        completed = date.fromisoformat(state["completed_until"])
        day = max(FIRST_DAY, completed + timedelta(days=1 - recheck_days))
    log_event(
        logger,
        "archive_started",
        completed_until=state["completed_until"],
        first_day=day.isoformat(),
        target=until.isoformat(),
    )
    checked = new_versions = changed = rows = 0
    status = "up_to_date"
    while day <= until:
        if clock() >= deadline:
            status = "partial"  # Progress is saved: the next run resumes from the checkpoint.
            break
        body, day_rows = fetch_day(client, day, sleep=sleep)
        if store_day(store, prefix, day, body, day_rows):
            new_versions += 1
            if completed is not None and day <= completed:
                changed += 1
                age_days = (utc_now().date() - day).days
                log_event(logger, "day_changed", day=day.isoformat(), age_days=age_days)
        if completed is None or day > completed:
            completed = day
            state["completed_until"] = day.isoformat()
            generation = store.write_json(checkpoint, state, generation=generation)
        checked += 1
        rows += day_rows
        day += timedelta(days=1)
    return {
        "status": status,
        "completed_until": state["completed_until"],
        "target": until.isoformat(),
        "days_checked": checked,
        "new_versions": new_versions,
        "changed_days": changed,
        "rows": rows,
    }
