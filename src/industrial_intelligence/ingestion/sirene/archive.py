"""Copy the monthly SIRENE Parquet stocks from data.gouv.fr to Bronze GCS, verified by SHA-1."""

import hashlib
import logging
import re
import time

import httpx

from ..rne.common import IngestionError, parse_time, timestamp, utc_now, validate_prefix
from ..rne.log import log_event
from ..rne.storage import CHUNK_SIZE, DigestWriter

DATASET_ID = "5b7ffc618b4c4169d30727e0"
DATASET_URL = f"https://www.data.gouv.fr/api/1/datasets/{DATASET_ID}/"
# data.gouv.fr replaces the file every month under the same resource id.
TABLES = {
    "unite_legale": "350182c9-148a-46e0-8389-76c2ec1374a3",
    "etablissement": "a29c1297-1f92-4e2a-8f6b-8c902ce96c5f",
    "unite_legale_historique": "1b9290ed-d0bc-461f-ba31-0250a99cc140",
    "etablissement_historique": "2b3a0c79-f97b-46b8-ac02-8be6c1f01a8c",
    "etablissement_liens_succession": "6ea8c0d2-a4b9-4ea2-ba63-7ec89c2dd4f4",
    "doublons": "ec0a0d5f-5d52-4603-a712-b6821076abf0",
}
FILE_URL = re.compile(
    r"https://static\.data\.gouv\.fr/resources/[a-z0-9-]+/[0-9]{8}-[0-9]{6}/[a-z0-9-]+\.parquet"
)
PARQUET = "application/vnd.apache.parquet"
LARGE_DOWNLOAD_BYTES = 100 * 1024**2
logger = logging.getLogger(__name__)


def fetch_resources(client: httpx.Client, *, attempts=5, sleep=time.sleep) -> dict[str, dict]:
    """Return the dataset resources by id, retrying throttling and server errors."""
    for attempt in range(1, attempts + 1):
        try:
            response = client.get(DATASET_URL)
        except httpx.TransportError as error:
            status, delay = type(error).__name__, min(2**attempt, 60)
        else:
            status = response.status_code
            if status == 200:
                try:
                    return {item["id"]: item for item in response.json()["resources"]}
                except (ValueError, KeyError, TypeError):
                    raise IngestionError("Unexpected data.gouv.fr catalog JSON") from None
            if status != 429 and status < 500:
                raise IngestionError(f"data.gouv.fr HTTP {status}")
            retry_after = response.headers.get("Retry-After", "")
            delay = int(retry_after) if retry_after.isdigit() else min(2**attempt, 60)
        log_event(logger, "catalog_retry", logging.WARNING, status=status, attempt=attempt)
        if attempt < attempts:
            sleep(delay)
    raise IngestionError("data.gouv.fr catalog unavailable after retries")


def remote_file(table: str, resources: dict[str, dict]) -> dict:
    """Validate the catalog entry before any byte is downloaded."""
    resource = resources.get(TABLES[table])
    if not isinstance(resource, dict):
        raise IngestionError(f"{table}: resource missing from the data.gouv.fr catalog")
    checksum, url, size = resource.get("checksum"), resource.get("url"), resource.get("filesize")
    if (
        resource.get("format") != "parquet"
        or not isinstance(checksum, dict)
        or checksum.get("type") != "sha1"
        or not re.fullmatch(r"[0-9a-f]{40}", str(checksum.get("value")))
        or type(size) is not int
        or size < 1
        or not FILE_URL.fullmatch(str(url))
    ):
        raise IngestionError(f"{table}: unexpected data.gouv.fr resource metadata")
    return {
        "resource_id": resource["id"],
        "title": resource.get("title"),
        "url": url,
        "filename": url.rsplit("/", 1)[1],
        "size": size,
        "sha1": checksum["value"],
        "last_modified": timestamp(parse_time(resource.get("last_modified"))),
    }


def transfer(client: httpx.Client, remote: dict, writer) -> dict:
    """Raising here aborts the GCS upload, so an unverified file is never finalized."""
    output = DigestWriter(writer)
    sha1 = hashlib.sha1()
    with client.stream("GET", remote["url"]) as response:
        if response.status_code != 200:
            raise IngestionError(f"data.gouv.fr download HTTP {response.status_code}")
        for chunk in response.iter_bytes(CHUNK_SIZE):
            sha1.update(chunk)
            output.write(chunk)
            if output.size > remote["size"]:
                raise IngestionError("Download larger than the announced file size")
    if output.size != remote["size"] or sha1.hexdigest() != remote["sha1"]:
        raise IngestionError("Downloaded file does not match the data.gouv.fr size and SHA-1")
    return {**output.metadata(), "sha1": sha1.hexdigest()}


def archive_table(store, prefix, table, remote, client, *, allow_large_download) -> bool:
    """Write the file once per publication date; the manifest is the success marker."""
    root = f"bronze/{prefix}/{table}/date={remote['last_modified'][:10]}"
    manifest_name = f"{root}/_manifest.json"
    manifest, _ = store.read_json(manifest_name)
    if manifest is not None:
        if manifest["remote"]["sha1"] != remote["sha1"]:
            raise IngestionError(f"{table}: another file is already archived for this date")
        return False
    if remote["size"] > LARGE_DOWNLOAD_BYTES and not allow_large_download:
        raise IngestionError(f"{table}: file exceeds 100 MiB; pass --allow-large-download")
    name = f"{root}/{remote['filename']}"
    retrieved_at = timestamp(utc_now())
    result = store.upload_stream(
        name, lambda writer: transfer(client, remote, writer), content_type=PARQUET
    )
    store.write_json(
        manifest_name,
        {
            "schema_version": 1,
            "source": "insee_sirene",
            "publisher": "data.gouv.fr",
            "dataset_id": DATASET_ID,
            "table": table,
            "remote": remote,
            "object": name,
            "uri": store.uri(name),
            "retrieved_at": retrieved_at,
            "content_type": PARQUET,
            **result,
        },
    )
    return True


def archive(store, prefix, client, tables, *, allow_large_download=False, sleep=time.sleep):
    validate_prefix(prefix)
    resources = fetch_resources(client, sleep=sleep)
    remotes = {table: remote_file(table, resources) for table in tables}
    archived = skipped = downloaded_bytes = 0
    for table, remote in remotes.items():
        date = remote["last_modified"][:10]
        if archive_table(
            store, prefix, table, remote, client, allow_large_download=allow_large_download
        ):
            archived += 1
            downloaded_bytes += remote["size"]
            log_event(logger, "table_archived", table=table, date=date, bytes=remote["size"])
        else:
            skipped += 1
            log_event(logger, "table_already_archived", table=table, date=date)
    return {
        "status": "completed",
        "archived": archived,
        "skipped": skipped,
        "downloaded_bytes": downloaded_bytes,
    }
