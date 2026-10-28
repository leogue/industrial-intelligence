"""Small shared contracts; never put credentials in errors or persisted state."""

import json
import re
from datetime import UTC, datetime


class IngestionError(Exception):
    """An operational error whose message is safe to log."""


class Conflict(IngestionError):
    """GCS generation precondition failed (another writer or existing object)."""


class Paused(IngestionError):
    """Execution budget exhausted; durable progress can be resumed."""


def utc_now() -> datetime:
    return datetime.now(UTC)


def parse_time(value: str) -> datetime:
    try:
        result = datetime.fromisoformat(value)
    except (ValueError, TypeError):
        raise IngestionError("Expected an ISO 8601 datetime with timezone") from None
    if result.tzinfo is None:
        raise IngestionError("A timezone is required; use e.g. 2026-03-01T00:00:00Z")
    return result.astimezone(UTC)


def timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def json_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def validate_prefix(prefix: str) -> str:
    if not re.fullmatch(r"[a-zA-Z0-9_-]+(?:/[a-zA-Z0-9_-]+)*", prefix):
        raise IngestionError("Prefix must contain safe non-empty path components")
    return prefix


def validate_company(company: object) -> tuple[str, datetime]:
    """Return the validated SIREN and technical update timestamp shared by raw/Bronze."""
    if not isinstance(company, dict):
        raise IngestionError("Expected a company object")
    formality = company.get("formality", {})
    if not isinstance(formality, dict):
        raise IngestionError("Invalid formality object")
    siren = company.get("siren", formality.get("siren"))
    if not isinstance(siren, str) or not re.fullmatch(r"[0-9]{9}", siren):
        raise IngestionError("Missing or invalid company SIREN")
    if formality.get("siren", siren) != siren:
        raise IngestionError("Conflicting SIREN values")
    return siren, parse_time(company.get("updatedAt"))
