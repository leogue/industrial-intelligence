"""Fixed Bronze schema: typed join/filter columns + complete canonical source JSON.

Optional RNE branches vary by company. Do not infer a different Parquet schema
for every ZIP entry or discard fields that are not projected into typed columns.
"""

import hashlib

import pyarrow as pa
import simplejson

from .common import IngestionError, parse_time, validate_company

SCHEMA_VERSION = 1
MAX_COMPANY_BYTES = 64 * 1024**2
COMPANY_SCHEMA = pa.schema(
    [
        pa.field("siren", pa.string(), nullable=False),
        pa.field("source_id", pa.string()),
        pa.field("source_updated_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("type_personne", pa.string()),
        pa.field("denomination", pa.string()),
        pa.field("forme_juridique", pa.string()),
        pa.field("code_ape", pa.string()),
        pa.field("code_ape_naf2025", pa.string()),
        pa.field("diffusion_insee", pa.string()),
        pa.field("diffusion_commerciale", pa.bool_()),
        pa.field("deleted", pa.bool_()),
        pa.field("company_json", pa.string(), nullable=False),
        pa.field("company_sha256", pa.string(), nullable=False),
        pa.field("source_kind", pa.string(), nullable=False),
        pa.field("source_sha256", pa.string(), nullable=False),
        pa.field("source_uri", pa.string(), nullable=False),
        pa.field("source_generation", pa.string(), nullable=False),
        pa.field("source_retrieved_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("source_member", pa.string()),
        pa.field("source_row_number", pa.int64(), nullable=False),
    ],
    metadata={b"source": b"inpi_rne", b"schema_version": str(SCHEMA_VERSION).encode()},
)


def mapping(value) -> dict:
    return value if isinstance(value, dict) else {}


def optional(value, expected_type):
    if value is not None and type(value) is not expected_type:
        raise IngestionError("Unexpected optional field type")
    return value


def first_present(*values):
    return next((value for value in values if value is not None), None)


def company_row(
    company: object,
    receipt: dict,
    member: str | None,
    row_number: int,
    *,
    source_kind: str = "stock",
) -> dict:
    if source_kind not in {"stock", "diff"} or (source_kind == "stock" and not member):
        raise IngestionError("Invalid company provenance")
    siren, updated_at = validate_company(company)
    formality = company.get("formality", {})
    content = mapping(formality.get("content"))
    person = next(
        (
            content[key]
            for key in ("personneMorale", "personnePhysique", "exploitation")
            if isinstance(content.get(key), dict)
        ),
        {},
    )
    enterprise = mapping(mapping(person.get("identite")).get("entreprise"))
    # Decimal values returned by ijson are encoded exactly, not coerced to floats.
    payload = simplejson.dumps(
        company,
        use_decimal=True,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )
    encoded = payload.encode("utf-8")
    if len(encoded) > MAX_COMPANY_BYTES:
        raise IngestionError("Company exceeds the 64 MiB normalized record limit")
    return {
        "siren": siren,
        "source_id": optional(company.get("id"), str),
        "source_updated_at": updated_at,
        "type_personne": optional(formality.get("typePersonne"), str),
        "denomination": optional(enterprise.get("denomination"), str),
        "forme_juridique": optional(
            first_present(
                enterprise.get("formeJuridique"),
                formality.get("formeJuridique"),
                mapping(content.get("natureCreation")).get("formeJuridique"),
            ),
            str,
        ),
        "code_ape": optional(enterprise.get("codeApe"), str),
        "code_ape_naf2025": optional(
            first_present(enterprise.get("codeApeNaf2025"), formality.get("codeApeNaf2025")),
            str,
        ),
        "diffusion_insee": optional(formality.get("diffusionINSEE"), str),
        "diffusion_commerciale": optional(formality.get("diffusionCommerciale"), bool),
        "deleted": optional(company.get("deleted"), bool),
        "company_json": payload,
        "company_sha256": hashlib.sha256(encoded).hexdigest(),
        "source_kind": source_kind,
        "source_sha256": receipt["sha256"],
        "source_uri": receipt["uri"],
        "source_generation": str(receipt["generation"]),
        "source_retrieved_at": parse_time(receipt["retrieved_at"]),
        "source_member": member,
        "source_row_number": row_number,
    }


def diff_rows(body: bytes, source: dict) -> list[dict]:
    """Bronze rows of one raw ``/companies/diff`` page: ``[{"company": {...}}, ...]``."""
    if len(body) > MAX_COMPANY_BYTES:
        raise IngestionError("Differential page exceeds 64 MiB; reduce API page size")
    try:
        items = simplejson.loads(body, use_decimal=True, allow_nan=False)
    except ValueError:
        raise IngestionError("Invalid differential JSON") from None
    if not isinstance(items, list) or len(items) > 100:
        raise IngestionError("Invalid differential page envelope or row limit")
    rows = []
    for number, item in enumerate(items, 1):
        if not isinstance(item, dict) or "company" not in item:
            raise IngestionError("Unexpected companies differential envelope")
        rows.append(company_row(item["company"], source, None, number, source_kind="diff"))
    return rows
