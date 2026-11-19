"""Cloud Run / terminal entry point: Silver Parquet run -> Iceberg tables (BigLake catalog)."""

import argparse
import logging
import os

from pyiceberg.catalog import load_catalog

from ..ingestion.rne.common import IngestionError, validate_prefix
from ..ingestion.rne.log import log_event, setup_logging
from ..ingestion.rne.storage import GcsStore
from .load import publish

logger = logging.getLogger(__name__)

BIGLAKE_REST_URI = "https://biglake.googleapis.com/iceberg/v1/restcatalog"


def parser():
    result = argparse.ArgumentParser(description="Publish the latest Silver run to Iceberg")
    result.add_argument("--bucket", default=os.getenv("LAKEHOUSE_BUCKET"))
    result.add_argument("--prefix", default=os.getenv("LAKEHOUSE_SILVER_PREFIX", "silver/samples"))
    result.add_argument("--project", default=os.getenv("LAKEHOUSE_PROJECT"))
    result.add_argument("--catalog", default=os.getenv("LAKEHOUSE_CATALOG"))
    result.add_argument("--namespace", default=os.getenv("LAKEHOUSE_NAMESPACE", "silver"))
    return result


def biglake_catalog(project: str, catalog: str):
    return load_catalog(
        "biglake",
        type="rest",
        uri=BIGLAKE_REST_URI,
        warehouse=f"bl://projects/{project}/catalogs/{catalog}",
        auth={"type": "google"},
        # User ADC needs an explicit quota project for the BigLake API.
        **{"header.x-goog-user-project": project},
    )


def main(argv=None):
    args = parser().parse_args(argv)
    setup_logging("publish", app="lakehouse-load")
    log_event(logger, "command_started", bucket=args.bucket, prefix=args.prefix)
    try:
        if not (args.bucket and args.project and args.catalog):
            raise IngestionError("Provide --bucket, --project and --catalog (or LAKEHOUSE_*)")
        result = publish(
            biglake_catalog(args.project, args.catalog),
            GcsStore(args.bucket),
            validate_prefix(args.prefix),
            args.namespace,
        )
    except IngestionError as error:
        log_event(logger, "command_failed", logging.ERROR, reason=str(error))
        return 1
    except Exception as error:
        # SDK messages can expose URLs or bodies; log only the exception class.
        log_event(logger, "command_failed", logging.ERROR, error_type=type(error).__name__)
        return 1
    log_event(logger, "command_completed", **result)
    return 0
