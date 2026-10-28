"""Cloud Run / terminal entry point. data.gouv.fr is public: no credentials are needed."""

import argparse
import logging
import os

import httpx

from ..rne.common import IngestionError
from ..rne.log import log_event, setup_logging
from ..rne.storage import GcsStore
from .archive import TABLES, archive

logger = logging.getLogger(__name__)


def parser():
    result = argparse.ArgumentParser(description="INSEE SIRENE: monthly Parquet stock to GCS")
    result.add_argument("--bucket", default=os.getenv("SIRENE_BUCKET"))
    result.add_argument("--prefix", default=os.getenv("SIRENE_PREFIX", "insee/sirene"))
    result.add_argument(
        "--table", action="append", choices=list(TABLES), help="Repeatable; defaults to all"
    )
    result.add_argument(
        "--allow-large-download", action="store_true", help="Required above 100 MiB per file"
    )
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    setup_logging("archive", app="sirene-ingest")
    log_event(logger, "command_started", bucket=args.bucket, prefix=args.prefix)
    try:
        if not args.bucket:
            raise IngestionError("Provide --bucket or SIRENE_BUCKET")
        with httpx.Client(timeout=60, follow_redirects=True) as client:
            result = archive(
                GcsStore(args.bucket),
                args.prefix,
                client,
                args.table or list(TABLES),
                allow_large_download=args.allow_large_download,
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
