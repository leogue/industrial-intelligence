"""Cloud Run / terminal entry point. The BODACC API is public: no credentials are needed."""

import argparse
import logging
import os
from datetime import date, timedelta

import httpx

from ..rne.common import IngestionError, utc_now
from ..rne.log import log_event, setup_logging
from ..rne.storage import GcsStore
from .archive import archive

logger = logging.getLogger(__name__)


def parser():
    result = argparse.ArgumentParser(description="DILA BODACC: daily notice exports to GCS")
    result.add_argument("--bucket", default=os.getenv("BODACC_BUCKET"))
    result.add_argument("--prefix", default=os.getenv("BODACC_PREFIX", "dila/bodacc"))
    result.add_argument("--until", help="Last publication day (YYYY-MM-DD); defaults to yesterday")
    result.add_argument(
        "--recheck-days", type=int, default=30, help="Archived days exported again to catch fixes"
    )
    result.add_argument("--max-seconds", type=int, default=20000)
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    setup_logging("archive", app="bodacc-ingest")
    log_event(logger, "command_started", bucket=args.bucket, prefix=args.prefix)
    try:
        if not args.bucket:
            raise IngestionError("Provide --bucket or BODACC_BUCKET")
        today = utc_now().date()
        try:
            until = date.fromisoformat(args.until) if args.until else today - timedelta(days=1)
        except ValueError:
            raise IngestionError("--until must be a YYYY-MM-DD date") from None
        if until >= today:
            raise IngestionError("--until must be before today: the day may still be published")
        with httpx.Client(timeout=120) as client:
            result = archive(
                GcsStore(args.bucket),
                args.prefix,
                client,
                until,
                recheck_days=args.recheck_days,
                max_seconds=args.max_seconds,
            )
    except IngestionError as error:
        log_event(logger, "command_failed", logging.ERROR, reason=str(error))
        return 1
    except Exception as error:
        # SDK messages can expose URLs or bodies; log only the exception class.
        log_event(logger, "command_failed", logging.ERROR, error_type=type(error).__name__)
        return 1
    level = logging.WARNING if result["status"] == "partial" else logging.INFO
    log_event(logger, "command_completed", level, **result)
    return 0
