"""Cloud Run / terminal entry point. Secrets come from the environment, never arguments."""

import argparse
import logging
import os
from datetime import timedelta

import httpx

from .common import IngestionError, Paused, parse_time, utc_now, validate_prefix
from .log import log_event, setup_logging
from .sources import RneApi, StockFtp
from .storage import GcsStore

logger = logging.getLogger(__name__)


def required_env(name):
    value = os.environ.get(name)
    if not value:
        raise IngestionError(f"Missing environment variable: {name}")
    return value


def parser():
    result = argparse.ArgumentParser(description="INPI RNE: FTP stock, then daily API changes")
    result.add_argument("--bucket", default=os.getenv("RNE_BUCKET"))
    result.add_argument("--prefix", default=os.getenv("RNE_PREFIX", "inpi/rne"))
    commands = result.add_subparsers(dest="command", required=True)

    bootstrap = commands.add_parser("bootstrap", help="Archive an explicit FTP stock ZIP in GCS")
    bootstrap.add_argument("--stock-file", default=os.getenv("RNE_STOCK_FILE"))
    bootstrap.add_argument("--allow-large-download", action="store_true")
    bootstrap.add_argument("--max-stock-bytes", type=int, default=20 * 1024**3)

    materialize = commands.add_parser(
        "materialize", help="Convert the archived stock to Bronze Parquet"
    )
    materialize.add_argument("--stock-revision", required=True)
    work = materialize.add_mutually_exclusive_group()
    work.add_argument("--max-members", type=int, default=1)
    work.add_argument("--all-members", action="store_true")
    materialize.add_argument("--max-input-bytes", type=int, default=128 * 1024**2)
    materialize.add_argument("--max-member-bytes", type=int, default=128 * 1024**2)
    materialize.add_argument("--batch-rows", type=int, default=500)
    materialize.add_argument("--max-batch-bytes", type=int, default=8 * 1024**2)
    materialize.add_argument("--max-seconds", type=int, default=1800)

    sync = commands.add_parser(
        "sync", help="Collect API changes since the stock, resuming the checkpoint"
    )
    sync.add_argument(
        "--start", required=True, help="Assumed stock boundary, e.g. 2026-03-01T00:00:00Z"
    )
    sync.add_argument("--until", help="ISO datetime; defaults to midnight UTC minus --lag-days")
    sync.add_argument("--lag-days", type=int, default=1)
    sync.add_argument("--page-size", type=int, default=100)
    sync.add_argument("--max-requests", type=int, default=10000)
    sync.add_argument("--max-seconds", type=int, default=20000)
    return result


def run(args):
    if not args.bucket:
        raise IngestionError("Provide --bucket or RNE_BUCKET")
    validate_prefix(args.prefix)
    store = GcsStore(args.bucket)
    if args.command == "bootstrap":
        from .bootstrap import archive_stock

        if not args.stock_file or args.max_stock_bytes < 1:
            raise IngestionError("Bootstrap requires --stock-file and a positive byte limit")
        with StockFtp(
            os.getenv("INPI_FTP_HOST", "www.inpi.net"),
            int(os.getenv("INPI_FTP_PORT", "21")),
            required_env("INPI_FTP_USERNAME"),
            required_env("INPI_FTP_PASSWORD"),
        ) as source:
            return archive_stock(
                store,
                args.prefix,
                source,
                args.stock_file,
                allow_large_download=args.allow_large_download,
                max_stock_bytes=args.max_stock_bytes,
            )
    if args.command == "materialize":
        from .materialize import StockMaterializer

        return StockMaterializer(store, args.prefix).run(
            args.stock_revision,
            max_members=None if args.all_members else args.max_members,
            max_input_bytes=args.max_input_bytes,
            max_member_bytes=args.max_member_bytes,
            batch_rows=args.batch_rows,
            max_batch_bytes=args.max_batch_bytes,
            max_seconds=args.max_seconds,
        )

    from .diff import DiffSync

    if args.lag_days < 0:
        raise IngestionError("--lag-days must be non-negative")
    midnight = utc_now().replace(hour=0, minute=0, second=0, microsecond=0)
    until = parse_time(args.until) if args.until else midnight - timedelta(days=args.lag_days)
    username, password = required_env("INPI_USERNAME"), required_env("INPI_PASSWORD")
    with httpx.Client(timeout=60, follow_redirects=False) as client:
        api = RneApi(
            username,
            password,
            client=client,
            page_size=args.page_size,
            max_requests=args.max_requests,
        )
        return DiffSync(store, args.prefix).run(
            api, parse_time(args.start), until, max_seconds=args.max_seconds
        )


def main(argv=None):
    args = parser().parse_args(argv)
    setup_logging(args.command)
    log_event(logger, "command_started", bucket=args.bucket, prefix=args.prefix)
    try:
        result = run(args)
    except Paused as error:
        log_event(logger, "command_paused", logging.WARNING, reason=str(error))
        return 2
    except IngestionError as error:
        log_event(logger, "command_failed", logging.ERROR, reason=str(error))
        return 1
    except Exception as error:
        # SDK messages can expose URLs or bodies; log only the exception class.
        log_event(logger, "command_failed", logging.ERROR, error_type=type(error).__name__)
        return 1
    level = logging.WARNING if result.get("status") == "partial" else logging.INFO
    log_event(logger, "command_completed", level, **result)
    return 0
