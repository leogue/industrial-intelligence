"""Cloud Run / terminal entry point. Environment secrets only, never CLI passwords."""

import argparse
import json
import logging
import os
from datetime import timedelta

import httpx

from .api import API_BASE, RneApi
from .common import IngestionError, Paused, parse_time, utc_now, validate_prefix
from .ftp import StockFtp
from .pipeline import RnePipeline
from .storage import GcsStore


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise IngestionError(f"Missing environment variable: {name}")
    return value


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Raw INPI RNE companies ingestion")
    result.add_argument("--bucket", default=os.getenv("RNE_BUCKET"))
    result.add_argument("--prefix", default=os.getenv("RNE_PREFIX", "inpi/rne"))
    commands = result.add_subparsers(dest="command", required=True)
    bootstrap = commands.add_parser("bootstrap", help="Copy an explicit FTP stock ZIP to GCS")
    bootstrap.add_argument("--stock-file", default=os.getenv("RNE_STOCK_FILE"))
    bootstrap.add_argument("--delta-from", default=os.getenv("RNE_INITIAL_FROM"))
    bootstrap.add_argument("--allow-large-download", action="store_true")
    bootstrap.add_argument("--max-stock-bytes", type=int, default=20 * 1024**3)
    refresh = commands.add_parser("refresh", help="Resume companies differential ingestion")
    refresh.add_argument("--until", help="ISO 8601 datetime; defaults to a delayed closed UTC day")
    refresh.add_argument("--lag-days", type=int, default=1)
    refresh.add_argument("--window-hours", type=int, default=6)
    refresh.add_argument("--overlap-hours", type=int, default=24)
    refresh.add_argument("--page-size", type=int, default=10)
    refresh.add_argument("--max-pages", type=int, default=4500)
    refresh.add_argument("--max-requests", type=int, default=5000)
    refresh.add_argument(
        "--restart-window",
        action="store_true",
        help="Replay the incomplete window from its beginning (e.g. to reduce page size)",
    )
    return result


def run(args) -> dict:
    if not args.bucket:
        raise IngestionError("Provide --bucket or RNE_BUCKET")
    validate_prefix(args.prefix)
    if args.command == "bootstrap":
        if not args.stock_file or not args.delta_from:
            raise IngestionError(
                "Bootstrap requires --stock-file and an explicitly chosen --delta-from"
            )
        boundary = parse_time(args.delta_from)
        if boundary >= utc_now() or args.max_stock_bytes < 1:
            raise IngestionError("Invalid bootstrap boundary or byte limit")
        username = required_env("INPI_FTP_USERNAME")
        password = required_env("INPI_FTP_PASSWORD")
        with StockFtp(
            os.getenv("INPI_FTP_HOST", "www.inpi.net"),
            int(os.getenv("INPI_FTP_PORT", "21")),
            username,
            password,
        ) as source:
            return RnePipeline(GcsStore(args.bucket), args.prefix).bootstrap(
                source,
                args.stock_file,
                boundary,
                allow_large_download=args.allow_large_download,
                max_stock_bytes=args.max_stock_bytes,
            )
    if args.lag_days < 0:
        raise IngestionError("Safety lag must be non-negative")
    until = (
        parse_time(args.until)
        if args.until
        else utc_now().replace(hour=0, minute=0, second=0, microsecond=0)
        - timedelta(days=args.lag_days)
    )
    if until > utc_now():
        raise IngestionError("Refresh end cannot be in the future")
    if os.getenv("INPI_API_BASE_URL", API_BASE).rstrip("/") != API_BASE:
        raise IngestionError("Only the documented production RNE API origin is supported")
    username = required_env("INPI_USERNAME")
    password = required_env("INPI_PASSWORD")
    with httpx.Client(timeout=60, follow_redirects=False) as client:
        api = RneApi(
            username,
            password,
            client=client,
            page_size=args.page_size,
            max_requests=args.max_requests,
        )
        return RnePipeline(GcsStore(args.bucket), args.prefix).refresh(
            api,
            until,
            window_hours=args.window_hours,
            overlap_hours=args.overlap_hours,
            max_pages=args.max_pages,
            restart_window=args.restart_window,
        )


def main(argv=None) -> int:
    # Do not enable wire logging: headers/login bodies contain secrets.
    for logger in ("httpx", "httpcore", "google", "urllib3"):
        logging.getLogger(logger).setLevel(logging.WARNING)
    args = parser().parse_args(argv)
    try:
        result = run(args)
    except Paused as error:
        print(json.dumps({"status": "paused", "reason": str(error)}))
        return 2
    except IngestionError as error:
        print(json.dumps({"status": "failed", "reason": str(error)}))
        return 1
    except Exception as error:
        # SDK/transport exceptions can include URLs or response bodies. No str(error).
        print(json.dumps({"status": "failed", "error_type": type(error).__name__}))
        return 1
    print(json.dumps(result))
    return 0
