"""Structured logs for Cloud Logging: one JSON object per line on stdout.

Cloud Run turns each JSON line into ``jsonPayload`` and reads the special fields
``severity``, ``message``, ``logging.googleapis.com/labels`` and
``logging.googleapis.com/sourceLocation``. No client library or API call is needed.

Never log secrets, tokens, company payloads or SIRENs: only technical counters.
"""

import json
import logging
import sys
from datetime import UTC, datetime

NOISY_LIBRARIES = ("httpx", "httpcore", "google", "urllib3")


def log_event(logger: logging.Logger, event: str, level: int = logging.INFO, **fields) -> None:
    """Log a named event; ``fields`` become filterable ``jsonPayload`` keys."""
    logger.log(level, event, extra={"event_fields": fields}, stacklevel=2)


class CloudLoggingFormatter(logging.Formatter):
    def __init__(self, labels: dict[str, str]):
        super().__init__()
        self.labels = labels

    def format(self, record: logging.LogRecord) -> str:
        fields = getattr(record, "event_fields", None)
        message = record.getMessage()
        payload = {
            "severity": record.levelname,  # DEBUG/INFO/WARNING/ERROR/CRITICAL match Cloud Logging.
            "time": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "logger": record.name,
            "logging.googleapis.com/labels": self.labels,
            "logging.googleapis.com/sourceLocation": {
                "file": record.pathname,
                "line": str(record.lineno),
                "function": record.funcName,
            },
        }
        if fields is None:
            payload["message"] = message  # Third-party record: keep its plain text.
        else:
            # Readable summary line in Logs Explorer, plus the same values as typed fields.
            summary = " ".join(
                f"{key}={value}" for key, value in fields.items() if value is not None
            )
            payload |= {"message": f"{message} {summary}".strip(), "event": message, **fields}
        return json.dumps(payload, ensure_ascii=False, default=str)


def setup_logging(command: str, stream=None, app: str = "rne-ingest") -> None:
    """Send every record to stdout as JSON; replaces a previous setup (tests, reruns)."""
    root = logging.getLogger()
    for handler in list(root.handlers):
        if isinstance(handler.formatter, CloudLoggingFormatter):
            root.removeHandler(handler)
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(CloudLoggingFormatter({"app": app, "command": command}))
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    for name in NOISY_LIBRARIES:
        logging.getLogger(name).setLevel(logging.WARNING)
