import io
import json
import logging

import httpx
import pytest

from industrial_intelligence.ingestion.rne.cli import main
from industrial_intelligence.ingestion.rne.common import Paused, parse_time
from industrial_intelligence.ingestion.rne.log import log_event, setup_logging
from industrial_intelligence.ingestion.rne.sources import RneApi


@pytest.fixture
def stream():
    output = io.StringIO()
    setup_logging("sync", output)
    yield output
    setup_logging("sync")  # Detach from the StringIO for the following tests.


def lines(output):
    return [json.loads(line) for line in output.getvalue().splitlines()]


def test_events_use_cloud_logging_special_fields(stream):
    log_event(logging.getLogger("industrial_intelligence.test"), "window_completed", records=3)
    (entry,) = lines(stream)
    assert entry["severity"] == "INFO"
    assert entry["event"] == "window_completed" and entry["records"] == 3
    assert entry["message"] == "window_completed records=3"
    assert entry["logging.googleapis.com/labels"] == {"app": "rne-ingest", "command": "sync"}
    assert entry["logging.googleapis.com/sourceLocation"]["function"] == (
        "test_events_use_cloud_logging_special_fields"
    )


def test_third_party_records_are_json_too(stream):
    logging.getLogger("some.library").warning("plain %s", "text")
    (entry,) = lines(stream)
    assert entry["severity"] == "WARNING" and entry["message"] == "plain text"
    assert "event" not in entry


def test_setup_is_idempotent(stream):
    setup_logging("sync", stream)
    logging.getLogger("industrial_intelligence.test").info("once")
    assert len(lines(stream)) == 1


def test_cli_failure_is_an_error_entry_without_secrets(monkeypatch, capsys):
    def fail(args):
        raise RuntimeError("never-log-this-password")

    monkeypatch.setattr("industrial_intelligence.ingestion.rne.cli.run", fail)
    assert main(["--bucket", "test", "sync", "--start", "2026-03-01T00:00:00Z"]) == 1
    output = capsys.readouterr().out
    entries = [json.loads(line) for line in output.splitlines()]
    assert [entry["event"] for entry in entries] == ["command_started", "command_failed"]
    assert entries[-1]["severity"] == "ERROR" and entries[-1]["error_type"] == "RuntimeError"
    assert "never-log" not in output


def test_partial_sync_is_a_warning_summary(monkeypatch, capsys):
    monkeypatch.setattr(
        "industrial_intelligence.ingestion.rne.cli.run",
        lambda args: {"status": "partial", "days_behind": 12.5, "api_requests": 10000},
    )
    assert main(["--bucket", "test", "sync", "--start", "2026-03-01T00:00:00Z"]) == 0
    summary = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert summary["event"] == "command_completed" and summary["severity"] == "WARNING"
    assert summary["days_behind"] == 12.5


def test_api_retries_and_quota_are_visible(stream):
    api = RneApi(
        "unit-test-user",
        "never-log-this-password",
        client=httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(429))),
        sleep=lambda _: None,
        attempts=2,
    )
    api._token = "synthetic-token"
    start = parse_time("2026-03-01T00:00:00Z")
    with pytest.raises(Paused):
        api.count(start, start)
    retries = [entry for entry in lines(stream) if entry["event"] == "api_retry"]
    assert [entry["status"] for entry in retries] == [429, 429]
    assert retries[0]["severity"] == "WARNING" and retries[0]["path"] == "/companies/diff/count"
    assert "synthetic-token" not in stream.getvalue()
    assert "never-log-this-password" not in stream.getvalue()
