from contextlib import nullcontext
from datetime import timedelta
from unittest.mock import Mock

import httpx
import pytest

from industrial_intelligence.ingestion.rne.cli import main
from industrial_intelligence.ingestion.rne.common import IngestionError, Paused, parse_time
from industrial_intelligence.ingestion.rne.sources import RneApi

START = parse_time("2020-01-01T00:00:00Z")
END = START + timedelta(hours=1)


def api_for(handler, **kwargs):
    return RneApi(
        "unit-test-user",
        "never-log-this-password",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=lambda _: None,
        **kwargs,
    )


def test_login_pagination_and_timezone_encoding():
    calls = []

    def handler(request):
        calls.append(request)
        if request.url.path.endswith("login"):
            return httpx.Response(200, json={"token": "private-test-token"})
        assert request.headers["Authorization"] == "Bearer private-test-token"
        assert request.url.params["searchAfter"] == "opaque+cursor/="
        assert request.url.params["from"].endswith("Z")
        return httpx.Response(200, content=b"[]")

    api = api_for(handler)
    assert api.page(START, END, "opaque+cursor/=")[1] == []
    assert len(calls) == 2


def test_transient_errors_retry_after_and_bounded_retry():
    delays = []
    statuses = iter([429, 503, 200])

    def handler(request):
        if request.url.path.endswith("login"):
            return httpx.Response(200, json={"token": "test-token"})
        return httpx.Response(next(statuses), content=b"[]", headers={"Retry-After": "2"})

    api = api_for(handler)
    api.sleep = delays.append
    api.page(START, END, None)
    assert delays.count(2) == 2


def test_network_failure_is_retried_without_leaking_exception():
    attempts = 0

    def handler(request):
        nonlocal attempts
        attempts += 1
        raise httpx.ConnectError("sensitive transport details", request=request)

    with pytest.raises(IngestionError, match="retries exhausted") as error:
        api_for(handler, attempts=3).page(START, END, None)
    assert attempts == 3
    assert "sensitive" not in str(error.value)


def test_unauthorized_renews_once():
    logins = pages = 0

    def handler(request):
        nonlocal logins, pages
        if request.url.path.endswith("login"):
            logins += 1
            return httpx.Response(200, json={"token": f"token-{logins}"})
        pages += 1
        return httpx.Response(401 if pages == 1 else 200, content=b"[]")

    api_for(handler).page(START, END, None)
    assert logins == 2 and pages == 2


@pytest.mark.parametrize("status", [400, 401, 403, 302])
def test_permanent_failure_has_no_body_or_credentials(status):
    def handler(request):
        return httpx.Response(status, text="never-log-this-password")

    with pytest.raises(IngestionError) as error:
        api_for(handler).page(START, END, None)
    assert "never-log" not in str(error.value)
    assert str(status) in str(error.value)


def test_long_retry_after_pauses_instead_of_retrying_early():
    def handler(request):
        return httpx.Response(429, headers={"Retry-After": "3600"})

    with pytest.raises(Paused):
        api_for(handler).page(START, END, None)


def test_request_budget_includes_login_and_retries():
    def handler(request):
        return httpx.Response(200, json={"token": "test-token"})

    api = api_for(handler, max_requests=1)
    with pytest.raises(Paused):
        api.page(START, END, None)
    assert api.requests == 1


@pytest.mark.parametrize("content", [b"not-json", b"{}", b"null"])
def test_invalid_page_json(content):
    def handler(request):
        if request.url.path.endswith("login"):
            return httpx.Response(200, json={"token": "test-token"})
        return httpx.Response(200, content=content)

    with pytest.raises(IngestionError):
        api_for(handler).page(START, END, None)


def test_response_size_is_bounded():
    def handler(request):
        if request.url.path.endswith("login"):
            return httpx.Response(200, json={"token": "test-token"})
        return httpx.Response(200, content=b" " * 100)

    with pytest.raises(IngestionError, match="too large"):
        api_for(handler, max_response_bytes=50).page(START, END, None)


@pytest.mark.parametrize(
    "value", [{}, {"isNbResultsOver10000": "false"}, {"isNbResultsOver10000": False}]
)
def test_count_must_have_documented_shape(value):
    def handler(request):
        if request.url.path.endswith("login"):
            return httpx.Response(200, json={"token": "test-token"})
        return httpx.Response(200, json=value)

    with pytest.raises(IngestionError):
        api_for(handler).count(START, END)


def test_count_can_signal_over_10000_without_exact_number():
    def handler(request):
        if request.url.path.endswith("login"):
            return httpx.Response(200, json={"token": "test-token"})
        return httpx.Response(200, json={"isNbResultsOver10000": True})

    assert api_for(handler).count(START, END) is None


def test_timezone_required():
    with pytest.raises(IngestionError):
        parse_time("2020-01-01")
    assert parse_time("2020-01-01T01:00:00+01:00") == START


def test_quota_exhaustion_pauses_cleanly_instead_of_failing():
    api = api_for(lambda request: httpx.Response(429), attempts=2)
    with pytest.raises(Paused, match="quota"):
        api.count(START, END)


def test_cli_fails_without_bootstrap_parameters_before_any_cloud_access(monkeypatch, capsys):
    monkeypatch.delenv("RNE_STOCK_FILE", raising=False)
    monkeypatch.setattr("industrial_intelligence.ingestion.rne.cli.GcsStore", lambda *a: object())
    assert main(["--bucket", "test", "bootstrap"]) == 1
    assert "requires" in capsys.readouterr().out


def test_cli_bootstrap_archives_with_ftp_credentials_only(monkeypatch, capsys):
    monkeypatch.setenv("INPI_FTP_USERNAME", "unit-user")
    monkeypatch.setenv("INPI_FTP_PASSWORD", "unit-password")
    archive = Mock(return_value={"status": "archived"})
    monkeypatch.setattr(
        "industrial_intelligence.ingestion.rne.cli.StockFtp", lambda *a: nullcontext(object())
    )
    monkeypatch.setattr("industrial_intelligence.ingestion.rne.cli.GcsStore", lambda *a: object())
    monkeypatch.setattr("industrial_intelligence.ingestion.rne.bootstrap.archive_stock", archive)
    assert (
        main(["--bucket", "test", "bootstrap", "--stock-file", "stock_RNE_formalites_test.zip"])
        == 0
    )
    assert archive.call_args.args[3] == "stock_RNE_formalites_test.zip"
    assert "unit-password" not in capsys.readouterr().out


@pytest.mark.parametrize(
    "args",
    [
        ["refresh"],
        ["prepare-sync", "--stock-revision", "b" * 64],
        ["current-status"],
        ["sync"],  # The stock boundary is always explicit.
    ],
)
def test_cli_only_has_bootstrap_materialize_and_sync(args):
    with pytest.raises(SystemExit) as error:
        main(args)
    assert error.value.code == 2


def test_cli_materialize_is_bounded_by_default_and_needs_no_inpi_credentials(monkeypatch):
    for variable in ("INPI_USERNAME", "INPI_PASSWORD", "INPI_FTP_USERNAME", "INPI_FTP_PASSWORD"):
        monkeypatch.delenv(variable, raising=False)
    worker = Mock()
    worker.run.return_value = {"status": "partial"}
    monkeypatch.setattr("industrial_intelligence.ingestion.rne.cli.GcsStore", lambda *a: object())
    monkeypatch.setattr(
        "industrial_intelligence.ingestion.rne.materialize.StockMaterializer", lambda *a: worker
    )
    assert main(["--bucket", "test", "materialize", "--stock-revision", "b" * 64]) == 0
    assert worker.run.call_args.kwargs["max_members"] == 1
    assert worker.run.call_args.kwargs["max_input_bytes"] == 128 * 1024**2


def test_cli_sync_passes_the_boundary_and_a_closed_day(monkeypatch):
    monkeypatch.setenv("INPI_USERNAME", "unit-user")
    monkeypatch.setenv("INPI_PASSWORD", "unit-password")
    worker = Mock()
    worker.run.return_value = {"status": "partial"}
    monkeypatch.setattr("industrial_intelligence.ingestion.rne.cli.GcsStore", lambda *a: object())
    monkeypatch.setattr("industrial_intelligence.ingestion.rne.diff.DiffSync", lambda *a: worker)
    assert main(["--bucket", "test", "sync", "--start", "2026-03-01T00:00:00Z"]) == 0
    api, start, until = worker.run.call_args.args
    assert api.page_size == 100 and api.max_requests == 10000
    assert start == parse_time("2026-03-01T00:00:00Z")
    assert until.hour == until.minute == until.second == 0


def test_cli_unknown_exception_is_redacted(monkeypatch, capsys):
    def fail(args):
        raise RuntimeError("never-log-this-password")

    monkeypatch.setattr("industrial_intelligence.ingestion.rne.cli.run", fail)
    assert main(["--bucket", "test", "sync", "--start", "2026-03-01T00:00:00Z"]) == 1
    assert "never-log" not in capsys.readouterr().out
