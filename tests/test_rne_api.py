from datetime import timedelta

import httpx
import pytest

from industrial_intelligence.ingestion.rne.api import RneApi
from industrial_intelligence.ingestion.rne.cli import main
from industrial_intelligence.ingestion.rne.common import IngestionError, Paused, parse_time

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
        api_for(handler).count_over_limit(START, END)


def test_count_can_signal_over_10000_without_exact_number():
    def handler(request):
        if request.url.path.endswith("login"):
            return httpx.Response(200, json={"token": "test-token"})
        return httpx.Response(200, json={"isNbResultsOver10000": True})

    assert api_for(handler).count_over_limit(START, END)


def test_timezone_required():
    with pytest.raises(IngestionError):
        parse_time("2020-01-01")
    assert parse_time("2020-01-01T01:00:00+01:00") == START


def test_cli_fails_without_bootstrap_parameters_before_any_cloud_access(monkeypatch, capsys):
    monkeypatch.delenv("RNE_STOCK_FILE", raising=False)
    monkeypatch.delenv("RNE_INITIAL_FROM", raising=False)
    assert main(["--bucket", "test", "bootstrap"]) == 1
    assert "requires" in capsys.readouterr().out


def test_cli_unknown_exception_is_redacted(monkeypatch, capsys):
    def fail(args):
        raise RuntimeError("never-log-this-password")

    monkeypatch.setattr("industrial_intelligence.ingestion.rne.cli.run", fail)
    assert main(["--bucket", "test", "refresh"]) == 1
    assert "never-log" not in capsys.readouterr().out
