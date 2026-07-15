"""OpenAQClient behavior: pagination, rate limiting, retries, auth failures.

All HTTP is mocked with `responses` (G12 — unit tests mock the API); the sleep
callable is injected so no test actually waits.
"""

import pytest
import requests
import responses

from ingestion.openaq.client import MAX_ATTEMPTS, PAGE_LIMIT, OpenAQAuthError, OpenAQClient

BASE = "https://api.test/v3"


def make_client(sleeps: list[float]) -> OpenAQClient:
    return OpenAQClient(api_key="k", base_url=BASE, sleep=sleeps.append)


@responses.activate
def test_paginate_stops_on_short_page():
    responses.get(f"{BASE}/countries", json={"results": [{}] * PAGE_LIMIT})
    responses.get(f"{BASE}/countries", json={"results": [{}]})

    pages = list(make_client([]).paginate("/countries"))

    assert len(pages) == 2
    assert "page=1" in responses.calls[0].request.url
    assert "page=2" in responses.calls[1].request.url
    assert f"limit={PAGE_LIMIT}" in responses.calls[0].request.url


@responses.activate
def test_429_waits_for_reset_then_retries():
    responses.get(f"{BASE}/countries", status=429, headers={"x-ratelimit-reset": "7"})
    responses.get(f"{BASE}/countries", json={"results": []})

    sleeps: list[float] = []
    response = make_client(sleeps).get("/countries")

    assert response.json() == {"results": []}
    assert sleeps == [7]


@responses.activate
def test_5xx_backs_off_exponentially():
    responses.get(f"{BASE}/countries", status=500)
    responses.get(f"{BASE}/countries", status=503)
    responses.get(f"{BASE}/countries", json={"results": []})

    sleeps: list[float] = []
    make_client(sleeps).get("/countries")

    assert sleeps == [2, 4]


@responses.activate
def test_connection_error_retries():
    responses.get(f"{BASE}/countries", body=requests.ConnectionError("boom"))
    responses.get(f"{BASE}/countries", json={"results": []})

    sleeps: list[float] = []
    response = make_client(sleeps).get("/countries")

    assert response.json() == {"results": []}
    assert len(sleeps) == 1


@responses.activate
def test_gives_up_after_max_attempts():
    for _ in range(MAX_ATTEMPTS):
        responses.get(f"{BASE}/countries", status=500)

    with pytest.raises(RuntimeError, match=f"after {MAX_ATTEMPTS} attempts"):
        make_client([]).get("/countries")

    assert len(responses.calls) == MAX_ATTEMPTS


@responses.activate
def test_max_attempts_is_tunable():
    """The Phase 3 DAG runs with max_attempts=2 so persistently-broken sensors
    (30 known on PK) don't burn the full 5-attempt backoff each."""
    responses.get(f"{BASE}/countries", status=500)
    responses.get(f"{BASE}/countries", status=500)

    client = OpenAQClient(api_key="k", base_url=BASE, sleep=lambda _: None, max_attempts=2)
    with pytest.raises(RuntimeError, match="after 2 attempts"):
        client.get("/countries")

    assert len(responses.calls) == 2


@responses.activate
def test_auth_error_fails_fast_without_retry():
    responses.get(f"{BASE}/countries", status=401)

    with pytest.raises(OpenAQAuthError, match="OPENAQ_API_KEY"):
        make_client([]).get("/countries")

    assert len(responses.calls) == 1


@responses.activate
def test_throttles_when_rate_window_exhausted():
    responses.get(
        f"{BASE}/countries",
        json={"results": []},
        headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": "30"},
    )

    sleeps: list[float] = []
    make_client(sleeps).get("/countries")

    assert sleeps == [30]


@responses.activate
def test_no_throttle_while_budget_remains():
    responses.get(
        f"{BASE}/countries",
        json={"results": []},
        headers={"x-ratelimit-remaining": "42", "x-ratelimit-reset": "60"},
    )

    sleeps: list[float] = []
    make_client(sleeps).get("/countries")

    assert sleeps == []
