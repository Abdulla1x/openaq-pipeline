"""Thin HTTP client for the OpenAQ v3 API: auth, throttling, retries, pagination.

The API allows 60 requests/minute (observed in x-ratelimit-* headers,
2026-07-12). The client throttles off those headers rather than a hardcoded
sleep, so a limit change on OpenAQ's side degrades gracefully.

Methods return the raw `requests.Response` — callers need the verbatim body
text for the GCS raw zone (G1), not a re-serialized parse.
"""

import logging
import time
from collections.abc import Callable, Iterator

import requests

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS = 30
MAX_ATTEMPTS = 5
BACKOFF_BASE_SECONDS = 2
PAGE_LIMIT = 1000
# Fallback when a 429 arrives without an x-ratelimit-reset header.
RATE_LIMIT_FALLBACK_SECONDS = 60


class OpenAQAuthError(RuntimeError):
    """401/403 from the API — retrying cannot help, fail immediately."""


class OpenAQClient:
    def __init__(
        self,
        api_key: str,
        base_url: str,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        max_attempts: int = MAX_ATTEMPTS,
    ):
        self._session = session or requests.Session()
        self._session.headers["X-API-Key"] = api_key
        self._base_url = base_url.rstrip("/")
        self._sleep = sleep
        # Tunable because retrying a *persistently* broken sensor is pure cost:
        # the Phase 3 DAG uses 2 attempts (~30 known-bad PK sensors × 62s of
        # backoff at 5 attempts was ~80% of a country run's wall time).
        self._max_attempts = max_attempts

    def get(self, path: str, params: dict | None = None) -> requests.Response:
        """GET with bounded retries: 429 waits for the rate window, 5xx and
        connection errors back off exponentially, 401/403 fail fast."""
        url = f"{self._base_url}{path}"
        last_error: Exception | None = None
        for attempt in range(self._max_attempts):
            # Sleeping after the *final* failed attempt is pure wasted latency —
            # there is no next attempt to back off for (the DAG's known-bad
            # sensors hit this on every run).
            is_last_attempt = attempt == self._max_attempts - 1
            try:
                response = self._session.get(url, params=params, timeout=DEFAULT_TIMEOUT_SECONDS)
            except requests.RequestException as exc:
                last_error = exc
                if is_last_attempt:
                    break
                wait = BACKOFF_BASE_SECONDS * 2**attempt
                logger.warning("GET %s failed (%s); retrying in %ss", path, exc, wait)
                self._sleep(wait)
                continue

            if response.status_code in (401, 403):
                raise OpenAQAuthError(
                    f"OpenAQ returned {response.status_code} for {path} — check OPENAQ_API_KEY"
                )
            if response.status_code == 429:
                last_error = requests.HTTPError(f"429 on {path}", response=response)
                if is_last_attempt:
                    break
                wait = _int_header(response, "x-ratelimit-reset", RATE_LIMIT_FALLBACK_SECONDS)
                logger.warning("Rate limited on %s; waiting %ss", path, wait)
                self._sleep(wait)
                continue
            if response.status_code >= 500:
                last_error = requests.HTTPError(
                    f"{response.status_code} on {path}", response=response
                )
                if is_last_attempt:
                    break
                wait = BACKOFF_BASE_SECONDS * 2**attempt
                logger.warning("HTTP %s on %s; retrying in %ss", response.status_code, path, wait)
                self._sleep(wait)
                continue

            response.raise_for_status()
            self._respect_rate_limit(response)
            return response

        raise RuntimeError(
            f"GET {path} failed after {self._max_attempts} attempts"
        ) from last_error

    def paginate(self, path: str, params: dict | None = None) -> Iterator[requests.Response]:
        """Yield one Response per page until a short page signals the end.

        Termination checks len(results) < limit rather than meta.found — the
        API sometimes reports found as a string (e.g. ">1000").
        """
        page = 1
        while True:
            response = self.get(path, {**(params or {}), "limit": PAGE_LIMIT, "page": page})
            yield response
            if len(response.json().get("results", [])) < PAGE_LIMIT:
                return
            page += 1

    def _respect_rate_limit(self, response: requests.Response) -> None:
        if _int_header(response, "x-ratelimit-remaining", 1) <= 0:
            wait = _int_header(response, "x-ratelimit-reset", RATE_LIMIT_FALLBACK_SECONDS)
            logger.info("Rate-limit window exhausted; waiting %ss", wait)
            self._sleep(wait)


def _int_header(response: requests.Response, name: str, default: int) -> int:
    try:
        return int(response.headers.get(name, default))
    except (TypeError, ValueError):
        return default
