"""Runtime configuration for the ingestion package, read from the environment.

Values come from the process environment (locally sourced from `.env`; in
Phase 3 injected by docker-compose). No file parsing here — the environment is
the single interface, so the same code runs unchanged under Airflow.
"""

import os
from dataclasses import dataclass

_REQUIRED_VARS = ("OPENAQ_API_KEY", "OPENAQ_API_BASE_URL", "GCS_BUCKET_NAME")


@dataclass(frozen=True)
class Settings:
    api_key: str
    base_url: str
    bucket_name: str


def load_settings(env: dict[str, str] | None = None) -> Settings:
    """Build Settings from the environment; fail loudly listing what's missing."""
    env = os.environ if env is None else env
    missing = [name for name in _REQUIRED_VARS if not env.get(name)]
    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}. "
            "Copy .env.example to .env, fill it in, and source it "
            "(set -a && source .env && set +a)."
        )
    return Settings(
        api_key=env["OPENAQ_API_KEY"],
        base_url=env["OPENAQ_API_BASE_URL"].rstrip("/"),
        bucket_name=env["GCS_BUCKET_NAME"],
    )
