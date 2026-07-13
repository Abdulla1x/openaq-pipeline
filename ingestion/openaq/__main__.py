"""Manual ingestion entry point (Phase 3's Airflow DAG will import the
functions directly instead of shelling out to this).

    set -a && source .env && set +a
    python -m ingestion.openaq --country AE --date 2026-07-12
"""

import argparse
import datetime as dt
import logging
import sys

from ingestion.openaq.client import OpenAQClient
from ingestion.openaq.config import load_settings
from ingestion.openaq.gcs import make_raw_zone_writer
from ingestion.openaq.ingest import ingest_country_day


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m ingestion.openaq",
        description="Fetch one UTC day of OpenAQ v3 measurements for one country into GCS.",
    )
    parser.add_argument("--country", required=True, help="ISO 3166-1 alpha-2 code, e.g. AE or PK")
    parser.add_argument(
        "--date",
        type=dt.date.fromisoformat,
        default=dt.datetime.now(dt.UTC).date() - dt.timedelta(days=1),
        help="UTC day to fetch, YYYY-MM-DD (default: yesterday)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = load_settings()
    client = OpenAQClient(api_key=settings.api_key, base_url=settings.base_url)
    writer = make_raw_zone_writer(settings.bucket_name)
    summary = ingest_country_day(client, writer, args.country.upper(), args.date)
    # Partial failure is not silent success: landed data stays, but the run
    # reports it so a rerun/orchestrator can catch up the failed sensors.
    return 1 if summary.sensors_failed else 0


if __name__ == "__main__":
    sys.exit(main())
