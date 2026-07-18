"""Manual ingestion entry points (the Airflow DAG imports the functions
directly instead of shelling out to this).

    set -a && source .env && set +a
    python -m ingestion.openaq ingest --country AE --date 2026-07-12
    python -m ingestion.openaq backfill --country PK --start 2025-06-01 --end 2026-07-18

The backfill is chunked and resumable: interrupt it freely, re-run the same
command and completed chunks are skipped (see backfill.py).
"""

import argparse
import datetime as dt
import logging
import sys

from ingestion.openaq.backfill import (
    DEFAULT_CHUNK_DAYS,
    BackfillState,
    load_skip_sensors,
    make_raw_loader,
    run_backfill,
)
from ingestion.openaq.client import OpenAQClient
from ingestion.openaq.config import load_settings
from ingestion.openaq.gcs import make_raw_zone_writer
from ingestion.openaq.ingest import ingest_country_day


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m ingestion.openaq",
        description="OpenAQ v3 → GCS raw-zone ingestion.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest = subparsers.add_parser(
        "ingest", help="fetch one UTC day for one country (the daily unit)"
    )
    ingest.add_argument("--country", required=True, help="ISO 3166-1 alpha-2 code, e.g. AE or PK")
    ingest.add_argument(
        "--date",
        type=dt.date.fromisoformat,
        default=dt.datetime.now(dt.UTC).date() - dt.timedelta(days=1),
        help="UTC day to fetch, YYYY-MM-DD (default: yesterday)",
    )

    backfill = subparsers.add_parser(
        "backfill", help="chunked, resumable history load over [start, end)"
    )
    backfill.add_argument("--country", required=True, help="ISO 3166-1 alpha-2 code")
    backfill.add_argument("--start", type=dt.date.fromisoformat, required=True,
                          help="window start (inclusive), YYYY-MM-DD")
    backfill.add_argument(
        "--end",
        type=dt.date.fromisoformat,
        default=dt.datetime.now(dt.UTC).date(),
        help="window end (exclusive), YYYY-MM-DD (default: today → through yesterday)",
    )
    backfill.add_argument("--chunk-days", type=int, default=DEFAULT_CHUNK_DAYS)
    backfill.add_argument("--state-file", default="backfill_state.json",
                          help="chunk checkpoint file (gitignored)")
    backfill.add_argument(
        "--skip-sensors-csv",
        help="CSV with a sensor_id column (e.g. dbt/seeds/known_bad_sensors.csv) "
             "to skip known-broken sensors instead of burning retries on them",
    )
    backfill.add_argument(
        "--max-attempts", type=int, default=2,
        help="client retry budget (default 2 — persistent-5xx sensors exist)",
    )

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = load_settings()

    if args.command == "ingest":
        client = OpenAQClient(api_key=settings.api_key, base_url=settings.base_url)
        writer = make_raw_zone_writer(settings.bucket_name)
        summary = ingest_country_day(client, writer, args.country.upper(), args.date)
        # Partial failure is not silent success: landed data stays, but the run
        # reports it so a rerun/orchestrator can catch up the failed sensors.
        return 1 if summary.sensors_failed else 0

    client = OpenAQClient(
        api_key=settings.api_key, base_url=settings.base_url,
        max_attempts=args.max_attempts,
    )
    writer = make_raw_zone_writer(settings.bucket_name)
    skip = load_skip_sensors(args.skip_sensors_csv) if args.skip_sensors_csv else frozenset()
    return run_backfill(
        client, writer, make_raw_loader(), BackfillState(args.state_file),
        args.country.upper(), args.start, args.end,
        chunk_days=args.chunk_days, skip_sensors=skip,
        bucket_name=settings.bucket_name,
    )


if __name__ == "__main__":
    sys.exit(main())
