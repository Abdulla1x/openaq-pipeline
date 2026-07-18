"""GCS raw-zone writer: the object-naming contract with Phase 3, plus upload.

Layout (PROJECT_CONTEXT.md §5): raw/openaq/{country}/{partition}/{leaf}.json
where partition is a UTC day (YYYY-MM-DD, the daily DAG) or a backfill window
segment (backfill/{start}_{end}), and leaf is a sensor id for measurements or
the literal "locations" for the country's location/sensor inventory (sensor
ids are numeric, so no collision). dbt staging parses only the country and the
leaf out of source_uri — the partition segment is organizational (load globs,
reconcile filters), which is what lets the two shapes coexist.

Object content is newline-delimited verbatim API page bodies — one line per
page response, byte-for-byte (G1: no parsing at ingest time). NDJSON lets the
Phase 3 BigQuery load ingest one row per page directly into a JSON column.
"""

import datetime as dt

RAW_PREFIX = "raw/openaq"
LOCATIONS_LEAF = "locations"


def object_name(country_code: str, partition: dt.date | str, leaf: str | int) -> str:
    segment = partition.isoformat() if isinstance(partition, dt.date) else partition
    return f"{RAW_PREFIX}/{country_code}/{segment}/{leaf}.json"


class RawZoneWriter:
    """Writes page bodies to the raw bucket. The bucket handle is injected so
    unit tests never touch google-cloud-storage or credentials."""

    def __init__(self, bucket):
        self._bucket = bucket

    def write_pages(
        self,
        country_code: str,
        partition: dt.date | str,
        leaf: str | int,
        page_bodies: list[str],
    ) -> str:
        if not page_bodies:
            raise ValueError("refusing to write an empty object to the raw zone")
        name = object_name(country_code, partition, leaf)
        blob = self._bucket.blob(name)
        blob.upload_from_string(
            "\n".join(page_bodies) + "\n", content_type="application/x-ndjson"
        )
        return f"gs://{self._bucket.name}/{name}"


def make_raw_zone_writer(bucket_name: str) -> RawZoneWriter:
    """Build a writer against the real bucket (credentials from
    GOOGLE_APPLICATION_CREDENTIALS). Imported lazily so tests don't need it."""
    from google.cloud import storage

    return RawZoneWriter(storage.Client().bucket(bucket_name))
