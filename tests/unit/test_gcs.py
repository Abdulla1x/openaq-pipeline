"""Raw-zone writer: the exact GCS layout is the Phase 2→3 interface contract."""

import datetime as dt

import pytest

from ingestion.openaq.gcs import RawZoneWriter, object_name

DATE = dt.date(2026, 7, 12)


class FakeBlob:
    def __init__(self, name):
        self.name = name

    def upload_from_string(self, data, content_type=None):
        self.data = data
        self.content_type = content_type


class FakeBucket:
    name = "test-bucket"

    def __init__(self):
        self.blobs = {}

    def blob(self, name):
        return self.blobs.setdefault(name, FakeBlob(name))


def test_object_name_matches_contract():
    assert object_name("AE", DATE, 12345) == "raw/openaq/AE/2026-07-12/12345.json"
    assert object_name("PK", DATE, "locations") == "raw/openaq/PK/2026-07-12/locations.json"


def test_object_name_accepts_window_partition():
    """Backfill (Phase 5) lands under a window-shaped segment; staging parses
    only country + leaf from the path, so the segment shape is free."""
    assert (
        object_name("PK", "backfill/2025-06-01_2025-07-31", 12345)
        == "raw/openaq/PK/backfill/2025-06-01_2025-07-31/12345.json"
    )


def test_write_pages_lands_verbatim_ndjson():
    bucket = FakeBucket()
    pages = ['{"results": [1, 2]}', '{"results":  [3]}']  # odd spacing must survive

    uri = RawZoneWriter(bucket).write_pages("AE", DATE, 42, pages)

    assert uri == "gs://test-bucket/raw/openaq/AE/2026-07-12/42.json"
    blob = bucket.blobs["raw/openaq/AE/2026-07-12/42.json"]
    assert blob.data == '{"results": [1, 2]}\n{"results":  [3]}\n'
    assert blob.content_type == "application/x-ndjson"


def test_write_pages_refuses_empty():
    with pytest.raises(ValueError):
        RawZoneWriter(FakeBucket()).write_pages("AE", DATE, 42, [])
