# ingestion/

Python ingestion layer. Fetches raw air quality measurements from the OpenAQ v3
API and writes newline-delimited JSON files to GCS.

## Contents

```
ingestion/
└── openaq/
    ├── client.py           Base HTTP client: auth, pagination, rate-limit handling
    └── fetchers/
        ├── uae.py          Fetches all UAE location measurements
        └── pakistan.py     Fetches all Pakistan location measurements
```

Each fetcher yields raw API response dicts; the caller (Airflow operator) is
responsible for serialising and uploading to GCS.
