# scripts/

Utility shell scripts for local development and operations.

## Contents

```
scripts/
├── bootstrap.sh      First-time setup: create GCS buckets, BQ datasets,
│                     initialize Airflow DB, create default Airflow connections
└── run_backfill.sh   Trigger Airflow DAG backfill for a historical date range
```
