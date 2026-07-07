# airflow/

Airflow Docker configuration for pipeline orchestration. DAGs and operators
land in Phase 3.

## Contents (current)

```
airflow/
├── dags/               Empty until Phase 3
├── plugins/operators/  Empty until Phase 3
├── Dockerfile          Custom Airflow 2.9.1 image with project dependencies
└── requirements.txt    Python packages baked into the Airflow image
```

Runtime logs are written to the repo-root `logs/` directory (gitignored,
mounted as a Docker volume by `docker-compose.yml`).

## Planned (Phase 3)

An ingest DAG using **dynamic task mapping** over the sensor list (G3), loading
raw JSON to BigQuery with WRITE_APPEND (G4), and emitting an Airflow Dataset; a
transform DAG scheduled on that Dataset runs dbt via astronomer-cosmos (G9).
