# airflow/

Airflow DAGs, custom operators, and Docker configuration for pipeline orchestration.

## Contents

```
airflow/
├── dags/
│   ├── openaq_ingest_dag.py          Fetches raw JSON from OpenAQ API → GCS
│   └── dbt_transform_dag.py          Runs dbt models after each ingest cycle
├── plugins/
│   └── operators/
│       └── openaq_to_gcs_operator.py Custom operator wrapping the ingestion client
├── config/
│   └── airflow.cfg                   Airflow configuration overrides
├── logs/                             Runtime logs — gitignored, mounted as Docker volume
├── Dockerfile                        Custom Airflow image with project dependencies
└── requirements.txt                  Python packages baked into the Airflow image
```
