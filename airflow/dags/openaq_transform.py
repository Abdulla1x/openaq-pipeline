"""dbt transform DAG (Phase 4): raw pages -> staging -> daily -> marts.

Design notes (guardrails in docs/PROJECT_CONTEXT.md):

- G9 — rendered by astronomer-cosmos as one Airflow task per dbt node (seed,
  model, test), so a failed model retries alone instead of rerunning an
  opaque `dbt build`. Tests run right after the model they cover
  (TestBehavior.AFTER_EACH, the cosmos default).
- G9 — scheduled on the raw_measurements Dataset the ingest DAG emits, not
  on a clock and not via TriggerDagRunOperator. Both DAGs import the Dataset
  from openaq_datasets (a non-DAG module) so the contract cannot drift
  between the two files.
- G1/G4 — the models parse raw JSON pages and dedup append batches; nothing
  here re-reads GCS. dbt owns all relations in openaq_dbt (Terraform
  deliberately owns no tables).

Runs whenever an ingest load lands, transforming the *whole* raw table
(staging views recompute; daily/mart tables rebuild) — at this data volume a
full rebuild is simpler and safer than incremental models (owned tradeoff:
revisit if the Phase 5 backfill makes rebuild cost visible).
"""

import os
from pathlib import Path

import pendulum
from cosmos import DbtDag, ProfileConfig, ProjectConfig
from openaq_datasets import RAW_MEASUREMENTS_DATASET

DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt"))
DBT_PROFILES_DIR = Path(os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt"))

openaq_transform = DbtDag(
    dag_id="openaq_transform",
    schedule=[RAW_MEASUREMENTS_DATASET],
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["openaq", "dbt", "transform"],
    doc_md=__doc__,
    project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
    profile_config=ProfileConfig(
        # Must name the profile/target in dbt/profiles.yml; the file itself
        # holds no secrets (G10 — env_var() indirection only).
        profile_name="openaq",
        target_name="dev",
        profiles_yml_filepath=DBT_PROFILES_DIR / "profiles.yml",
    ),
)
