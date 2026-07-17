"""Dataset contracts between DAGs (G9: data-aware scheduling).

Lives in its own non-DAG module: a DAG file must never import another DAG
file — executing it re-registers the imported DAG under the importing file
and Airflow rejects the duplicate (seen in the DagBag tests). Both
openaq_ingest (producer) and openaq_transform (consumer) import from here,
so the URI cannot drift between them.
"""

import os

from airflow.datasets import Dataset

# The google provider enforces the full bigquery://project/dataset/table
# form for this URI scheme.
RAW_MEASUREMENTS_DATASET = Dataset(
    "bigquery://{}/{}/raw_measurements".format(
        os.environ.get("GCP_PROJECT_ID", ""),
        os.environ.get("BIGQUERY_RAW_DATASET", "openaq_raw"),
    )
)
