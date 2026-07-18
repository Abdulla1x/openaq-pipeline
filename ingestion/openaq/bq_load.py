"""The GCS→BigQuery raw-load contract, shared by the daily DAG and the
Phase 5 backfill CLI so the two load paths cannot drift.

The pattern (Phase 3, G1/G4): a temp external table reads each NDJSON line as
a single CSV "column" (delimiter \\x01 never occurs in JSON text, quoting
disabled), so no parsing happens outside PARSE_JSON; the INSERT stamps one
CURRENT_TIMESTAMP() per statement (= one ingested_at batch) and _FILE_NAME
becomes source_uri, which carries sensor/country identity (measurement
payloads have no ids).

This module is import-light (no google-cloud deps) because the DAG only needs
the SQL strings and the external-table dict for BigQueryInsertJobOperator;
the CLI builds real client objects from the same dicts.
"""

from ingestion.openaq.gcs import LOCATIONS_LEAF

# The name the load SQL joins against in tableDefinitions.
RAW_LINES_TABLE = "raw_lines"


def raw_table_name(project_id: str, dataset: str) -> str:
    return f"{project_id}.{dataset}.raw_measurements"


def ensure_raw_table_sql(raw_table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS `{raw_table}` (
  raw_payload JSON OPTIONS (description = 'Verbatim OpenAQ v3 API page body (G1: schema-on-read)'),
  ingested_at TIMESTAMP OPTIONS (description = 'Load-batch timestamp; dbt dedups on it (G4)'),
  source_uri STRING OPTIONS (description = 'GCS object path — carries sensor/country/day identity')
)
PARTITION BY DATE(ingested_at)
"""


def load_sql(raw_table: str) -> str:
    return f"""
INSERT INTO `{raw_table}` (raw_payload, ingested_at, source_uri)
SELECT PARSE_JSON(line, wide_number_mode => 'round'), CURRENT_TIMESTAMP(), _FILE_NAME
FROM {RAW_LINES_TABLE}
"""


def external_table_definition(source_uris: list[str]) -> dict:
    """API-shaped externalDataConfiguration for the temp raw_lines table.

    Used verbatim as the operator's tableDefinitions value and via
    ExternalConfig.from_api_repr in the CLI. Callers must pass URIs ending in
    /* (not /*.json): in an Airflow templated field, any string ending in
    .json is treated as a template FILE to load (template_ext) and fails with
    TemplateNotFound.
    """
    return {
        "sourceFormat": "CSV",
        "sourceUris": source_uris,
        "schema": {"fields": [{"name": "line", "type": "STRING"}]},
        "csvOptions": {"fieldDelimiter": "\u0001", "quote": ""},
    }


def measurement_count_sql(raw_table: str, uri_like: str) -> str:
    """Count measurement records in the latest ingested batch whose source_uri
    matches uri_like (SQL LIKE pattern), excluding locations inventory pages.

    ingested_at is CURRENT_TIMESTAMP() of one INSERT, i.e. one constant per
    load — MAX(...) selects exactly the latest batch under the filter, which
    keeps reconciliation correct across reruns (each rerun appends).
    """
    uri_filter = f"source_uri LIKE '{uri_like}'"
    return f"""
        SELECT COALESCE(SUM(ARRAY_LENGTH(JSON_QUERY_ARRAY(raw_payload, '$.results'))), 0)
        FROM `{raw_table}`
        WHERE {uri_filter}
          AND source_uri NOT LIKE '%/{LOCATIONS_LEAF}.json'
          AND ingested_at = (SELECT MAX(ingested_at) FROM `{raw_table}` WHERE {uri_filter})
    """
