# dbt/

dbt transformation project with a three-layer model architecture targeting BigQuery.

## Layer responsibilities

| Layer | Directory | Purpose |
|---|---|---|
| Staging | `models/staging/` | Cast types, rename columns, apply basic filters — one model per source table |
| Intermediate | `models/intermediate/` | Join staging models, apply business logic, denormalize |
| Mart | `models/mart/` | Final analytical tables consumed by Looker Studio |

## Contents

```
dbt/
├── models/
│   ├── staging/
│   │   ├── _sources.yml    Declares BigQuery raw tables as dbt sources
│   │   └── _schema.yml     Column-level docs and tests for staging models
│   ├── intermediate/
│   │   └── _schema.yml
│   └── mart/
│       └── _schema.yml
├── macros/                 Reusable Jinja macros shared across models
├── tests/                  Custom singular data tests (SQL assertions)
├── seeds/                  Static reference CSVs loaded into BigQuery
├── analyses/               Ad-hoc SQL explorations (not materialized)
├── dbt_project.yml         Project config: name, model paths, materializations
├── packages.yml            dbt package dependencies (dbt-utils, etc.)
└── profiles.yml.example    BigQuery connection template — copy to profiles.yml
```

> `profiles.yml` is gitignored. Copy `profiles.yml.example` and fill in your
> GCP project and dataset details before running dbt locally.
