# infra/

Terraform for all GCP resources (guardrail G11). One environment, one state.

## Contents

```
infra/
├── main.tf                   Provider requirements + GCS remote-state backend
├── variables.tf              project_id, region (+ derived bucket name)
├── apis.tf                   Enables storage / bigquery / iam APIs
├── gcs.tf                    Raw JSON landing bucket (versioned, no public access)
├── bigquery.tf               Datasets: openaq_raw, openaq_dbt, openaq_dbt_elementary (no tables — see below)
├── iam.tf                    Pipeline service account + least-privilege grants
├── outputs.tf                Bucket name, dataset IDs, SA email
├── terraform.tfvars.example  Committed template; real terraform.tfvars is gitignored
└── .terraform.lock.hcl       Committed — pins exact provider versions
```

Tables are deliberately not managed here: `raw_measurements` is created by the
ingest DAG (`CREATE TABLE IF NOT EXISTS`; schema verified live in Phase 3), and
dbt owns its own relations.

## One-time bootstrap (before first `terraform init`)

The bucket holding the remote tfstate cannot be created by the state it stores.
Create it once, manually:

```bash
gcloud storage buckets create gs://<PROJECT_ID>-tfstate \
  --location=us-central1 --uniform-bucket-level-access
gcloud storage buckets update gs://<PROJECT_ID>-tfstate --versioning
```

Then put the real bucket name in the `backend "gcs"` block in `main.tf`
(backend blocks cannot read variables).

## Usage

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # fill in project_id
terraform init
terraform plan
terraform apply
```

A second `terraform apply` must report "No changes" (Phase 1 exit criterion).

## Service-account key

The SA key is created with gcloud, **not** Terraform — a
`google_service_account_key` resource would store the private key in plaintext
inside the tfstate (violates G10's secret-handling intent):

```bash
gcloud iam service-accounts keys create ~/gcp-keys/openaq-pipeline-key.json \
  --iam-account=openaq-pipeline@<PROJECT_ID>.iam.gserviceaccount.com
```

Keep the key outside the repo (all `*-key.json` patterns are gitignored
regardless). docker-compose bind-mounts it read-only into the Airflow
containers via the `GCP_KEY_FILE` env var (wired in Phase 3).
