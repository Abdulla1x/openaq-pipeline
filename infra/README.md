# infra/

Terraform infrastructure-as-code for all GCP resources.

## Contents

```
infra/
├── main.tf         Provider config and backend (GCS remote state)
├── variables.tf    Input variables (project ID, region, bucket names)
├── outputs.tf      Exported values referenced by other modules
├── gcs.tf          Cloud Storage buckets: raw JSON landing zone + processed
└── bigquery.tf     BigQuery datasets and table schemas
```

## Usage

```bash
cd infra
terraform init
terraform plan -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars
```
