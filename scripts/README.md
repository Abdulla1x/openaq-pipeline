# scripts/

Utility shell scripts for local development.

## Contents (current)

```
scripts/
└── bootstrap.sh    First-time local setup: verifies docker/git/python3 are
                    installed and creates .env from .env.example. Does NOT
                    touch GCP — all cloud resources are provisioned via
                    Terraform in infra/ (guardrail G11).
```

A backfill helper is planned for Phase 5.
