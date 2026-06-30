#!/usr/bin/env bash
set -euo pipefail

echo "=== OpenAQ Pipeline Bootstrap ==="

# Check required tools
for cmd in docker git python3; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "ERROR: $cmd is not installed or not in PATH"
    exit 1
  fi
done
echo "✓ Required tools found (docker, git, python3)"

# Copy .env.example to .env if it does not already exist
if [ ! -f .env ]; then
  cp .env.example .env
  echo "✓ Created .env from .env.example"
  echo "  Fill in: GCP_PROJECT_ID, GCS_BUCKET_NAME, OPENAQ_API_KEY, GOOGLE_APPLICATION_CREDENTIALS"
else
  echo "✓ .env already exists — skipping"
fi

echo ""
echo "Next steps:"
echo "  1. Fill in placeholder values in .env"
echo "  2. Run: make up"
echo "  3. Visit: http://localhost:8080  (admin / admin)"
