#!/bin/sh

set -eu

source_dir=${1:?source directory is required}
bucket=${2:-}
suite=${3:?JUnit suite name is required}
suffix=${4:?JUnit upload suffix is required}
label=${5:-JUnit}

sanitize() {
  value=$(printf '%s' "$1" | tr -c 'A-Za-z0-9._-' '-' | sed 's/--*/-/g; s/^-//; s/-$//')
  if [ -n "$value" ]; then
    printf '%s' "$value"
  else
    printf 'unknown'
  fi
}

if [ -z "$bucket" ]; then
  echo "S3 bucket is empty; skipping $label upload"
  exit 0
fi

if [ ! -d "$source_dir" ] || ! find "$source_dir" -type f -name '*.xml' -print -quit | grep -q .; then
  echo "No $label XML files found under $source_dir; skipping S3 upload"
  exit 0
fi

if ! command -v aws >/dev/null 2>&1; then
  pip3 install --break-system-packages awscli
fi

safe_suite=$(sanitize "$suite")
safe_workflow=$(sanitize "${GITHUB_WORKFLOW:-unknown-workflow}")
safe_job=$(sanitize "${GITHUB_JOB:-unknown-job}")
safe_suffix=$(sanitize "$suffix")
upload_year=$(date -u +%Y)
upload_month=$(date -u +%m)
upload_day=$(date -u +%d)
prefix="test-results/junit/$safe_suite/year=$upload_year/month=$upload_month/day=$upload_day/$safe_workflow/${GITHUB_RUN_ID:-unknown-run}/${GITHUB_RUN_ATTEMPT:-1}/$safe_job/$safe_suffix"
dest="s3://$bucket/$prefix/"

echo "Uploading $label XML files from $source_dir to $dest"
aws s3 sync "$source_dir/" "$dest" --exclude "*" --include "*.xml"
