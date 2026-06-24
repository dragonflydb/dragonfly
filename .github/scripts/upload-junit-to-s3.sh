#!/bin/sh

set -eu

SOURCE_DIR=${1:?source directory is required}
BUCKET=${2:-}
TEST_SUITE=${3:?JUnit TEST_SUITE name is required}
UPLOAD_SUFFIX=${4:?JUnit upload UPLOAD_SUFFIX is required}
LABEL=${5:-JUnit}

SCRIPT_DIR=$(dirname "$0")

sanitize() {
  "$SCRIPT_DIR/sanitize-token.sh" "$1"
}

if [ -z "$BUCKET" ]; then
  echo "S3 BUCKET is empty; skipping $LABEL upload"
  exit 0
fi

if [ ! -d "$SOURCE_DIR" ] || ! find "$SOURCE_DIR" -type f -name '*.xml' -print -quit | grep -q .; then
  echo "No $LABEL XML files found under $SOURCE_DIR; skipping S3 upload"
  exit 0
fi

if ! command -v aws >/dev/null 2>&1; then
  pip3 install --break-system-packages awscli
fi

SAFE_SUITE=$(sanitize "$TEST_SUITE")
SAFE_WORKFLOW=$(sanitize "${GITHUB_WORKFLOW:-unknown-workflow}")
SAFE_JOB=$(sanitize "${GITHUB_JOB:-unknown-job}")
SAFE_SUFFIX=$(sanitize "$UPLOAD_SUFFIX")
UPLOAD_YEAR=$(date -u +%Y)
UPLOAD_MONTH=$(date -u +%m)
UPLOAD_DAY=$(date -u +%d)
PREFIX="test-results/junit/$SAFE_SUITE/year=$UPLOAD_YEAR/month=$UPLOAD_MONTH/day=$UPLOAD_DAY/$SAFE_WORKFLOW/${GITHUB_RUN_ID:-unknown-run}/${GITHUB_RUN_ATTEMPT:-1}/$SAFE_JOB/$SAFE_SUFFIX"
DEST="s3://$BUCKET/$PREFIX/"

echo "Uploading $LABEL XML files from $SOURCE_DIR to $DEST"
aws s3 sync "$SOURCE_DIR/" "$DEST" --exclude "*" --include "*.xml"
