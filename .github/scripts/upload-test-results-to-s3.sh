#!/bin/sh

set -eu

MODE=${1:?upload mode is required: file or dir}
RESULT_KIND=${2:?result kind is required}
SOURCE=${3:?source path is required}
BUCKET=${4:-}
TEST_SUITE=${5:?TEST_SUITE name is required}
UPLOAD_SUFFIX=${6:?upload UPLOAD_SUFFIX is required}
LABEL=${7:-$RESULT_KIND}
INCLUDE_PATTERN=${8:-}
EXCLUDE_PATTERN=${9:-}

SCRIPT_DIR=$(dirname "$0")

sanitize() {
  "$SCRIPT_DIR/sanitize-token.sh" "$1"
}

if [ "$MODE" != "file" ] && [ "$MODE" != "dir" ]; then
  echo "Unknown upload mode '$MODE'; expected 'file' or 'dir'" >&2
  exit 2
fi

if [ -z "$BUCKET" ]; then
  echo "S3 BUCKET is empty; skipping $LABEL upload"
  exit 0
fi

if [ "$MODE" = "file" ]; then
  if [ ! -f "$SOURCE" ]; then
    echo "No $LABEL file found at $SOURCE; skipping S3 upload"
    exit 0
  fi
else
  if [ -z "$INCLUDE_PATTERN" ]; then
    echo "Include pattern is required for directory upload" >&2
    exit 2
  fi

  if [ ! -d "$SOURCE" ] ||
    ! find "$SOURCE" -type f -name "$INCLUDE_PATTERN" -print -quit | grep -q .; then
    echo "No $LABEL files matching $INCLUDE_PATTERN found under $SOURCE; skipping S3 upload"
    exit 0
  fi
fi

if ! command -v aws >/dev/null 2>&1; then
  pip3 install --break-system-packages awscli
fi

SAFE_KIND=$(sanitize "$RESULT_KIND")
SAFE_SUITE=$(sanitize "$TEST_SUITE")
SAFE_WORKFLOW=$(sanitize "${GITHUB_WORKFLOW:-unknown-workflow}")
SAFE_JOB=$(sanitize "${GITHUB_JOB:-unknown-job}")
SAFE_SUFFIX=$(sanitize "$UPLOAD_SUFFIX")
UPLOAD_YEAR=$(date -u +%Y)
UPLOAD_MONTH=$(date -u +%m)
UPLOAD_DAY=$(date -u +%d)
PREFIX="test-results/$SAFE_KIND/$SAFE_SUITE/year=$UPLOAD_YEAR/month=$UPLOAD_MONTH"
PREFIX="$PREFIX/day=$UPLOAD_DAY/$SAFE_WORKFLOW/${GITHUB_RUN_ID:-unknown-run}"
PREFIX="$PREFIX/${GITHUB_RUN_ATTEMPT:-1}/$SAFE_JOB/$SAFE_SUFFIX"

if [ "$MODE" = "file" ]; then
  DEST="s3://$BUCKET/$PREFIX/$(basename "$SOURCE")"
  echo "Uploading $LABEL from $SOURCE to $DEST"
  aws s3 cp "$SOURCE" "$DEST"
else
  DEST="s3://$BUCKET/$PREFIX/"
  echo "Uploading $LABEL files matching $INCLUDE_PATTERN from $SOURCE to $DEST"

  if [ -n "$EXCLUDE_PATTERN" ]; then
    aws s3 sync "$SOURCE/" "$DEST" --exclude "*" --include "$INCLUDE_PATTERN" \
      --exclude "$EXCLUDE_PATTERN"
  else
    aws s3 sync "$SOURCE/" "$DEST" --exclude "*" --include "$INCLUDE_PATTERN"
  fi
fi
