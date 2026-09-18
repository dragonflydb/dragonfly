#!/bin/bash

set -euo pipefail

if [[ -z "${PUBLISH_BUCKET:-}" ]]; then
  echo "No publish bucket configured; skipping S3 publish."
  exit 0
fi

destination="s3://${PUBLISH_BUCKET}"
if [[ -n "${PUBLISH_PREFIX:-}" ]]; then
  destination="${destination}/${PUBLISH_PREFIX%/}"
fi

gzip_dir="${RUNNER_TEMP}/ci-test-dashboard-gzip"
mkdir -p "${gzip_dir}"

publish_started_seconds=$SECONDS
phase_started_seconds=$SECONDS
mapfile -d '' -t json_files < <(find "${DASHBOARD_ROOT}/site/data" -type f -name '*.json' -print0)
json_count=${#json_files[@]}
compressed_count=0
echo "Compressing ${json_count} dashboard JSON files"
for json_file in "${json_files[@]}"; do
  relative_path="${json_file#${DASHBOARD_ROOT}/site/}"
  gzip_file="${gzip_dir}/${relative_path}"
  mkdir -p "$(dirname "${gzip_file}")"
  gzip -9 -c "${json_file}" > "${gzip_file}"
  compressed_count=$((compressed_count + 1))
  if ((compressed_count % 1000 == 0 || compressed_count == json_count)); then
    echo "Compressed ${compressed_count}/${json_count} JSON files in $((SECONDS - phase_started_seconds)) seconds"
  fi
done

manifest_file="${gzip_dir}/data/manifest.json"
if [[ ! -f "${manifest_file}" ]]; then
  echo "Missing generated manifest: ${manifest_file}"
  exit 1
fi

echo "Publishing $((json_count - 1)) dashboard JSON files to ${destination}/"
phase_started_seconds=$SECONDS
aws s3 cp "${gzip_dir}/" "${destination}/" \
  --recursive \
  --no-progress \
  --exclude "*" \
  --include "*.json" \
  --exclude "data/manifest.json" \
  --content-type "application/json" \
  --content-encoding "gzip" \
  --cache-control "public,max-age=3600" | \
  awk -v total="$((json_count - 1))" '
    BEGIN { started = systime() }
    function progress() {
      printf "Uploaded %d/%d JSON files (%.1f%%; %d remaining; %ds elapsed)\n", \
        completed, total, 100 * completed / total, total - completed, systime() - started
      fflush()
      reported = completed
    }
    /^upload: / {
      completed++
      if (completed % 500 == 0 || completed == total) progress()
      next
    }
    { print; fflush() }
    END { if (completed != reported) progress() }
  '
echo "Published $((json_count - 1)) dashboard JSON files in $((SECONDS - phase_started_seconds)) seconds"

echo "Publishing dashboard manifest"
aws s3 cp "${manifest_file}" "${destination}/data/manifest.json" \
  --content-type "application/json" \
  --content-encoding "gzip" \
  --cache-control "public,max-age=300"

echo "Publishing dashboard assets to ${destination}/"
phase_started_seconds=$SECONDS
aws s3 sync "${DASHBOARD_ROOT}/site/" "${destination}/" \
  --delete \
  --exclude "*.json" \
  --cache-control "public,max-age=300"
echo "Published dashboard assets in $((SECONDS - phase_started_seconds)) seconds"
echo "Dashboard publication finished in $((SECONDS - publish_started_seconds)) seconds"
