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

while IFS= read -r -d '' json_file; do
  relative_path="${json_file#${DASHBOARD_ROOT}/site/}"
  gzip_file="${gzip_dir}/${relative_path}"
  mkdir -p "$(dirname "${gzip_file}")"
  gzip -9 -c "${json_file}" > "${gzip_file}"
done < <(find "${DASHBOARD_ROOT}/site/data" -type f -name "*.json" -print0)

manifest_file="${gzip_dir}/data/manifest.json"
if [[ ! -f "${manifest_file}" ]]; then
  echo "Missing generated manifest: ${manifest_file}"
  exit 1
fi

echo "Publishing dashboard JSON to ${destination}/"
while IFS= read -r -d '' gzip_file; do
  relative_path="${gzip_file#${gzip_dir}/}"
  if [[ "${relative_path}" == "data/manifest.json" ]]; then
    continue
  fi
  aws s3 cp "${gzip_file}" "${destination}/${relative_path}" \
    --content-type "application/json" \
    --content-encoding "gzip" \
    --cache-control "public,max-age=3600"
done < <(find "${gzip_dir}" -type f -name "*.json" -print0 | sort -z)

aws s3 cp "${manifest_file}" "${destination}/data/manifest.json" \
  --content-type "application/json" \
  --content-encoding "gzip" \
  --cache-control "public,max-age=300"

echo "Publishing dashboard assets to ${destination}/"
aws s3 sync "${DASHBOARD_ROOT}/site/" "${destination}/" \
  --delete \
  --exclude "*.json" \
  --cache-control "public,max-age=300"
