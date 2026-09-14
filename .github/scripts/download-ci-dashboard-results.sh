#!/bin/bash

set -euo pipefail

if [[ -z "${SOURCE_BUCKET:-}" ]]; then
  echo "S3_REGTEST_BUCKET is empty"
  exit 1
fi

if [[ ! "${LOOKBACK_DAYS:-}" =~ ^[0-9]+$ ]] || ((LOOKBACK_DAYS < 1)); then
  echo "lookback_days must be a positive integer; got '${LOOKBACK_DAYS:-}'"
  exit 2
fi

INPUT_DIR="${RUNNER_TEMP}/ci-test-dashboard/input"
JUNIT_DIR="${INPUT_DIR}/junit"
DASHBOARD_JSON_DIR="${INPUT_DIR}/dashboard"
mkdir -p "${JUNIT_DIR}" "${DASHBOARD_JSON_DIR}"

echo "INPUT_DIR=${INPUT_DIR}" >> "${GITHUB_ENV}"

for offset in $(seq 0 "$((LOOKBACK_DAYS - 1))"); do
  year=$(date -u -d "${offset} days ago" +%Y)
  month=$(date -u -d "${offset} days ago" +%m)
  day=$(date -u -d "${offset} days ago" +%d)

  cpp_junit_source="s3://${SOURCE_BUCKET}/test-results/junit/cpp/year=${year}/month=${month}/day=${day}/"
  cpp_junit_target="${JUNIT_DIR}/cpp/year=${year}/month=${month}/day=${day}/"
  echo "Downloading C++ CTest XML from ${cpp_junit_source}"
  aws s3 sync "${cpp_junit_source}" "${cpp_junit_target}" \
    --exclude "*" \
    --include "*/ctest/*.xml"

  regression_source="s3://${SOURCE_BUCKET}/test-results/junit/regression/year=${year}/month=${month}/day=${day}/"
  regression_target="${JUNIT_DIR}/regression/year=${year}/month=${month}/day=${day}/"
  echo "Downloading regression JUnit XML from ${regression_source}"
  aws s3 sync "${regression_source}" "${regression_target}" \
    --exclude "*" \
    --include "*.xml"

  dashboard_source="s3://${SOURCE_BUCKET}/test-results/dashboard/cpp/year=${year}/month=${month}/day=${day}/"
  dashboard_target="${DASHBOARD_JSON_DIR}/cpp/year=${year}/month=${month}/day=${day}/"
  echo "Downloading C++ GTest dashboard JSON from ${dashboard_source}"
  aws s3 sync "${dashboard_source}" "${dashboard_target}" \
    --exclude "*" \
    --include "*/gtest-summary.json"
done

echo "Downloaded XML files: $(find "${JUNIT_DIR}" -type f -name "*.xml" | wc -l)"
echo "Downloaded dashboard JSON files: $(find "${DASHBOARD_JSON_DIR}" -type f -name "gtest-summary.json" | wc -l)"
