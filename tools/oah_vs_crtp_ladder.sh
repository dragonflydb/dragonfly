#!/usr/bin/env bash
# Builds and runs the OAHSet vs CrtpDenseSet ladder benchmark comparison.
#
# Usage: ./tools/oah_vs_crtp_ladder.sh [output_dir]
#
# Run from the repo root (or anywhere - the script locates the repo root
# from its own path). Everything runs strictly serially, single-threaded,
# pinned to one core, so results are reproducible run-to-run:
#   1. ninja-build string_set_test and oah_set_test in build-opt.
#   2. Run the CRTP correctness gates (CrtpHashTagCorrectness,
#      CrtpNoTagCorrectness, CrtpBlobCorrectness, CrtpFusedCorrectness) -
#      abort before benchmarking anything if these fail.
#   3. Run string_set_test's ladder benchmarks (StringSet baseline, NoTag,
#      Crtp, Blob, Fused), then oah_set_test's benchmarks - one binary at a
#      time, never in parallel - each with 5 repetitions, JSON output.
#   4. Hand both JSON files to oah_vs_crtp_ladder.py, which builds the
#      consolidated ladder/comparison table and writes the report.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${REPO_ROOT}/build-opt"
OUT_DIR="${1:-${REPO_ROOT}}"
TS="$(date +%Y%m%d_%H%M%S)"

STRING_SET_JSON="${OUT_DIR}/ladder_string_set_${TS}.json"
OAH_JSON="${OUT_DIR}/ladder_oah_${TS}.json"
REPORT_TXT="${OUT_DIR}/ladder_report_${TS}.txt"

CORE="${LADDER_TASKSET_CORE:-0}"
REPS="${LADDER_BENCHMARK_REPETITIONS:-10}"

echo "== Repo root: ${REPO_ROOT}"
echo "== Build dir: ${BUILD_DIR}"
echo "== Pinned to core: ${CORE}, repetitions: ${REPS}"

cd "${BUILD_DIR}"

echo "== [1/4] Building string_set_test and oah_set_test (ninja -j2)"
ninja string_set_test oah_set_test -j2

echo "== [2/4] Running correctness gates (must pass before trusting any timing)"
./string_set_test --gtest_filter='StringSetTest.CrtpHashTagCorrectness:StringSetTest.CrtpNoTagCorrectness:StringSetTest.CrtpBlobCorrectness:StringSetTest.CrtpFusedCorrectness'

echo "== [3/4] Running benchmarks serially (single-threaded, one binary at a time)"
echo "   -> string_set_test (StringSet baseline + NoTag + Crtp + Blob + Fused)"
taskset -c "${CORE}" ./string_set_test --bench \
  --benchmark_filter='^BM_(Add|Get|Erase)(_NoTag|_Crtp|_Blob|_Fused)?/' \
  --benchmark_repetitions="${REPS}" \
  --benchmark_report_aggregates_only=true \
  --benchmark_out="${STRING_SET_JSON}" \
  --benchmark_out_format=json

echo "   -> oah_set_test (OAHSet, tuned config)"
taskset -c "${CORE}" ./oah_set_test --bench \
  --benchmark_filter='^BM_(Add|Get|Erase)/' \
  --benchmark_repetitions="${REPS}" \
  --benchmark_report_aggregates_only=true \
  --benchmark_out="${OAH_JSON}" \
  --benchmark_out_format=json

echo "== [4/4] Building consolidated comparison report"
python3 "${SCRIPT_DIR}/oah_vs_crtp_ladder.py" \
  --string-set-json "${STRING_SET_JSON}" \
  --oah-json "${OAH_JSON}" \
  --out "${REPORT_TXT}"

echo "== Done. Report: ${REPORT_TXT}"
echo "== Raw JSON kept for reproducibility: ${STRING_SET_JSON}, ${OAH_JSON}"
