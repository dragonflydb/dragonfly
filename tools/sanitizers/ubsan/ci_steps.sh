#!/bin/bash
#
# Step bodies for .github/workflows/ubsan.yml. Each workflow step longer than a
# couple of lines sources this file and calls one step_* function:
#
#   source "${GITHUB_WORKSPACE}/tools/sanitizers/ubsan/ci_steps.sh"
#   step_<name>
#
# Contract:
#   - Sourced (not executed), so functions run in the step's own shell and
#     inherit its environment: the step `env:` block, plus GitHub's own vars
#     ($GITHUB_WORKSPACE, $GITHUB_OUTPUT, $GITHUB_STEP_SUMMARY, ...).
#   - GitHub ${{ ... }} expressions are NOT visible here (they are substituted
#     into the YAML). Values that come from ${{ }} (e.g. matrix.arch) are passed
#     in as function arguments.
#   - set -eo pipefail below makes any failing command abort the function, which
#     fails the step (the function call is the step's last command).
set -eo pipefail

# Guard: these functions rely on the GitHub Actions environment (GITHUB_WORKSPACE,
# GITHUB_OUTPUT, ...). The runner always sets GITHUB_ACTIONS=true; bail out early
# with a clear error if it is missing (we are not in a job).
if [[ "${GITHUB_ACTIONS:-}" != "true" ]]; then
  echo "Error: ci_steps.sh must run inside GitHub Actions (GITHUB_ACTIONS is unset)." >&2
  echo "       step_* functions need GITHUB_WORKSPACE / GITHUB_OUTPUT / ... and will fail here." >&2
  exit 1
fi

# Free host disk before the large UBSan build (host root is mounted at /hostroot).
step_free_disk_space() {
  echo "=== disk before ==="; df -h
  rm -rf /hostroot/usr/share/dotnet || true
  rm -rf /hostroot/usr/local/share/boost || true
  rm -rf /hostroot/usr/local/lib/android || true
  rm -rf /hostroot/opt/ghc || true
  echo "=== disk after ==="; df -h
}

# Read + validate inputs and write the config banner to $GITHUB_OUTPUT.
# Reads the ENABLE_HELIO / COMPARE / ... env vars set on the step.
step_resolve_run_configuration() {
  helio="${ENABLE_HELIO:-true}"          # empty on scheduled runs -> default ON
  compare="${COMPARE:-true}"
  ref="${COMPARE_REF:-}"
  fail_on_new="${FAIL_ON_NEW:-false}"
  supp_file="tools/sanitizers/ubsan/ubsan-suppressions.txt"

  # Validate the runtime options before the build, so a typo fails fast.
  # Each colon-separated token must be key=value, with no spaces.
  rt_opts="${UBSAN_RUNTIME_OPTIONS:-}"
  if [[ -n "${rt_opts}" ]]; then
    IFS=':' read -ra _rt <<< "${rt_opts}"
    for tok in "${_rt[@]}"; do
      [[ "${tok}" =~ ^[A-Za-z0-9_]+=[^[:space:]]+$ ]] \
        || { echo "::error::ubsan_runtime_options: invalid token '${tok}' (expected key=value, colon-separated, no spaces)."; exit 1; }
    done
    echo "runtime options: ${rt_opts}"
  fi

  if [[ "${helio}" == "false" ]]; then
    # Enable the ignore-list rule so helio is NOT instrumented.
    sed -i 's|^#[[:space:]]*src:\*/helio/\*|src:*/helio/*|' \
      tools/sanitizers/ubsan/ubsan-ignorelist.txt
    echo "helio instrumentation: DISABLED"
  else
    echo "helio instrumentation: ENABLED (default)"
  fi

  # Suppressions: the phase-2 run applies the committed suppressions file
  # (may be empty, which suppresses nothing).
  real_entries() { grep -cvE '^[[:space:]]*(#|$)' "$1" 2>/dev/null || true; }
  supp_src="repo default ($(real_entries "${supp_file}") entries)"
  echo "Using ${supp_src}."
  echo "::group::effective ${supp_file}"
  cat "${supp_file}"
  echo "::endgroup::"

  on_off() { [[ "$1" == "$2" ]] && echo OFF || echo ON; }
  fail_on_new_str="$([[ "${fail_on_new}" == true ]] && echo ON || echo OFF)"
  # Build the config banner + repro command. Each must be ONE line for
  # $GITHUB_OUTPUT; the summary step shows them in the "Notes" banner.
  cfg="helio: **$(on_off "${helio}" false)** | compare: **$(on_off "${compare}" false)**"
  cfg+=" | baseline: **${ref:-last successful scheduled run}**"
  cfg+=" | fail_on_new: **${fail_on_new_str}** | suppressions: **${supp_src}**"
  repro="gh workflow run ubsan.yml --ref ${REF_NAME}"
  repro+=" -f enable_helio=${helio} -f compare=${compare}"
  repro+=" -f compare_ref='${ref}' -f fail_on_new=${fail_on_new}"
  {
    echo "cfg_summary=${cfg}"
    echo "cfg_repro=${repro}"
  } >> "$GITHUB_OUTPUT"
}

# Validate compare_ref and resolve the baseline to a concrete run_id (empty if
# none). Emits run_id / lenient / desc to $GITHUB_OUTPUT for the download step.
# Reads COMPARE_REF + GH_TOKEN from the step env.
step_resolve_findings_baseline() {
  fail() { echo "::error::$1"; exit 1; }
  api="${GITHUB_API_URL:-https://api.github.com}/repos/${GITHUB_REPOSITORY}"
  gh_get() { curl -sS -H "Authorization: Bearer ${GH_TOKEN}" -H "Accept: application/vnd.github+json" "$1"; }
  emit() { printf '%s\n' "$@" >> "$GITHUB_OUTPUT"; }
  # base is an ancestor of HEAD if the compare API status is ahead/identical
  # (works even on a shallow checkout, where git merge-base would fail).
  ensure_ancestor() {
    local base="$1" msg="$2" st
    st="$(gh_get "${api}/compare/${base}...${GITHUB_SHA}" | jq -r '.status // "error"')" || st="error"
    case "${st}" in
      ahead|identical) return 0 ;;
      behind|diverged) fail "${msg}" ;;
      *) fail "${msg} (GitHub compare API returned status=${st})" ;;
    esac
  }

  ref="${COMPARE_REF:-}"

  # Default: the latest successful SCHEDULED run of this workflow (lenient - if
  # there is none yet, emit an empty run_id and the download step is skipped).
  if [[ -z "${ref}" ]]; then
    run_id="$(gh_get "${api}/actions/workflows/ubsan.yml/runs?event=schedule&status=success&per_page=1" \
              | jq -r '.workflow_runs[0].id // ""')" || run_id=""
    [[ -n "${run_id}" ]] || echo "No successful scheduled run yet - skipping the baseline diff."
    emit "run_id=${run_id}" "lenient=true" "desc=the last successful scheduled run"
    exit 0
  fi

  # R<run-number> (e.g. R3): that specific run, must be GREEN + an ancestor.
  if [[ "${ref}" =~ ^[Rr][0-9]+$ ]]; then
    num="${ref#[Rr]}"
    row="$(gh_get "${api}/actions/workflows/ubsan.yml/runs?per_page=100" \
           | jq -c --argjson n "${num}" 'first(.workflow_runs[] | select(.run_number==$n))')" || true
    [[ -n "${row}" && "${row}" != "null" ]] || fail "compare_ref '${ref}': no run #${num} for ubsan.yml (or beyond the latest 100 runs)."
    conclusion="$(printf '%s' "${row}" | jq -r '.conclusion')"
    sha="$(printf '%s' "${row}" | jq -r '.head_sha')"
    run_id="$(printf '%s' "${row}" | jq -r '.id')"
    [[ "${conclusion}" == "success" ]] || fail "compare_ref '${ref}': run #${num} is not green (conclusion=${conclusion})."
    ensure_ancestor "${sha}" "compare_ref '${ref}': run #${num} commit ${sha} is not an ancestor of HEAD."
    emit "run_id=${run_id}" "lenient=false" "desc=run #${num} (${sha:0:12})"
    exit 0
  fi

  # A git ref (sha / branch / tag): resolve to a sha, confirm ancestry, then find
  # the most recent successful ubsan.yml run for that commit.
  sha="$(git rev-parse --verify "${ref}^{commit}" 2>/dev/null)" || true
  [[ -n "${sha}" ]] || sha="$(git rev-parse --verify "origin/${ref}^{commit}" 2>/dev/null)" || true
  [[ -n "${sha}" || ! "${ref}" =~ ^[0-9a-fA-F]{7,40}$ ]] || sha="${ref}"
  [[ -n "${sha}" ]] || fail "compare_ref '${ref}' is not a valid git ref."
  ensure_ancestor "${sha}" "compare_ref '${ref}' (${sha}) is not an ancestor of HEAD (must be on the current branch history)."
  run_id="$(gh_get "${api}/actions/workflows/ubsan.yml/runs?head_sha=${sha}&status=success&per_page=1" \
            | jq -r '.workflow_runs[0].id // ""')" || run_id=""
  [[ -n "${run_id}" ]] || fail "compare_ref '${ref}' (${sha}): no successful ubsan.yml run for that commit."
  emit "run_id=${run_id}" "lenient=false" "desc=${ref} (${sha:0:12})"
}

# Resolve the latest successful regression-tests run on main to a run_id (empty if
# none) for the timing-baseline download. Best-effort; reads GH_TOKEN from env.
step_resolve_regression_baseline() {
  api="${GITHUB_API_URL:-https://api.github.com}/repos/${GITHUB_REPOSITORY}"
  run_id="$(curl -sS -H "Authorization: Bearer ${GH_TOKEN}" -H "Accept: application/vnd.github+json" \
            "${api}/actions/workflows/regression-tests.yml/runs?branch=main&status=success&per_page=1" \
            | jq -r '.workflow_runs[0].id // ""')" || run_id=""
  [[ -n "${run_id}" ]] || echo "No successful regression-tests run on main - skipping the timing baseline."
  printf 'run_id=%s\n' "${run_id}" >> "$GITHUB_OUTPUT"
}

# Run the injected self-test and assert every UBSan check fired.
step_run_self_test() {
  cd "${GITHUB_WORKSPACE}/build"
  mkdir -p ubsan-logs
  # No halt_on_error -> UBSan prints and continues, so one run exercises all cases.
  export UBSAN_OPTIONS="${UBSAN_COMMON_RUNTIME_OPTS}:log_path=${PWD}/ubsan-logs/selftest"
  ./dragonfly || true
  # UBSan writes to <log_path>.<pid>; gather them all.
  cat ubsan-logs/selftest.* > selftest.ubsan 2>/dev/null || true
  echo "================ self-test UBSan output ================"
  cat selftest.ubsan || true
  echo "======================================================="

  # Each enabled check must appear at least once. Substrings are matched
  # against UBSan's "runtime error:" / SUMMARY lines (report_error_type=1).
  declare -A checks=(
    ["implicit-conversion"]="implicit conversion"
    ["implicit-integer-sign-change"]="implicit-integer-sign-change"
    ["array-bounds"]="out of bounds"
    ["nullability"]="null pointer passed as argument"
    ["float-divide-by-zero"]="division by zero"
    ["signed-integer-overflow"]="signed integer overflow"
    ["unsigned-integer-overflow"]="unsigned integer overflow"
    ["shift"]="shift"
    ["function"]="incorrect function type"
    ["vptr"]="member call on address"
    ["object-size"]="object-size"
  )
  missing=0
  for name in "${!checks[@]}"; do
    if grep -qiF "${checks[$name]}" selftest.ubsan; then
      echo "  PASS  $name"
    else
      echo "  MISS  $name  (expected substring: '${checks[$name]}')"
      missing=$((missing + 1))
    fi
  done
  if [[ $missing -gt 0 ]]; then
    echo "::error::UBSan self-test: $missing expected check(s) did not fire -- toolchain/flags may be broken."
    exit 1
  fi
  echo "All expected UBSan checks fired."

  # Each case should produce exactly one finding. Read the expected count from
  # the case table in ubsan_selftest.cc so it updates itself when cases change.
  # Match on the source path to skip startup / third-party noise.
  selftest_src="${GITHUB_WORKSPACE}/tools/sanitizers/ubsan/ubsan_selftest.cc"
  expected=$(grep -cE '\{[0-9]+, "(base|strict)", &Case_' "${selftest_src}")
  selftest_count=$(grep "runtime error" selftest.ubsan | grep -c "ubsan_selftest.cc" || true)
  echo "self-test produced ${selftest_count} finding(s) in ubsan_selftest.cc (expected ${expected})"
  if [[ "${selftest_count}" -ne "${expected}" ]]; then
    echo "::error::UBSan self-test expected exactly ${expected} findings, got ${selftest_count}"
    exit 1
  fi
}

# Sanity-check that UBSAN_OPTIONS actually change behavior (same binary).
step_assert_runtime_options() {
  cd "${GITHUB_WORKSPACE}/build"
  # halt_on_error=1 stops at the first finding (fewer "runtime error" lines).
  all=$(UBSAN_OPTIONS="report_error_type=1" ./dragonfly 2>&1 | grep -c "runtime error" || true)
  one=$(UBSAN_OPTIONS="halt_on_error=1" ./dragonfly 2>&1 | grep -c "runtime error" || true)
  echo "findings without halt_on_error=$all, with halt_on_error=1=$one"
  if [[ "$one" -ge "$all" || "$all" -lt 2 ]]; then
    echo "::warning::halt_on_error did not reduce the number of findings as expected"
  fi
  # silence_unsigned_overflow=1 must drop the unsigned-overflow finding.
  noisy=$(UBSAN_OPTIONS="report_error_type=1" ./dragonfly 2>&1 | grep -c "unsigned integer overflow" || true)
  quiet=$(UBSAN_OPTIONS="report_error_type=1:silence_unsigned_overflow=1" ./dragonfly 2>&1 | grep -c "unsigned integer overflow" || true)
  echo "unsigned-overflow findings: noisy=$noisy silenced=$quiet"
}

# Run the whole pytest suite (minus 'large') under UBSan. Ends with exit $rc so
# the step outcome reflects pytest pass/fail; the step is continue-on-error.
step_run_tests() {
  cd "${GITHUB_WORKSPACE}/tests"
  mkdir -p "${GITHUB_WORKSPACE}/build/ubsan-logs"

  # Bundle the triage helper into the artifact root so it runs directly
  # from the unzipped download (see the summary's INFO note).
  cp "${GITHUB_WORKSPACE}/tools/sanitizers/ubsan/ubsan_trace.sh" \
    "${GITHUB_WORKSPACE}/build/ubsan-logs/"

  export DRAGONFLY_PATH="${GITHUB_WORKSPACE}/build/dragonfly"

  # halt_on_error=0 keeps dragonfly alive so one run logs every finding.
  # strip_path_prefix shortens paths to src/... / helio/...; instance.py gives
  # each test its own log_path (build/ubsan-logs/<suite>/<case>/ubsan.<pid>).
  opts="halt_on_error=0:${UBSAN_COMMON_RUNTIME_OPTS}:strip_path_prefix=${GITHUB_WORKSPACE}/"
  opts+=":suppressions=${GITHUB_WORKSPACE}/tools/sanitizers/ubsan/ubsan-suppressions.txt"
  opts+=":log_path=${GITHUB_WORKSPACE}/build/ubsan-logs/ubsan"
  # Append operator-supplied options (workflow_dispatch); later keys override ours.
  [[ -n "${UBSAN_EXTRA_RUNTIME_OPTS:-}" ]] && opts+=":${UBSAN_EXTRA_RUNTIME_OPTS}"
  export UBSAN_OPTIONS="${opts}"

  # Run the whole suite except 'large' tests and valkey_search (needs setup).
  # --continue-on-collection-errors: one bad import must not abort the session.
  # --junitxml records per-test time + failure for the timing report.
  # -rfE + tee save the full console (with failure logs) to pytest-console.log.
  # The timeout is generous: the suite is much slower under UBSan.
  rc=0
  timeout 110m pytest -m "not large" \
    --ignore=dragonfly/valkey_search --timeout=600 --color=yes -rfE \
    --continue-on-collection-errors \
    --junitxml="${GITHUB_WORKSPACE}/build/ubsan-logs/ubsan-junit.xml" \
    dragonfly/ 2>&1 | tee "${GITHUB_WORKSPACE}/build/ubsan-logs/pytest-console.log" || rc=$?

  # Mark that phase 2 ran (pass or fail) so the report steps below gate on one
  # flag. Keep pytest's exit code so the step outcome still shows pass/fail.
  echo "ran=true" >> "$GITHUB_OUTPUT"
  exit $rc
}

# Render the findings report into the job summary. Args: <arch> <baseline-desc>.
step_publish_findings() {
  local arch="$1" desc="$2"
  bash "${GITHUB_WORKSPACE}/tools/sanitizers/ubsan/ubsan_summarize_findings.sh" \
    "${GITHUB_WORKSPACE}/build/ubsan-logs" "${arch}" \
    "${GITHUB_WORKSPACE}/build/ubsan-baseline" "${desc}" \
    >> "$GITHUB_STEP_SUMMARY"
}

# Opt-in gate: fail on NEW real-UB / implicit-conversion findings vs the baseline.
step_enforce_fail_on_new() {
  f="${GITHUB_WORKSPACE}/build/ubsan-logs/new-findings.txt"
  if [[ ! -s "$f" ]]; then
    echo "No new findings vs baseline (or no baseline available)."
    exit 0
  fi
  # new-findings.txt rows are BUCKET<TAB>KIND<TAB>file:line:col<TAB>example-test.
  gated="$(awk -F'\t' '$1=="UB" || $2=="implicit-conversion"' "$f")"
  n="$(printf '%s\n' "${gated}" | grep -c . || true)"
  if [[ "$n" -gt 0 ]]; then
    echo "::error::fail_on_new: ${n} new gated finding(s) vs baseline (UB + implicit-conversion)."
    printf '%s\n' "${gated}" | awk -F'\t' '{ print "  + "$2"  "$3"  (e.g. "$4")" }'
    exit 1
  fi
  echo "No new gated findings (UB / implicit-conversion) vs baseline."
}

# Render the timings + failures report into the job summary. Args: <arch>.
step_publish_timings() {
  local arch="$1"
  python3 "${GITHUB_WORKSPACE}/tools/sanitizers/ubsan/ubsan_test_report.py" \
    "${GITHUB_WORKSPACE}/build/ubsan-logs/ubsan-junit.xml" \
    "${arch}" \
    "${GITHUB_WORKSPACE}/build/regression-baseline" >> "$GITHUB_STEP_SUMMARY" || true
}
