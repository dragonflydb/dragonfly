#!/bin/bash

set -eo pipefail

# This helper supports scheduled and manually triggered regression runs:
# 1. Input validation for manual workflow dispatches.
# 2. Pytest execution for scheduled and manual runs.
# 3. GoogleTest execution for manual runs only.

PrintCommand() {
  printf 'Command: '
  printf '%q ' "$@"
  printf '\n'
}

PrintIteration() {
  printf '\033[32m=== %s iteration %s/%s ===\033[0m\n' "$1" "$2" "$3"
}

GetDeadlineSeconds() {
  local max_tests_run_time_minutes=$1

  if [[ -n "${REGRESSION_DEADLINE_EPOCH:-}" ]]; then
    printf '%s\n' "${REGRESSION_DEADLINE_EPOCH}"
  elif [[ -z "${max_tests_run_time_minutes}" ]]; then
    printf '%s\n' ""
  else
    printf '%s\n' "$(( $(date +%s) + max_tests_run_time_minutes * 60 ))"
  fi
}

PrintBudgetExhausted() {
  echo "Shared regression time budget of $1 minutes exhausted; this is not an error: test input requested aborting the run"
}

ExitOnTimeout() {
  local test_failed=$1
  local max_tests_run_time_minutes=$2

  if [[ -n "${REGRESSION_DEADLINE_EPOCH:-}" ]]; then
    PrintBudgetExhausted "${max_tests_run_time_minutes}"
    [[ "${test_failed}" == true ]] && exit 1
    exit 0
  fi

  echo "Scheduled regression test timeout exhausted"
  exit 1
}

ArchiveAndCleanPytestLogs() {
  local iteration=$1
  local test_failed=$2
  local junit_file=$3
  local log_root=/tmp/dragonfly_logs

  if [[ "${test_failed}" == true ]]; then
    local archive_dir=/tmp/failed
    local archive_path="${archive_dir}/iteration_${iteration}_logs.tar.gz"
    local start_seconds=$SECONDS

    mkdir -p "${archive_dir}"
    if ! python3 "${GITHUB_WORKSPACE}/.github/scripts/write-pytest-failure-report.py" \
      "${junit_file}" "${iteration}" "${archive_dir}/pytest-failures-by-iteration.txt"; then
      echo "Failed to write Pytest failure report for iteration ${iteration} (continuing)"
    fi
    if [[ ! -d "${log_root}" ]]; then
      echo "No Pytest logs found for failed iteration ${iteration}"
      return
    fi
    echo "Archiving Pytest logs from iteration ${iteration}: ${archive_path}"
    if ! tar -czf "${archive_path}" -C /tmp dragonfly_logs; then
      echo "Failed to archive Pytest logs from iteration ${iteration}"
      return 1
    fi
    echo "Archived Pytest logs from iteration ${iteration} in $((SECONDS - start_seconds)) seconds"
  elif [[ ! -d "${log_root}" ]]; then
    return
  fi

  rm -rf "${log_root}"
  echo "Removed Pytest logs from iteration ${iteration}"
}

# --- Core dump capture --------------------------------------------------------
#
# Each dragonfly test instance runs with cwd set to a pytest-owned mkdtemp() dir
# (tests/dragonfly/conftest.py's tmp_dir fixture) that gets shutil.rmtree'd at
# fixture teardown — often within moments of a crash, i.e. before a single
# end-of-run sweep would ever see a plain "core" file dropped there by the
# kernel's default core_pattern. Overriding core_pattern to an absolute path
# avoids that races entirely, but the override itself is not guaranteed to
# stick everywhere (e.g. verified locally: an unprivileged/sandboxed container
# can get "Permission denied" writing /proc/sys/kernel/core_pattern and silently
# fall back to the default pattern). So this does not rely on the override
# succeeding: COREDUMP_WATCH_PID runs a tight poll loop for the whole test run
# that relocates any core file the instant it appears, from every location a
# core could land in (our override target, or the kernel default's cwd-relative
# "core"/"core.<pid>" wherever that cwd happens to be), before its owning tmp
# dir can be cleaned up.
COREDUMP_STAGING_DIR=/tmp/df_ci_collected_cores
COREDUMP_WATCH_PID=""

# Enables core dumps so a crash (SIGSEGV/SIGABRT/SIGFPE/...) in a dragonfly process
# under test leaves a core file we can pull from CI and debug offline with gdb,
# instead of only a bare signal number. Called once per RunPytests invocation, in the
# same shell that (via fork/exec) launches every dragonfly process pytest starts, so
# the resulting ulimit is inherited by all of them.
SetupCoreDumps() {
  # ulimit -c only ever raises the *soft* limit up to the current *hard* limit; if the
  # hard limit was inherited as something low (e.g. 0), "ulimit -c unlimited" alone
  # would silently stay at 0. Try to raise the hard limit first (root can always do
  # this; a non-root, non-root-owned hard cap would make this a no-op, which
  # VerifyCoreDumpCapture below will catch and report).
  ulimit -Hc unlimited 2>/dev/null || true
  if ! ulimit -Sc unlimited; then
    echo "Warning: 'ulimit -c unlimited' failed (hard limit: $(ulimit -Hc)); core dumps may be truncated or disabled"
  fi
  echo "Core dump ulimit: soft=$(ulimit -Sc) hard=$(ulimit -Hc)"

  mkdir -p /tmp/core_dumps "${COREDUMP_STAGING_DIR}"
  # Docker bind-mounts /proc/sys read-only by default even when the container has
  # CAP_SYS_ADMIN (this is deliberate: a writable core_pattern piping to an
  # arbitrary program is a known container-escape vector). CAP_SYS_ADMIN only grants
  # the *ability* to remount it read-write; it doesn't do so automatically the way
  # --privileged would. So remount it ourselves before attempting the write below.
  # Best-effort: if the container lacks CAP_SYS_ADMIN this just fails and falls
  # through to the same "could not write" fallback path as before.
  mount -o remount,rw /proc/sys 2>/dev/null || true
  # An absolute path (not a "|pipe-to-handler" pattern) makes the kernel write the
  # core file directly, bypassing any apport/systemd-coredump helper that might
  # otherwise swallow it somewhere we can't retrieve it from. This is a bonus, not
  # a dependency: StartCoreDumpWatcher below also catches the default cwd-relative
  # pattern in case this write is refused.
  if echo '/tmp/core_dumps/core.%e.%p.%t' > /proc/sys/kernel/core_pattern 2>/dev/null; then
    echo "core_pattern set to: $(cat /proc/sys/kernel/core_pattern 2>/dev/null)"
  else
    echo "Warning: could not write /proc/sys/kernel/core_pattern; current value: $(cat /proc/sys/kernel/core_pattern 2>/dev/null || echo unreadable)"
    echo "Relying on the core dump watcher to still catch dumps at the default location"
  fi
}

# Moves any file matching the kernel's core-dump naming (exactly "core", or
# "core.*"/"core-*" as produced by common core_pattern templates, including ours)
# out of harm's way and into COREDUMP_STAGING_DIR, tagged with a discovery
# timestamp to avoid collisions. Searches everywhere a dump could land: our own
# override target, every pytest tmp_dir under /tmp (mkdtemp()'s default root),
# and the pytest working directory itself.
_RelocateCoreDumps() {
  local f base
  while IFS= read -r -d '' f; do
    base="$(basename "$f")"
    mv "$f" "${COREDUMP_STAGING_DIR}/$(date +%s%N)-${base}" 2>/dev/null || true
  done < <(find /tmp/core_dumps /tmp/tmp* "${GITHUB_WORKSPACE}/tests" \
    -maxdepth 1 -type f \( -name 'core' -o -name 'core.*' -o -name 'core-*' \) \
    -print0 2>/dev/null)
}

# Background loop: catches a core file within ~0.2s of it being written, well
# before the owning pytest tmp_dir fixture can rmtree it at teardown.
StartCoreDumpWatcher() {
  ( while true; do _RelocateCoreDumps; sleep 0.2; done ) &
  COREDUMP_WATCH_PID=$!
  disown "${COREDUMP_WATCH_PID}" 2>/dev/null || true
}

StopCoreDumpWatcher() {
  if [[ -n "${COREDUMP_WATCH_PID}" ]]; then
    kill "${COREDUMP_WATCH_PID}" 2>/dev/null || true
    wait "${COREDUMP_WATCH_PID}" 2>/dev/null || true
    COREDUMP_WATCH_PID=""
  fi
  _RelocateCoreDumps # final sweep in case the watcher's last poll missed one

  # Safety net: anything still sitting in staging wasn't claimed by a
  # CollectCoreDumps call for a specific iteration (e.g. a crash right before an
  # early "exit" on the timeout path). Archive it anyway rather than lose it.
  shopt -s nullglob
  local leftover=("${COREDUMP_STAGING_DIR}"/*)
  shopt -u nullglob
  if [[ ${#leftover[@]} -gt 0 ]]; then
    echo "Found ${#leftover[@]} unclaimed core dump(s); archiving to /tmp/failed/leftover_cores"
    mkdir -p /tmp/failed/leftover_cores
    mv "${COREDUMP_STAGING_DIR}"/* /tmp/failed/leftover_cores/ 2>/dev/null || true
    [[ -n "${DRAGONFLY_PATH:-}" ]] && cp "${DRAGONFLY_PATH}" /tmp/failed/leftover_cores/ 2>/dev/null
  fi
}

# Deliberately crashes a disposable child process in a throwaway mkdtemp() dir —
# mirroring exactly how a real dragonfly test instance is launched — with the
# exact ulimit and core_pattern this function's caller just set up, then checks
# the watcher actually relocated the resulting core file. This is the only way
# to know FOR SURE that a real crash later in this job will leave a usable dump:
# the constraints that can silently break it (hard ulimit caps, a read-only
# /proc/sys, disk quota, a runner security profile) live outside dragonfly's code
# and can't be verified by reading source alone. Loud, but non-fatal: broken
# core-dump plumbing shouldn't block the regression run itself, only the ability
# to debug a crash after the fact.
VerifyCoreDumpCapture() {
  echo "=== Verifying core dump capture works in this environment ==="
  local canary_cwd
  canary_cwd="$(mktemp -d)"
  ( cd "${canary_cwd}" && exec sh -c 'kill -SEGV $$' ) || true

  local canary=""
  for _ in $(seq 1 30); do
    canary=$(find "${COREDUMP_STAGING_DIR}" -maxdepth 1 -name '*-core*' 2>/dev/null | head -1 || true)
    [[ -n "${canary}" ]] && break
    sleep 0.1
  done
  rm -rf "${canary_cwd}"

  if [[ -n "${canary}" ]]; then
    echo "Core dump capture verified OK (canary: ${canary})"
    rm -f "${canary}"
  else
    echo "############################################################################"
    echo "# WARNING: core dump capture canary FAILED."
    echo "# A real crash in this job will NOT leave a usable core file to debug."
    echo "# soft=$(ulimit -Sc) hard=$(ulimit -Hc) core_pattern=$(cat /proc/sys/kernel/core_pattern 2>/dev/null || echo unreadable)"
    echo "############################################################################"
  fi
}

# Picks up whatever the watcher relocated to COREDUMP_STAGING_DIR for this
# iteration and copies it, together with a matching copy of the binary (needed
# to symbolize it), into /tmp/failed so it rides along with the existing
# "regression_logs" artifact upload on failure. Best effort; a missing core is
# not itself an error since not every test failure is a crash.
CollectCoreDumps() {
  local iteration=$1
  local core_dir="/tmp/failed/iteration_${iteration}_cores"
  local found=false

  shopt -s nullglob
  for f in "${COREDUMP_STAGING_DIR}"/*; do
    mkdir -p "${core_dir}"
    mv "$f" "${core_dir}/"
    found=true
  done
  shopt -u nullglob

  if [[ "${found}" == true ]]; then
    echo "Found core dump(s) for iteration ${iteration} in ${core_dir}, copying the binary alongside"
    cp "${DRAGONFLY_PATH}" "${core_dir}/" || echo "Failed to copy binary alongside core dump"
  fi
}

ValidateInputs() {
  ITERATIONS_INPUT=${ITERATIONS_INPUT:-1}

  if ! [[ "${ITERATIONS_INPUT}" =~ ^[0-9]+$ ]]; then
    echo "iterations must be a non-negative integer, got: ${ITERATIONS_INPUT}"
    exit 2
  fi
  ITERATIONS_INPUT=$((10#${ITERATIONS_INPUT}))

  if [[ -n "${GTEST_ITERATIONS_INPUT}" ]] && \
     ! [[ "${GTEST_ITERATIONS_INPUT}" =~ ^[1-9][0-9]*$ ]]; then
    echo "gtest-iterations must be a positive integer, got: ${GTEST_ITERATIONS_INPUT}"
    exit 2
  fi
  if [[ -n "${GTEST_ITERATIONS_INPUT}" ]]; then
    GTEST_ITERATIONS_INPUT=$((10#${GTEST_ITERATIONS_INPUT}))
  fi

  if [[ -n "${MAX_TESTS_RUN_TIME_INPUT}" ]] && ! [[ "${MAX_TESTS_RUN_TIME_INPUT}" =~ ^[1-9][0-9]*$ ]]; then
    echo "max-tests-run-time must be a positive integer, got: ${MAX_TESTS_RUN_TIME_INPUT}"
    exit 2
  fi

  if [[ -n "${MAX_TESTS_RUN_TIME_INPUT}" ]] && ((10#${MAX_TESTS_RUN_TIME_INPUT} > 360)); then
    echo "max-tests-run-time must be between 1 and 360 minutes, got: ${MAX_TESTS_RUN_TIME_INPUT}"
    exit 2
  fi
  if [[ -n "${MAX_TESTS_RUN_TIME_INPUT}" ]]; then
    MAX_TESTS_RUN_TIME_INPUT=$((10#${MAX_TESTS_RUN_TIME_INPUT}))
  fi

  case "${CONTINUE_ON_TEST_FAILURE_INPUT}" in
    true|false) ;;
    *)
      echo "continue-on-test-failure must be true or false, got: ${CONTINUE_ON_TEST_FAILURE_INPUT}"
      exit 2
      ;;
  esac

  if [[ -n "${TEST_CASES_INPUT}" ]]; then
    regex_status=0
    grep -E -q -- "${TEST_CASES_INPUT}" /dev/null || regex_status=$?
    if [[ "${regex_status}" -eq 2 ]]; then
      echo "test-cases is not a valid extended regular expression: ${TEST_CASES_INPUT}"
      exit 2
    fi
  fi

  NormalizePytestSuites
}

NormalizePytestSuites() {
  NORMALIZED_TEST_PATHS=()
  [[ -z "${TEST_SUITES_INPUT}" ]] && return

  normalized_suites="${TEST_SUITES_INPUT//,/ }"
  for suite in ${normalized_suites}; do
    suite="${suite#tests/dragonfly/}"
    suite="${suite%.py}.py"
    if [[ "${suite}" == /* || "${suite}" == .. || "${suite}" == ../* || \
          "${suite}" == */../* || "${suite}" == */.. ]]; then
      echo "Test suite must be relative to tests/dragonfly: ${suite}"
      exit 2
    fi
    suite="dragonfly/${suite}"
    if [[ ! -f "${GITHUB_WORKSPACE}/tests/${suite}" ]]; then
      echo "Test suite not found: ${suite}"
      exit 2
    fi
    NORMALIZED_TEST_PATHS+=("${suite}")
  done
}

RunPytests() {
  MAX_TESTS_RUN_TIME_INPUT="${MAX_TESTS_RUN_TIME_MINUTES}"
  ValidateInputs
  if [[ "${ITERATIONS_INPUT}" -eq 0 ]]; then
    echo "Pytest iterations set to 0; skipping Pytest"
    return 0
  fi
  max_tests_run_time_minutes=${MAX_TESTS_RUN_TIME_INPUT}
  deadline_seconds=$(GetDeadlineSeconds "${max_tests_run_time_minutes}")

  if [[ "${DELETE_FAILED_LOGS:-true}" == false ]]; then
    rm -rf /tmp/failed
    mkdir -p /tmp/failed
  fi

  ls -l ${GITHUB_WORKSPACE}/
  cd ${GITHUB_WORKSPACE}/tests || exit 2
  echo "Current commit is ${GITHUB_SHA}"

  SetupCoreDumps
  StartCoreDumpWatcher
  trap StopCoreDumpWatcher EXIT
  VerifyCoreDumpCapture

  # used by PyTests
  export DRAGONFLY_PATH="${GITHUB_WORKSPACE}/${BUILD_FOLDER_NAME}/${REGRESSION_DFLY_EXECUTABLE}"
  export ROOT_DIR="${GITHUB_WORKSPACE}/tests/dragonfly/valkey_search"
  export UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1 # to crash on errors
  export FILTER="${TEST_FILTER}"
  JUNIT_DIR="${REGRESSION_JUNIT_DIR}"

  # Runtime flags forwarded to the dragonfly process via --df.
  # Globbing is disabled while splitting so values like vmodule=*=1 are preserved,
  # and the flags are collected into an array to avoid re-splitting/globbing
  # when expanded on the pytest command line.
  DF_RUNTIME_ARGS=()
  if [[ -n "${DF_RUNTIME_FLAGS_INPUT}" ]]; then
    set -f # disables filename expansion (globbing)
    for flag in ${DF_RUNTIME_FLAGS_INPUT}; do
      DF_RUNTIME_ARGS+=(--df "$flag")
    done
    set +f
  fi

  # Exclude large tests unless explicitly requested
  if [[ "$FILTER" == "large" ]]; then
    : # keep as-is, run only large tests
  elif [[ -n "$FILTER" ]]; then
    FILTER="(not large) and ($FILTER)"
  else
    FILTER="not large"
  fi

  if [[ "${REGRESSION_JUNIT_KIND}" == 'epoll' ]]; then
    FILTER="$FILTER and not exclude_epoll"
  fi

  test_paths=()
  if [[ -n "${TEST_SUITES_INPUT}" ]]; then
    test_paths=("${NORMALIZED_TEST_PATHS[@]}")
  else
    test_paths=(dragonfly)
  fi

  selected_tests=()
  if [[ -n "${TEST_CASES_INPUT}" ]]; then
    if [[ -n "${deadline_seconds}" ]]; then
      remaining_seconds=$((deadline_seconds - $(date +%s)))
      if [[ "${remaining_seconds}" -le 0 ]]; then
        ExitOnTimeout false "${max_tests_run_time_minutes}"
      fi
    else
      remaining_seconds=""
    fi
    pytest_collect_command=(pytest -m "${FILTER}" --collect-only -q --color=no "${test_paths[@]}")
    if [[ -n "${remaining_seconds}" ]]; then
      pytest_collect_command=(timeout "${remaining_seconds}s" "${pytest_collect_command[@]}")
    fi
    collection_output_file=$(mktemp)
    collection_code=0
    "${pytest_collect_command[@]}" >"${collection_output_file}" 2>/dev/null || collection_code=$?
    if [[ "${collection_code}" -eq 124 ]]; then
      rm -f "${collection_output_file}"
      ExitOnTimeout false "${max_tests_run_time_minutes}"
    fi
    if [[ "${collection_code}" -ne 0 ]]; then
      cat "${collection_output_file}"
      rm -f "${collection_output_file}"
      exit "${collection_code}"
    fi
    mapfile -t selected_tests < <(
      grep -E '^dragonfly/.*\.py::' "${collection_output_file}" | grep -E -- "${TEST_CASES_INPUT}" || true
    )
    rm -f "${collection_output_file}"
    if [[ "${#selected_tests[@]}" -eq 0 ]]; then
      echo "No tests matched test-cases regex: ${TEST_CASES_INPUT}"
      exit 2
    fi
  else
    selected_tests=("${test_paths[@]}")
  fi

  pytest_failed=false
  for iteration in $(seq 1 "${ITERATIONS_INPUT}"); do
    PrintIteration "Regression test" "${iteration}" "${ITERATIONS_INPUT}"
    junit_file="${JUNIT_DIR}/pytest-${REGRESSION_JUNIT_KIND}-${iteration}.xml"
    if [[ -n "${deadline_seconds}" ]]; then
      remaining_seconds=$((deadline_seconds - $(date +%s)))
      if [[ "${remaining_seconds}" -le 0 ]]; then
        ExitOnTimeout "${pytest_failed}" "${max_tests_run_time_minutes}"
      fi
    else
      remaining_seconds=""
    fi
    code=0
    if [[ "${REGRESSION_JUNIT_KIND}" == "epoll" ]]; then
      # Run only replication tests with epoll
      pytest_command=(pytest -m "${FILTER}" --durations=10
        --timeout=300 --color=yes --json-report --json-report-file=report.json
        --junitxml="${junit_file}" "${selected_tests[@]}" --df force_epoll=true
        "${DF_RUNTIME_ARGS[@]}" --log-cli-level=INFO)
    else
      # Run only replication tests with iouring
      pytest_command=(pytest -m "${FILTER}" --durations=10
        --timeout=300 --color=yes --json-report --json-report-file=report.json
        --junitxml="${junit_file}" "${selected_tests[@]}" "${DF_RUNTIME_ARGS[@]}"
        --log-cli-level=INFO)
    fi
    if [[ -n "${remaining_seconds}" ]]; then
      pytest_command=(timeout "${remaining_seconds}s" "${pytest_command[@]}")
    else
      pytest_command=(timeout 80m "${pytest_command[@]}")
    fi
    PrintCommand "${pytest_command[@]}"
    "${pytest_command[@]}" || code=$?

    if [[ "${code}" -ne 0 ]]; then
      CollectCoreDumps "${iteration}"
    fi

    # timeout returns 124 if we exceeded the timeout duration
    if [[ "${code}" -eq 124 ]]; then
      # Add an extra new line here because when tests timeout the first line below continues from the test failure name
      echo "\n"
      echo "🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑"
      echo "🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 TESTS TIMEDOUT 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑"
      echo "🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑"
      # Copy the last log file because we timed out and pytest did not copy it
      # to the /tmp/failed/ folder.
      if [[ -f /tmp/last_test_log_dir.txt ]]; then
        while IFS= read -r log_dir; do
          if [[ -d "${log_dir}" ]]; then
            mkdir -p /tmp/failed
            mv "${log_dir}" /tmp/failed/
          fi
        done </tmp/last_test_log_dir.txt
      fi
      ExitOnTimeout "${pytest_failed}" "${max_tests_run_time_minutes}"
    fi

    if [[ "${code}" -eq 0 ]]; then
      if [[ "${CONTINUE_ON_TEST_FAILURE_INPUT}" == true ]]; then
        ArchiveAndCleanPytestLogs "${iteration}" false "${junit_file}" || exit 1
      fi
      continue
    fi
    if [[ "${code}" -eq 1 ]]; then
      pytest_failed=true
      if [[ "${CONTINUE_ON_TEST_FAILURE_INPUT}" == true ]]; then
        ArchiveAndCleanPytestLogs "${iteration}" true "${junit_file}" || exit 1
        continue
      fi
    fi
    # when a test fails in pytest it returns 1 but there are other return codes as well so we just check if the code is non zero
    exit "${code}"
  done

  if [[ "${pytest_failed}" == true ]]; then
    exit 1
  fi
}

RunGtests() {
  if [[ -z "${GTEST_ITERATIONS_INPUT}" ]]; then
    GTEST_ITERATIONS_INPUT=1
  fi
  MAX_TESTS_RUN_TIME_INPUT="${MAX_TESTS_RUN_TIME_MINUTES}"
  ValidateInputs
  max_tests_run_time_minutes=${MAX_TESTS_RUN_TIME_INPUT}
  deadline_seconds=$(GetDeadlineSeconds "${max_tests_run_time_minutes}")

  cd "${GITHUB_WORKSPACE}" || exit 2
  mapfile -t available_gtest_suites < <(
    find src/core src/facade src/server -name CMakeLists.txt -print0 |
      xargs -0 grep -hE '^[[:space:]]*helio_cxx_test\(' |
      sed -E 's/^[[:space:]]*helio_cxx_test\(([[:alnum:]_/-]+).*/\1/' |
      awk -F/ '{print $NF}' | sort -u
  )
  if [[ "${#available_gtest_suites[@]}" -eq 0 ]]; then
    echo "No GoogleTest suites were discovered under src/core, src/facade, or src/server"
    exit 2
  fi

  selected_gtest_suites=()
  if [[ -n "${GTEST_SUITES_INPUT}" ]]; then
    normalized_suites="${GTEST_SUITES_INPUT//,/ }"
    for suite in ${normalized_suites}; do
      suite="${suite##*/}"
      suite="${suite%.cc}"
      if ! printf '%s\n' "${available_gtest_suites[@]}" | grep -Fxq -- "${suite}"; then
        echo "GoogleTest suite not found: ${suite}"
        exit 2
      fi
      selected_gtest_suites+=("${suite}")
    done
  else
    selected_gtest_suites=("${available_gtest_suites[@]}")
  fi

  echo "Building GoogleTest suites: ${selected_gtest_suites[*]}"
  cd "${GITHUB_WORKSPACE}/${BUILD_FOLDER_NAME}" || exit 2
  if [[ -n "${deadline_seconds}" ]]; then
    remaining_seconds=$((deadline_seconds - $(date +%s)))
    if [[ "${remaining_seconds}" -le 0 ]]; then
      ExitOnTimeout false "${max_tests_run_time_minutes}"
    fi
  else
    remaining_seconds=""
  fi
  gtest_build_code=0
  gtest_build_command=(ninja "${selected_gtest_suites[@]}")
  if [[ -n "${remaining_seconds}" ]]; then
    gtest_build_command=(timeout "${remaining_seconds}s" "${gtest_build_command[@]}")
  fi
  PrintCommand "${gtest_build_command[@]}"
  "${gtest_build_command[@]}" || gtest_build_code=$?
  if [[ "${gtest_build_code}" -eq 124 ]]; then
    ExitOnTimeout false "${max_tests_run_time_minutes}"
  fi
  if [[ "${gtest_build_code}" -ne 0 ]]; then
    exit "${gtest_build_code}"
  fi

  gtest_failed=false
  for iteration in $(seq 1 "${GTEST_ITERATIONS_INPUT}"); do
    PrintIteration "GoogleTest" "${iteration}" "${GTEST_ITERATIONS_INPUT}"
    for suite in "${selected_gtest_suites[@]}"; do
      if [[ -n "${deadline_seconds}" ]]; then
        remaining_seconds=$((deadline_seconds - $(date +%s)))
        if [[ "${remaining_seconds}" -le 0 ]]; then
          ExitOnTimeout "${gtest_failed}" "${max_tests_run_time_minutes}"
        fi
      else
        remaining_seconds=""
      fi
      binary_path=$(find "${GITHUB_WORKSPACE}/${BUILD_FOLDER_NAME}" -type f -name "${suite}" \
        -executable -print -quit)
      if [[ -z "${binary_path}" ]]; then
        echo "Built GoogleTest executable not found: ${suite}"
        exit 2
      fi

      gtest_args=()
      if [[ -n "${GTEST_CASES_INPUT}" ]]; then
        gtest_args+=("--gtest_filter=${GTEST_CASES_INPUT}")
      fi
      code=0
      gtest_command=("${binary_path}" "${gtest_args[@]}")
      if [[ -n "${remaining_seconds}" ]]; then
        gtest_command=(timeout "${remaining_seconds}s" "${gtest_command[@]}")
      fi
      PrintCommand "${gtest_command[@]}"
      gtest_output_file=$(mktemp)
      "${gtest_command[@]}" >"${gtest_output_file}" 2>&1 || code=$?
      cat "${gtest_output_file}"
      if [[ -n "${GTEST_CASES_INPUT}" ]] && \
        (grep -Eq 'filter ".*" did not match any test; no tests were run' "${gtest_output_file}" || \
         grep -Eq '\[ *PASSED *\] 0 tests\.' "${gtest_output_file}"); then
        echo "Skipping ${suite}: GoogleTest filter matched no tests: ${GTEST_CASES_INPUT}"
        rm -f "${gtest_output_file}"
        continue
      fi
      rm -f "${gtest_output_file}"
      if [[ "${code}" -eq 124 ]]; then
        ExitOnTimeout "${gtest_failed}" "${max_tests_run_time_minutes}"
      fi
      if [[ "${code}" -eq 0 ]]; then
        continue
      fi
      if [[ "${code}" -eq 1 ]]; then
        gtest_failed=true
        if [[ "${CONTINUE_ON_TEST_FAILURE_INPUT}" == true ]]; then
          continue
        fi
      fi
      exit "${code}"
    done
  done

  if [[ "${gtest_failed}" == true ]]; then
    exit 1
  fi
}

case "${1:-}" in
  validate) ValidateInputs ;;
  pytest) RunPytests ;;
  gtest) RunGtests ;;
  *)
    echo "Usage: $0 {validate|pytest|gtest}"
    exit 2
    ;;
esac
