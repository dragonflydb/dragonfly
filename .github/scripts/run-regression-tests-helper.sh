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

ValidateInputs() {
  ITERATIONS_INPUT=${ITERATIONS_INPUT:-1}

  if ! [[ "${ITERATIONS_INPUT}" =~ ^[0-9]+$ ]]; then
    echo "iterations must be a non-negative integer, got: ${ITERATIONS_INPUT}"
    exit 2
  fi
  ITERATIONS_INPUT=$((10#${ITERATIONS_INPUT}))

  if [[ -n "${GTEST_ITERATIONS_INPUT}" ]] && ! [[ "${GTEST_ITERATIONS_INPUT}" =~ ^[0-9]+$ ]]; then
    echo "gtest-iterations must be a non-negative integer, got: ${GTEST_ITERATIONS_INPUT}"
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
    GTEST_ITERATIONS_INPUT=0
  fi
  MAX_TESTS_RUN_TIME_INPUT="${MAX_TESTS_RUN_TIME_MINUTES}"
  ValidateInputs
  if [[ "${GTEST_ITERATIONS_INPUT}" -eq 0 ]]; then
    echo "Skipping GoogleTests: gtest-iterations is 0"
    return
  fi
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
