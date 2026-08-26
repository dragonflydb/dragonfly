#!/bin/bash

# This helper has three sections:
# 1. Input validation for manual workflow dispatches.
# 2. Pytest execution for Python regression tests.
# 3. GoogleTest execution for opt-in C++ test runs.

PrintCommand() {
  printf 'Command: '
  printf '%q ' "$@"
  printf '\n'
}

PrintIteration() {
  printf '\033[32m=== %s iteration %s/%s ===\033[0m\n' "$1" "$2" "$3"
}

GetDeadlineSeconds() {
  local max_run_time_minutes=$1

  if [[ -n "${REGRESSION_DEADLINE_EPOCH:-}" ]]; then
    printf '%s\n' "${REGRESSION_DEADLINE_EPOCH}"
  else
    printf '%s\n' "$(( $(date +%s) + max_run_time_minutes * 60 ))"
  fi
}

PrintBudgetExhausted() {
  echo "Shared regression time budget of $1 minutes exhausted"
}

ValidateInputs() {
  ITERATIONS_INPUT=${ITERATIONS_INPUT:-1}

  if ! [[ "${ITERATIONS_INPUT}" =~ ^[1-9][0-9]*$ ]]; then
    echo "iterations must be a positive integer, got: ${ITERATIONS_INPUT}"
    exit 2
  fi

  if [[ -n "${GTEST_ITERATIONS_INPUT}" ]] && \
     ! [[ "${GTEST_ITERATIONS_INPUT}" =~ ^[1-9][0-9]*$ ]]; then
    echo "gtest-iterations must be a positive integer, got: ${GTEST_ITERATIONS_INPUT}"
    exit 2
  fi

  if [[ -n "${MAX_RUN_TIME_INPUT}" ]] && ! [[ "${MAX_RUN_TIME_INPUT}" =~ ^[1-9][0-9]*$ ]]; then
    echo "max-run-time must be a positive integer, got: ${MAX_RUN_TIME_INPUT}"
    exit 2
  fi

  if [[ -n "${MAX_RUN_TIME_INPUT}" ]] && ((10#${MAX_RUN_TIME_INPUT} > 360)); then
    echo "max-run-time must be between 1 and 360 minutes, got: ${MAX_RUN_TIME_INPUT}"
    exit 2
  fi

  case "${CONTINUE_ON_TEST_FAILURE_INPUT}" in
    true|false) ;;
    *)
      echo "continue-on-test-failure must be true or false, got: ${CONTINUE_ON_TEST_FAILURE_INPUT}"
      exit 2
      ;;
  esac

  if [[ -n "${TEST_CASES_INPUT}" ]]; then
    grep -E -q -- "${TEST_CASES_INPUT}" /dev/null
    regex_status=$?
    if [[ "${regex_status}" -eq 2 ]]; then
      echo "test-cases is not a valid extended regular expression: ${TEST_CASES_INPUT}"
      exit 2
    fi
  fi

  if [[ -n "${TEST_SUITES_INPUT}" ]]; then
    normalized_suites="${TEST_SUITES_INPUT//,/ }"
    for suite in ${normalized_suites}; do
      suite="${suite%.py}.py"
      if [[ "${suite}" != */* ]]; then
        suite="tests/dragonfly/${suite}"
      else
        suite="tests/${suite#tests/}"
      fi
      if [[ ! -f "${GITHUB_WORKSPACE}/${suite}" ]]; then
        echo "Test suite not found: ${suite}"
        exit 2
      fi
    done
  fi
}

RunPytests() {
  MAX_RUN_TIME_INPUT="${MAX_RUN_TIME_MINUTES}" ValidateInputs
  max_run_time_minutes=${MAX_RUN_TIME_MINUTES}
  deadline_seconds=$(GetDeadlineSeconds "${max_run_time_minutes}")

  ls -l "${GITHUB_WORKSPACE}/"
  cd "${GITHUB_WORKSPACE}/tests" || exit 2
  echo "Current commit is ${GITHUB_SHA}"

  export DRAGONFLY_PATH="${GITHUB_WORKSPACE}/${BUILD_FOLDER_NAME}/${REGRESSION_DFLY_EXECUTABLE}"
  export ROOT_DIR="${GITHUB_WORKSPACE}/tests/dragonfly/valkey_search"
  export UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1
  export FILTER="${TEST_FILTER}"

  test_paths=()
  if [[ -n "${TEST_SUITES_INPUT}" ]]; then
    normalized_suites="${TEST_SUITES_INPUT//,/ }"
    for suite in ${normalized_suites}; do
      suite="${suite%.py}.py"
      if [[ "${suite}" != */* ]]; then
        suite="dragonfly/${suite}"
      fi
      test_paths+=("${suite}")
    done
  else
    test_paths=(dragonfly)
  fi

  df_runtime_args=()
  if [[ -n "${DF_RUNTIME_FLAGS_INPUT}" ]]; then
    set -f
    for flag in ${DF_RUNTIME_FLAGS_INPUT}; do
      df_runtime_args+=(--df "${flag}")
    done
    set +f
  fi

  if [[ "${FILTER}" == "large" ]]; then
    :
  elif [[ -n "${FILTER}" ]]; then
    FILTER="(not large) and (${FILTER})"
  else
    FILTER="not large"
  fi

  if [[ "${REGRESSION_JUNIT_KIND}" == "epoll" ]]; then
    FILTER="${FILTER} and not exclude_epoll"
  fi

  selected_tests=()
  if [[ -n "${TEST_CASES_INPUT}" ]]; then
    mapfile -t selected_tests < <(
      pytest -m "${FILTER}" --collect-only -q --color=no "${test_paths[@]}" 2>/dev/null |
        grep -E -- "${TEST_CASES_INPUT}" || true
    )
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
    junit_file="${REGRESSION_JUNIT_DIR}/pytest-${REGRESSION_JUNIT_KIND}-${iteration}.xml"
    remaining_seconds=$((deadline_seconds - $(date +%s)))
    if [[ "${remaining_seconds}" -le 0 ]]; then
      PrintBudgetExhausted "${max_run_time_minutes}"
      exit 124
    fi
    code=0
    if [[ "${REGRESSION_JUNIT_KIND}" == "epoll" ]]; then
      pytest_command=(timeout --foreground "${remaining_seconds}s" pytest -m "${FILTER}" --durations=10
        --timeout=300 --color=yes --json-report --json-report-file=report.json
        --junitxml="${junit_file}" "${selected_tests[@]}" --df force_epoll=true
        "${df_runtime_args[@]}" --log-cli-level=INFO)
    else
      pytest_command=(timeout --foreground "${remaining_seconds}s" pytest -m "${FILTER}" --durations=10
        --timeout=300 --color=yes --json-report --json-report-file=report.json
        --junitxml="${junit_file}" "${selected_tests[@]}" "${df_runtime_args[@]}"
        --log-cli-level=INFO)
    fi
    PrintCommand "${pytest_command[@]}"
    "${pytest_command[@]}" || code=$?

    if [[ "${code}" -eq 124 ]]; then
      PrintBudgetExhausted "${max_run_time_minutes}"
      if [[ -f /tmp/last_test_log_dir.txt ]]; then
        while IFS= read -r log_dir; do
          if [[ -d "${log_dir}" ]]; then
            mkdir -p /tmp/failed
            mv "${log_dir}" /tmp/failed/
          fi
        done </tmp/last_test_log_dir.txt
      fi
      exit 1
    fi

    if [[ "${code}" -eq 0 ]]; then
      continue
    fi
    if [[ "${code}" -eq 1 ]]; then
      pytest_failed=true
      if [[ "${CONTINUE_ON_TEST_FAILURE_INPUT}" == true ]]; then
        continue
      fi
    fi
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
  MAX_RUN_TIME_INPUT="${MAX_RUN_TIME_MINUTES}" ValidateInputs
  max_run_time_minutes=${MAX_RUN_TIME_MINUTES}
  deadline_seconds=$(GetDeadlineSeconds "${max_run_time_minutes}")

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
  remaining_seconds=$((deadline_seconds - $(date +%s)))
  if [[ "${remaining_seconds}" -le 0 ]]; then
    PrintBudgetExhausted "${max_run_time_minutes}"
    exit 124
  fi
  gtest_build_code=0
  gtest_build_command=(timeout --foreground "${remaining_seconds}s" ninja "${selected_gtest_suites[@]}")
  PrintCommand "${gtest_build_command[@]}"
  "${gtest_build_command[@]}" || gtest_build_code=$?
  if [[ "${gtest_build_code}" -eq 124 ]]; then
    PrintBudgetExhausted "${max_run_time_minutes}"
    exit 1
  fi
  if [[ "${gtest_build_code}" -ne 0 ]]; then
    exit "${gtest_build_code}"
  fi

  gtest_failed=false
  for iteration in $(seq 1 "${GTEST_ITERATIONS_INPUT}"); do
    PrintIteration "GoogleTest" "${iteration}" "${GTEST_ITERATIONS_INPUT}"
    for suite in "${selected_gtest_suites[@]}"; do
      remaining_seconds=$((deadline_seconds - $(date +%s)))
      if [[ "${remaining_seconds}" -le 0 ]]; then
        PrintBudgetExhausted "${max_run_time_minutes}"
        exit 124
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
      gtest_command=(timeout --foreground "${remaining_seconds}s" "${binary_path}" "${gtest_args[@]}")
      PrintCommand "${gtest_command[@]}"
      gtest_output_file=$(mktemp)
      "${gtest_command[@]}" >"${gtest_output_file}" 2>&1 || code=$?
      cat "${gtest_output_file}"
      if [[ "${code}" -eq 0 ]] && \
        grep -Eq 'filter ".*" did not match any test; no tests were run' "${gtest_output_file}"; then
        echo "GoogleTest filter matched no tests: ${GTEST_CASES_INPUT}"
        code=1
      fi
      rm -f "${gtest_output_file}"
      if [[ "${code}" -eq 124 ]]; then
        PrintBudgetExhausted "${max_run_time_minutes}"
        exit 1
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
