# Manual Regression Tests

`Regression Tests` and `Epoll Regression Tests` run Pytest automatically on a
schedule. To run them manually, use **Actions** > select one of the two workflow > **Run workflow** to choose Pytest
or GoogleTest inputs. You can also choose your own branch.

## Execution Model

- Scheduled runs execute Pytest once and do not build or run GoogleTests.
- Manual runs execute the Pytest step first when `iterations` is positive.
  GoogleTests are disabled when `gtest-iterations` is `0` and run when it is
  positive. When both test families are enabled, they run one after another:
  GoogleTests are attempted after Pytest even if Pytest fails.
- Each workflow fans out across its configured matrix. Inputs apply to every
  matrix job; they cannot select an architecture, runner, or build type.

## Pytest Inputs

With default inputs, Pytest runs the full `tests/dragonfly` suite once.

| Input | Default | Description |
| --- | --- | --- |
| `test-suites` | empty | Comma- or space-separated Python filenames relative to `tests/dragonfly/`. A name without a path is resolved there. Empty runs all suites. Example: `connection_test,eval_test`. |
| `test-cases` | empty | Extended regular expression matched against collected Pytest node IDs. Empty runs all cases in the selected suites. Example: `test_timeout\|test_eval`. |
| `iterations` | `1` | Non-negative Pytest run count. Set `0` to skip Pytest for a GoogleTest-only manual run. |



`test-cases` applies globally to all selected suites; there is no per-suite filter
input. The selected node IDs are passed to Pytest explicitly. To select two cases
from `connection_test` and all cases from `eval_test`, use:

```text
test-suites: connection_test,eval_test
test-cases:  (^|/)connection_test\.py::(test_case_one|test_case_two)$|(^|/)eval_test\.py::
```

## GoogleTest Inputs

GoogleTests run only for manual dispatches of these two regression workflows.
`gtest-iterations=0` disables them, including when GoogleTest selectors are supplied.
Set `gtest-iterations` to a positive value to enable them. With no selectors, every
discovered target runs. Targets are found by recursively scanning `CMakeLists.txt`
files for `helio_cxx_test(...)` under `src/core`, `src/facade`, and `src/server`. This
includes tests in nested directories such as `src/core/json` and `src/server/cluster`;
the available targets can still vary with the selected build configuration.

| Input | Default | Description |
| --- | --- | --- |
| `gtest-suites` | empty | Comma- or space-separated GoogleTest target names. A target path and `.cc` suffix are accepted, but only the target name is used. Empty runs all discovered targets. Example: `set_family_test,generic_family_test`. |
| `gtest-cases` | empty | Value passed directly as `--gtest_filter`. Empty runs all cases in each selected target. Example: `SetFamilyTest.*:HSetFamilyTest.*`. |
| `gtest-iterations` | `0` | GoogleTest run count. `0` skips GoogleTests; a positive value enables and repeats them. |

Specify `gtest-suites` when using `gtest-cases` for only a few binaries. Otherwise
the workflow builds every target and evaluates the filter against each one. A
selected target whose filter matches no tests is skipped.

## Common Inputs

| Input | Default | Description |
| --- | --- | --- |
| `max-tests-run-time` | `360` manual; not used for scheduled runs | Positive shared budget from 1 through 360 minutes. For manual runs, the deadline starts immediately before the main Pytest step and covers Pytest and GoogleTest steps. Scheduled runs retain their existing timeout behavior. |
| `continue-on-test-failure` | `false` | When `true`, Pytest and GoogleTest each complete all selected iterations and report failure afterward. When `false`, the first failed iteration stops that test system. |

The iteration count and `max-tests-run-time` are concurrent limits: each test system ends
when either is reached. Budget expiry stops the active test command and is
treated as a controlled successful completion. A job cannot exceed GitHub Actions'
six-hour timeout.

## Continuation Mode

Set `continue-on-test-failure` to `true` when results from the entire run matter
more than stopping at the first failure. Common uses include measuring how often one
test fails across thousands of repetitions, collecting a complete six-hour CI
stability result, and future scheduled stress tests.

For a manual run with this option enabled, matrix `fail-fast` is disabled: a failed
matrix job does not cancel its siblings, so every selected configuration reaches its
own result. GitHub Actions cannot distinguish a test result failure from a build or
infrastructure failure for this setting, so siblings also continue after those
failures. Scheduled runs and manual runs with the option disabled keep fail-fast
matrix behavior.

For manual runs, Pytest keeps `/tmp/failed/` between processes and the helper
clears it once before the run begins. With continuation enabled, every failed
iteration archives the complete `/tmp/dragonfly_logs/` directory as
`/tmp/failed/iteration_<n>_logs.tar.gz`. Clean iteration logs are deleted. The
failed matrix job uploads its `logs` artifact from `/tmp/failed/*`, containing all
failed-iteration archives and `pytest-failures-by-iteration.txt`. The report lists
failed cases from every failed Pytest iteration. Archives are standard gzip tar
files. Scheduled runs retain the existing cleanup behavior.

## Validation And Scale

Manual inputs are validated after checkout and before the Dragonfly build. Invalid
counts, suite names, targets, and regular expressions fail early. GoogleTest targets
are checked after CMake configuration because target availability can vary by build
configuration.

Iteration counts apply per matrix job. For example, three Pytest iterations in the
regular Uring workflow run across eight matrix jobs, producing 24 Pytest runs. The
lint job does not add test iterations.

Before every Pytest or GoogleTest iteration, the regression action logs an iteration
banner and the exact shell-escaped command. Expand `Run regression tests action` in
the job log to inspect them.
