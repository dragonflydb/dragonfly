# Manual CI Test Runs

Dragonfly provides three test workflows that run on a schedule and can also be
dispatched manually. Use **Actions**, select a workflow, choose **Run workflow**,
select the branch, and enter the optional inputs.

| Workflow | Pytest selection | Runners | Build | GoogleTests |
| --- | --- | --- | --- | --- |
| `Regression Tests` | All tests except `large` | `ubuntu-latest`, self-hosted ARM64 | Debug, Release | Supported |
| `Epoll Regression Tests` | All tests except `large` and `exclude_epoll` | `ubuntu-latest`, self-hosted ARM64 | Debug | Supported; forces Epoll |
| `Heavy Tests` | Only tests marked `large` | `CI-LARGE-86`, `CI-LARGE-ARM` | Release | Not supported |

All three workflows accept the Pytest and common inputs below; only the two
regression workflows accept the GoogleTest inputs. Only `Heavy Tests` accepts
`df-runtime-flags`, a space-separated list passed to every Dragonfly process (for example
`proactor_threads=2`).

Besides the selection above, each workflow first runs the S3 snapshot tests
(`pytest -k s3 dragonfly/snapshot_test.py`) against a local MinIO, regardless of the
`large` marker. This happens on scheduled runs and on manual runs with default
selection inputs; setting `test-suites` or `test-cases`, or `iterations` other than
`1`, skips it.

## Execution Model

- Scheduled runs execute Pytest once and do not build or run GoogleTests.
- Manual runs execute the Pytest step first when `iterations` is positive.
- In `Regression Tests` and `Epoll Regression Tests`, GoogleTests are disabled when
  `gtest-iterations` is `0` and run when it is positive. When both test families are
  enabled, they run one after another: GoogleTests are attempted after Pytest even if
  Pytest fails. `Heavy Tests` runs only Pytest.
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

GoogleTests run only for manual dispatches of `Regression Tests` and
`Epoll Regression Tests`; `Heavy Tests` does not support them.
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
failed matrix job uploads its logs artifact (`logs` in the regression workflows,
`logs-<runner>` in `Heavy Tests`) from `/tmp/failed/*`, containing all
failed-iteration archives and `pytest-failures-by-iteration.txt`. The report lists
failed cases from every failed Pytest iteration. Archives are standard gzip tar
files. Scheduled runs retain the existing cleanup behavior.

## Validation And Scale

Manual inputs are validated after checkout and before the Dragonfly build. Invalid
counts, suite names, targets, and regular expressions fail early. GoogleTest targets
are checked after CMake configuration because target availability can vary by build
configuration.

Iteration counts apply per matrix job. For example, three Pytest iterations in the
regular Uring workflow run across eight matrix jobs, producing 24 Pytest runs, while
`Heavy Tests` has two matrix jobs. The lint job does not add test iterations.

Before every Pytest or GoogleTest iteration, the regression action logs an iteration
banner and the exact shell-escaped command. To inspect them, expand
`Run regression tests action` in the regression workflows or `Run heavy tests` in
`Heavy Tests` in the job log.

## Loopback MTU

Every workflow that runs the regression tests (`tests/dragonfly`) lowers the loopback MTU
of its job container from the default `65536` to `9000` before the tests, using
`.github/actions/limit-loopback-mtu`. The `regression-tests` and `repeat` actions run it
automatically, and `cov.yml`, which calls `pytest` directly, runs it as an explicit step.

With loopback's ~64 KB MSS, some Linux kernels collapse a socket's receive buffer and
window to about the MSS, and the sender then crawls in a 200 ms zero-window probe loop.
This made replication and slot migration tests hang on CI
([#4834](https://github.com/dragonflydb/dragonfly/issues/4834)). A NIC sized MTU avoids
that path.

The job container has its own network namespace, so only its loopback changes. Changing
an interface MTU needs the `NET_ADMIN` capability, which Docker does not grant even to
root, so these jobs run their container with `--cap-add=NET_ADMIN`. A new workflow that
runs the regression tests needs both the capability and the action.
