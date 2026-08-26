# Manual Regression Tests

The `Regression Tests` and `Epoll Regression Tests` workflows run automatically on
schedule. Their `workflow_dispatch` inputs also support short, targeted runs when
you need to reproduce or repeat a regression test.

For the workflow UI, open **Actions**, select the workflow, choose **Run workflow**,
and fill in only the inputs needed for the scenario.

## Workflow Input Fields

The following names match the `workflow_dispatch` fields in the YAML exactly.

### Python tests

- `test-suites`: Comma- or space-separated Python test filenames. A filename without
  a path is resolved under `tests/dragonfly/`. Leave empty to use the full
  `tests/dragonfly` suite.
- `test-cases`: An extended regular expression matched against collected pytest
  node IDs. Leave empty to run all cases in the selected suites. The same regex
  applies to every suite in `test-suites`; it is not a separate filter per file.
- `iterations`: A positive integer controlling how many times the selected pytest
  tests run. It defaults to `1` when left empty.
- `max-run-time`: An optional positive number of minutes for the complete selected
  test run, including all requested iterations. It applies to Python or GoogleTest
  runs and stops the run when the time budget or requested iterations finish first.
  Manual runs default to `360` minutes (6 hours) and have no internal 80-minute cap.
  Leave empty to use the six-hour default. GitHub Actions' job timeout also defaults
  to and is capped at 6 hours; GitHub-hosted runners cannot run longer than 6 hours.

Examples:

`test-suites`:

```text
pymemcached_test.py
```

`test-cases`:

```text
test_basic
```

`iterations`:

```text
2
```

For another Python selection:

`test-suites`:

```text
connection_test,eval_test
```

`test-cases`:

```text
test_timeout|test_eval
```

`iterations`:

```text
5
```

To run only two cases from `connection_test` and every case from `eval_test`, put
the following in the `test-cases` field. The suite names in the regex scope each
alternative to its file:

`test-cases`:

```text
(^|/)connection_test\.py::(test_case_one|test_case_two)$|(^|/)eval_test\.py::
```

Set `test-suites` to `connection_test,eval_test` as well. Pytest node IDs use `::`
to separate the file, test class, and test function, for example
`eval_test.py::TestEval::test_basic`. The trailing `::` after `eval_test.py` is
therefore needed to scope that alternative to node IDs from `eval_test`; it is not
an extra test name and does not need to appear at the end of the complete regex.

Pytest receives the resulting collected node IDs as explicit test arguments. A
short regex such as `test_case_one|test_case_two` is global and can select matching
case names from both suites. There is currently no per-suite filter syntax.

## GoogleTest

Python and GoogleTest selection are independent. Set Python inputs to run Python,
GoogleTest inputs to run GoogleTest, or both sets of inputs to run both. When every
selection input is left at its default, both full suites run once. Both iteration
fields visibly default to `1` in the workflow form.

Python is selected by a non-empty `test-suites` or `test-cases` value, or by setting
`iterations` to a value other than its default of `1`. GoogleTest is selected by a
non-empty `gtest-suites` or `gtest-cases` value, or by setting `gtest-iterations` to
a value other than its default of `1`. For example,
setting `iterations` to `3` and `gtest-suites` to `dfly_core_test` runs the selected
Python tests three times and `dfly_core_test` once by default.

- `gtest-suites`: Comma- or space-separated target names discovered under
  `src/core`, `src/facade`, and `src/server`. You may also provide a target path or
  `.cc` suffix; only the target name is used.
- `gtest-cases`: A value passed directly to GoogleTest as `--gtest_filter`.
- `gtest-iterations`: A positive integer controlling how many times the selected
  GoogleTest targets run. It defaults to `1`.

Example:

`gtest-suites`:

```text
generic_family_test
```

`gtest-cases`:

```text
StringMapTest.*:DashTest.*
```

`gtest-iterations`:

```text
2
```

`max-run-time`:

```text
30
```

### Failure handling

- `continue-on-test-failure`: A boolean that defaults to `false`. When `false`, the
  first failing test stops the selected run. Set it to `true` to complete all
  selected iterations and report a failure at the end. This applies to both Python
  and GoogleTest runs.

## Validation and scheduling

Manual inputs are validated immediately after checkout and before the Dragonfly
build. Invalid counts, suite names, targets, or regular expressions fail early.
The boolean failure setting is also validated by GitHub because it is a typed
workflow input.

Scheduled runs do not provide manual inputs. They skip the manual-input validation
step and retain the original 80-minute Python command timeout. Manual runs use the
longer `max-run-time` budget described above. GitHub Actions can still terminate a
run when its job or runner timeout expires.

Both workflows fan out over their configured build matrix. Targeted inputs reduce
test execution time, but each matrix job still builds the configured Dragonfly
variant before running tests.

## Runner Capacity And Iterations

A regression run in the pool uses one runner. A regular regression run fans out
across eight matrix runners and can use a ninth runner for the lint job. Plan the
`iterations` and `gtest-iterations` values accordingly: each iteration runs once
per matrix job, not once for the workflow as a whole. For example, setting
`iterations` to `3` in a regular run means 24 matrix test runs, not three total
runs. The optional
ninth runner performs linting and does not add test iterations.

Manual runs cannot filter the architecture, runner, Debug build, or Release build.
The configured variants always run together: all variants or none. Use suite, case,
and iteration inputs to control the test workload instead.

GoogleTest targets are validated after CMake configures the build. This avoids
rejecting a target merely because its source declaration is conditional on the
selected build configuration.
