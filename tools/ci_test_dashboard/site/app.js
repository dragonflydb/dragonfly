const state = {
  data: null,
  suite: 'all',
  view: 'often',
  range: 'all',
  date: 'all',
  workflow: 'all',
  variant: 'all',
  search: '',
  selectedId: null,
};

const suiteOptions = [
  ['all', 'All'],
  ['cpp', 'C++'],
  ['regression', 'Pytest'],
];

const viewOptions = [
  ['failing', 'Failing'],
  ['recent', 'Recent Failures'],
  ['often', 'Often Failing'],
  ['flaky', 'Flaky'],
  ['started', 'Started Failing'],
  ['all', 'All Tests'],
];

const rangeOptions = [
  ['all', 'All history'],
  ['7', 'Last 7 days'],
  ['14', 'Last 14 days'],
  ['30', 'Last 30 days'],
];

const statusRank = {
  failed: 4,
  error: 3,
  skipped: 2,
  passed: 1,
  unknown: 0,
};

const failStatuses = new Set(['failed', 'error']);
const recentFailureDays = 7;

async function boot() {
  try {
    const response = await fetch('data/summary.json');
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    state.data = await response.json();
    state.selectedId = firstVisibleTest()?.id ?? null;
    render();
  } catch (error) {
    document.getElementById('dataStatus').textContent = 'No data';
    document.getElementById('subtitle').textContent =
        'Run python3 build_dashboard.py tmp/junit-results site/data/summary.json';
    document.getElementById('detailsPane').textContent = String(error);
  }
}

function render() {
  renderHeader();
  renderMetrics();
  renderFilters();
  renderTable();
  renderDetails();
}

function renderHeader() {
  const {generated_at: generatedAt, date_range: dateRange} = state.data;
  document.getElementById('subtitle').textContent = `Data ${dateRange.first ?? 'unknown'} to ${
      dateRange.last ?? 'unknown'}; generated ${formatDateTime(generatedAt)}`;
  document.getElementById('dataStatus').textContent = 'Ready';
}

function renderMetrics() {
  const totals = summarizeRows(baseFilteredRows());
  const metrics = [
    ['Tests in slice', totals.unique_tests],
    ['Occurrences', totals.total],
    ['Currently failing', totals.currently_failing],
    ['Flaky', totals.flaky],
    ['Failed occurrences', totals.failures],
    ['Skipped', totals.skipped],
  ];

  document.getElementById('metrics').innerHTML = metrics
                                                     .map(
                                                         ([label, value]) => `
        <article class="metric">
          <div class="label">${escapeHtml(label)}</div>
          <div class="value">${formatNumber(value)}</div>
        </article>
      `,
                                                         )
                                                     .join('');
}

function renderFilters() {
  renderSegmented('suiteFilter', suiteOptions, state.suite, (value) => {
    state.suite = value;
    state.selectedId = firstVisibleTest()?.id ?? null;
    render();
  });

  renderSegmented('viewFilter', viewOptions, state.view, (value) => {
    state.view = value;
    state.selectedId = firstVisibleTest()?.id ?? null;
    render();
  });

  const searchInput = document.getElementById('searchInput');
  if (searchInput.value !== state.search) {
    searchInput.value = state.search;
  }
  searchInput.oninput = () => {
    state.search = searchInput.value;
    state.selectedId = firstVisibleTest()?.id ?? null;
    renderTable();
    renderDetails();
  };

  renderSelect('rangeFilter', rangeOptions, state.range, (value) => {
    state.range = value;
    state.date = 'all';
    state.selectedId = firstVisibleTest()?.id ?? null;
    render();
  });

  renderSelect('dateFilter', facetOptions('dates', 'All dates'), state.date, (value) => {
    state.date = value;
    if (value !== 'all') {
      state.range = 'all';
    }
    state.selectedId = firstVisibleTest()?.id ?? null;
    render();
  });

  renderSelect(
      'workflowFilter',
      facetOptions('workflows', 'All workflows'),
      state.workflow,
      (value) => {
        state.workflow = value;
        state.selectedId = firstVisibleTest()?.id ?? null;
        render();
      },
  );

  renderSelect(
      'variantFilter',
      facetOptions('variants', 'All variants'),
      state.variant,
      (value) => {
        state.variant = value;
        state.selectedId = firstVisibleTest()?.id ?? null;
        render();
      },
  );

  document.getElementById('resetFilters').onclick = () => {
    state.suite = 'all';
    state.view = 'often';
    state.range = 'all';
    state.date = 'all';
    state.workflow = 'all';
    state.variant = 'all';
    state.search = '';
    state.selectedId = firstVisibleTest()?.id ?? null;
    render();
  };
}

function renderSegmented(elementId, options, value, onChange) {
  const element = document.getElementById(elementId);
  element.innerHTML = options
                          .map(
                              ([optionValue, label]) => `
        <button class="${optionValue === value ? 'active' : ''}" data-value="${
                                  escapeHtml(optionValue)}">
          ${escapeHtml(label)}
        </button>
      `,
                              )
                          .join('');

  for (const button of element.querySelectorAll('button')) {
    button.onclick = () => onChange(button.dataset.value);
  }
}

function renderSelect(elementId, options, value, onChange) {
  const element = document.getElementById(elementId);
  element.innerHTML =
      options
          .map(
              ([optionValue, label]) =>
                  `<option value="${escapeHtml(optionValue)}">${escapeHtml(label)}</option>`,
              )
          .join('');
  element.value = value;
  element.onchange = () => onChange(element.value);
}

function facetOptions(name, allLabel) {
  const values = state.data.facets?.[name] ?? [];
  return [['all', allLabel], ...values.map((value) => [value, value])];
}

function renderTable() {
  const tests = visibleTests();
  document.getElementById('tableTitle').textContent = titleForView(state.view);
  document.getElementById('tableCount').textContent = `${formatNumber(tests.length)} tests`;

  const rows = tests.slice(0, 500).map((test) => {
    const selected = test.id === state.selectedId ? 'selected' : '';
    const status = test.last_status;
    return `
      <tr class="${selected}" data-id="${escapeHtml(test.id)}">
        <td>
          <div class="test-name">
            <strong>${escapeHtml(test.display_name)}</strong>
            <span>${escapeHtml(test.classname)}</span>
          </div>
        </td>
        <td><span class="badge ${escapeHtml(status)}">${escapeHtml(status)}</span></td>
        <td>${formatNumber(test.failures)}</td>
        <td>${escapeHtml(formatShortDateTime(test.last_failed, 'never'))}</td>
        <td>${escapeHtml(formatShortDateTime(test.last_seen, 'unknown'))}</td>
        <td>${escapeHtml(test.suite)} / ${escapeHtml(test.level)}</td>
        <td>${formatPercent(test.failure_rate)}</td>
        <td>${formatNumber(test.total)}</td>
        <td>${formatDuration(test.avg_time)}</td>
      </tr>
    `;
  });

  document.getElementById('testRows').innerHTML = rows.join('');
  for (const row of document.querySelectorAll('#testRows tr')) {
    row.onclick = () => {
      state.selectedId = row.dataset.id;
      renderTable();
      renderDetails();
    };
  }
}

function renderDetails() {
  const pane = document.getElementById('detailsPane');
  const source = state.data.tests.find((item) => item.id === state.selectedId);
  const test = source ? deriveRow(source) : null;
  if (!test) {
    pane.className = 'empty-state';
    pane.textContent = 'Select a test row.';
    return;
  }

  pane.className = '';
  pane.innerHTML = `
    <div class="detail-title">
      <strong>${escapeHtml(test.display_name)}</strong>
      <span class="badge ${escapeHtml(test.last_status)}">${escapeHtml(test.last_status)}</span>
    </div>

    <div class="kv">
      <span>Suite</span><span>${escapeHtml(test.suite)} / ${escapeHtml(test.level)}</span>
      <span>Failures</span><span>${formatNumber(test.failures)} of ${formatNumber(test.total)} (${
      formatPercent(test.failure_rate)})</span>
      <span>Last failed</span><span>${escapeHtml(formatDateTime(test.last_failed, 'never'))}</span>
      <span>Last seen</span><span>${escapeHtml(formatDateTime(test.last_seen))}</span>
      <span>Workflow</span><span>${escapeHtml(test.last_workflow)}</span>
      <span>Run</span><span>${runLink(test.last_run_id)}</span>
      <span>Variant</span><span>${escapeHtml(test.last_variant)}</span>
      <span>Report</span><span>${escapeHtml(test.last_report || 'unknown')}</span>
    </div>

    ${chips('Groups', test.groups)}
    ${chips('Dates', test.active_dates)}
    ${chips('Workflows', test.active_workflows)}
    ${chips('Variants', test.active_variants)}
    ${failureRuns(test.failure_runs)}
    ${history(test.recent)}
    ${failureExamples(test.failure_examples)}
  `;
}

function chips(label, values) {
  if (!values || values.length === 0) {
    return '';
  }
  return `
    <section>
      <h2>${escapeHtml(label)}</h2>
      <div class="chips">
        ${values.map((value) => `<span class="chip">${escapeHtml(value)}</span>`).join('')}
      </div>
    </section>
  `;
}

function history(recent) {
  if (!recent || recent.length === 0) {
    return '';
  }
  return `
    <section>
      <h2>Recent History</h2>
      <div class="history">
        ${
      recent
          .map(
              (item) => `<span class="dot ${escapeHtml(item.status)}" title="${
                  escapeHtml(item.label ?? item.status)} ${
                  escapeHtml(formatDateTime(item.time))}"></span>`,
              )
          .join('')}
      </div>
    </section>
  `;
}

function failureExamples(examples) {
  if (!examples || examples.length === 0) {
    return '';
  }
  return `
    <section>
      <h2>Failure Examples</h2>
      ${
      examples
          .map(
              (example) => `
            <div class="failure-example">
              <div class="failure-meta">
                <strong>${escapeHtml(formatDateTime(example.time))}</strong>
                ${runLink(example.run_id)}
              </div>
              <code>${escapeHtml(example.message)}</code>
            </div>
          `,
              )
          .join('')}
    </section>
  `;
}

function failureRuns(runs) {
  if (!runs || runs.length === 0) {
    return '';
  }
  return `
    <section>
      <h2>Failure Runs</h2>
      <div class="failure-runs">
        ${
      runs.map(
              (run) => `
              <div class="failure-run">
                ${runLink(run.run_id, `Run ${run.run_id}`)}
                <span>${escapeHtml(formatShortDateTime(run.time))} · ${
                  escapeHtml(run.workflow)}</span>
                <span>${escapeHtml(run.variant)} · ${formatNumber(run.failures)} ${
                  run.failures === 1 ? 'failure' : 'failures'}</span>
              </div>
            `,
              )
          .join('')}
      </div>
    </section>
  `;
}

function visibleTests() {
  const query = state.search.trim().toLowerCase();
  return baseFilteredRows()
      .filter((test) => viewPredicate(test, state.view))
      .filter((test) => {
        if (!query) {
          return true;
        }
        return [
          test.display_name,
          test.classname,
          test.name,
          test.suite,
          test.level,
          test.last_workflow,
          test.last_variant,
          ...(test.filters?.workflows ?? []),
          ...(test.filters?.variants ?? []),
        ].join(' ')
            .toLowerCase()
            .includes(query);
      })
      .sort(sortForView(state.view));
}

function baseFilteredRows() {
  return state.data.tests.filter((test) => state.suite === 'all' || test.suite === state.suite)
      .map(deriveRow)
      .filter(Boolean);
}

function deriveRow(test) {
  const segments = (test.segments ?? []).filter(segmentMatches);
  if (segments.length === 0) {
    return null;
  }

  let total = 0;
  let passed = 0;
  let failed = 0;
  let errored = 0;
  let skipped = 0;
  let totalTime = 0;
  let firstSeen = null;
  let lastFailed = null;
  let lastFailedSegment = null;
  let lastSegment = null;

  for (const segment of segments) {
    total += segment.total;
    passed += segment.passed;
    failed += segment.failed;
    errored += segment.errored;
    skipped += segment.skipped;
    totalTime += segment.total_time;

    if (!firstSeen || segment.first_seen < firstSeen) {
      firstSeen = segment.first_seen;
    }
    if (segment.last_failed && (!lastFailed || segment.last_failed >= lastFailed)) {
      lastFailed = segment.last_failed;
      lastFailedSegment = segment;
    }
    if (!lastSegment || segment.last_seen >= lastSegment.last_seen) {
      lastSegment = segment;
    }
  }

  const failures = failed + errored;
  const actionable = passed + failed + errored;
  const failureRate = actionable ? failures / actionable : 0;
  const recent = segments
                     .map((segment) => ({
                            status: segment.last_status,
                            time: segment.last_seen,
                            label: `${segment.last_status} ${segment.workflow} ${segment.variant}`,
                          }))
                     .sort((left, right) => left.time.localeCompare(right.time))
                     .slice(-12);
  const failureRuns = failureRunsFromSegments(segments);

  return {
    ...test,
    total,
    passed,
    failed,
    errored,
    skipped,
    failures,
    failure_rate: failureRate,
    avg_time: total ? totalTime / total : 0,
    first_seen: firstSeen,
    last_seen: lastSegment?.last_seen ?? null,
    last_failed: lastFailed,
    last_failed_run_id: lastFailedSegment?.last_failed_run_id ?? '',
    last_failed_run_attempt: lastFailedSegment?.last_failed_run_attempt ?? '',
    last_failed_report: lastFailedSegment?.last_failed_report ?? '',
    last_status: lastSegment?.last_status ?? 'unknown',
    last_workflow: lastSegment?.workflow ?? '',
    last_run_id: lastSegment?.last_run_id ?? '',
    last_variant: lastSegment?.variant ?? '',
    last_report: lastSegment?.last_report ?? '',
    is_currently_failing: failStatuses.has(lastSegment?.last_status),
    is_flaky: failures > 0 && passed > 0,
    started_failing_in_sample: startedFailingInHistory(recent),
    recent,
    failure_runs: failureRuns,
    active_dates: uniqueSorted(segments.map((segment) => segment.date)),
    active_workflows: uniqueSorted(segments.map((segment) => segment.workflow)),
    active_variants: uniqueSorted(segments.map((segment) => segment.variant)),
  };
}

function failureRunsFromSegments(segments) {
  return segments.filter((segment) => segment.last_failed)
      .map((segment) => ({
             time: segment.last_failed,
             workflow: segment.workflow,
             run_id: segment.last_failed_run_id,
             run_attempt: segment.last_failed_run_attempt,
             variant: segment.variant,
             report: segment.last_failed_report,
             failures: (segment.failed ?? 0) + (segment.errored ?? 0),
           }))
      .sort((left, right) => compareNullableDates(right.time, left.time))
      .slice(0, 20);
}

function segmentMatches(segment) {
  return (
      rangeMatches(segment.date) && (state.date === 'all' || segment.date === state.date) &&
      (state.workflow === 'all' || segment.workflow === state.workflow) &&
      (state.variant === 'all' || segment.variant === state.variant));
}

function rangeMatches(dateValue) {
  if (state.range === 'all' || state.date !== 'all') {
    return true;
  }
  const days = Number.parseInt(state.range, 10);
  if (!Number.isFinite(days)) {
    return true;
  }
  const latestDay = state.data.date_range?.last;
  if (!latestDay) {
    return true;
  }
  const latest = new Date(`${latestDay}T00:00:00Z`);
  const cutoff = new Date(latest);
  cutoff.setUTCDate(cutoff.getUTCDate() - (days - 1));
  const current = new Date(`${dateValue}T00:00:00Z`);
  return current >= cutoff && current <= latest;
}

function summarizeRows(rows) {
  return rows.reduce(
      (totals, row) => {
        totals.unique_tests += 1;
        totals.total += row.total;
        totals.failures += row.failures;
        totals.skipped += row.skipped;
        if (row.is_currently_failing) {
          totals.currently_failing += 1;
        }
        if (row.is_flaky) {
          totals.flaky += 1;
        }
        return totals;
      },
      {
        unique_tests: 0,
        total: 0,
        failures: 0,
        skipped: 0,
        currently_failing: 0,
        flaky: 0,
      },
  );
}

function startedFailingInHistory(recent) {
  if (recent.length < 4) {
    return false;
  }
  const split = Math.max(1, Math.floor(recent.length * 0.7));
  const earlier = recent.slice(0, split);
  const later = recent.slice(split);
  return (
      earlier.every((item) => !failStatuses.has(item.status)) &&
      later.some((item) => failStatuses.has(item.status)));
}

function uniqueSorted(values) {
  return [...new Set(values.filter(Boolean))].sort();
}

function firstVisibleTest() {
  return visibleTests()[0] ?? null;
}

function viewPredicate(test, view) {
  if (view === 'failing') {
    return test.is_currently_failing;
  }
  if (view === 'often') {
    return test.failures > 0;
  }
  if (view === 'recent') {
    return test.failures > 0 && isRecentFailure(test.last_failed);
  }
  if (view === 'flaky') {
    return test.is_flaky;
  }
  if (view === 'started') {
    return test.started_failing_in_sample;
  }
  return true;
}

function sortForView(view) {
  return (left, right) => {
    if (view === 'recent') {
      return (
          compareNullableDates(right.last_failed, left.last_failed) ||
          right.failures - left.failures || right.failure_rate - left.failure_rate ||
          left.display_name.localeCompare(right.display_name));
    }
    if (view === 'all') {
      return (
          statusRank[right.last_status] - statusRank[left.last_status] ||
          right.failures - left.failures || right.total - left.total ||
          left.display_name.localeCompare(right.display_name));
    }
    return (
        right.failures - left.failures || right.failure_rate - left.failure_rate ||
        right.total - left.total || left.display_name.localeCompare(right.display_name));
  };
}

function isRecentFailure(value) {
  if (!value) {
    return false;
  }
  const latestDay = state.data.date_range?.last;
  if (!latestDay) {
    return true;
  }
  const latest = new Date(`${latestDay}T23:59:59Z`);
  const cutoff = new Date(latest);
  cutoff.setUTCDate(cutoff.getUTCDate() - (recentFailureDays - 1));
  return new Date(value) >= cutoff;
}

function compareNullableDates(left, right) {
  if (!left && !right) {
    return 0;
  }
  if (!left) {
    return -1;
  }
  if (!right) {
    return 1;
  }
  return left.localeCompare(right);
}

function runLink(runId, label = runId) {
  if (!runId) {
    return 'unknown';
  }
  const href = `https://github.com/dragonflydb/dragonfly/actions/runs/${encodeURIComponent(runId)}`;
  return `<a href="${href}" target="_blank" rel="noopener noreferrer">${escapeHtml(label)}</a>`;
}

function titleForView(view) {
  const option = viewOptions.find(([value]) => value === view);
  return option ? option[1] : 'Tests';
}

function formatNumber(value) {
  return new Intl.NumberFormat().format(value ?? 0);
}

function formatPercent(value) {
  return `${Math.round((value ?? 0) * 1000) / 10}%`;
}

function formatDuration(seconds) {
  if (!seconds) {
    return '0s';
  }
  if (seconds < 1) {
    return `${Math.round(seconds * 1000)}ms`;
  }
  return `${Math.round(seconds * 10) / 10}s`;
}

function formatDateTime(value, fallback = 'unknown') {
  if (!value) {
    return fallback;
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toISOString().replace('T', ' ').replace(/\.\d+Z$/, 'Z');
}

function formatShortDateTime(value, fallback = 'unknown') {
  if (!value) {
    return fallback;
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toISOString().slice(0, 16).replace('T', ' ');
}

function escapeHtml(value) {
  return String(value ?? '')
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll('\'', '&#039;');
}

boot();
