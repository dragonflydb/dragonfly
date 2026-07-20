#!/usr/bin/env bash
#
# Summarize UBSan findings into GitHub-flavored markdown on stdout. The workflow
# appends it to the job summary:
#
#   bash ubsan_summarize_findings.sh <ubsan-logs-dir> <arch> >> "$GITHUB_STEP_SUMMARY"
#
# Run it locally too -- see tools/sanitizers/ubsan/README.md ("Summarizing
# findings"). Example after a local sanitized run that wrote logs with
# UBSAN_OPTIONS=...:log_path=build-dbg/ubsan-logs/pytest :
#
#   bash tools/sanitizers/ubsan/ubsan_summarize_findings.sh build-dbg/ubsan-logs local | less
#
# Color in job summaries: GitHub strips inline CSS, so we colorize via
#   - GitHub "alerts" (blockquote callouts): [!CAUTION] = red, [!WARNING] = amber.
#     (Alerts only color the icon/border/heading; body text stays theme-readable.)
#   - ```diff fences: lines beginning with '+' render green.
#
set -euo pipefail

logs_dir="${1:?usage: ubsan_summarize_findings.sh <logs-dir> [arch] [baseline-logs-dir] [baseline-desc]}"
arch="${2:-local}"   # display label only; defaults to "local" for ad-hoc local runs
# Optional baseline: a previous run's ubsan-logs dir. When present and non-empty,
# a "Newly added" section at the top lists only locations here-but-not-in-baseline.
baseline_dir="${3:-}"
baseline_desc="${4:-the last successful scheduled run}"

UB_LINK="https://en.cppreference.com/w/cpp/language/ub"
# Official list of every UBSan check name (unsigned-integer-overflow, implicit-
# conversion, vptr, ...) with a one-line definition of each.
UBSAN_CHECKS_DOC="https://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html#available-checks"

# grep -rH keeps the source filename on every line so each finding can be
# attributed to an example test. Logs live at <suite>/<case>/ubsan.<pid> (one
# folder per test); we recurse and match only the ubsan.* files.
# (Full symbolized stacks stay in the uploaded artifact; here we keep headlines.)
raw="$(grep -rH -i "runtime error:" --include='ubsan.*' "${logs_dir}" 2>/dev/null || true)"

# Classify each finding: UB (real bugs) vs SUSP (defined-but-flagged by the extra
# integer / implicit-conversion checks). The operand TYPE in the message tells
# unsigned wrap/shift/negation (defined) apart from the signed counterpart (UB).
# Output: BUCKET<TAB>KIND<TAB>file:line:col<TAB>example-test
classify() {
  awk '
    # Only real findings carry "runtime error:". Skipping everything else keeps
    # empty / blank input from being miscounted as a bogus "other" finding.
    !/runtime error:/ { next }
    {
      # grep -rH prefixed "<path>/<suite>/<case>/ubsan.<pid>:" -- split it off the
      # finding text at the first colon (log paths contain no colon).
      ci = index($0, ":");
      fname = substr($0, 1, ci - 1);
      rest  = substr($0, ci + 1);
      # The last two path components are the suite (file) and case (name + params)
      # -- that is the example test we attribute this location to.
      nseg = split(fname, seg, "/");
      test = (nseg >= 3) ? seg[nseg-2] "/" seg[nseg-1] : seg[nseg];

      loc=rest; sub(/ runtime error:.*/, "", loc); sub(/^[ \t]+/, "", loc);
      low=tolower(rest); b="UB"; k="other";
      if (low ~ /implicit conversion/)                       { b="SUSP"; k="implicit-conversion" }
      else if (low ~ /unsigned integer overflow/)            { b="SUSP"; k="unsigned-overflow" }
      else if (low ~ /negation of/) {
        if (low ~ /type .(unsigned|uint|size_t|size_type|value_type)/) { b="SUSP"; k="unsigned-negation" }
        else { b="UB"; k="signed-negation" } }
      else if (low ~ /left shift of/) {
        if (low ~ /type .(unsigned|uint)/) { b="SUSP"; k="unsigned-shift-base" }
        else { b="UB"; k="signed-shift-base" } }
      else if (low ~ /shift exponent/)                       { b="UB"; k="shift-exponent" }
      else if (low ~ /misaligned address/)                   { b="UB"; k="misaligned-load" }
      else if (low ~ /member call on address|does not point to an object/) { b="UB"; k="vptr" }
      else if (low ~ /out of bounds/)                        { b="UB"; k="out-of-bounds" }
      else if (low ~ /null pointer/)                         { b="UB"; k="null-argument" }
      else if (low ~ /incorrect function type/)              { b="UB"; k="function-type" }
      else if (low ~ /signed integer overflow/)              { b="UB"; k="signed-overflow" }
      else if (low ~ /division by zero/)                     { b="UB"; k="divide-by-zero" }
      print b "\t" k "\t" loc "\t" test;
    }'
}

# Sorted-unique file:line:col of every finding under a logs dir (for baseline diff).
extract_locs() {
  grep -rh -i "runtime error:" --include='ubsan.*' "$1" 2>/dev/null \
    | sed -E 's/ runtime error:.*//; s/^[[:space:]]+//' | sort -u
}

tagged="$(printf '%s\n' "${raw}" | classify)"

# Baseline (a previous run's ubsan-logs). When present + non-empty, findings are
# split into New (not in baseline) vs Existing, and the new ones are written to
# new-findings.txt for the optional fail_on_new gate.
base_locs=""
has_baseline=0
if [[ -n "${baseline_dir}" && -d "${baseline_dir}" ]]; then
  base_locs="$(extract_locs "${baseline_dir}")"
  [[ -n "${base_locs}" ]] && has_baseline=1
fi

new_findings_file="${logs_dir}/new-findings.txt"
rm -f "${new_findings_file}" 2>/dev/null || true
if [[ "${has_baseline}" -eq 1 ]]; then
  # Unique-by-location rows (BUCKET<TAB>KIND<TAB>loc<TAB>example-test) new vs baseline.
  printf '%s\n' "${tagged}" \
    | awk -F'\t' 'NR==FNR { seen[$0]=1; next }
                  NF && !($3 in seen) && !($3 in done) { done[$3]=1; print }' \
        <(printf '%s\n' "${base_locs}") - \
    > "${new_findings_file}" 2>/dev/null || true
fi

# Occurrence totals (for the footer). Location counts are computed per section.
count_bucket() { printf '%s\n' "${tagged}" | awk -F'\t' -v b="$1" '$1==b' | grep -c . || true; }
count_locs()   { printf '%s\n' "$1" | awk -F'\t' 'NF{print $3}' | sort -u | grep -c . || true; }
count_rows()   { printf '%s\n' "$1" | grep -c . || true; }  # occurrences (one per finding)
bucket_rows()  { printf '%s\n' "${tagged}" | awk -F'\t' -v b="$1" '$1==b'; }
split_new()      { printf '%s\n' "$1" | awk -F'\t' 'NR==FNR{seen[$0]=1;next} NF && !($3 in seen)' <(printf '%s\n' "${base_locs}") -; }
split_existing() { printf '%s\n' "$1" | awk -F'\t' 'NR==FNR{seen[$0]=1;next} NF &&  ($3 in seen)' <(printf '%s\n' "${base_locs}") -; }
emit_types_of()  { printf '%s\n' "$1" | awk -F'\t' 'NF{print $2}' | sort | uniq -c | sort -rn | awk 'NF{printf "+ %s\n", $0}'; }
emit_locs_of()   { printf '%s\n' "$1" \
                     | awk -F'\t' 'NF { c[$3]++; kind[$3]=$2; if (!($3 in ex)) ex[$3]=$4 }
                                   END { for (l in c) printf "%d\t%s\t%s\t%s\n", c[l], kind[l], l, ex[l] }' \
                     | sort -rn | head -300 \
                     | awk -F'\t' '{ printf "%7d  %-18s %s  (e.g. %s)\n", $1, $2, $3, $4 }'; }

ub_total="$(count_bucket UB)"
susp_total="$(count_bucket SUSP)"
total=$(( ub_total + susp_total ))

# --- One findings block: check-type breakdown + expandable locations --------
emit_findings_block() {
  local rows="$1" label="$2"
  echo "By check type:"
  echo '```diff'
  emit_types_of "${rows}"
  echo '```'
  echo ""
  echo "<details><summary>${label} Locations (count &middot; type &middot; file:line:column &middot; example test) - Press the arrow to expand</summary>"
  echo ""
  echo '```'
  emit_locs_of "${rows}"
  echo '```'
  echo ""
  echo "</details>"
  echo ""
}

# --- One bucket (UB or SUSP), FULL list (no New/Existing split here -- the diff
# is shown separately, above, by emit_diff).
emit_section() {
  local bucket="$1" label
  [[ "${bucket}" == "UB" ]] && label="Undefined Behaviors" || label="Suspicious Behaviors"
  local rows; rows="$(bucket_rows "${bucket}")"
  local total_locs; total_locs="$(count_locs "${rows}")"
  local total_occ; total_occ="$(count_rows "${rows}")"
  if [[ "${bucket}" == "UB" ]]; then
    echo "## Undefined behaviors - ${total_locs} location(s), ${total_occ} occurrence(s) · ${arch}"
    echo ""
    echo "> [!CAUTION]"
    echo "> These are **real C++ undefined behavior**: the program violates the C++"
    echo "> standard, so the standard imposes **no requirements** on the result - the"
    echo "> compiler may miscompile, crash, or silently corrupt data. These should be fixed."
  else
    echo "## Suspicious / defined-but-flagged - ${total_locs} location(s), ${total_occ} occurrence(s) · ${arch}"
    echo ""
    echo "> [!WARNING]"
    echo "> Well-defined behavior surfaced by the extra integer & implicit-conversion"
    echo "> checks (unsigned wrap/shift/negation, narrowing conversions). Not C++"
    echo "> standard violations, but worth a look for unintended truncation / sign bugs."
  fi
  echo ""
  if [[ "${total_locs}" -eq 0 ]]; then
    echo "_none_"
    echo ""
    return
  fi
  emit_findings_block "${rows}" "${label}"
}

# --- Diff area: the NEWLY ADDED findings (both buckets) shown BEFORE the full
# report. Only when a baseline is available. Same findings also appear in full.
emit_diff() {
  [[ "${has_baseline}" -eq 1 ]] || return 0
  local ub_new susp_new ubn suspn
  ub_new="$(split_new "$(bucket_rows UB)")"
  susp_new="$(split_new "$(bucket_rows SUSP)")"
  ubn="$(count_locs "${ub_new}")"
  suspn="$(count_locs "${susp_new}")"
  echo "> [!CAUTION]"
  echo "> **Newly added since ${baseline_desc}:** **${ubn}** undefined behavior +"
  echo "> **${suspn}** suspicious location(s). Each also appears in the full report further down."
  echo ""
  echo "## New undefined behaviors - ${ubn} location(s) · ${arch}"
  echo ""
  if [[ "${ubn}" -eq 0 ]]; then echo "_none_"; echo ""; else emit_findings_block "${ub_new}" "New Undefined Behaviors"; fi
  echo "## New suspicious / defined-but-flagged - ${suspn} location(s) · ${arch}"
  echo ""
  if [[ "${suspn}" -eq 0 ]]; then echo "_none_"; echo ""; else emit_findings_block "${susp_new}" "New Suspicious Behaviors"; fi
  echo "---"
  echo ""
  echo "# Full report · ${arch}"
  echo ""
}

# Single blue INFO banner at the very top, combining (1) the run configuration,
# (2) the occurrence totals, and (3) how to read the report + reach the artifact.
# CFG_SUMMARY / CFG_REPRO are passed in by the workflow (absent for local runs).
emit_notes() {
  local i=0
  echo "> [!NOTE]"
  echo "> "
  if [[ -n "${CFG_SUMMARY:-}" ]]; then
    i=$((i + 1))
    echo "> **${i}. Run configuration:** ${CFG_SUMMARY}"
    if [[ -n "${CFG_REPRO:-}" ]]; then
      echo "> Re-run with these settings: **Actions -> Run workflow** (pick the same"
      echo "> values), or with the GitHub CLI: \`${CFG_REPRO}\`"
    fi
    echo "> "
  fi
  i=$((i + 1))
  echo "> **${i}. Totals (${arch}):** **${total}** finding occurrence(s) -"
  echo "> **${ub_total}** undefined behavior, **${susp_total}** suspicious /"
  echo "> defined-but-flagged. Locations are deduplicated by file:line:column."
  echo "> Full symbolized stack traces: download the \`ubsan-logs-${arch}\` artifact."
  echo "> "
  i=$((i + 1))
  echo "> **${i}. How to read:** each row below is one UBSan diagnostic"
  echo "> (\`file:line:column\`), deduplicated and counted. **These findings do NOT"
  echo "> fail the job** - UBSan here is *recoverable*: it prints the diagnostic and"
  echo "> lets the program keep running. The summary tells you **what / where**; the"
  echo "> uploaded \`ubsan-logs-${arch}\` artifact tells you **who / why** - the exact"
  echo "> test and the full call stack. Each location lists **one example test**"
  echo "> (\`suite/case\`). References: [what is C++ undefined behavior](${UB_LINK}) ·"
  echo "> [what each UBSan check means](${UBSAN_CHECKS_DOC})."
  echo ""
  echo "Triage a \`file:line:column\` from the unzipped \`ubsan-logs-${arch}\` artifact root - list the tests that hit it, open the full stack, or let the helper do both:"
  echo ""
  echo '```bash'
  echo "# which tests hit this location (each match is <suite>/<case>/ubsan.<pid>):"
  echo "> grep -rl 'src/core/dense_set.cc:494:17' ."
  echo "# print ONLY that finding's stack (its error line through its SUMMARY line):"
  echo "> sed -n '\\%src/core/dense_set.cc:494:17%,/^SUMMARY/p' <suite>/<case>/ubsan.*"
  echo "# ...or let the bundled helper do both (it takes any grep pattern):"
  echo "> bash ubsan_trace.sh 'src/core/dense_set.cc:494:17'"
  echo '```'
  echo ""
}

# --- Suppression help: how to silence a confirmed false positive (once) -----
emit_suppress_help() {
  echo "> [!TIP]"
  echo "> **Suppressing a confirmed false positive (suspicious list):** These extra"
  echo "> checks (implicit-conversion, unsigned wrap/shift/negation) are well-defined"
  echo "> and often intentional. After you REVIEW a finding and confirm it is benign,"
  echo "> exclude just that check for its file or function by adding a per-check section"
  echo "> to \`tools/sanitizers/ubsan/ubsan-ignorelist.txt\`, then rebuild. The section"
  echo "> header scopes it to ONE check, so real UB elsewhere in that file is still"
  echo "> caught. Granularity is file/function, not line - document WHY it is safe, and"
  echo "> prefer fixing the code when practical."
  echo ""
  echo '```'
  echo "# tools/sanitizers/ubsan/ubsan-ignorelist.txt"
  echo "[implicit-conversion]                 # only this check is suppressed"
  echo "fun:*YourFunctionName*                # or  src:src/path/to/file.cc"
  echo '```'
  echo ""
}

emit_notes
emit_suppress_help
emit_diff
emit_section UB
emit_section SUSP
echo ""
