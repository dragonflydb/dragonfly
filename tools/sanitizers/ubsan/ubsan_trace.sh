#!/usr/bin/env bash
#
# UBSan artifact helper script: run it from the root of an unzipped
# `ubsan-logs-<arch>` artifact (or pass the folder as the 2nd argument).
#
#   ./ubsan_trace.sh <grep-pattern> [logs-dir]
#
# Examples:
#   ./ubsan_trace.sh 'src/core/dense_set.cc:494'      # a file:line from the summary
#   ./ubsan_trace.sh 'member call on address'         # any substring works
#   ./ubsan_trace.sh 'histogram.cc:318' ~/Downloads/ubsan-logs-x86_64
#
# UBSan logs are laid out one folder per test: <suite>/<case>/ubsan.<pid>.
# This lists every test (suite/case) whose log matched the pattern, then prints
# the first full symbolized stack for it. The pattern is matched literally (-F).
#
set -euo pipefail

pat="${1:?usage: ubsan_trace.sh <grep-pattern> [logs-dir]}"
dir="${2:-.}"

# Blue section headers on a terminal; no color codes when piped/redirected.
if [[ -t 1 ]]; then B=$'\033[1;34m'; R=$'\033[0m'; else B=""; R=""; fi

echo "${B}== tests that hit: ${pat} ==${R}"
mapfile -t hits < <(grep -rlF --include='ubsan.*' -- "${pat}" "${dir}" 2>/dev/null || true)
if [[ "${#hits[@]}" -eq 0 ]]; then
  echo "(no matches under ${dir})"
  exit 0
fi
# <suite>/<case> is the parent folder of each matching ubsan.<pid> file. Strip the
# logs-dir prefix with bash string ops (not a sed regex) so paths with metacharacters work.
prefix="${dir%/}/"
for f in "${hits[@]}"; do
  d="$(dirname "${f}")"
  printf '%s\n' "${d#"$prefix"}"
done | sort -u

echo ""
echo "${B}== first full stack (${hits[0]}) ==${R}"
# Print from the matching "runtime error:" line through its SUMMARY line.
awk -v p="${pat}" 'index($0, p) { f = 1 } f { print } f && /^SUMMARY/ { exit }' "${hits[0]}"
