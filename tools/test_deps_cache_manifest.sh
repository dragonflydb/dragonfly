#!/usr/bin/env bash

# Tests for tools/deps_cache_manifest.py.
# - Run with: bash tools/test_deps_cache_manifest.sh
# - Set RUN_MANIFEST_BENCHMARK=1 to time manifest generation and validation for a 1,000-file fixture.
# - Default tests:
#   1) Confirm a tar archive can be extracted and still pass manifest validation, with file mtimes unchanged.
#   2) Confirm validation rejects changed, added, deleted, unreadable, or unsupported cache entries and malformed
#      manifests.
# - No build directory is needed. Keep this script and deps_cache_manifest.py in tools/ , the test creates its own
#   temporary workspace.

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
manifest_tool="$repo_root/tools/deps_cache_manifest.py"
workspace=$(mktemp -d)
template="$workspace/template"

cleanup() {
  rm -rf "$workspace"
}
trap cleanup EXIT

now_ns() {
  python3 -c 'import time; print(time.time_ns())'
}

elapsed_ms() {
  echo $((($2 - $1) / 1000000))
}

make_case() {
  local case_dir="$workspace/case"
  rm -rf "$case_dir"
  cp -a "$template" "$case_dir"
  python3 "$manifest_tool" generate --root "$case_dir" --manifest "$case_dir/manifest" cache
  python3 "$manifest_tool" validate --root "$case_dir" --manifest "$case_dir/manifest" cache
  printf '%s\n' "$case_dir"
}

expect_failure() {
  local description="$1"
  local expected_status="$2"
  local command="$3"
  local case_dir
  case_dir=$(make_case)
  bash -c "$command" -- "$case_dir"
  local actual_status=0
  python3 "$manifest_tool" validate --root "$case_dir" --manifest "$case_dir/manifest" cache \
    >/dev/null 2>&1 || actual_status=$?
  case ",$expected_status," in
    *",$actual_status,"*) ;;
    *)
      echo "expected exit $expected_status for $description, got $actual_status" >&2
      exit 1
      ;;
  esac
  echo "PASS: $description"
}

expect_accepted_after_tar_round_trip() {
  local case_dir
  case_dir=$(make_case)
  tar --posix -cf "$workspace/cache.tar" -P -C "$case_dir" cache
  mkdir "$workspace/restored"
  tar -xf "$workspace/cache.tar" -P -C "$workspace/restored"
  cp "$case_dir/manifest" "$workspace/restored/manifest"
  if [ "$(stat -c '%Y.%y' "$case_dir/cache/nested/file-0001")" != \
    "$(stat -c '%Y.%y' "$workspace/restored/cache/nested/file-0001")" ]; then
    echo "tar did not preserve mtime; Ninja will re-run patch steps" >&2
    exit 1
  fi
  python3 "$manifest_tool" validate --root "$workspace/restored" \
    --manifest "$workspace/restored/manifest" cache
  echo "PASS: tar round-trip"
}

expect_accepted_with_missing_optional_path() {
  local case_dir
  case_dir=$(make_case)
  rm -rf "$case_dir/cache"
  python3 "$manifest_tool" generate --root "$case_dir" --manifest "$case_dir/optional-manifest" \
    cache optional-path
  python3 "$manifest_tool" validate --root "$case_dir" --manifest "$case_dir/optional-manifest" \
    cache optional-path
  echo "PASS: missing optional paths"
}

expect_failure_with_requested_paths() {
  local description="$1"
  local expected_status="$2"
  shift 2
  local case_dir
  case_dir=$(make_case)
  local actual_status=0
  python3 "$manifest_tool" validate --root "$case_dir" --manifest "$case_dir/manifest" "$@" \
    >/dev/null 2>&1 || actual_status=$?
  if [ "$actual_status" -ne "$expected_status" ]; then
    echo "expected exit $expected_status for $description, got $actual_status" >&2
    exit 1
  fi
  echo "PASS: $description"
}

expect_rejected_path_escape() {
  local case_dir
  case_dir=$(make_case)
  mkdir "$workspace/outside"
  ln -s "$workspace/outside" "$case_dir/escape"
  local actual_status=0
  python3 "$manifest_tool" generate --root "$case_dir" --manifest "$case_dir/escape-manifest" escape \
    >/dev/null 2>&1 || actual_status=$?
  if [ "$actual_status" -ne 2 ]; then
    echo "expected exit 2 for symlinked requested-path escape, got $actual_status" >&2
    exit 1
  fi
  echo "PASS: symlinked requested-path escape"
}

expect_deduplicated_overlapping_paths() {
  local case_dir
  case_dir=$(make_case)
  python3 "$manifest_tool" generate --root "$case_dir" --manifest "$case_dir/overlapping-manifest" \
    cache cache/nested
  tail -n +2 "$case_dir/manifest" > "$case_dir/single-records"
  tail -n +2 "$case_dir/overlapping-manifest" > "$case_dir/overlapping-records"
  cmp "$case_dir/single-records" "$case_dir/overlapping-records"
  echo "PASS: overlapping requested paths are deduplicated"
}

expect_large_file_chunking() {
  local case_dir
  case_dir=$(make_case)
  python3 -c 'import sys; open(sys.argv[1], "wb").write(b"x" * (3 * 1024 * 1024))' \
    "$case_dir/cache/large-file"
  python3 "$manifest_tool" generate --root "$case_dir" --manifest "$case_dir/manifest" cache
  python3 "$manifest_tool" validate --root "$case_dir" --manifest "$case_dir/manifest" cache
  echo "PASS: multi-chunk regular file"
}

expect_unsupported_filesystem_entry() {
  local case_dir
  case_dir=$(make_case)
  if ! mkfifo "$case_dir/cache/fifo"; then
    echo "SKIP: unsupported filesystem entry (mkfifo unavailable)"
    return
  fi
  local actual_status=0
  python3 "$manifest_tool" validate --root "$case_dir" --manifest "$case_dir/manifest" cache \
    >/dev/null 2>&1 || actual_status=$?
  if [ "$actual_status" -ne 1 ]; then
    echo "expected exit 1 for unsupported filesystem entry, got $actual_status" >&2
    exit 1
  fi
  echo "PASS: unsupported filesystem entry"
}

mkdir -p "$template/cache/nested"
printf 'cache fixture 0001\n' > "$template/cache/nested/file-0001"
printf 'cache fixture 0002\n' > "$template/cache/nested/file-0002"
ln -s nested/file-0001 "$template/cache/link"

if [ "${RUN_MANIFEST_BENCHMARK:-0}" = "1" ]; then
  timing_template="$workspace/timing-template"
  mkdir -p "$timing_template/cache/nested"
  for number in $(seq 1 1000); do
    printf -v file_name 'file-%04d' "$number"
    printf 'cache fixture %04d\n' "$number" > "$timing_template/cache/nested/$file_name"
  done
  ln -s nested/file-0001 "$timing_template/cache/link"

  case_dir="$workspace/timed-case"
  cp -a "$timing_template" "$case_dir"
  started=$(now_ns)
  python3 "$manifest_tool" generate --root "$case_dir" --manifest "$case_dir/manifest" cache
  manifest_generate_ms=$(elapsed_ms "$started" "$(now_ns)")
  started=$(now_ns)
  python3 "$manifest_tool" validate --root "$case_dir" --manifest "$case_dir/manifest" cache
  manifest_validate_ms=$(elapsed_ms "$started" "$(now_ns)")
  echo "Timing for 1,000 regular files, 2 directories, and 1 symlink:"
  echo "  generate: ${manifest_generate_ms} ms"
  echo "  validate: ${manifest_validate_ms} ms"
fi

expect_accepted_after_tar_round_trip
expect_accepted_with_missing_optional_path
expect_failure_with_requested_paths "requested-path invocation drift" 1 cache cache/nested
expect_rejected_path_escape
expect_deduplicated_overlapping_paths
expect_large_file_chunking
expect_failure "same-size regular-file content change" 1 \
  'printf "other fixture 0001\n" > "$1/cache/nested/file-0001"'
expect_failure "regular-file size change" 1 \
  'printf "larger content\n" >> "$1/cache/nested/file-0001"'
expect_failure "regular-file deletion" 1 \
  'rm "$1/cache/nested/file-0001"'
expect_failure "selected cache directory deletion" 1 \
  'rm -rf "$1/cache"'
expect_failure "new regular file" 1 \
  'printf "new\n" > "$1/cache/nested/new-file"'
expect_failure "regular-file permission change" 1 \
  'chmod 600 "$1/cache/nested/file-0001"'
expect_failure "unreadable regular file" 1,3 \
  'chmod 000 "$1/cache/nested/file-0001"'
expect_failure "new directory" 1 \
  'mkdir "$1/cache/new-directory"'
expect_failure "directory permission change" 1 \
  'chmod 700 "$1/cache/nested"'
expect_failure "symlink target change" 1 \
  'rm "$1/cache/link" && ln -s nested/file-0002 "$1/cache/link"'
expect_failure "symlink deletion" 1 \
  'rm "$1/cache/link"'
expect_unsupported_filesystem_entry
expect_failure "truncated manifest" 1 \
  ': > "$1/manifest"'
expect_failure "invalid manifest format" 1 \
  'sed -i "1c\\deps-cache-manifest-v999" "$1/manifest"'
expect_failure "invalid manifest record" 1 \
  'printf "deps-cache-manifest-v1\nnot-json\n" > "$1/manifest"'

echo "All manifest mutation checks passed."
