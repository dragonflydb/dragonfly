#!/usr/bin/env bash

# This file is not standalone - source it from the builder action or cache-reaper workflow;
# Do not set any shell options - assume callers own shell options such as set -euo pipefail.

actions_cache_ccache_validate_suffix() {
  local suffix="$1"

  if [[ -z "${suffix//[[:space:]]/}" || "$suffix" == "default" ]]; then
    echo "::error title=ccache::ccache-save is 'true' but cache-key-suffix is empty or 'default'. Pass a specific cache-key-suffix (e.g. the container image) so writes don't collide across configs."
    return 1
  fi
}

actions_cache_ccache_print_repo_usage() {
  local jq_script

  read -r -d '' jq_script <<'JQ' || true
    def cat:
      if (.key | test("^ccache-dfly-ccache-")) then "ccache"
      elif (.key | test("^dfly-deps-")) then "3rd-party deps"
      else "other" end;
    (map(.sizeInBytes) | add // 0) as $total
    | (group_by(cat) | map({cat: (.[0] | cat), count: length, mb: ((map(.sizeInBytes) | add) / 1048576 | floor)})
       | sort_by(-.mb)) as $groups
    | "Repo cache: \($total/1048576|floor) MB used, \(length) caches",
      ($groups[] | "  \(.cat): \(.count) caches, \(.mb) MB")
# The terminator must be unindented and contain no other characters.
JQ

  gh cache list --limit 10000 --json sizeInBytes,key --jq "$jq_script"
}

actions_cache_ccache_reap() {
  local event_name="$1"
  local pr_ref="$2"
  local before_count
  local deleted
  local id
  local key
  local ref
  local stale
  local stale_jq_script

  echo "== Before cleanup =="
  actions_cache_ccache_print_repo_usage || true

  if [[ "$event_name" == "pull_request" ]]; then
    before_count=$(gh cache list --ref "$pr_ref" --limit 10000 --json id --jq 'length' || echo '?')
    echo "PR closed - deleting all caches for $pr_ref ($before_count found)"
    if ! gh cache delete --all --ref "$pr_ref" --succeed-on-no-caches; then
      echo "::warning::Some caches for $pr_ref may not have been deleted; 7-day/LRU will reclaim leftovers."
    fi
    echo "Deleted $before_count cache(s) for $pr_ref"
  else
    echo "Scheduled cleanup - keeping newest cache per (ref, config)"
    read -r -d '' stale_jq_script <<'JQ' || true
      [ .[] | select(.key | test("^ccache-dfly-ccache-|^dfly-deps-")) ]
      | group_by(
          .ref + "|"
          + (.key | sub("-[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z$|-[0-9a-f]{64}$"; ""))
        )
      | map(sort_by(.createdAt) | .[:-1])
      | flatten[]
      | "\(.id)\t\(.ref)\t\(.key)"
# The terminator must be unindented and contain no other characters.
JQ
    stale=$(gh cache list --limit 10000 --json id,key,ref,createdAt --jq "$stale_jq_script")
    if [[ -z "$stale" ]]; then
      echo "No stale duplicate caches."
    else
      deleted=0
      while IFS=$'\t' read -r id ref key; do
        [[ -n "$id" ]] || continue
        echo "Deleting stale cache: $key (ref=$ref, id=$id)"
        if gh cache delete "$id"; then
          deleted=$((deleted + 1))
        else
          echo "::warning::failed to delete id=$id"
        fi
      done <<< "$stale"
      echo "Deleted $deleted stale duplicate cache(s)"
    fi
  fi

  echo "== After cleanup =="
  actions_cache_ccache_print_repo_usage || true
}
