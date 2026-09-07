#!/usr/bin/env bash

# Lists Dragonfly and Helio worktrees with the pull request status of their
# checked-out branch. GitHub lookups run concurrently to keep the report quick.

set -euo pipefail

readonly MAX_PARALLEL_JOBS="${MAX_PARALLEL_JOBS:-8}"
readonly -a WORKTREE_ROOTS=(
  "$HOME/workspaces/dragonfly_worktrees"
  "$HOME/workspaces/helio_worktrees"
)

usage() {
  cat <<'EOF'
Usage:
  s_git_worktrees_status

Scans Git worktrees under:
  ~/workspaces/dragonfly_worktrees
  ~/workspaces/helio_worktrees

Environment:
  MAX_PARALLEL_JOBS  Maximum concurrent GitHub CLI requests (default: 8)
EOF
}

require_gh_auth() {
  if gh auth status >/dev/null 2>&1; then
    return
  fi

  cat >&2 <<'EOF'
GitHub CLI authentication is unavailable or has expired.
Authenticate with your normal GitHub CLI account, for example:
  gh auth login

Then retry this command.
EOF
  exit 1
}

wait_for_capacity() {
  while (( $(jobs -pr | wc -l) >= MAX_PARALLEL_JOBS )); do
    wait -n
  done
}

query_pr_status() {
  local worktree="$1"
  local result_file="$2"
  local branch status

  branch=$(git -C "$worktree" symbolic-ref --quiet --short HEAD 2>/dev/null || true)
  if [[ -z "$branch" ]]; then
    printf '%s\t%s\t%s\n' "$worktree" "DETACHED HEAD" "NO PR UPLOADED" >"$result_file"
    return
  fi

  if ! status=$(cd "$worktree" && gh pr list \
    --head "$branch" \
    --state all \
    --limit 100 \
    --json number,state,mergedAt \
    --jq 'if length == 0 then "NO PR UPLOADED" elif any(.[]; .state == "OPEN") then "PR OPEN" elif any(.[]; .mergedAt != null) then ([.[] | select(.mergedAt != null)] | max_by(.mergedAt).number) as $number | "CLOSED (MERGED) #\($number)" else "CLOSED (NOT MERGED)" end' \
    2>/dev/null); then
    status="PR LOOKUP FAILED"
  fi

  printf '%s\t%s\t%s\n' "$worktree" "$branch" "$status" >"$result_file"
}

print_table() {
  local results_dir="$1"
  local -a no_pr_rows=()
  local -a open_rows=()
  local -a closed_rows=()
  local -a merged_rows=()
  local worktree branch status
  local worktree_header="WORKTREE"
  local branch_header="BRANCH"
  local status_header="PR STATUS"
  local worktree_width=${#worktree_header}
  local branch_width=${#branch_header}
  local status_width=${#status_header}
  local worktree_rule branch_rule status_rule

  while IFS=$'\t' read -r worktree branch status; do
    (( ${#worktree} > worktree_width )) && worktree_width=${#worktree}
    (( ${#branch} > branch_width )) && branch_width=${#branch}
    (( ${#status} > status_width )) && status_width=${#status}

    case "$status" in
      "PR OPEN")
        open_rows+=("$worktree"$'\t'"$branch"$'\t'"$status")
        ;;
      "CLOSED (NOT MERGED)")
        closed_rows+=("$worktree"$'\t'"$branch"$'\t'"$status")
        ;;
      "CLOSED (MERGED)"*)
        merged_rows+=("$worktree"$'\t'"$branch"$'\t'"$status")
        ;;
      *)
        no_pr_rows+=("$worktree"$'\t'"$branch"$'\t'"$status")
        ;;
    esac
  done < <(sort "$results_dir"/*)

  printf -v worktree_rule '%*s' "$((worktree_width + 2))" ''
  printf -v branch_rule '%*s' "$((branch_width + 2))" ''
  printf -v status_rule '%*s' "$((status_width + 2))" ''
  worktree_rule=${worktree_rule// /-}
  branch_rule=${branch_rule// /-}
  status_rule=${status_rule// /-}

  print_section "NO PR UPLOADED" "${no_pr_rows[@]}"
  print_section "PR OPEN" "${open_rows[@]}"
  print_section "CLOSED (NOT MERGED)" "${closed_rows[@]}"
  print_section "CLOSED (MERGED)" "${merged_rows[@]}"
}

print_section() {
  local section="$1"
  shift
  local row worktree branch status

  printf '\n%s\n' "$section"
  printf '+%s+%s+%s+\n' "$worktree_rule" "$branch_rule" "$status_rule"
  printf '| %-*s | %-*s | %-*s |\n' "$worktree_width" "$worktree_header" "$branch_width" "$branch_header" "$status_width" "$status_header"
  printf '+%s+%s+%s+\n' "$worktree_rule" "$branch_rule" "$status_rule"
  for row in "$@"; do
    IFS=$'\t' read -r worktree branch status <<<"$row"
    printf '| %-*s | %-*s | %-*s |\n' "$worktree_width" "$worktree" "$branch_width" "$branch" "$status_width" "$status"
  done
  printf '+%s+%s+%s+\n' "$worktree_rule" "$branch_rule" "$status_rule"
}

main() {
  case "${1:-}" in
    -h|--help|help)
      usage
      return
      ;;
    '')
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac

  (( MAX_PARALLEL_JOBS > 0 )) || {
    printf 'MAX_PARALLEL_JOBS must be greater than zero.\n' >&2
    exit 2
  }

  require_gh_auth

  local results_dir
  results_dir=$(mktemp -d)
  trap 'rm -rf "${results_dir:-}"' EXIT

  local worktree result_file
  local worktree_count=0
  while IFS= read -r worktree; do
    result_file="$results_dir/$worktree_count"
    wait_for_capacity
    query_pr_status "$worktree" "$result_file" &
    ((++worktree_count))
  done < <(
    find "${WORKTREE_ROOTS[@]}" -mindepth 1 -maxdepth 1 -type d -print 2>/dev/null |
      while IFS= read -r directory; do
        worktree=$(git -C "$directory" rev-parse --show-toplevel 2>/dev/null || true)
        [[ "$worktree" == "$directory" ]] && printf '%s\n' "$directory"
      done | sort -u
  )

  (( worktree_count > 0 )) || {
    printf 'No Git worktrees found.\n' >&2
    exit 1
  }

  wait
  print_table "$results_dir"
}

main "$@"
