#!/usr/bin/env bash

# Creates a timestamped archive of the requested local configuration and notes.

set -euo pipefail

readonly BACKUP_NAME="backup_$(date +%Y-%m-%d_%H-%M-%S).tar.gz"
readonly ARCHIVE_PATH="$PWD/$BACKUP_NAME"
readonly -a BACKUP_SOURCES=(
  ".bashrc"
  "scripts"
  "notes"
  ".copilot/agents"
  ".config/mcp/mcp.json"
)

usage() {
  cat <<'EOF'
Usage:
  s_create_backup_folder

Creates a timestamped tar.gz archive in the current directory containing:
  ~/.bashrc
  ~/scripts/
  ~/notes/
  ~/.copilot/agents/
  ~/.config/mcp/mcp.json
EOF
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

  local source
  for source in "${BACKUP_SOURCES[@]}"; do
    [[ -e "$HOME/$source" ]] || {
      printf 'Required backup source is missing: %s\n' "$HOME/$source" >&2
      exit 1
    }
  done

  [[ ! -e "$ARCHIVE_PATH" ]] || {
    printf 'Backup archive already exists: %s\n' "$ARCHIVE_PATH" >&2
    exit 1
  }

  local temporary_archive
  temporary_archive=$(mktemp "${TMPDIR:-/tmp}/$BACKUP_NAME.XXXXXX")
  trap 'rm -f "${temporary_archive:-}"' EXIT

  tar -C "$HOME" -czf "$temporary_archive" "${BACKUP_SOURCES[@]}"

  mv "$temporary_archive" "$ARCHIVE_PATH"
  printf 'Created backup archive: %s\n' "$ARCHIVE_PATH"
}

main "$@"
