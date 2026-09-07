#!/usr/bin/env bash
set -euo pipefail

show_help() {
  echo "Usage: $0 <logfile> <exec-path>"
  echo "Replaces addresses in <logfile> with source file:line using addr2line and <exec-path>."
  echo "  -h, --help   Show this help message."
  exit 0
}

if [[ $# -lt 2 ]]; then
  show_help
fi

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  show_help
fi

LOGFILE="$1"
EXECPATH="$2"

if ! command -v addr2line &>/dev/null; then
  echo "Error: addr2line is not installed." >&2
  exit 1
fi

if [[ ! -f "$LOGFILE" ]]; then
  echo "Error: Log file '$LOGFILE' not found." >&2
  exit 1
fi

if [[ ! -x "$EXECPATH" && ! -f "$EXECPATH" ]]; then
  echo "Error: Executable '$EXECPATH' not found." >&2
  exit 1
fi

TMPFILE="$(mktemp)"

# Replace addresses in the form 0x123456789abc with file:line
awk -v exe="$EXECPATH" '
function resolve_addr(addr,   cmd, res) {
  cmd = "addr2line -f -C -e \"" exe "\" " addr
  cmd | getline res
  close(cmd)
  return res
}
{
  out = $0
  match($0, /0x[0-9a-fA-F]+/)
  while (RSTART > 0) {
    addr = substr($0, RSTART, RLENGTH)
    resolved = resolve_addr(addr)
    out = substr(out, 1, RSTART-1) resolved substr(out, RSTART+RLENGTH)
    match(out, /0x[0-9a-fA-F]+/)
  }
  print out
}
' "$LOGFILE" > "$TMPFILE"

mv "$TMPFILE" "$LOGFILE"

echo "Success: '$LOGFILE' updated in-place with source line info using '$EXECPATH'."
