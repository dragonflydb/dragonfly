#!/bin/sh

# Sanitize a string into a token safe for use in S3 key components and GitHub
# artifact names: replace every character outside [A-Za-z0-9._-] with '-',
# collapse repeated '-', and trim leading/trailing '-'. Falls back to $2
# (default "unknown") when the sanitized result is empty.
#
# Usage: sanitize-token.sh <value> [fallback]

set -eu

VALUE=$(printf '%s' "${1:-}" | tr -c 'A-Za-z0-9._-' '-' | sed 's/--*/-/g; s/^-//; s/-$//')
printf '%s' "${VALUE:-${2:-unknown}}"
