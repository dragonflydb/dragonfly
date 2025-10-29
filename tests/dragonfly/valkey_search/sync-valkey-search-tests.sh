#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INTEGRATION_DIR="$SCRIPT_DIR/integration"
VALKEY_SEARCH_REPO="https://github.com/valkey-io/valkey-search.git"
TEMP_DIR=$(mktemp -d)

echo "Syncing valkey-search tests..."

# Remove old integration directory
rm -rf "$INTEGRATION_DIR"

# Clone to temp directory
git clone --depth=1 "$VALKEY_SEARCH_REPO" "$TEMP_DIR" >/dev/null 2>&1

# Copy integration directory
cp -r "$TEMP_DIR/integration" "$INTEGRATION_DIR"

# Cleanup
rm -rf "$TEMP_DIR"

echo "Done. Synced $(find "$INTEGRATION_DIR" -name '*test*.py' | wc -l) test files."
