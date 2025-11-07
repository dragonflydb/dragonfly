#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INTEGRATION_DIR="$SCRIPT_DIR/integration"
VALKEY_SEARCH_REPO="https://github.com/valkey-io/valkey-search.git"
TEMP_DIR=$(mktemp -d)

# Accept optional tag/revision parameter
TAG_OR_REV="${1:-}"

if [ -n "$TAG_OR_REV" ]; then
  echo "Syncing valkey-search tests from tag/revision: $TAG_OR_REV"
else
  echo "Syncing valkey-search tests from latest commit..."
fi

# Remove old integration directory
rm -rf "$INTEGRATION_DIR"

# Clone to temp directory
if [ -n "$TAG_OR_REV" ]; then
  # Clone with full history and checkout specific tag/revision
  git clone "$VALKEY_SEARCH_REPO" "$TEMP_DIR" >/dev/null 2>&1
  pushd "$TEMP_DIR" >/dev/null
  git checkout "$TAG_OR_REV" >/dev/null 2>&1
  popd >/dev/null
else
  # Clone only the latest commit (shallow clone)
  git clone --depth=1 "$VALKEY_SEARCH_REPO" "$TEMP_DIR" >/dev/null 2>&1
fi

# Copy integration directory
cp -r "$TEMP_DIR/integration" "$INTEGRATION_DIR"

# Patch all Python files for Python 3.8 compatibility
# Add 'from __future__ import annotations' to support modern type hints
echo "Patching Python files for Python 3.8 compatibility..."
find "$INTEGRATION_DIR" -name "*.py" -type f | while read -r file; do
  # Check if the file doesn't already have 'from __future__ import annotations'
  if ! grep -q "from __future__ import annotations" "$file"; then
    sed -i '1i from __future__ import annotations' "$file"
  fi
done

# Cleanup
rm -rf "$TEMP_DIR"

echo "Done. Synced $(find "$INTEGRATION_DIR" -name '*test*.py' | wc -l) test files."
