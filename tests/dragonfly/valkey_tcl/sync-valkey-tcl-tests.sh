#!/bin/bash
# Sync the Valkey TCL test harness + stream test files into ./upstream (gitignored),
# pinned to a specific revision. Mirrors tests/dragonfly/valkey_search/sync-valkey-search-tests.sh.
#
# Usage: ./sync-valkey-tcl-tests.sh [<git-sha-or-tag>]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST="$SCRIPT_DIR/upstream"
REPO="https://github.com/valkey-io/valkey.git"
# Pinned revision the skiplist was validated against. Override via arg 1.
REV="${1:-79fc841577be18d5e789f741ca92a4157e8315fa}"

echo "Syncing Valkey TCL tests from $REPO @ $REV"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# Shallow-fetch just the pinned revision instead of cloning the whole repo.
git init --quiet "$TMP"
git -C "$TMP" remote add origin "$REPO"
git -C "$TMP" fetch --quiet --depth=1 origin "$REV"
git -C "$TMP" checkout --quiet FETCH_HEAD

rm -rf "$DEST"
mkdir -p "$DEST/tests/unit/type"
cp    "$TMP/tests/test_helper.tcl"               "$DEST/tests/"
cp -r "$TMP/tests/support"                        "$DEST/tests/"
cp -r "$TMP/tests/helpers"                        "$DEST/tests/"
cp -r "$TMP/tests/assets"                         "$DEST/tests/"
cp    "$TMP/tests/unit/type/stream.tcl"           "$DEST/tests/unit/type/"
cp    "$TMP/tests/unit/type/stream-cgroups.tcl"   "$DEST/tests/unit/type/"
echo "$REV" > "$DEST/TESTED_REVISION.txt"

echo "Done. Synced harness + $(ls "$DEST"/tests/unit/type | wc -l) stream test file(s) to $DEST"
