#!/usr/bin/bash
# Ref-aware ccache/dependency dedup: within each (ref, config) bucket keep the NEWEST cache
# and delete older duplicate copies. ccache-action persists ccache keys with a "ccache-" prefix.
# Complements the Cache reaper workflow, which deletes a PR's caches on close.

REPO=dragonflydb/dragonfly

# Emit "id<TAB>ref<TAB>key" for every stale ccache or dependency cache entry.
stale=$(gh cache list -R "$REPO" --limit 1000 --json id,key,ref,createdAt \
  --jq '[ .[] | select(.key | test("^ccache-dfly-ccache-|^dfly-deps-")) ]
        | group_by(.ref + "|" + (.key | sub("-[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z$|-[0-9a-f]{64}$";"")))
        | map(sort_by(.createdAt) | .[:-1]) | flatten
        | .[] | "\(.id)\t\(.ref)\t\(.key)"')

if [ -z "$stale" ]; then
  echo "No stale duplicate caches to delete."
  exit 0
fi

count=0
while IFS=$'\t' read -r id ref key; do
  [ -n "$id" ] || continue
  echo "Deleting stale cache: $key  (ref=$ref, id=$id)"
  gh cache delete "$id" -R "$REPO" >/dev/null && count=$((count + 1))
done <<< "$stale"

echo "Done! Deleted $count stale cache(s)."
