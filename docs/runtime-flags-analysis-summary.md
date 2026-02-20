# Quick Reference: Runtime Flags Analysis

## TL;DR

**Question:** Do any PRs introduce new runtime flags that break backward compatibility?

**Answer:** YES - PR #6553 introduces 2 flags that break compatibility.

---

## The Flags

PR #6553 introduces:

1. **`--serialize_hnsw_index`** (default: false)
   - Controls HNSW vector index serialization during snapshots/replication

2. **`--deserialize_hnsw_index`** (default: false)
   - Controls HNSW vector index deserialization during load/replication

---

## The Problem

### When They Break Compatibility

✅ **Safe scenarios:**
- All instances with flags=false (default, maintains old behavior)
- All instances v1.37+ with flags=true (full feature enabled)

❌ **Breaking scenarios:**
- Master with serialize=true, Replica with deserialize=false
  → Index data NOT replicated, vector search broken on replica

- Snapshot with serialize=true, restore on version < v1.37
  → Restore FAILS with unknown opcode error

- Rolling upgrade with flags enabled
  → Temporary inconsistency during upgrade

---

## The Fix

PR #6664 partially fixes the issue by:
- Adding version checking (DflyVersion::VER6)
- Preventing HNSW data from being sent to old replicas
- Allowing graceful replication without crashes

**But still requires:**
- Consistent flag settings across all instances
- All instances v1.37+ to enable flags
- Careful planning for upgrades

---

## What Users Should Do

### Default Configuration (Safe)
```bash
# Keep defaults - maintains backward compatibility
--serialize_hnsw_index=false
--deserialize_hnsw_index=false
```

### Enable Feature (v1.37+ only)
```bash
# Set on ALL instances simultaneously
--serialize_hnsw_index=true
--deserialize_hnsw_index=true
```

### Upgrade Path
1. Upgrade all to v1.37+ (keep flags=false)
2. Verify replication works
3. Enable flags on all instances during maintenance window
4. Force full sync to replicas
5. Verify vector search works everywhere

---

## Risk Level

| Scenario | Risk |
|----------|------|
| Keep defaults | ✅ LOW - Safe |
| Enable with v1.37+ cluster | ⚠️ MEDIUM - Test first |
| Enable in mixed versions | ❌ HIGH - Don't do it |
| Enable without understanding | ❌ CRITICAL - Data loss |

---

## Other PRs

All other 192 PRs analyzed do NOT introduce new runtime flags:
- PR #6442 (protected mode) - uses existing flags
- PR #6590 (ASAN support) - build-time only
- PR #6325 (HSCAN NOVALUES) - command argument
- Others - refactorings, fixes, features

---

## References

- Full report: `docs/runtime-flags-analysis-report.md`
- PR #6553: https://github.com/dragonflydb/dragonfly/pull/6553
- PR #6664: https://github.com/dragonflydb/dragonfly/pull/6664
- Flag definitions:
  - `src/server/snapshot.cc:29` (serialize_hnsw_index)
  - `src/server/rdb_load.cc:61` (deserialize_hnsw_index)
