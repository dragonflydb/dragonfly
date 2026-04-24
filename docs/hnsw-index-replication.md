# HNSW Vector Index Replication

## 1. Overview

An HNSW (Hierarchical Navigable Small World) vector index in Dragonfly is global: a
single graph per `(index, field)` pair is shared across all shards, while the documents
it indexes are distributed per-shard. The full-sync RDB stream is by construction
per-shard. Replication must therefore carry two orthogonal pieces of state — the graph
and each shard's key space — and reassemble them on the replica, including the case where
master and replica shard counts differ.

This document specifies the wire format, the master/replica protocol, the index state
machine, and the invariants that make the scheme safe under concurrent writes.

### 1.1 Terminology

- **AUX field** — an RDB auxiliary record (key/value pair) interleaved with the data
  stream.
- **Summary flow** — the RDB sub-stream that carries global state (scripts, index
  definitions). A single summary flow exists per save.
- **Shard flow** — the per-shard RDB sub-stream that carries that shard's key/value
  data.
- **MRMW mutex** — a multi-reader / multi-writer mutex used by the global graph to
  serialize structural mutations against the serializer.
- **sds** — simple dynamic string, the backing storage of a hash field value.

## 2. Data Model

### 2.1 Identifiers

`DocId` is a 32-bit shard-local document identifier. `GlobalDocId` is a 64-bit value
containing the shard id in its upper 32 bits and the `DocId` in its lower 32 bits, and is
the only identifier used inside the HNSW graph. Because `GlobalDocId` encodes the
master's shard id, it must be rewritten when shard counts differ (§6).

### 2.2 Components

- **Global HNSW index.** One graph per `(index_name, field_name)`, addressed by
  `GlobalDocId`.
- **Per-shard key index.** Each shard maps its keys to shard-local `DocId`s. DocIds
  are issued independently per shard; together with the shard id they form the
  `GlobalDocId` stored in the graph.
- **Per-shard HNSW adapter.** Shard-local state associated with the global graph,
  including the preservation list required by the borrowed-vector invariant (§4.4).

### 2.3 Serialized structure

**`HnswNodeData`** — `internal_id`, `global_id`, `level`, and for each level `l` in
`0..level` the neighbour-link list (so `level + 1` link arrays). Vector payloads are
not part of this record; they are restored from the normal key stream.

**`HnswIndexMetadata`** accompanies each graph with the identifying
`index_name`/`field_name` pair and the graph-level parameters the replica needs for
restore.

## 3. Wire Format

### 3.1 AUX fields

| Key | Payload | Scope |
|-----|---------|-------|
| `search-index` | `"<index_name> <FT.CREATE arguments…>"` | Summary flow and `SINGLE_SHARD_WITH_SUMMARY` flows unconditionally; per-shard flows only if the replica advertises capability `VER6` or later. Omitted entirely from RDB-to-disk saves. |
| `hnsw-index-metadata` | `HnswIndexMetadata` record, one per HNSW-indexed index | Summary flow, replicas at `VER6` or later. |
| `search-synonyms` | `"<index_name> <group_id> <terms…>"` | Summary flow. |
| `shard-count` | integer | Summary and `SINGLE_SHARD_WITH_SUMMARY` flows. Load-bearing: controls the remap branch (§6). |

### 3.2 Opcodes

`RDB_OPCODE_VECTOR_INDEX` (value `222`). One block per global HNSW index, emitted by
shard 0 only.

```
opcode                   u8   = 222
index_key                string    "<index_name>:<field_name>"
elements_number          len
repeated elements_number times:
  internal_id            u32         (little-endian raw)
  global_id              u64
  level                  u32
  for l in 0..=level:
    links_num            u32
    links                u32 × links_num
```

`index_key` is split on the final `:`. Integer fields inside the node record are packed
little-endian rather than RDB varints.

`RDB_OPCODE_SHARD_DOC_INDEX` (value `223`). One block per HNSW-indexed index, emitted by
every shard.

```
opcode        u8  = 223
shard_id      len
index_name    string
mapping_count len
repeated mapping_count times:
  key         string
  doc_id      len
```

## 4. Index States and Invariants

### 4.1 States

An index carries exactly one of four states per shard.

| State | Initial condition | HNSW mutation behaviour |
|-------|-------------------|-------------------------|
| `kProhibit` | Default at index construction. | Buffered. |
| `kRestoring` | Entered after graph restore, before vector hydration. | Buffered. |
| `kSerializing` | Entered on the master before a graph dump. | Buffered. |
| `kBuilding` | Steady state. | Applied inline to the graph. |

Transitions are strictly `{kProhibit, kRestoring, kSerializing} → kBuilding`, performed
by a drain that replays the buffered operations. A serialization request transitions
only an index already in `kBuilding`; indices in `kProhibit` or `kRestoring` keep their
state, so a serialization pass cannot disturb an index that is still restoring. A
serialization drain acts only on indices in `kSerializing`, so a master drain cannot
terminate a concurrent replica restore.

### 4.2 Single-writer invariant

The global graph is mutated only when the owning shard is in `kBuilding`. All other
states divert `Add` and `Remove` calls to a per-shard pending-updates list, which is
replayed on drain.

### 4.3 Consistent-snapshot invariant

While a graph dump is in progress, shard-level writes are accepted at the key layer;
their index side-effects are buffered. The MRMW read lock held on the graph prevents any
buffered mutation from committing until the dump completes.

### 4.4 Borrowed-vector invariant

When the graph stores pointers into hash field sds (i.e. vectors are not copied into the
graph), any mutation that would free or overwrite such an sds while the index is not in
`kBuilding` must retain the original sds until the drain. Retention is per-shard,
per-field. The containing `PrimeValue` additionally suppresses defragmentation for the
lifetime of the node to prevent relocation of borrowed storage.

### 4.5 Identifier uniqueness

`GlobalDocId`s are unique across shards by construction. On fresh mapping load and on
remap (§6), per-shard DocId counters issue DocIds in the same order in which keys are
later installed, so counter values coincide with the DocIds materialized by the replica's
key index.

### 4.6 Restore ordering

Graph structure is restored before vector payloads are hydrated. A node whose document
has disappeared by the hydration step is removed; the graph never retains a live node
without a vector payload.

## 5. Protocol

### 5.1 Master

Per shard, in order of appearance in the RDB stream:

1. **AUX fields.** `search-index` is written for every index. On the summary flow,
   `hnsw-index-metadata` is written once per HNSW-indexed index, `search-synonyms` once
   per synonym group, and `shard-count` once.
2. **Mapping dump.** For every HNSW-indexed index, the shard emits one
   `RDB_OPCODE_SHARD_DOC_INDEX` block containing a snapshot of its current key→DocId
   table.
3. **Graph dump (shard 0 only).** Every shard first transitions each of its
   `kBuilding` indices to `kSerializing`; indices in other states retain their state.
   Shard 0 then acquires the read half of each global index's MRMW mutex and emits one
   `RDB_OPCODE_VECTOR_INDEX` block per index. The lock is released, and the serializer
   output is flushed, at the end of each index.
4. **Drain.** Each shard replays its pending updates for indices currently in
   `kSerializing` and returns those indices to `kBuilding`.
5. **Key stream.** Bucket iteration proceeds as for any other data.

### 5.2 Replica

Inline processing:

- `search-index` dispatches `FT.CREATE` with idempotent semantics (existing definitions
  are left in place).
- `shard-count` is recorded and used to select the restore branch.
- Every `RDB_OPCODE_SHARD_DOC_INDEX` block is parked as a pending mapping keyed by the
  master's shard id.
- `RDB_OPCODE_VECTOR_INDEX` is either restored in place (same shard count) or parked
  as pending nodes (different shard count).

Post-load reconciliation, run once after the stream has been fully consumed:

1. **Apply mappings.** If shard counts match, each parked mapping is restored on the
   replica shard whose id equals the master shard id in the block. If they differ, the
   remap of §6 runs first and each replica shard receives its pre-distributed slice.
2. **Rebuild.** Each shard rebuilds its local index in `kRestoring`.
3. **Synonyms.** Parked synonym commands are replayed; the replica waits for index
   construction to complete on all shards.
4. **Hydrate.** Each shard iterates its key index, loads every live document, and
   populates vector data for the corresponding `GlobalDocId`. The key→DocId mapping is
   revalidated against the snapshot before any removal, because concurrent fibers may
   reuse freed DocIds. Nodes whose document is missing are removed from the graph. Keys
   whose hydration cannot complete immediately are queued for the final drain.
5. **Drain.** Each shard replays its pending updates and transitions its indices to
   `kBuilding`.

## 6. Shard Count Remap

When the `shard-count` AUX value differs from the replica's shard count, `GlobalDocId`s
carried in graph blocks refer to the master's shard layout and must be rewritten before
the graph can be installed.

1. **Build remap table.** For every `(index, master_shard, key, old_doc_id)` received in
   the mapping stream, compute the replica target shard for the key and issue a fresh
   DocId from a per-target counter. The counter order must match the order in which the
   replica later installs keys, so that counter values coincide with materialized DocIds.
2. **Remap and restore.** Rewrite every `global_id` in parked node records through the
   table, then restore the graph. Any index that cannot be fully remapped is added to a
   failed set.
3. **Pre-distribute mappings.** Group remapped keys by target shard into ordered key
   lists whose positions match the DocIds issued in step 1.
4. **Fallback.** Indices in the failed set, and indices for which no mappings arrived,
   are discarded and rebuilt from scratch from the key stream.

| Master shards | Replica shards | Mappings present | Action |
|---------------|----------------|------------------|--------|
| N | N | yes | Direct restore, no remap. |
| N | M (≠ N) | yes | Remap, restore, pre-distributed mapping apply. |
| N | any | no | Discard graph, rebuild from key stream. |
| any | any | remap fails | Discard graph, rebuild from key stream. |

## 7. Version Compatibility

The wire format is gated on the replica capability `VER6`, advertised through
`REPLCONF capa dragonfly`. Replicas below `VER6` receive only the `FT.CREATE` definition
through the summary flow and reconstruct each index from the key stream alone. RDB saves
to disk omit all search-index data: search indices are a replication-only concern.

## 8. Open Issues

1. Multiple HNSW vector fields per index are not supported.
