# Bug Report: SIGABRT in Global HNSW KNN Search Under Concurrent Deletes

**PR:** [#6936 – fix(search): guard LoadEntry against freed DocIds in global HNSW KNN](https://github.com/dragonflydb/dragonfly/pull/6936)
**Severity:** Critical (SIGABRT / process crash in production)
**Component:** Full-text / vector search — `src/server/search/doc_index.cc`

---

## 1. Summary

When a global HNSW KNN search runs concurrently with document deletions, the search can try
to resolve a `DocId` that was already freed (and possibly reused) between the two phases of
the search pipeline.  Because `DocKeyIndex::Get()` contains a debug assertion that the
requested id is not in the free pool, the process terminates with SIGABRT.

---

## 2. Background: Key Data Structures

### 2.1 DocKeyIndex – the local id ↔ key mapping

Every `ShardDocIndex` owns a `DocKeyIndex` that maps between compact integer IDs (`DocId`)
and Redis key strings.  It maintains three fields:

| Field | Type | Role |
|-------|------|------|
| `keys_` | `vector<string>` | slot `id` holds the key for that `DocId` |
| `ids_` | `flat_hash_map<string, DocId>` | reverse map key → id |
| `free_ids_` | `vector<DocId>` | pool of recycled (freed) slots |

```cpp
// doc_index.cc – DocKeyIndex::Add
DocId ShardDocIndex::DocKeyIndex::Add(string_view key) {
  DocId id;
  if (!free_ids_.empty()) {
    id = free_ids_.back();   // ← reuse freed slot
    free_ids_.pop_back();
    keys_[id] = key;
  } else {
    id = last_id_++;
    keys_.emplace_back(key);
  }
  ids_[key] = id;
  return id;
}

// doc_index.cc – DocKeyIndex::Remove
void ShardDocIndex::DocKeyIndex::Remove(DocId id) {
  ids_.extract(keys_[id]);  // remove from reverse map
  keys_[id] = "";            // clear the slot
  free_ids_.push_back(id);   // ← mark as reusable
}

// doc_index.cc – DocKeyIndex::Get  (contains the crashing DCHECK)
string_view ShardDocIndex::DocKeyIndex::Get(DocId id) const {
  DCHECK_LT(id, keys_.size());
  DCHECK(id < last_id_ &&                                               // must be allocated
         std::find(free_ids_.begin(), free_ids_.end(), id) == free_ids_.end()); // must NOT be freed
  return keys_[id];
}
```

The DCHECK on the last line fires—and terminates the process—when code calls `Get()` with an
id that is already sitting in `free_ids_`.

---

### 2.2 Global HNSW Index – a shared, cross-shard graph

HNSW (Hierarchical Navigable Small World) vector search in Dragonfly uses a **single global
index** shared across all engine shards, referenced via `GlobalHnswIndexRegistry`.  Documents
from every shard are added with a **`GlobalDocId`** that encodes both the shard number and the
local `DocId`:

```
GlobalDocId = (shard_id << 32) | local_doc_id
```

The global HNSW graph is protected by a read/write lock.  Queries hold the **read lock**;
deletions need the **write lock**.

---

## 3. The Two-Phase Search Pipeline

A `FT.SEARCH … [KNN K @vec $vec]` query that hits the global HNSW index goes through **two
distinct transactions** (two "hops"):

```
╔══════════════════════════════════════════════════════════════╗
║  PHASE 1  –  SearchGlobalHnswIndex()                        ║
║  (runs outside any shard transaction)                        ║
║                                                              ║
║  1a. Acquire HNSW **read lock**                              ║
║  1b. Call index->Knn(...)                                    ║
║      → returns vector<(score, GlobalDocId)>                  ║
║  1c. Release HNSW read lock                                  ║
║  1d. Decompose each GlobalDocId → (shard_id, local_doc_id)   ║
╚══════════════════════════════════════════════════════════════╝
           ↕  NO LOCKS HELD  ←── race window starts here
╔══════════════════════════════════════════════════════════════╗
║  PHASE 2  –  ScheduleSingleHop (per shard)                  ║
║  (new transaction on each shard's engine thread)             ║
║                                                              ║
║  2a. Acquire shard lock                                      ║
║  2b. For each local_doc_id from Phase 1:                     ║
║      index->SerializeDocWithKey(local_doc_id, …)             ║
║        → LoadEntry(local_doc_id, …)                          ║
║          → key_index_.Get(local_doc_id)   ← DCHECK HERE     ║
╚══════════════════════════════════════════════════════════════╝
```

In code:

```cpp
// search_family.cc:1106 – Phase 1
knn_results = index->Knn(knn->vec.first.get(), knn->limit, knn->ef_runtime);
// ↑ returns GlobalDocIds. After this line NO locks are held.

// search_family.cc:1121-1126 – decompose ids
for (const auto& [score, global_doc_id] : knn_results) {
    auto [shard_id, local_doc_id] = search::DecomposeGlobalDocId(global_doc_id);
    shard_docs[shard_id].push_back({local_doc_id, score});
}

// search_family.cc:1168 – Phase 2 (new hop per shard)
cmd_cntx.tx()->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    for (auto& shard_doc : shard_docs[es->shard_id()]) {
        index->SerializeDocWithKey(shard_doc.id, …);  // calls Get()
    }
});
```

---

## 4. The Race Condition

### 4.1 The crashing scenario (simple case: just deletion)

The following interleaving of events produces the SIGABRT:

```
Thread A: FT.SEARCH KNN                Thread B: DEL doc:42
─────────────────────────────────────────────────────────────
Phase 1: Knn() returns doc_id=7
         (HNSW read lock released)
                                        RemoveDoc(7)
                                          key_index_.Remove(7)
                                            free_ids_.push_back(7)  ← doc_id 7 is freed
Phase 2: ScheduleSingleHop
         LoadEntry(7)
           key_index_.Get(7)
             DCHECK fires: 7 is in free_ids_
             ↳ SIGABRT
```

### 4.2 The deferred HNSW removal

The PR description explains why the DEL can proceed while the HNSW read lock is still held.
The sequence inside `RemoveDoc` (called by the shard thread) is:

1. **`RemoveDocFromGlobalVectorIndex(id, …)`** – tries to acquire the HNSW write lock.  
   If the KNN read lock is currently held by Phase 1, the write-lock attempt fails and the
   HNSW removal is **deferred**.
2. **`key_index_.Remove(id)`** – this happens unconditionally, immediately, on the same
   shard thread.  The `DocId` is added to `free_ids_` right now.

So the key-index is updated (id freed) *before* the HNSW graph entry is removed.  Phase 2
then arrives with a freed id.

```cpp
// doc_index.cc:474-478
void ShardDocIndex::RemoveDoc(DocId id, const DbContext& db_cntx, const PrimeValue& pv) {
  auto accessor = GetAccessor(db_cntx, pv);
  key_index_.Remove(id);         // ← frees id NOW, unconditionally
  indices_->Remove(id, *accessor);
}
```

### 4.3 The broader "two-hop" problem: replacement, not just deletion

A reviewer noted that the race window is actually **wider** than just crashes:

> "The two hop problem is broader in a sense – the entry can not only be deleted in that time,
> it can be replaced – so it will return wrong data."

If between Phase 1 and Phase 2, the original document is deleted **and a new document reuses
the same `DocId` slot** (via `free_ids_`), then:

- `key_index_.Get(id)` does not crash (the slot is occupied again).
- But it returns the **key of the new document**, not the original one.
- The KNN result silently contains wrong data.

```
Thread A: FT.SEARCH KNN             Thread B: DEL doc:X      Thread C: HSET doc:Y (new)
──────────────────────────────────────────────────────────────────────────────────────────
Phase 1: Knn() → doc_id=7
         (key="user:123")
                                     Remove(7)
                                     free_ids_ = [7]
                                                               Add("user:456")
                                                               reuses id=7
                                                               keys_[7] = "user:456"
Phase 2: Get(7) → "user:456"
         Loads and returns user:456
         ← WRONG RESULT (no crash, silent data corruption)
```

This makes the overall problem a **two-tier bug**:

| Scenario | Symptom |
|----------|---------|
| `id` freed, not yet reused | SIGABRT (DCHECK in `Get`) |
| `id` freed then reused for different key | Silent wrong result |

---

## 5. Affected Code Paths

Two separate call sites in `ShardDocIndex::Search()` can reach `key_index_.Get()` with a
stale id from the global HNSW path:

### 5.1 `LoadEntry()` – normal document loading

```cpp
// doc_index.cc:696-707  (BEFORE the fix)
optional<ShardDocIndex::LoadedEntry> ShardDocIndex::LoadEntry(DocId id,
                                                              const OpArgs& op_args) const {
  // ← no validity check here; id may be in free_ids_
  auto& db_slice = op_args.GetDbSlice();
  string_view key = key_index_.Get(id);   // ← DCHECK fires if id is freed
  …
}
```

This is the primary crash site.  Called unconditionally for every non-`IdsOnly` result.

### 5.2 `IdsOnly()` fast-path – `NOCONTENT` / `RETURN 0` queries

When the caller requests only document keys (no field values), `Search()` skips `LoadEntry()`
and calls `key_index_.Get()` directly:

```cpp
// doc_index.cc:820-824  (BEFORE the fix)
if (params.IdsOnly()) {
  string_view key = key_index_.Get(result.ids[i]);  // ← same DCHECK, same crash
  out.push_back({result.ids[i], string{key}, {}, knn_score, sort_score});
  continue;
}
```

A Copilot review comment on the PR identified this second path:

> "`LoadEntry()` now guards `key_index_.IsValid(id)`, but `ShardDocIndex::Search()` still has
> an `IdsOnly()` fast-path that calls `key_index_.Get(result.ids[i])` without an `IsValid`
> check."

---

## 6. Root Cause Summary

| Layer | What goes wrong |
|-------|-----------------|
| **Architecture** | Global HNSW search is inherently split across two transactions; no single lock spans both phases. |
| **DocKeyIndex** | `Remove()` adds the id to `free_ids_` immediately; `Get()` has a hard DCHECK that fires on freed ids. |
| **Concurrency** | The HNSW read lock prevents the HNSW graph from being updated, but does *not* protect the key-index; `RemoveDoc()` frees the key-index slot without waiting for the read lock to be released. |
| **Both call sites** | Both `LoadEntry()` and the `IdsOnly()` fast-path call `Get()` without first testing `IsValid()`. |

---

## 7. Proposed Fix (from PR #6936 + Review Comments)

### 7.1 Guard `LoadEntry()` with `IsValid`

```cpp
optional<ShardDocIndex::LoadedEntry> ShardDocIndex::LoadEntry(DocId id,
                                                              const OpArgs& op_args) const {
  if (!key_index_.IsValid(id))      // ← NEW: skip stale ids
    return std::nullopt;
  auto& db_slice = op_args.GetDbSlice();
  string_view key = key_index_.Get(id);
  …
}
```

`IsValid()` checks that the id slot is occupied and the reverse map still maps its key back to
the same id:

```cpp
bool ShardDocIndex::DocKeyIndex::IsValid(DocId id) const {
  if (id >= last_id_ || id >= keys_.size())
    return false;
  auto it = ids_.find(keys_[id]);
  return it != ids_.end() && it->second == id;
}
```

### 7.2 Guard the `IdsOnly()` fast-path with `IsValid`

```cpp
if (params.IdsOnly()) {
  if (!key_index_.IsValid(result.ids[i])) {   // ← NEW: mirror the LoadEntry guard
    expired_count++;
    continue;
  }
  string_view key = key_index_.Get(result.ids[i]);
  out.push_back({result.ids[i], string{key}, {}, knn_score, sort_score});
  continue;
}
```

### 7.3 Regression test

The fix is accompanied by `HnswRaceTest::HnswKnnDeleteRaceCrash`:

- 5 000 documents, DIM=128, M=16, EF_CONSTRUCTION=200.
- 1 search fiber continuously issuing `FT.SEARCH … KNN 200` from thread 3.
- 3 delete fibers (threads 0–2) continuously deleting and re-inserting the 200 nearest docs.
- Without the fix, this crashes with SIGABRT within a handful of iterations.
- With the fix, all 50 search iterations complete and each result is checked to be
  non-error (`EXPECT_NE(resp.type, RespExpr::ERROR)`).

---

## 8. Limitations of the Fix

The `IsValid` guard eliminates the **crash** and the **stale-slot read** within a single
`IsValid`+`Get` pair, but it does not solve the broader two-hop problem noted by the reviewer.
Between `Knn()` returning an id and `ScheduleSingleHop` loading it, the following sequences
are still possible even after the fix:

| Event between hops | Observable effect after fix |
|--------------------|-----------------------------|
| Delete only (id not reused) | Doc silently dropped from results (safe) |
| Delete + re-add same key | Doc re-indexed; result may reflect new data |
| Delete + add *different* key reusing the slot | `IsValid` returns `true` (slot occupied), `Get` returns new key, wrong doc returned |

The last case is the silent data-corruption variant of the bug.  A full fix would require
either (a) holding a shard lock across both phases of the KNN pipeline, or (b) recording the
key alongside the `DocId` at Phase 1 time and validating the match at Phase 2.

---

## 9. Reproduction

The regression test added in PR #6936 is deterministic and reproduces the crash reliably:

```
cd build-dbg && ninja search_family_test
./search_family_test --gtest_filter="HnswRaceTest.HnswKnnDeleteRaceCrash"
# Without fix: SIGABRT within ~5 iterations
# With fix:    PASSED
```
