# Sharded Append-Only File (AOF) Design

> **Status:** design proposal. Nothing described here is implemented yet.

This document proposes durable, log-based persistence for Dragonfly. It is built on the per-shard
journal that replication already uses. Every shard writes its own append-only log with no global
serialization point. Periodic DFS checkpoints keep the logs bounded.

The document is ordered by stage:
- **Part 1** describes the MVP: a working, bounded AOF with a fixed once-per-second sync.
- **Part 2** describes features that build on the MVP.
- **Part 3** is reference material: configuration, implementation map, phasing and testing.

## Table of Contents

**Part 1: MVP**

1. [Motivation](#motivation)
2. [Building Blocks We Reuse](#building-blocks-we-reuse)
3. [MVP Scope](#mvp-scope)
4. [Architecture](#architecture)
5. [On-Disk Layout](#on-disk-layout)
6. [Write Path](#write-path)
7. [I/O Mode](#io-mode)
8. [Pure Async Disk I/O vs. Writer Fiber](#pure-async-disk-io-vs-writer-fiber)
9. [Checkpoints: Bounding File Growth](#checkpoints-bounding-file-growth)
10. [Replay at Startup](#replay-at-startup)

**Part 2: Later Stages**

11. [Fsync Policy](#fsync-policy)
12. [Atomic Groups](#atomic-groups)
13. [Re-base: Paths That Bypass the Journal](#re-base-paths-that-bypass-the-journal)
14. [Multi-Shard Tail Atomicity](#multi-shard-tail-atomicity)
15. [Other Extensions](#other-extensions)

**Part 3: Reference**

16. [Configuration and Observability](#configuration-and-observability)
17. [Implementation Map](#implementation-map)
18. [Phasing](#phasing)
19. [Testing](#testing)
20. [Open Questions](#open-questions)

---

# Part 1: MVP

## Motivation

Today Dragonfly persists data only through point-in-time snapshots (RDB/DFS). A crash loses
every write made since the last snapshot. Valkey solves this with AOF, but its AOF is a single
global log. Adopting that design would reintroduce a global serialization point that
Dragonfly's shared-nothing architecture avoids ([df-share-nothing.md](df-share-nothing.md)).

Goals:
- Configurable durability. The MVP loses at most about one second of writes. A later stage
  (`always`) makes every acknowledged write survive a crash.
- Nothing added to the write hot path that is shared across shards.
- Disk usage stays bounded without operator intervention.
- Recovery is parallel across shards.

Non-goals:
- The epoll backend. AOF requires io_uring and refuses to start with `--proactor_type=epoll`.
- Loading or producing Valkey-format AOF files.

---

## Building Blocks We Reuse

- **The journal serializes each record once and fans it out.**
  `JournalSlice::AddLogRecord` ([journal_slice.cc](../src/server/journal/journal_slice.cc))
  serializes each record, assigns it a per-shard LSN, and hands it to every registered
  `JournalConsumerInterface` through `ConsumeJournalChange` and `ThrottleIfNeeded`. Replication
  (`ReplicaStreamer`) and slot migration (`SlotMigrationStreamer`) are consumers already. AOF
  becomes a third. (These names come from the streamer refactor in
  [#8395](https://github.com/dragonflydb/dragonfly/pull/8395); on older code they are
  `JournalStreamer` and `RestoreStreamer`.)
- **Journal records replay independently.** Each shard's journal holds only that shard's part of
  a command. The deprecated `shard_cnt` field is always written as 1
  ([serializer.cc](../src/server/journal/serializer.cc)), and a replica applies each flow on its
  own. The one exception is global commands (FLUSHALL, FLUSHDB, FLUSHSLOTS). Those go through a
  `MultiShardExecution` barrier keyed by txid (`DflyShardReplica::ExecuteTx` in
  [replica.cc](../src/server/replica.cc)).
- **Journal records are deterministic.** TTLs are journaled as absolute times (PEXPIREAT, PXAT).
  Expiry and eviction are journaled as `DEL`. Replaying the log hours later therefore produces
  the same state.
- **Snapshots are point-in-time per shard.** The cut happens at
  `SerializerBase::RegisterChangeListener`. Replication full sync registers the journal consumer
  at that same point ([snapshot.cc](../src/server/snapshot.cc)). This is exactly the
  "base + log from LSN L" cut that AOF checkpoints need.

---

## MVP Scope

What the MVP provides:
- **Logging:** one append-only log per shard, fed by the journal.
- **Durability:** a fixed policy. Blocks are written as soon as they are sealed, and each shard
  runs `fdatasync` about once per second. Replies are never delayed. A crash loses at most about
  one second of writes. The configurable policies (`always`, `no`) come in a later stage
  ([Fsync Policy](#fsync-policy)).
- **Bounded disk usage:** automatic checkpoints.
- **Recovery:** parallel replay at startup, including a changed shard count.
- **Errors:** after a failed write or fsync, write commands are rejected with `-MISCONF` until a
  later write succeeds, as in Valkey.

What the MVP guarantees after a crash:
- Each shard's log recovers to a prefix of what that shard wrote.
- A multi-command transaction (MULTI/EXEC, EVAL) can be applied partially, if the crash cut
  through its records. This is weaker than Valkey, whose AOF records MULTI/EXEC markers and
  drops an incomplete transaction when it loads a truncated file. [Atomic groups](#atomic-groups)
  close this gap in a later stage.
- A multi-shard transaction can be applied on some shards and not others. See
  [Multi-Shard Tail Atomicity](#multi-shard-tail-atomicity).

MVP constraints, each lifted by the [re-base](#re-base-paths-that-bypass-the-journal) stage:
- AOF can only be enabled at startup with `--aof`. `CONFIG SET appendonly` is not supported.
- AOF is not supported on replicas: `--aof` together with `--replicaof`, and `REPLICAOF` while
  AOF is on, are rejected.
- `DFLY LOAD`, `DEBUG RELOAD` and `DEBUG LOAD` are rejected while AOF is on.

---

## Architecture

```
            shard thread i
  tx ──► journal::RecordEntry ──► JournalSlice (LSN_i, one serialization)
                                        │ ConsumeJournalChange / ThrottleIfNeeded
             ┌──────────────────────────┼──────────────────────────┐
      ReplicaStreamer          SlotMigrationStreamer          AofStreamer (new)
             │                          │                          │
    BufferedSocketWriter       BufferedSocketWriter         AofSegmentWriter (new)
             ▼                          ▼                          ▼
       replica socket            target node socket            segment files
```

- **Naming.** The naming follows the pattern of the other streamers: a consumer named after its
  purpose (`AofStreamer`) owns a writer named after its destination (`AofSegmentWriter`), just as
  `ReplicaStreamer` owns a `BufferedSocketWriter`.
- **`AofStreamer`** is a new `JournalConsumerInterface`, one per shard (thread-local, like the
  journal slice). It turns journal records into blocks.
  - It registers with `journal::RegisterConsumer`. That permanently acquires a journal user, so
    the journal never auto-stops (`MaybeStop()`) while AOF is on.
  - The replication ring buffer and its retention policy don't change, and AOF does not depend
    on them.
- **`AofSegmentWriter`** owns the segment files: writes, syncs, rotation, and spare segments. It
  runs on the shard's own proactor and uses `fb2::LinuxFile` over io_uring.
  - Unlike a socket, a file write carries its own offset. So writes are submitted in parallel as
    soon as blocks are sealed, and the offset keeps them in order (see [Write Path](#write-path)).
  - How the writer drives its I/O is discussed in
    [Pure Async Disk I/O vs. Writer Fiber](#pure-async-disk-io-vs-writer-fiber).
- **AOF LSN = journal LSN.** There is no second sequence number. Checkpoints, replay, and any
  future partial sync from disk all use the same coordinates.
- **Shared apply logic.** The body of `DflyShardReplica::ExecuteTx` (the global-command barrier
  plus `JournalExecutor`) is extracted into a reusable `JournalApplier`. Both replica stable sync
  and AOF replay use it.

---

## On-Disk Layout

All files live in `--aof_dir`, which defaults to `--dir`:

```
<name>.manifest                       # source of truth; replaced atomically
<name>-base-<ckpt>-summary.dfs        # checkpoint base (regular DFS format)
<name>-base-<ckpt>-0003.dfs
<name>-0003-000017.aof                # shard 3, segment seq 17
```

### Segment format

- **Header:**
  - magic `DFAOF1`, format version
  - shard_id, shard_count
  - checkpoint id, segment seq
  - start_lsn
  - `segment_uid`: a random 64-bit id, generated each time the file starts a new segment
    (including when a recycled file is reused).
  - `max_unsynced_bytes`: the writer's bound on data not yet covered by a completed sync. Replay
    uses it to tell a torn tail from corruption; see [Replay](#replay-at-startup).
  - header CRC
- **Body:** a sequence of blocks, each laid out as:

  ```
  [u32 len][u32 crc32c][u64 first_lsn][u32 n_records][u8 flags][payload]
  ```

  - `payload` is the concatenated `JournalWriter` bytes, i.e. exactly what is already in
    `JournalItem::data`. Nothing is serialized a second time.
  - `crc32c` covers the block and is seeded with the header's `segment_uid`. Stale blocks left in
    a recycled file were written under a different uid, so they never validate, whatever their
    LSN.
  - `flags` is reserved in the MVP (always 0). [Atomic groups](#atomic-groups) use one bit of it
    later.
  - The end of data is a block with `len == 0` (a hole or unwritten space reads as zeros), a
    block whose CRC does not validate, or a block whose `first_lsn` is not the expected next
    LSN. That rule is what allows segments to be recycled (see [I/O Mode](#io-mode)).

### Manifest

The manifest is a small text file, replaced atomically (write temp → fsync → rename → fsync
directory). It contains:
- format version and shard_count
- current checkpoint id, base file prefix, and `snapshot_id`
- for each shard: `cut_lsn` and `cut_seq`

Segment files describe themselves through their headers, so rotating a segment does not touch
the manifest. The manifest changes only when a checkpoint commits.

---

## Write Path

`ConsumeJournalChange(item)` runs on the shard thread and must not preempt, because it may be
called inside an atomic section. It appends `item.journal_item.data` to the open block and bumps
`last_appended_lsn`.

`ThrottleIfNeeded()` seals the open block and queues it for submission:
1. Assign the block the next file offset (`next_offset += block size`).
2. Append it to the current submission batch. Blocks are sealed at every flush point, often
   once per transaction and only a few hundred bytes each. Submitting each one on its own would
   cost one submission and one completion per transaction, and on ext4 one io-wq work item each.
   So consecutive sealed blocks are combined, and the batch is submitted as one async write at
   its first block's offset when either:
   - it reaches `--aof_write_batch_bytes`, or
   - the current proactor loop iteration ends, i.e. before the proactor waits for events again
     (for example from a `ProactorBase::AddOnIdleTask` hook).

   Batching never waits for earlier writes to complete; it only groups blocks sealed close
   together in time. The batch buffer stays alive until its write completes.
3. Apply backpressure: if more than `--aof_max_inflight_bytes` are in flight, or more than
   `max_unsynced_bytes` are written but not yet synced, block until completions bring the amount
   back under the limit. It waits on an `EventCount` that completions notify. This stalls the
   shard, the same way replication throttling in `BufferedSocketWriter` does today.

**Why parallel writes.** On a socket, the next write must wait for the previous one, so data has
to be buffered until then. A file write names its own offset, so order is encoded in the offset
and writes do not need to be serialized.
- The logic is simpler: there is no pending buffer that waits for the previous write. The
  batching above only bounds the number of requests; it is not tied to completions.
- Several requests in flight let the kernel merge adjacent writes, and keep the device busy at a
  higher queue depth.
- How much of this reaches the device depends on how the kernel executes buffered writes; see
  [Buffered writes in io_uring](#buffered-writes-in-io_uring).

**Completion tracking.** Writes can complete out of order.
- In-flight writes are kept in a FIFO ordered by offset.
- When a write completes, it is marked done. `written_lsn` then advances over the contiguous
  prefix of completed writes, and their buffers are released.
- A short write is resubmitted for the remainder at `offset + written`.

**Sync.** Once per second, if `written_lsn` moved since the last sync:
1. Record `sync_target = written_lsn`.
2. Issue an async `fdatasync`. At most one sync is in flight per shard.
3. When it completes, publish `durable_lsn = sync_target` (an atomic readable from any thread),
   and wake waiters.

An `fdatasync` only covers writes that completed before it was issued. That is why the target is
the contiguous `written_lsn`, not `last_appended_lsn`.

A sync is also forced early when the unsynced amount reaches `max_unsynced_bytes`. This bound is
what makes a torn tail recognizable at replay.

**Rotation.** When the next block would cross `--aof_segment_max_bytes`, it goes into the spare
segment instead, right after the header. The old segment then gets its final `fdatasync` and is
closed once its in-flight writes complete. `durable_lsn` advances past a segment's records only
after that segment's final sync.

**Spare segments.** A new segment's directory entry must be durable before any record in it is
reported durable. Otherwise, after a power loss, the file could vanish together with
acknowledged records. So each shard always keeps one spare segment ready in the background:
- create an empty file, or rename a recycled one into place;
- `fsync` the directory.

No data is written while preparing a spare. There is no zero-filling and no `fallocate` (see
[I/O Mode](#io-mode)). The header is written as part of the segment's first batch.

Rotation, and the switch at a checkpoint cut, only ever move to a spare that is already durable,
so no directory fsync sits on the write path. Preparation is only an open or a rename plus a
directory fsync. It starts right after a rotation, so it normally completes long before the next
one. If rotation ever does find the spare not ready, sealed blocks accumulate and normal
backpressure applies.

**Errors.** A failed write or sync puts the shard's AOF into an error state. While in that
state, write commands are rejected with `-MISCONF` until a later write succeeds. The state is
exposed in `INFO persistence` as `aof_last_write_status`.

---

## I/O Mode

Segments are written through the page cache (`O_WRONLY | O_CLOEXEC`, no `O_DIRECT`).

**Why not O_DIRECT.** O_DIRECT needs the offset, length and buffer aligned to 4K, and a sealed
block is often only a few hundred bytes. Every write would then have to do one of two things:
- Rewrite the partial tail page. That costs 4K of write amplification per block. Worse, if the
  rewrite tears on power loss, it can destroy bytes in that page that were already durable.
- Pad every block out to a fresh 4K boundary. At low concurrency that wastes a lot of space.

O_DIRECT also does not make anything durable. We would still need `fdatasync` to flush the
device cache.

**No preallocation.** Segments are neither zero-filled nor `fallocate`d.
- Zero-filling writes every segment twice, once as zeros and once as data. Under sustained
  writes close to disk bandwidth, that roughly halves the effective bandwidth.
- `fallocate` latency can be significant. It also leaves *unwritten* extents on ext4 and XFS,
  and converting them on the first write is a metadata change anyway.

**What an append costs at sync time.** Appending to a fresh segment grows the file, so each
`fdatasync` also persists the new file size: one filesystem journal commit per sync. With the
MVP's once-per-second sync per shard, that cost is negligible.

**Recycling removes it in steady state.**
- After a checkpoint, garbage-collected segments are renamed and overwritten in place, similar to
  how PostgreSQL recycles WAL segments. Only a shard's own segments are recycled.
- Writes into a recycled file stay within its existing size and allocated blocks, so
  `fdatasync` flushes data only.
- Once checkpoints run, recycling is the steady state. A fresh AOF appends until the first
  checkpoint.
- This matters most for `always` ([Fsync Policy](#fsync-policy)), which syncs many times per
  second. Until the first checkpoint, `always` pays the size-metadata commit on every group
  commit; this should be measured.
- **Why recycling is safe:** block CRCs are seeded with the new segment's `segment_uid`. Stale
  blocks left in the file do not validate, so replay reads them as end of log, even if their
  LSNs are higher than the expected one.

**Page cache.** AOF data is written once and almost never read, so it should not push other data
out of the page cache.
- Once a region of a segment is durable, the writer issues `IORING_OP_SYNC_FILE_RANGE` (WRITE)
  followed by `IORING_OP_FADVISE` (DONTNEED) on that region.
- This also caps the dirty pages that count against a container's cgroup memory limit.

**Checkpoint base files** keep using the existing O_DIRECT snapshot path. Those are large
sequential writes that are already aligned. The only change is a final `FSync()` before the
rename, since snapshot files are not fsynced today.

---

## Pure Async Disk I/O vs. Writer Fiber

Both options submit block writes the same way: asynchronously, in parallel, at their offsets, as
described in [Write Path](#write-path). They share the on-disk format and the sync semantics.
They differ in who runs the control logic: tracking completions, syncing, rotating, and
preparing spare segments.

### Option A: pure async disk I/O

All I/O is issued asynchronously, and the control logic runs in io_uring completion callbacks.
This is the model tiering already uses: `DiskStorage::Stash` issues `WriteAsync` or
`WriteFixedAsync`, and grows its file with `FallocateAsync`
([disk_storage.cc](../src/server/tiering/disk_storage.cc)).

- **Write completions:** the completion callback of a write:
  - marks the write done and advances `written_lsn`;
  - resubmits a short write;
  - notifies the `EventCount` that backpressure waits on.
- **Sync:** a `ProactorBase::AddPeriodic` task issues an async `fdatasync` when there is unsynced
  data. Its completion publishes `durable_lsn`, and chains `sync_file_range` and `fadvise` for
  page-cache hygiene.
- **Rotation:** handled from the completion of the old segment's last write.
- **Buffers:** each block can be built in a registered buffer (`RequestRegisteredSlice`) and
  written with `WriteFixedAsync`, as `DiskStorage` does.
- **Constraint:** completion callbacks run in the proactor context, outside any fiber. They must
  never call a fiber-blocking function, such as `FSync()` or `Throttle()`.

Additional work this option needs:
- **helio `LinuxFile` API** ([uring_file.h](../helio/util/fibers/uring_file.h)):
  - An async fsync (`FSyncAsync(flags, cb)`). Today `FSync` is fiber-blocking only; it is built
    on `FiberCall`.
  - Async `sync_file_range` and `fadvise`.

  Each is a few lines on top of `GetSubmitEntry`. They are upstream helio changes, unless the
  AOF code issues the SQEs itself.
- **Spare-segment preparation:** preparing a spare takes two steps (an open or a rename, then a
  directory fsync). It becomes either a small callback chain, which also needs an async open and
  rename, or a short-lived background fiber started after each rotation.
- **Error and shutdown handling:**
  - Errors are recorded from the callbacks. `-MISCONF` gating reads that status.
  - Closing a segment must first drain its in-flight operations, using a pending-ops counter like
    the one in `DiskStorage`.

Pros:
- No permanent fiber per shard.
- No fiber wakeup per completion or per sync.
- The model matches existing tiering code.

Cons:
- The helio additions above.
- Control flow is split across callbacks (completion tracking, rotation, spare switching,
  errors). That is harder to read and test than straight-line code.

### Option B: writer fiber

Block writes are still submitted asynchronously from `ThrottleIfNeeded()`. A dedicated fiber per
shard runs the control loop:
1. wait for completions or the sync timer;
2. advance `written_lsn`;
3. issue a fiber-blocking `FSync()` when a sync is due, then publish `durable_lsn`;
4. rotate, and prepare spare segments inline.

Additional work this option needs:
- None in helio. The existing fiber-blocking `FSync()` is enough.

Pros:
- Straight-line control logic: syncs, rotation, spare preparation and errors live in one place.
- Same style as the snapshot save path.

Cons:
- A permanent fiber and its stack on every shard.
- A fiber wakeup per completion batch and per sync.
- A blocking fsync still runs in an io-wq kernel thread (see below), so the fiber only adds a
  user-space hop on top of it.

### Buffered writes in io_uring

Historically, io_uring could complete writes to regular files without blocking only with
`O_DIRECT`. Every buffered write was punted to io-wq kernel worker threads.
- Async buffered *reads* came in Linux 5.9.
- Async buffered *writes*, using the non-blocking `IOCB_NOWAIT` path, landed for XFS in 6.0 and
  for btrfs in 6.1 ([LWN](https://lwn.net/Articles/896909/)).

In current mainline:
- **Which filesystems are covered:** a filesystem opts in with `FOP_BUFFER_WASYNC`. XFS and
  btrfs set it; ext4 does not (`fs/ext4/file.c`).
- **What happens without it:** for a regular file with neither `IOCB_DIRECT` nor that flag,
  `io_uring/rw.c` returns `-EAGAIN` from the non-blocking attempt and punts the write to io-wq.
- **Syncs:** `fsync`/`fdatasync`, `sync_file_range` and `fallocate` are always executed in
  io-wq. `io_uring/sync.c` forces them async.

What this means for the AOF:
- **Buffered writes work on every filesystem.** They complete inline or through io-wq, but in
  both cases the submitting thread never blocks. So neither option depends on the filesystem.
- **Parallelism depends on the filesystem.**
  - io-wq serializes buffered writes to one file (hashed by inode). So on ext4, parallel
    submissions are executed one at a time; they are still ordered by their offsets.
  - On XFS and btrfs they complete inline.
  - For buffered I/O, the device queue depth is ultimately set by writeback, and parallel
    submission mainly cuts per-write latency. The queue-depth benefit is largest with a
    [direct I/O](#other-extensions) mode.
- **Worker threads under a slow disk.** Every sync, and every punted write, occupies an io-wq
  worker. Consider capping workers with `IORING_REGISTER_IOWQ_MAX_WORKERS`, so a stalled disk
  does not spawn many kernel threads. helio does not set a cap today.
- **The kernel I/O is the same for A and B.** The choice only affects scheduling in user space.

### Recommendation

Option A, pure async I/O.
- With parallel submission, the writes are already async in both options. The remaining control
  logic (a completion FIFO, a periodic sync, rotation) is small and event-driven, which callbacks
  express well.
- It avoids a permanent fiber per shard and a wakeup per completion.
- It reuses the pattern `DiskStorage` already follows.

The rare, multi-step spare-segment preparation should still run in a short-lived fiber rather
than a callback state machine.

The helio additions (async fsync, `sync_file_range`, `fadvise`) are the main cost. If they should
not block the MVP, option B is an acceptable interim step. The format and semantics are
identical, so moving from B to A later changes only `AofSegmentWriter`.

---

## Checkpoints: Bounding File Growth

Rotating segments only splits the log into files; on its own, it does not bound disk usage. That
comes from **checkpoints**. A checkpoint writes a new DFS base and records a cut LSN for each
shard. Once it commits, every older segment can be deleted.

This is what Valkey's `BGREWRITEAOF` does as well. It never reads the old AOF. A forked child
dumps the in-memory dataset as the new BASE while the parent starts a new INCR file at the fork
point. Here, the snapshot's point-in-time cut takes the place of `fork()`.

### Triggers

- **Automatic:** the log size since the last base exceeds
  `max(--aof_rewrite_min_size, base_size * --aof_rewrite_percentage / 100)`. This is Valkey's
  `auto-aof-rewrite-*` rule. The defaults are 64MB and 100%.
- **Manual:** `DEBUG AOF CHECKPOINT`, meant for tests and operations. `BGREWRITEAOF` is not
  implemented (see [Other Extensions](#other-extensions)).
- **Shard count change at restart:** see [Replay](#replay-at-startup), step 6.
- Later stages add more forced checkpoints; see
  [Re-base](#re-base-paths-that-bypass-the-journal).

### Flow for checkpoint C

1. Refuse or defer if another save is already running. The exclusion is shared with BGSAVE in
   `SaveStagesController`.
2. **Take the cut.**
   - Run the cut as a *global* transaction, like SAVE. That guarantees every shard orders
     global commands the same way relative to the cut.
   - On each shard, perform these three steps together, with no preemption in between:
     - `RegisterChangeListener` (the snapshot's point-in-time version)
     - `L_i = journal::GetLsn()`
     - `aof_streamer.SealAndRotate(start_lsn = L_i, ckpt = C)`
   - This is the same point where replication full sync starts. As a result, every record with
     LSN < L_i is reflected in base C, and every record with LSN ≥ L_i is in segments from the
     cut segment onward.
   - Because the log rotates exactly at the cut, each older segment lies entirely before L_i.
     Garbage collection can then delete whole files.
3. Serialize the base with the existing `RdbSaver`/`SliceSnapshot` (`stream_journal = false`)
   into `<name>-base-C-*.dfs.tmp`. Appending to the log continues the whole time.
4. `fsync` every base file, rename it into place, and `fsync` the directory.
5. Commit by writing the new manifest atomically.
6. Delete base C-1, and every segment of shard i whose seq is below `cut_seq_i`.

### Crash safety

- **Crash before step 5:** the old manifest is still valid. Its segment chain continues without
  a gap from the old cut through L_i and onward, because nothing has been deleted.
- **Crash between steps 5 and 6:** at startup, files the manifest doesn't reference are
  garbage-collected.
- **Checkpoint failure:** the temp files are deleted and appending simply continues. The chain
  stays intact.

Disk usage stays at roughly `base + rewrite threshold + one in-progress base`.

---

## Replay at Startup

When AOF is enabled and a manifest exists, the AOF takes precedence over `--dbfilename`, as it
does in Valkey.

1. **Enter LOADING.** Switch to `GlobalState::LOADING` using the existing `ServerFamily::Load`
   machinery. Clients get `-LOADING` until replay finishes.
2. **Load the base.** Load base C through the existing parallel DFS load path. Base files are
   not tied to shards; each key goes to whichever shard owns it.
3. **Validate the segment chains.** For each shard i:
   - Glob its segments and read their headers.
   - Starting at `cut_seq_i`, require that segment seqs are contiguous.
   - Require that each segment's `start_lsn` equals the previous segment's last LSN + 1.
   - A gap is a hard error.
4. **Replay each chain in its own fiber.** There is one fiber per source shard, all running in
   parallel, each on that shard's proactor.
   - Read blocks and verify their CRCs.
   - Skip records with LSN < L_i. The LSN is counted from `first_lsn`, reusing the LSN tracking
     in `TransactionReader`.
   - Apply records through `JournalApplier` (`JournalExecutor` → `Service::DispatchCommand`).
     Keys are routed to their owning shard, so the current shard count may differ from the one
     that wrote the files.
   - **Why this is correct:** all of a key's records live in a single source shard's log, in
     order. Per-key ordering is therefore preserved. This is the same argument that makes
     replication between masters and replicas with different shard counts correct.
   - **Global commands** (FLUSHALL, FLUSHDB, FLUSHSLOTS) meet at the `MultiShardExecution`
     barrier, keyed by txid. The barrier's participant count is not fixed at `shard_count`. It
     is the number of chains that have not reached end of log.
     - A chain that reaches end of log counts as having arrived at every pending and future
       barrier. Reaching end of log also wakes fibers already waiting at a barrier.
     - Without this, a crash tail can deadlock replay. Chain A may have a FLUSHALL durable while
       chain B's log ends before it, so the fibers holding the FLUSHALL would wait forever for B.
     - **Why applying it is correct:** the global command did run on every shard. If B's log ends
       before it, every surviving record of B comes before it in B's order. So running it after
       B is exhausted matches what happened.
     - The global cut at checkpoints still guarantees that no chain starts past a global command
       that another chain has before its cut.
5. **Handle a damaged tail.**
   - **Why the tail can have holes.** Data written after the last completed sync can reach the
     disk in any order. Writes are submitted in parallel, and page-cache writeback does not follow
     file order either. After a crash, the unsynced end of a chain can therefore contain zeroed
     or stale regions and torn blocks, followed by blocks that did make it to disk.
   - **The bound.** The writer never has more than `max_unsynced_bytes` (W) written without a
     completed sync (see [Write Path](#write-path)). W is recorded in each segment header.
   - **Where replay stops.** Replay stops at the first bad block B: a short read, `len == 0`, a
     CRC mismatch, or a `first_lsn` that is not the expected LSN. Everything after B is
     discarded, including valid blocks. None of those were covered by `durable_lsn`, so they were
     inside the loss window.
   - **When truncation is accepted.** It is accepted only if B lies within the last W bytes of
     the chain: no CRC-valid block with `first_lsn` above the expected LSN appears more than W
     bytes after B.
     - Distance is measured in chain bytes, so the window can straddle a rotation.
     - Holes (`len == 0`) and stale recycled blocks (their CRC is seeded with another
       `segment_uid`) do not count as valid blocks.
   - **What happens then.** With `--aof_load_truncated=true` (the default), log a warning and
     truncate the file at B. Otherwise, fail. Truncation is required before resuming, so that
     stale valid blocks after B can never be read again.
   - **Everything else is corruption, and is fatal:** a bad block followed by valid data more
     than W bytes later, or damage before the last W bytes of the chain. Loading past corruption
     needs an explicit repair step, such as a future `dfly-aof-check --fix`.
   - **Limitation.** Corruption within the last W bytes cannot be told apart from a torn tail.
     That is the price of not writing sync markers into the log.
6. **Resume the log.**
   - **Same shard count:** on each shard, call `journal::StartInThreadAtLsn(last_lsn_i + 1)`,
     open a new segment at that LSN, and only then register `AofStreamer`. Registering it this
     late keeps replayed records from being appended to the log again.
   - **Shard count changed** (the manifest's `shard_count` differs from the current one): the
     old chains cannot be continued. Run a checkpoint while still in LOADING. Its cut registers
     the new `AofStreamer`s, and the server switches to ACTIVE only after that.
7. **Finish.** Run `PerformPostLoad` and `ForceReplicasToFullSync()`, then switch to ACTIVE.

---

# Part 2: Later Stages

## Fsync Policy

This stage replaces the MVP's fixed once-per-second sync with `--aof_fsync`. The Valkey name
`appendfsync` is also accepted for CONFIG compatibility.

| mode | writer behavior | client visibility | loss window |
|---|---|---|---|
| `always` | Group commit: one `fdatasync` covers every write completed before it was issued. While it is in flight, new writes keep being submitted, and the next sync covers them. | The reply is held until `durable_lsn[s] >= L_s` on every shard the transaction touched. | No acknowledged writes |
| `everysec` (default) | The MVP behavior. | Replies are never delayed. | About 1s plus in-flight data |
| `no` | No periodic sync. The kernel decides when to flush, optionally smoothed with `sync_file_range`. `max_unsynced_bytes` still forces a sync, which keeps replay's torn-tail bound valid. | Replies are never delayed. | Up to OS writeback |

`always` deliberately does not use `RWF_DSYNC` on every write. With parallel writes, each write
would pay for its own sync. A single pipelined `fdatasync` amortizes one sync over many writes.

### Reply gating in `always` mode

- When a write transaction concludes, the coordinator reads the atomic `last_appended_lsn` of
  each active shard and keeps the maximum per shard in `ConnectionContext`.
  - Hop completion already provides the required happens-before ordering.
  - The value is conservative: it may include records from other clients. Under group commit
    that costs nothing extra.
- The connection waits on the shards' `EventCount`s **right before the reply builder flushes to
  the socket**, not after each command. A pipeline of N writes, or a squashed MULTI/EXEC, then
  pays for a single wait.
- Read-only and non-journaled commands never wait.
- `-MISCONF` also applies in `always` mode after a failed sync. `INFO persistence` adds
  `aof_delayed_fsync`.

---

## Atomic Groups

A multi-command transaction on one shard (MULTI/EXEC, EVAL, FCALL) writes several journal
records. Today nothing prevents a block boundary from falling between them. `DisableFlushGuard`
is used only around expiry and eviction, not around EXEC or script execution. This stage marks
transaction boundaries explicitly:

- **Guard.** A new `journal::AtomicGroupGuard` is opened by the transaction when a multi-command
  transaction starts executing on a shard, and closed when it finishes there (its last hop). The
  count is kept per shard, so guards can nest.
- **Sealing inside a group.** `AofStreamer` still seals blocks while a group is open. Otherwise a
  large script would pin memory, and throttling would deadlock, because only sealed blocks can
  be written.
  - Blocks sealed inside a group carry the `GROUP_CONT` bit in `flags`: the block ends inside a
    group that continues in the next block.
  - The block that closes the group has the bit cleared.
- **Replay.** Records from `GROUP_CONT` blocks are buffered, and applied only when the closing
  block arrives. A group cut off by a torn tail is dropped whole.
- **Interleaved records.** Records of unrelated transactions on the same shard can land inside an
  open group, for example between the hops of a lock-ahead MULTI. They share the group's
  durability: they are applied with it, or dropped with it. Per-key order is unchanged.
- **Durability.** `durable_lsn` only advances to the end of a block without `GROUP_CONT`.
  Otherwise, under `always`, a write could be acknowledged and then dropped with an incomplete
  group.

With this stage, single-shard transactions and scripts become atomic across a crash, which
matches what Valkey's AOF provides for MULTI/EXEC. The change is small (a guard, one flag bit,
and buffering in replay), so it is a good candidate for the first stage after the MVP.

---

## Re-base: Paths That Bypass the Journal

The invariant is: **whatever reaches this node's replicas also reaches its AOF.** A path that
changes the dataset without going through the journal breaks the chain. Every such path must
trigger a **re-base**: a checkpoint whose cut also registers `AofStreamer` if it is not
registered yet.

This stage lifts the MVP constraints:
- **First enable** with `CONFIG SET appendonly yes`.
- **Replica full sync.** `RdbLoader` bypasses the journal.
  - When a full sync starts, stop appending and mark the manifest invalid.
  - When it finishes, re-base at the start of stable sync.
  - From then on AOF works on the replica with no further special handling. Stable sync already
    records applied entries into the replica's own journal (see the PING handling in
    `replica.cc`).
- **Other `RdbLoader`-driven loads:** `DFLY LOAD`, `DEBUG RELOAD`, `DEBUG LOAD`.

Incoming slot-migration data needs nothing extra. The target applies it through the journal
executor and journals it like any other write.

---

## Multi-Shard Tail Atomicity

A multi-shard transaction (an MSET across shards, a cross-shard EXEC or EVAL) is not atomic
across a crash: shard A's part can be durable while shard B's is not.
- Under `always`, this can only affect transactions that were never acknowledged.
- Replicas have the same semantics today.

Possible fix:
- Reuse the deprecated per-entry field (currently always written as `1u`) to carry the number
  of shards that participate in the transaction.
- During replay, hold back multi-shard txids at the tail until every other chain has shown its
  part or reached end of log.
- Drop the incomplete ones. Only the tail of the log can contain them.

---

## Other Extensions

- **`--aof_direct_io`** for very high-throughput workloads. The writer would keep the partial
  tail page in memory and pad only at sync points. With O_DIRECT, parallel writes also map
  directly to device queue depth. This is worth doing only if benchmarks show that double
  buffering in the page cache hurts.
- **Sync markers.** Recording completed sync points in the log would let replay tell corruption
  in the last `max_unsynced_bytes` from a torn tail.
- **Partial sync for replicas from AOF segments.** This is cheap because AOF LSN equals journal
  LSN.
- **`dfly-aof-check --fix`**, a repair tool for corrupted segments.
- **`BGREWRITEAOF`** as a thin alias for a checkpoint, if Valkey tooling compatibility calls for
  it.

---

# Part 3: Reference

## Configuration and Observability

| Flag | Default | Stage | Notes |
|---|---|---|---|
| `--aof` | `false` | MVP | Alias: `appendonly` |
| `--aof_dir` | `--dir` | MVP | |
| `--aof_name` | `appendonly` | MVP | File name prefix |
| `--aof_segment_max_bytes` | 256MB | MVP | Segment rotation size |
| `--aof_rewrite_percentage` | 100 | MVP | Auto-checkpoint growth factor |
| `--aof_rewrite_min_size` | 64MB | MVP | Auto-checkpoint minimum size |
| `--aof_write_batch_bytes` | TBD | MVP | Maximum size of one submitted batch of sealed blocks |
| `--aof_max_inflight_bytes` | TBD | MVP | Backpressure threshold for in-flight writes per shard |
| `--aof_max_unsynced_bytes` | TBD | MVP | Forces a sync; bounds the torn-tail window at replay |
| `--aof_load_truncated` | `true` | MVP | Truncate a torn tail within the unsynced window instead of failing |
| `--aof_fsync` | `everysec` | Fsync Policy | `always` / `everysec` / `no`; alias `appendfsync` |

Commands:
- `DEBUG AOF CHECKPOINT` (MVP)
- `CONFIG SET appendfsync` ([Fsync Policy](#fsync-policy))
- `CONFIG SET appendonly` ([Re-base](#re-base-paths-that-bypass-the-journal))

`INFO persistence` gains these fields:
- **MVP, global:**
  - `aof_enabled`, `aof_rewrite_in_progress`, `aof_last_bgrewrite_status`
  - `aof_current_size`, `aof_base_size`
  - `aof_last_write_status`
- **MVP, per shard:** `aof_inflight_bytes`, `aof_unsynced_bytes`, `aof_throttle_usec`,
  `aof_fsync_latency`
- **Fsync Policy stage:** `aof_delayed_fsync`

Base files carry a per-shard `aof-cut-lsn` aux field. It replaces the `aof-preamble` aux field,
which is currently hardcoded to 0. It is informational only; the manifest is authoritative.

---

## Implementation Map

| Area | Files | Stage |
|---|---|---|
| `AofStreamer`, `AofSegmentWriter`, segment reader, manifest | `src/server/journal/aof.{h,cc}` (new) | MVP |
| Async fsync, `sync_file_range`, `fadvise` (option A) | `helio/util/fibers/uring_file.{h,cc}` | MVP |
| Shared apply logic for replica and AOF replay | `src/server/journal/journal_applier.{h,cc}` (new, extracted from `replica.cc`) | MVP |
| End-of-log-aware global-command barrier | [tx_executor.cc](../src/server/journal/tx_executor.cc) (`MultiShardExecution`) | MVP |
| Always-on journal user; seal and rotate at the cut | [journal_slice.cc](../src/server/journal/journal_slice.cc), [journal.cc](../src/server/journal/journal.cc) | MVP |
| Atomic cut capture, fsync of base files, checkpoint mode | [snapshot.cc](../src/server/snapshot.cc), [save_stages_controller.cc](../src/server/detail/save_stages_controller.cc) | MVP |
| Startup precedence, INFO, `-MISCONF` gating | [server_family.cc](../src/server/server_family.cc), [main_service.cc](../src/server/main_service.cc) | MVP |
| Durability wait for `always` | [main_service.cc](../src/server/main_service.cc), connection reply flush | Fsync Policy |
| `AtomicGroupGuard` around multi-command transactions | [journal.h](../src/server/journal/journal.h), [transaction.cc](../src/server/transaction.cc) | Atomic Groups |
| Switch to `JournalApplier`, re-base after full sync | [replica.cc](../src/server/replica.cc) | Re-base |

---

## Phasing

1. **Log and replay** (MVP, part 1):
   - `AofStreamer` and `AofSegmentWriter`: parallel async writes, periodic sync, spare segments,
     rotation, `-MISCONF`.
   - Segment format.
   - Replay without a base, including the global-command barrier and the torn-tail rules.
   - Files are not bounded yet, so this phase is for development only.
2. **Checkpoints** (MVP, part 2):
   - Manifest, garbage collection, the automatic trigger, and `DEBUG AOF CHECKPOINT`.
   - Restart with a changed shard count.
   - File growth becomes bounded, and the MVP is complete.
3. **Fsync policy:** `always` with group commit and reply gating coalesced across a pipeline,
   and `no`.
4. **Atomic groups:** single-shard transaction and script atomicity.
5. **Re-base:** `CONFIG SET appendonly`, AOF on replicas, `DFLY LOAD` and `DEBUG RELOAD`.
6. **Optional:** multi-shard tail atomicity, direct I/O, sync markers, partial sync from AOF,
   repair tool.

---

## Testing

- **MVP unit tests** in `aof_test.cc`, alongside `journal_test.cc`:
  - block framing and CRC checks
  - out-of-order write completions advance `written_lsn` only over the contiguous prefix
  - blocks sealed in one proactor loop iteration are submitted as one write
  - a torn tail within the unsynced window gets truncated, including valid blocks after a hole
  - a bad block followed by valid data beyond the window is fatal
  - corruption before the last `max_unsynced_bytes` of a chain is fatal
  - stale blocks in a recycled segment are read as end of log, including stale blocks whose LSN is
    higher than the expected one
  - rotation keeps LSNs continuous
  - atomic manifest replacement
  - garbage collection of unreferenced files
- **MVP integration tests** in `tests/dragonfly/aof_test.py`, using the seeder and `capture()`:
  - Seeder load, then `kill -9`, then restart: the captures match, up to the one-second loss
    window.
  - Checkpoint under load, with a crash injected at each checkpoint step: after restart, the
    data matches.
  - A FLUSHALL in the middle of the log replays correctly through the barrier.
  - A FLUSHALL present in only some chains (the others truncated before it) replays without
    deadlock.
  - Restart with a different `--proactor_threads`: the data matches and a checkpoint happened.
  - Disk full (simulated with a small tmpfs) produces `-MISCONF`.
- **Later stages:**
  - `always`: after `kill -9`, every acknowledged write survives.
  - Atomic groups: `kill -9` in the middle of a large EVAL or MULTI/EXEC leaves the transaction
    applied either entirely or not at all. A group cut off by the tail is dropped whole.
  - Re-base: a replica with AOF enabled goes through full sync, re-base, then stable sync, and is
    killed. After restart it matches the master.
- **Benchmarks:**
  - memtier SET throughput and p99 latency, MVP sync against AOF off. Target: less than 10%
    regression.
  - Parallel vs. serialized write submission, on ext4 and on XFS.
  - `always` with pipelining, to show that group commit amortizes the sync cost.

---

## Open Questions

- **Default windows.** What should the defaults for `--aof_max_inflight_bytes` and
  `--aof_max_unsynced_bytes` be? A larger unsynced window means fewer forced syncs, but a larger
  region in which replay cannot tell corruption from a torn tail.
- **Blocking vs. dropping under backpressure.** Should `ThrottleIfNeeded` ever give up on AOF
  instead of stalling the shard, for example after a timeout? Or should it always block?
- **Group latency coupling** (atomic groups stage). Under `always`, an open atomic group holds
  back `durable_lsn`. Replies to unrelated writes on that shard then wait until the group closes,
  which can be milliseconds for a multi-hop MULTI/EXEC. Is that acceptable, or should
  interleaved records be kept out of open groups?
- **Multi-shard tail atomicity.** Should it be on by default, or behind a flag?
