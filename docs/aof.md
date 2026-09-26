# Sharded Append-Only File (AOF) Design

> **Status:** design proposal. Nothing described here is implemented yet.

This document proposes durable, log-based persistence built on the per-shard journal that
replication already uses. Each shard writes its own append-only log, avoiding a global
serialization point. Periodic DFS checkpoints keep the logs bounded.

## Table of Contents

- [Motivation](#motivation)

**Part 1: MVP**

- [Building Blocks We Reuse](#building-blocks-we-reuse)
- [Definitions](#definitions)
- [MVP Scope](#mvp-scope)
- [Architecture](#architecture)
- [On-Disk Layout](#on-disk-layout)
- [Write Path](#write-path)
- [I/O Mode](#io-mode)
- [Pure Async Disk I/O vs. Writer Fiber](#pure-async-disk-io-vs-writer-fiber)
- [Checkpoints: Bounding File Growth](#checkpoints-bounding-file-growth)
- [Replay at Startup](#replay-at-startup)
- [Corruption Scenarios Not Covered](#corruption-scenarios-not-covered)

**Part 2: Later Stages**

- [Fsync Policy](#fsync-policy)
- [Atomic Groups](#atomic-groups)
- [Re-base: Paths That Bypass the Journal](#re-base-paths-that-bypass-the-journal)
- [Multi-Shard Tail Atomicity](#multi-shard-tail-atomicity)
- [Other Extensions](#other-extensions)

**Part 3: Reference**

- [Configuration and Observability](#configuration-and-observability)
- [Implementation Map](#implementation-map)
- [Phasing](#phasing)
- [Testing](#testing)
- [Open Questions](#open-questions)

---

# Motivation

Today Dragonfly persists data only through point-in-time snapshots (RDB/DFS). A crash loses
every write made since the last snapshot. Valkey solves this with AOF, but its AOF is a single
global log. Dragonfly's shared-nothing architecture ([df-share-nothing.md](df-share-nothing.md))
lets each shard write its own log, so there is no global serialization point. Writing into
multiple files also maps well onto modern disks and filesystems that keep several I/O queues.

Goals:
- Configurable durability. On a healthy disk, periodic flushes limit write loss. A stricter
  configuration (`always`) makes every acknowledged write survive a crash.
- Nothing added to the write hot path that is shared across shards.
- Disk usage stays bounded without operator intervention.
- Recovery is parallel across shards, and therefore much faster than replaying a single global
  file.

Non-goals:
- The epoll backend. AOF requires io_uring and refuses to start with `--proactor_type=epoll`.
- Loading or producing Valkey-format AOF files.
- Tiered AOF storage (S3). The AOF is local only for now, keeping the metadata simple: a
  manifest and self-describing segment files, with no index of remote objects. In the MVP, only
  local saves become checkpoints. Using an S3 save as the base is an extension (see
  [Other Extensions](#other-extensions)).
- A hard bound on disk usage. Checkpoints keep the log proportional to the dataset, but nothing
  stops writes when the disk fills up. A full disk surfaces as metrics and, depending on the
  error policy, as failed write replies (see [Checkpoints](#checkpoints-bounding-file-growth)).
- Detecting media corruption of data that was already synced. Hardware corruption and crashes
  are both rare, so the design does not try to tell a corrupted block from a torn one. After a
  crash, replay keeps the longest valid prefix of each shard's log. See
  [Corruption Scenarios Not Covered](#corruption-scenarios-not-covered) for the failures this
  recovery rule does not protect against.

---

# Part 1: MVP

## Building Blocks We Reuse

- **The journal serializes each record once and fans it out.**
  `JournalSlice::AddLogRecord` ([journal_slice.cc](../src/server/journal/journal_slice.cc))
  serializes each record, assigns it a per-shard LSN, and hands it to every registered
  `JournalConsumerInterface` through `ConsumeJournalChange` and `ThrottleIfNeeded`. AOF
  becomes a third possible consumer after `ReplicaStreamer` and `SlotMigrationStreamer`.
- **Journal records replay independently.** Each shard's journal holds only that shard's part of
  a command. Global commands (FLUSHALL, FLUSHDB, FLUSHSLOTS) are the exception: they use a
  `MultiShardExecution` barrier keyed by txid (`DflyShardReplica::ExecuteTx` in
  [replica.cc](../src/server/replica.cc)).
- **Journal records are deterministic.** TTLs are journaled as absolute times (PEXPIREAT, PXAT).
  Expiry and eviction are journaled as `DEL`. Replaying the log hours later therefore produces
  the same state, provided replay does not expire keys on its own (see
  [Replay](#replay-at-startup), steps 2 and 4).
- **Snapshots are point-in-time per shard.** The cut happens at
  `SerializerBase::RegisterChangeListener`. Replication full sync registers the journal consumer
  at that same point ([snapshot.cc](../src/server/snapshot.cc)). This is exactly the
  "base + log from LSN L" cut that AOF checkpoints need.

---

## Definitions

Key terms. They are per shard unless stated otherwise.

- **Journal record:** one serialized write as produced by `JournalSlice`. The AOF stores these
  bytes unchanged.
- **LSN:** the journal's per-shard sequence number of a record. AOF LSNs are journal LSNs.
- **Block:** a 25-byte header with a CRC, followed by journal records. The unit of writing and
  of validation ([Segment format](#segment-format)).
- **Segment:** one AOF file of one shard: a header followed by blocks.
- **Chain:** the segments of one shard that replay reads, starting at the checkpoint's cut.
  Normally this is a single segment, because segments rotate only at a checkpoint cut and at
  restart, and a committed checkpoint deletes everything before its cut.
- **Spare segment:** a segment prepared in the background, so that rotation never waits on file
  creation.
- **`durable_lsn`:** the end of the prefix of records covered by a completed `fdatasync`.
- **Base:** a full snapshot of the dataset, taken at a checkpoint's cut.
- **Cut and cut LSN:** the point on each shard where the base's snapshot starts. Records before
  it are in the base. The cut LSN (`L_i`) is shard i's first record after it.
- **Checkpoint:** a committed base plus its per-shard cut LSNs. While AOF is on, a qualifying
  save (a local save; see [Checkpoints](#checkpoints-bounding-file-growth)) is a checkpoint.
- **Manifest:** the file naming the current base and each shard's cut. It is the source of
  truth, and is replaced atomically.
- **Global command:** FLUSHALL, FLUSHDB or FLUSHSLOTS. It runs on every shard, and replay pairs
  its copies across shards.
- **Torn tail and end of log:** after a crash, a chain can end in torn blocks and holes. Its end
  of log is the first invalid block. Replay keeps everything before it (the longest valid
  prefix) and discards the rest.

---

## MVP Scope

What the MVP provides:
- **Logging:** one append-only log per shard, fed by the journal.
- **Durability:** a fixed policy. Blocks are written as soon as they are sealed, and each shard
  runs `fdatasync` every `kFSyncMs` milliseconds (a hard-coded interval). Replies are never delayed.
  [Fsync Policy](#fsync-policy) adds the configurable `always` and `no` policies in a later stage.
  - **What a crash loses:** writes from approximately the last `kFSyncMs` milliseconds, plus
    data still queued or in flight.
  - **Why it is not a hard bound:** replies do not wait for the disk. A slow device can delay
    queued writes and the in-flight sync arbitrarily, and everything acknowledged in the
    meantime is at risk.
  - **What limits it:** `--aof_max_buffered_bytes` backpressure caps how much can be queued. It
    does not cap how long a stalled sync takes.
  - `aof_fsync_latency` and `aof_buffered_bytes` in `INFO persistence` show when the window
    grows.
- **Bounded disk usage:** automatic checkpoints. Every local save (SAVE, BGSAVE, scheduled
  saves) doubles as a checkpoint, so there is no second snapshot next to regular backups.
- **Recovery:** parallel replay at startup, including a changed shard count.
- **Errors:** a failed write or sync puts the shard into a not-durable state (subject to the
  error policy). Write failures clear once the failed ranges are rewritten and synced. Sync
  failures clear only after a successful checkpoint (see [Errors](#write-path)).

What the MVP guarantees after a crash:
- Each shard's log recovers to a prefix of what that shard wrote.
  - **Exception: global commands.** A FLUSHALL, FLUSHDB or FLUSHSLOTS that survives in any
    shard's log is applied on every shard (see [Replay](#replay-at-startup)). A shard whose log
    ends earlier is still flushed. That is well defined: a flush does not depend on the state
    before it.
- A multi-command transaction (MULTI/EXEC, EVAL) can be applied partially if the crash cut
  through its records. This is the same guarantee replication gives today: a replica can also
  receive a partial transaction when the master crashes mid-way. It is weaker than Valkey's AOF,
  which records MULTI/EXEC markers and drops an incomplete transaction when it loads a truncated
  file. [Atomic groups](#atomic-groups) close this gap for AOF loads in a later stage.
- Replay can apply a multi-shard transaction on some shards but not others.
  [Multi-Shard Tail Atomicity](#multi-shard-tail-atomicity) describes this limitation and a
  possible solution.

The MVP imposes these restrictions, which the later
[Re-base](#re-base-paths-that-bypass-the-journal) stage removes:
- AOF can only be enabled at startup with `--aof`. `CONFIG SET appendonly` is not supported.
- AOF is not supported on replicas: `--aof` together with `--replicaof`, and `REPLICAOF` while
  AOF is on, are rejected.
- `DFLY LOAD`, `DEBUG RELOAD` are rejected while AOF is on.

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

- **`AofStreamer`** is a new `JournalConsumerInterface`, one per shard (thread-local, like the
  journal slice) that turns journal records into blocks.
  - It first starts the journal on its shard (`journal::StartInThread()`), then registers with
    `journal::RegisterConsumer`. Registration only increments a user count; it does not start
    the journal. On a standalone node without replicas, journaling is off
    (`EngineShard::journal()` is null), so `StartInThread()` is required. The registration then
    holds the reference for as long as AOF is on, so `MaybeStop()` never stops the journal.
  - AOF does not use the replication ring buffer. That buffer and its retention policy stay
    unchanged. A later stage can drop it when AOF is on (see
    [Other Extensions](#other-extensions)).
- **`AofSegmentWriter`** owns the segment files: writes, syncs, rotation, and spare segments.
  It runs on the shard's own proactor and uses `fb2::LinuxFile` over io_uring.
  - Unlike a socket write, a file write specifies its offset, so sealed blocks can be submitted
    in parallel and still land in order (see [Write Path](#write-path)).
  - [Pure Async Disk I/O vs. Writer Fiber](#pure-async-disk-io-vs-writer-fiber) compares
    completion callbacks with a dedicated fiber for managing I/O.
- **AOF LSN = journal LSN.** There is no second sequence number. Checkpoints, replay, and any
  future partial sync from disk all use the same coordinates.
- **Shared apply logic.** Extract the global-command barrier and `JournalExecutor` logic from
  `DflyShardReplica::ExecuteTx` into a reusable `JournalApplier`. Replica stable sync and AOF
  replay both use this component.

---

## On-Disk Layout

All files live in `--aof_dir`, which defaults to `--dir` if empty and if `--dir` points
to a local file system. If `--dir` is remote (for example `s3://...`) and `--aof_dir` is empty,
AOF refuses to start.

```
<name>.manifest                       # source of truth; replaced atomically
<name>-base-<ckpt>-summary.dfs        # checkpoint base (regular DFS format)
<name>-base-<ckpt>-0003.dfs
<name>-0003-000017.aof                # shard 3, segment seq 17
```

### Segment format

- **Header.** The writer writes and syncs the header when preparing a spare, before writing any
  blocks. [Write Path](#write-path) describes spare preparation. The header contains:
  - magic `DFAOF1`, format version
  - shard_id, shard_count
  - segment seq
  - `segment_uid`: a random 64-bit id, generated each time a file is prepared as a spare. It
    seeds the block CRCs, so a block can only validate in the segment it was written to.
  - `reserved`: a 64-bit field, always 0 in this version. Writers set it to 0, and readers ignore
    it. A later feature can use it, such as a per-run `run_id`, without changing the header
    layout or the format version.
  - header CRC32c

  The header omits the start LSN and checkpoint id because neither is known when the writer
  prepares the spare. The first block's `first_lsn` identifies the segment's start LSN.
  The manifest records checkpoint boundaries using segment sequence numbers.
- **Body:** a sequence of blocks, each laid out as:

  ```
  [u64 len][u32 crc32c][u64 first_lsn][u32 n_records][u8 flags][payload]
  ```

  - The block header is 25 bytes. All integers are fixed-width and little-endian. `len` is the size
    of the whole block, header included, so a valid block has `len >= 25`. It is 64-bit, because a
    single journal record can exceed 4GB (a `SET` of a very large value), and a block holds whole
    records. `len == 0` therefore always means a hole or unwritten space, never a real block, even
    one with no records.
  - **Bounds.** A block is valid only if `len >= 25` and the `len` bytes fit in what remains of
    the file. Replay checks this before it reads or allocates anything. A torn `len` is thus
    just an invalid block, and a reader never allocates more than the file size.

  - `payload` is the concatenated `JournalWriter` bytes, i.e. exactly what is already in
    `JournalItem::data`. Nothing is serialized a second time.
  - **CRC.** `crc32c` is computed over the header's `segment_uid` (8 bytes, little-endian),
    followed by the block's `len`, `first_lsn`, `n_records`, `flags` and `payload`, in that
    order. The CRC field itself is excluded.
  - `flags` is reserved in the MVP (always 0). [Atomic groups](#atomic-groups) use one bit of it
    later.
  - Replay treats the log as ended at the first of: end of file; a block with `len == 0` (a hole
    or unwritten space reads as zeros); a block whose CRC does not validate; or a block whose
    `first_lsn` is not the expected next LSN.

### Manifest

The manifest is a small text file. To replace it atomically, the checkpoint process writes a
temporary file, fsyncs it, renames it over the current manifest, and fsyncs the directory.
The manifest contains:
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
2. Append the block to the current submission batch. Each flush point seals a block, often once
   per transaction. These blocks are typically only a few hundred bytes. Submitting each block
   separately would require one submission and one completion per block, plus one io-wq work
   item on ext4.

   The writer combines consecutive sealed blocks into one batch and submits it as an async
   write at the first block's offset when either:

   - it reaches `--aof_write_batch_bytes`, or
   - the current proactor loop iteration ends, i.e. before the proactor waits for events again
     (for example from a `ProactorBase::AddOnIdleTask` hook).

   Batching never waits for earlier writes to complete; it only groups blocks sealed close
   together in time. The batch buffer stays alive until its write completes.
3. Apply backpressure. The limit covers every AOF buffer that is not yet written: batches that are
   queued (for example while waiting for a spare segment), writes being retried, and writes in
   flight. If their total exceeds `--aof_max_buffered_bytes`, wait on an `EventCount` until it falls
   below the limit. Completion callbacks notify the `EventCount`. This applies backpressure in the
   same way as replication throttling in `BufferedSocketWriter`.

**Why parallel writes.** On a socket, the next write must wait for the previous one, so data has
to be buffered until then. A file write specifies its offset, so order is encoded in the offset
and writes do not need to be serialized.
- The logic is simpler: there is no pending buffer that waits for the previous write. The
  batching above only bounds the number of requests; it is not tied to completions.
- Several requests in flight let the kernel merge adjacent writes, and keep the device busy at a
  higher queue depth.
- [Buffered writes in io_uring](#buffered-writes-in-io_uring) explains how filesystem support
  and kernel writeback affect the parallelism that reaches the device.

**Completion tracking.** Writes can complete out of order.
- The writer tracks in-flight writes in a FIFO ordered by offset.
- Each completion marks its write as done. The writer advances `written_lsn` over the
  contiguous prefix of completed writes and releases their buffers.
- After a short write, the writer resubmits the remaining bytes at `offset + written`.

**Sync.** Every `kFSyncMs` milliseconds, if the active segment has completed writes that are
not synced yet:
1. Record `sync_target` as the end of the contiguous completed prefix *within the active
   segment*.
2. Issue an async `fdatasync` on the active segment. At most one sync is in flight per shard.
3. When it completes, advance `durable_lsn` (an atomic readable from any thread), and wake
   waiters.

An `fdatasync` covers only one file, and only writes that completed before it was issued.
That is why the target is per segment and based on completed writes, not on
`last_appended_lsn`.
- During a rotation, writes can be outstanding on both the old and the new segment. A sync of
  the new segment says nothing about the old one.
- `durable_lsn` therefore advances only over records whose own segment has been synced past
  them, and only across segments in order. It moves into the new segment only after the old
  segment's final sync (see Rotation).

**Clean shutdown.** On shutdown, the writer waits for its in-flight writes to complete and syncs
once. The log then ends at a block boundary, and replay has nothing to discard.

**Rotation.** A shard starts a new segment only at two points: at a checkpoint cut (see
[Checkpoints](#checkpoints-bounding-file-growth)), and when the log resumes after a restart. There
is no size-based rotation in the MVP. Between checkpoints, each shard appends to a single active
segment. The checkpoint trigger is what bounds that file's size.

When rotating:
- The next block goes into the spare segment, right after its header.
- Once every in-flight write to the old segment has completed, issue its final `fdatasync`, then
  close the segment. The sync is issued only after the writes drain, because an `fdatasync` does
  not cover writes that are still in flight.
- `durable_lsn` advances past a segment's records only after that segment's final sync.

**Spare segments.** A new segment's directory entry must be durable before any record in it is
reported durable. Otherwise, a power loss could remove the file and its acknowledged records.
Each shard therefore keeps one spare segment ready in the background:
1. Create an empty file under a temporary name (`<name>-<shard>.spare.tmp`).
2. Write the header (a new `segment_uid` and seq), and `fdatasync` the file.
3. Rename it to its final segment name, and `fsync` the directory.

The final name only ever points at a file whose header is already durable. If the node crashes
earlier, the file is left under its temporary name. The segment glob does not match that name,
so startup either deletes the file or reuses it as the next spare.

Apart from the header, no data is written while preparing a spare. There is no zero-filling and
no `fallocate` (see [I/O Mode](#io-mode)).

A header-only file is an **empty segment**. A crash can leave more than one:
- an unused spare;
- an active segment that had not received a block yet;
- several of them in a row, when a checkpoint cut rotates to the spare and the next spare is
  prepared before the first write.

Replay skips empty segments (see [Replay](#replay-at-startup)).

Rotation, including the switch at a checkpoint cut, only ever moves to a spare that is already
durable, so no directory fsync sits on the write path. Preparing a spare is only an open or a
rename, one header write and sync, a second rename, and a directory fsync. That work starts
right after a rotation, so it normally finishes long before the next one. If rotation ever
finds the spare not ready, sealed blocks accumulate and normal backpressure applies.

**Errors.** The writer handles write failures and sync failures differently.

**Write failures.** A successful later write does not repair an earlier failed range;
`written_lsn` cannot advance past it.
- The writer retains the failed write's batch buffer and retries at the same offset with backoff.
- While a retry is pending, write commands are rejected with `-MISCONF`.
- The error clears once the failed ranges are written, `written_lsn` is contiguous past them,
  and a sync covering them completes.

**Sync failures cannot be retried.** After a failed `fsync` or `fdatasync`, Linux may already have
marked the affected dirty pages clean or dropped them. A later sync can then succeed without
that data ever reaching the disk. PostgreSQL ran into this in 2018 ("fsyncgate"). By then the
batch buffers are already released, so the data cannot be rewritten from them either. A sync
failure therefore breaks the shard's chain:
- The shard's AOF enters a failed state. `durable_lsn` stops advancing, and write commands are
  rejected with `-MISCONF`.
- The only way out is a successful checkpoint. Its base is taken from memory, so it covers the
  lost data, and its cut starts a new chain. The checkpoint is scheduled right away.
- If the checkpoint also fails, the shard remains in the failed state.
  [Checkpoint health](#checkpoint-health) describes the failure metrics and retry behavior.

`INFO persistence` reports the AOF error state through `aof_last_write_status`.

---

## I/O Mode

The writer uses page-cache I/O for segment files (`O_WRONLY | O_CLOEXEC`, without `O_DIRECT`).

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

**What an append costs at sync time.** The MVP only ever appends to fresh segments. Each
`fdatasync` therefore also persists the new file size and extents: one filesystem journal commit
per sync. With the MVP's periodic sync per shard, that cost is negligible. Garbage collection
simply deletes old segments. Reusing them to avoid this cost only pays off with frequent syncs,
so it belongs to the [Fsync Policy](#fsync-policy) stage.

**Page cache.** AOF data is written once and almost never read, so it should not push other data
out of the page cache. Keeping it out also caps the dirty pages that count against a container's
cgroup memory limit.
- **Primary: `RWF_DONTCACHE`.** Segment writes carry the `RWF_DONTCACHE` flag in
  `sqe->rw_flags`. This is "uncached buffered I/O", merged in Linux 6.14
  ([Phoronix](https://www.phoronix.com/news/Uncached-Buffered-IO-Linux-6.14),
  [LWN](https://lwn.net/ml/all/20241203153232.92224-2-axboe@kernel.dk/)).
  - The write still goes through the page cache. Before the call returns, the kernel starts
    writeback of the written range (`filemap_dontcache_kick_writeback()`), and it drops the
    pages once that writeback completes.
  - It is not a durability flag. The periodic `fdatasync` is still needed to flush the device
    cache and the file size.
  - Because writeback has already started, much of the data may be on its way to disk by the
    time the sync runs, which could make the sync cheaper. The cost is that writeback starts
    per write, instead of being batched by the kernel's flusher. Both effects should be
    benchmarked.
  - A filesystem opts in with `FOP_DONTCACHE`. In current mainline, ext4 and XFS set it, and
    btrfs does not. On an unsupported filesystem, a DAX mapping, or a kernel older than 6.14,
    the write fails with `-EOPNOTSUPP`.
- **Fallback.** The writer detects `-EOPNOTSUPP` on its first write and then drops the flag for
  that file. After each completed sync, it issues `IORING_OP_SYNC_FILE_RANGE` (WRITE) followed by
  `IORING_OP_FADVISE` (DONTNEED) on the synced region.

**Checkpoint base files** keep using the existing O_DIRECT snapshot path. Those are large
sequential writes that are already aligned. The only change is a final `FSync()` before the
rename, since snapshot files are not fsynced today. While AOF is on, this applies to every
qualifying save, because it becomes the base (see [Checkpoints](#checkpoints-bounding-file-growth)).

---

## Pure Async Disk I/O vs. Writer Fiber

Both options use the parallel, offset-based write submission described in
[Write Path](#write-path). They share the on-disk format and sync semantics.
Option A uses completion callbacks to track completed writes, schedule syncs, and rotate
segments; option B uses a dedicated fiber. Option A can prepare spares through a callback chain
or a short-lived fiber; option B prepares them in its dedicated fiber.

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
  data. Its completion publishes `durable_lsn`. Only on the fallback path (no `RWF_DONTCACHE`),
  it also chains `sync_file_range` and `fadvise` for page-cache hygiene.
- **Rotation:** handled from the completion of the old segment's last write.
- **Buffers:** each block can be built in a registered buffer (`RequestRegisteredSlice`) and
  written with `WriteFixedAsync`, as `DiskStorage` does.
- **Constraint:** completion callbacks run in the proactor context, outside any fiber. They must
  never call a fiber-blocking function, such as `FSync()` or `Throttle()`.

Additional work this option needs:
- **helio `LinuxFile` API** ([uring_file.h](../helio/util/fibers/uring_file.h)):
  - An async fsync (`FSyncAsync(flags, cb)`). Today `FSync` is fiber-blocking only; it is built
    on `FiberCall`.
  - An `rw_flags` argument on `WriteAsync` and `WriteFixedAsync`, to pass `RWF_DONTCACHE`.
  - Async `sync_file_range` and `fadvise`, for the fallback when `RWF_DONTCACHE` is not
    supported.

  Each is a few lines on top of `GetSubmitEntry`. They are upstream helio changes, unless the
  AOF code issues the SQEs itself.
- **Spare-segment preparation:** preparing a spare takes a few steps (an open or a rename to a
  temporary name, a header write and `fdatasync`, a rename to the final name, then a directory
  fsync). That sequence can be a small callback chain, which also needs an async open and
  rename, or a short-lived background fiber started after each rotation.
- **Error and shutdown handling:**
  - Completion callbacks record errors. The write-command handler checks that status before
    accepting a command and returns `-MISCONF` when necessary.
  - Before closing a segment, the writer waits for all in-flight operations to finish. A
    pending-operation counter tracks them, as in `DiskStorage`.

Pros:
- No permanent fiber per shard.
- No fiber wakeup per completion or per sync.
- The model matches existing tiering code.

Cons:
- The helio additions above.
- Control flow is split across callbacks (completion tracking, rotation, spare switching,
  errors). That is harder to read and test than straight-line code.

### Option B: writer fiber

`ThrottleIfNeeded()` still submits block writes asynchronously. A dedicated fiber on each
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
- **Uncached writes:** `RWF_DONTCACHE` (Linux 6.14) goes in `sqe->rw_flags` of a buffered write.
  It combines with the paths above: the write completes inline or in io-wq as usual, and in
  both cases writeback starts before it completes. See [I/O Mode](#io-mode).

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

The helio additions (async fsync, `rw_flags` on async writes, and the fallback's
`sync_file_range` and `fadvise`) are the main cost. If those
changes would delay the MVP, option B is an acceptable interim step. The format and semantics
are identical, so moving from B to A later changes only `AofSegmentWriter`.

---

## Checkpoints: Bounding File Growth

Splitting the log into files does not limit disk usage. A checkpoint must first capture the
records' effects in a new DFS base before garbage collection can delete those records.
Each checkpoint records a cut LSN for every shard. After the checkpoint commits, garbage
collection can delete all segments before those cuts.

This is also why segments rotate only at the cut. The rotation puts everything before the cut in
older files, so garbage collection deletes whole files, and never has to trim the front of a live
file (with a hole punch or a rewrite). Rotating by size between cuts would free nothing earlier,
because nothing can be deleted before the next checkpoint anyway.

This is what Valkey's `BGREWRITEAOF` does as well. It never reads the old AOF. A forked child
dumps the in-memory dataset as the new BASE while the parent starts a new INCR file at the fork
point. Here, the snapshot's point-in-time cut takes the place of `fork()`.

### Saves and checkpoints are one operation

While AOF is on, there is no separate checkpoint mechanism. A checkpoint is simply a save whose
output can serve as the base. The goal is for every save to be one, and the MVP gets there for
local saves. This covers SAVE, BGSAVE, `--snapshot_cron`, and the automatic trigger. Whether a
save commits as a checkpoint depends only on where its output lives:

| Save | Output | Becomes the checkpoint? |
|---|---|---|
| Automatic trigger, shard-count change at restart | `--aof_dir` | Yes |
| SAVE, BGSAVE or `--snapshot_cron` to a local path on the same filesystem as `--aof_dir` | The user's path | Yes: its files are hard-linked into `--aof_dir` |
| A save to S3 or other cloud storage, or to a local path on another filesystem | The user's destination | No in the MVP: it stays a plain save. S3 saves can count in a later extension ([Other Extensions](#other-extensions)). |

- **Hard links.** The AOF owns its links. The user can move or delete their dump files without
  breaking recovery, and garbage collection deletes only the AOF's links, never the user's
  files. A save qualifies when its destination directory and `--aof_dir` are on the same
  filesystem (same `st_dev`).
- **Plain saves leave the AOF alone.** A save that does not qualify neither takes the AOF cut
  nor rotates the log: it runs exactly as it does without AOF. It does not commit a manifest,
  and it does not reset the automatic trigger. So such saves never add segments, and the chain
  stays a single segment in steady state. Once S3 saves become checkpoints (see
  [Other Extensions](#other-extensions)), they take the cut like local saves.
- **Fsync.** A qualifying save fsyncs its files, because a base must be durable. That adds some
  latency to those saves. Plain saves, and saves without AOF, are unchanged.
- **Formats.** Both DFS and RDB output work as a base. Both come from per-shard
  `SliceSnapshot`s, each with its own cut. The manifest records which format the base uses.
- **One save at a time.** This is the existing `SaveStagesController` rule, not an extra one.
  If an automatic checkpoint comes due while a save is running, it waits. If that running save
  qualifies, it becomes the checkpoint, and the automatic one is dropped.
- **Result reporting.** SAVE's reply reports the save itself. If the AOF part fails after the
  save succeeded (hard links, fsync, or the manifest), SAVE still reports success. The failure
  is reported through [checkpoint health](#checkpoint-health), and the next save or trigger
  retries.

### Triggers

- **Automatic:** the log size since the last base exceeds
  `max(--aof_rewrite_min_size, base_size * --aof_rewrite_percentage / 100)`. This is Valkey's
  `auto-aof-rewrite-*` rule. The defaults are 64MB and 100%.
- **Any qualifying save:** SAVE, BGSAVE, or `--snapshot_cron`. With scheduled saves, the AOF
  stays bounded without extra snapshots.
- **Manual:** a SAVE or BGSAVE to a qualifying destination. There is no separate checkpoint command.
  In the MVP, a deployment that saves only to S3 or to another filesystem has no manual checkpoint,
  and relies on the automatic trigger. `BGREWRITEAOF` is not implemented (see [Other
  Extensions](#other-extensions)).
- **Shard count change at restart:** see [Replay](#replay-at-startup), step 6.
- Later stages add more forced checkpoints; see
  [Re-base](#re-base-paths-that-bypass-the-journal).

### Flow for checkpoint C

1. At most one save runs at a time, through `SaveStagesController`.
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
3. Serialize with the existing `RdbSaver`/`SliceSnapshot` (`stream_journal = false`) to the
   save's destination: `<name>-base-C-*.dfs.tmp` in `--aof_dir`, or the user's path. Appending to
   the log continues the whole time.
4. `fsync` every file, rename it into place, and `fsync` the directory. For a save to the user's
   path, then hard-link each file into `--aof_dir` as `<name>-base-C-*`, and `fsync` `--aof_dir`.
5. Commit by writing the new manifest atomically.
6. Delete base C-1 (the AOF's own files or links, never the user's files), and every segment of
   shard i whose seq is below `cut_seq_i`.

### Crash safety

- **Crash before step 5:** the old manifest is still valid. Its segment chain continues without
  a gap from the old cut through L_i and onward, because nothing has been deleted.
- **Crash between steps 5 and 6:** at startup, files the manifest doesn't reference are
  garbage-collected.
- **Crash after a save to the user's path, before step 5:** the user's dump files exist and are
  valid. The AOF's links are not referenced by the manifest, so startup removes them.
- **Checkpoint failure:** the temp files are deleted and appending simply continues. The chain
  stays intact.

Disk usage is roughly `base + rewrite threshold + one in-progress base`, plus everything written to
the log while the checkpoint runs. That tail is kept, because it lies after the cut. A slow
snapshot under a high write rate can therefore push usage well past the threshold. The trigger
keeps the log proportional to the dataset, but it is not a hard bound on disk space (see
[Non-goals](#motivation)). Running out of space shows up as write errors and `-MISCONF`.

### Checkpoint health

Checkpoints limit log growth only while they succeed. A full disk can make checkpoints fail;
other saves can repeatedly prevent them from starting. In either case, active segments keep
growing. Size-based rotation cannot reclaim that space, so the server must report checkpoint
failures and delays:
- `INFO persistence` reports `aof_last_bgrewrite_status`, `aof_checkpoint_failures`
  (consecutive failures), and the log size next to the trigger size (`aof_current_size`,
  `aof_rewrite_trigger_size`).
- The server logs each checkpoint failure and its cause, then schedules a retry with backoff.

---

## Replay at Startup

When AOF is enabled and a manifest exists, the AOF takes precedence over `--dbfilename`, as it
does in Valkey.

**Bootstrap: AOF enabled but no manifest yet.** A missing manifest does not prove this is the
first start with `--aof`, because a manifest can also be deleted or lost. Bootstrap therefore
runs only when `--aof_dir` has no AOF files at all: no segments, base files, `.spare.tmp` or
`.discarded` files. If any exist without a manifest, startup fails loudly. Loading `--dbfilename`
then could silently replace a newer AOF.

On a real first start, the server loads its snapshot from `--dbfilename` as usual, if one
exists. That load goes through
`RdbLoader` and bypasses the journal, so the loaded data must become the AOF's first base:
- While still in LOADING, the server runs an initial checkpoint. It writes the first manifest,
  and its cut registers the `AofStreamer`s.
- The server switches to ACTIVE only after that checkpoint commits. With no snapshot, the base is
  simply empty.

1. **Enter LOADING.** Switch to `GlobalState::LOADING` using the existing `ServerFamily::Load`
   machinery. Clients get `-LOADING` until replay finishes.
2. **Load the base without expiring data.** Load base C through the existing snapshot load path, but
   add an AOF-replay mode that preserves keys and collection members even when their absolute
   deadlines are already past. Expiry remains suspended from base loading through tail replay;
   otherwise a tail command that ran before the deadline can observe a different base state. Base
   files are not tied to shards; each key goes to whichever shard owns it.
3. **Validate the segment chains.** For each shard i:
   - Glob its segments and read their headers.
   - Starting at `cut_seq_i`, require that segment seqs are contiguous.
   - Skip empty segments (a valid header but no valid first block; see
     [Write Path](#write-path)) wherever they appear.
   - **LSN continuity.** The first non-empty segment must start at the manifest's `cut_lsn_i`.
     Each later non-empty segment must start where the previous non-empty segment's valid
     records end. The check spans empty segments, so skipping them cannot hide lost records.
   - A missing segment file (a gap in seqs) is a hard error. It points to an operator or
     filesystem problem, not to a crash.
   - An LSN discontinuity means an earlier segment ended early, for example because a crash
     during rotation tore its tail while the next segment already had blocks on disk. The chain
     then ends at that point (step 5).
   - At startup, empty segments at the end of the chain are reused as the next spare, or
     deleted.
4. **Replay each chain in its own fiber.** There is one fiber per source-shard chain, all
   running in parallel. They are distributed over the current proactor pool (chain i on proactor
   `i % pool size`), as replica flows are in `replica.cc`. A source shard id can exceed the
   current pool size after the shard count shrinks.
   - Read blocks and verify their CRCs.
   - Skip records with LSN < L_i. Derive each record's LSN from the block's `first_lsn` using
     the existing LSN tracking in `TransactionReader`.
   - Apply records through `JournalApplier` (`JournalExecutor` → `Service::DispatchCommand`).
     Keys are routed to their owning shard, so the current shard count may differ from the one
     that wrote the files.
   - **Expiry is suspended during replay.** Keys are neither lazily nor actively expired, and a
     command with an absolute expiry that is already in the past sets that expiry instead of
     deleting the key. Valkey does the same while loading its AOF.
     - Without this, replay after the deadline diverges. `SET k 0 PXAT T; INCR k` ran before
       `T`. Replayed after `T`, the SET would delete `k`, and INCR would recreate it without a
       TTL.
     - Expirations that really happened are already in the log as `DEL` records, so replay
       loses nothing by not expiring.
     - Normal expiry resumes after replay, and removes keys whose deadline has passed.
   - **Why this is correct:** all of a key's records live in a single source shard's log, in
     order. Per-key ordering is therefore preserved. This is the same argument that makes
     replication between masters and replicas with different shard counts correct.
   - **Global commands** (FLUSHALL, FLUSHDB, FLUSHSLOTS) meet at the `MultiShardExecution`
     barrier, keyed by txid. The barrier's participant count is not fixed at `shard_count`. It
     is the number of chains that have not reached end of log.
     - **The txid alone is enough.** Txids restart in every process, and a chain can span
       restarts. But global commands run on every shard in one global order, so every chain holds
       the same sequence of them. Each barrier waits for all live chains, so chains pass their
       global commands in lockstep, and a completed entry is erased. A reused txid can therefore
       never pair the wrong commands. The case that breaks lockstep, a chain that ended early,
       forces a checkpoint (step 6).
     - A chain that reaches end of log counts as having arrived at every pending and future
       barrier. Reaching end of log also wakes fibers already waiting at a barrier.
     - Without this, a truncated tail can deadlock replay. Chain A may have a FLUSHALL on disk
       while chain B's log ends before that command, so the fibers applying the FLUSHALL would
       wait forever for B.
     - **Why applying it is correct:** the global command did run on every shard. If B's log ends
       before that command, every surviving record of B comes before it in B's order. Applying
       the flush after B is exhausted therefore matches what happened.
     - This is the one exception to "each shard recovers to a prefix" (see
       [MVP Scope](#mvp-scope)).
     - The checkpoint cut is itself a global transaction, so no chain can start after a global
       command that another chain still has before its cut.
5. **Find the end of the log: keep the longest valid prefix.** Within a segment, replay reads
   blocks until the first invalid position: end of file, `len == 0`, a CRC mismatch, or a
   `first_lsn` that is not the expected LSN. At a segment's end, it continues with the next
   non-empty segment, if that segment starts at the expected LSN. Otherwise, the chain ends.
   - **Why the tail can have holes.** Data written after the last completed sync can reach the
     disk in any order. Writes are submitted in parallel, and page-cache writeback does not follow
     file order either. After a crash, the end of a chain can therefore contain zeroed or stale
     regions and torn blocks, followed by blocks that did make it to disk.
   - **What is kept.** Replay applies every valid record before the chain's end.
   - **What is discarded.** Everything after the end: valid blocks after a hole, and any later
     segments of the chain.
   - **Before resuming:**
     - Truncate the segment containing the end of the valid log at that position. Rename later
       segments with a `.discarded` suffix and retain them until the next successful checkpoint.
     - This keeps stale valid blocks from ever being read again.
     - These changes must themselves survive a crash. So `fsync` the truncated file and the
       directory before any new write is accepted. Otherwise a second crash could bring the
       discarded files back.
     - The resumed segment reuses the seq of the first discarded segment. That keeps seqs
       contiguous, so the next startup does not see a seq gap (step 3).
     - The warning log and `INFO persistence` (`aof_load_discarded_bytes`) report how much each
       shard discarded.
   - **No classification.** Replay treats torn tails and corruption of synced data alike.
     [Corruption Scenarios Not Covered](#corruption-scenarios-not-covered) explains the resulting
     data-loss risks.
6. **Resume the log.** The files must describe exactly the state replay produced. Otherwise a
   second crash replays them differently.
   - **A strict prefix needs only truncation.** When every chain replayed a prefix of its own
     log, truncating at each chain's end (step 5) is enough.
   - **A global command completed by an exhausted chain needs a checkpoint.** Say replay applied
     a FLUSH from shard A after shard B's log had already ended.
     - If B's log simply resumed, the FLUSH would exist only in A's log. After new writes on B
       and another crash, B would replay those writes first, reach its end of log, and A's old
       FLUSH would erase them.
     - So replay runs a checkpoint while still in LOADING. That writes a new base, and new chains
       that start after the FLUSH.
     - The case needs a crash that tore a flush on some shards but not others. It is rare enough
       that the cost of a full checkpoint at startup is acceptable.
     - A cheaper alternative, not chosen for the MVP: append the missing global command, with its
       original txid, to each shorter chain before resuming it.
   - **Same shard count:** on each shard, call `journal::StartInThreadAtLsn(last_lsn_i + 1)`,
     open a new segment at that LSN, and only then register `AofStreamer`. Registering it this
     late keeps replayed records from being appended to the log again.
   - **Shard count changed** (the manifest's `shard_count` differs from the current one): the
     old chains cannot be continued. Run a checkpoint while still in LOADING. Its cut registers
     the new `AofStreamer`s, and the server switches to ACTIVE only after that.
7. **Finish.** Run `PerformPostLoad` and `ForceReplicasToFullSync()`, then switch to ACTIVE.

---

## Corruption Scenarios Not Covered

Replay handles crash damage by recovering a valid prefix: it stops at torn blocks, holes left
by reordered writeback. It uses the same rule for corruption
of previously synced data, without distinguishing the cause. At the first invalid block,
replay ends that shard's log and discards the remaining chain.

This rule leaves the following scenarios unprotected:

- **Media corruption of synced data** (bit rot, a bad sector, a firmware bug).
  - Replay truncates at the damaged block.
  - Every later record of that shard is lost. Under `always`, that includes writes that were
    acknowledged as durable.
  - The loss is reported only through the load warning and `aof_load_discarded_bytes`.
- **Damage in the middle of the log, not just at the tail.** A damaged block early in an old
  segment discards all later segments of that chain, not just the damaged block. There is no
  skipping ahead to the next readable block.
- **Cross-shard inconsistency after a truncation.** Other shards replay their full logs, so the
  dataset combines a shorter prefix for one shard with full state for the others. A multi-key
  operation that spanned the damaged shard is then partially applied. This goes beyond the
  crash case in [Multi-Shard Tail Atomicity](#multi-shard-tail-atomicity), because the lost
  records may be arbitrarily old.
- **Corruption that keeps a valid CRC.** A damaged block that still matches its CRC32C is
  undetected, about 1 in 2^32 per damaged block. Replay applies it as data: wrong values, or
  commands that fail. A damaged `len` that still passes can make replay skip over or misread
  the following blocks.
- **Devices that do not honor flushes.** Examples are volatile write caches without power-loss
  protection, and some virtualized or network storage. `fdatasync` can then return before the
  data is durable, and a power loss removes synced data. Replay finds a shorter valid prefix,
  and acknowledged writes are lost without any error.
- **Lost or misdirected writes.** A write that the device reports as done but never lands, or
  lands at another offset, leaves a hole in synced data. Replay ends the shard's log at the hole.
- **Corruption outside the segments.**
  - Base DFS files and the manifest are not covered by this design. Damage there is caught, if
    at all, by the existing snapshot loader's checks or by the manifest failing to parse. Either
    case fails the load.
  - Filesystem metadata damage that removes a segment file shows up as a seq gap, which is a
    hard error (step 3).
- **Operator errors.** Partially copied directories and files mixed in from another node are
  caught only in part: by `shard_id`, `shard_count`, `segment_uid`, seq and LSN checks. For
  example, a whole chain copied from an older backup of the same node passes all of these checks.
- **Transaction ids reused across restarts.** This is not corruption, but a related replay gap.
  Txids restart at 1 in every process, and a chain spans restarts.
  - The MVP is not affected: it pairs only global commands by txid, and those pass through replay
    in lockstep (see [Replay](#replay-at-startup), step 4).
  - Pairing that is not in lockstep is affected, such as
    [Multi-Shard Tail Atomicity](#multi-shard-tail-atomicity). A shard that got no writes after
    a restart still has records from the previous run in its tail. A new-run transaction on
    another shard with the same txid could then be matched with one of them.
  - A per-run `run_id` in each segment header (the `reserved` field), with pairing by `(run_id,
    txid)`, partially solves this: it separates runs, but only at segment granularity. Starting the
    txid counter past the highest txid seen during replay would make txids unique across the AOF's
    lifetime.

What can be added later, if any of these turn out to matter:
- A durable-offset record in each segment header (two alternating slots holding the last synced
  offset), so replay can report damage before that offset as corruption instead of truncating.
- Fixed framing, as in the LevelDB/RocksDB WAL, so replay can skip a damaged frame and continue.
- A strict load mode, and `dfly-aof-check`, that fail or report instead of truncating.
- At the infrastructure level: filesystems with data checksums (ZFS, btrfs), storage with
  power-loss protection, and replicas.

---

# Part 2: Later Stages

## Fsync Policy

This stage replaces the MVP's fixed `kFSyncMs` sync interval with the configurable `--aof_fsync`
policy.
The server also accepts Valkey's `appendfsync` name for CONFIG compatibility.

| mode | writer behavior | client visibility | loss window |
|---|---|---|---|
| `always` | Group commit: one `fdatasync` covers every write completed before it was issued. While it is in flight, new writes keep being submitted, and the next sync covers them. | The reply is held until `durable_lsn[s] >= L_s` on every shard the transaction touched. | No acknowledged writes |
| `everysec` (default) | The MVP behavior. | Replies are never delayed. | About 1s plus in-flight data |
| `no` | No periodic sync. The kernel decides when to flush, optionally smoothed with `sync_file_range`. Syncs still happen at checkpoint cuts, rotation, and shutdown. | Replies are never delayed. | Up to OS writeback |

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

**Sync failure under `always`.** When a sync fails, `durable_lsn` never reaches the targets of
replies that are already held. Without special handling, those connections would wait forever,
while new commands get `-MISCONF`.
- The failure transition wakes every durability waiter on that shard.
- A woken connection whose target was not reached discards its buffered success replies. It
  replies with an error instead, so the client does not take the write as durable.
  - The error says the write was applied in memory but not persisted.
  - For a pipeline, the connection replaces every reply from the first unconfirmed write onward.
  - If it cannot rewrite the buffered replies, it closes the connection.
- Later writes get `-MISCONF` until a checkpoint restores the chain (see [Errors](#write-path)).
- For comparison, Valkey handles an fsync failure under `appendfsync always` by exiting the
  process. We can offer that too, as a `--aof_exit_on_sync_error` flag, but the default keeps
  serving reads.

`INFO persistence` adds `aof_delayed_fsync`.

### Segment recycling

Under `always`, a shard syncs many times per second. Appending to a fresh file makes each sync
also commit file-size and extent metadata, which adds latency to every group commit. This stage
can reuse garbage-collected segments instead, the way PostgreSQL recycles WAL segments:
- A shard renames one of its own obsolete segments to the spare's temporary name, and writes a
  new header with a new `segment_uid`.
- Writes stay within the file's existing size and allocated blocks, so `fdatasync` flushes data
  only.
- Stale blocks left in the file were written under a different `segment_uid`, so they fail their
  CRC and read as end of log. For the same reason, an empty segment can also be a recycled file
  whose first block does not validate.
- This should be benchmarked first. With a single segment per checkpoint interval, the recycled
  file is often smaller than needed, and appending past its end brings the metadata cost back.

---

## Atomic Groups

A multi-command transaction on one shard (MULTI/EXEC, EVAL, FCALL) writes several journal
records. Today nothing prevents a block boundary from falling between them. `DisableFlushGuard`
is used only around expiry and eviction, not around EXEC or script execution. This stage marks
transaction boundaries explicitly:

- **Guard.** A multi-command transaction opens a `journal::AtomicGroupGuard` when it starts
  executing on a shard and closes it after its last hop on that shard. Each shard tracks the
  number of open guards to support nesting.
- **Sealing inside a group.** `AofStreamer` still seals blocks while a group is open. Otherwise a
  large script would pin memory, and throttling would deadlock, because only sealed blocks can
  be written.
  - Blocks sealed inside a group carry the `GROUP_CONT` bit in `flags`: the block ends inside a
    group that continues in the next block.
  - `JournalSlice::AddLogRecord` calls `ThrottleIfNeeded()` right after each record, while the
    guard is still open. So the transaction's last record is sealed with `GROUP_CONT` as well.
  - To end the group, closing the guard seals the open block with the bit cleared. If that block
    is empty, because the last record was already sealed, it writes a group-end block instead:
    no records, `GROUP_CONT` cleared. Its `len` is 25, the header alone, so replay does not
    mistake it for a hole.
  - Without this explicit end, a group whose last block is followed by no further record would
    never close. `durable_lsn` could then not advance past it, and an `always` reply could wait
    forever.
- **Replay.** Replay buffers records from `GROUP_CONT` blocks until it reads the closing block,
  then applies the complete group. If a torn tail removes the closing block, replay discards
  the entire group.
  - The chain's end then moves back to the group's first block, and the file is truncated there
    (see [Replay](#replay-at-startup), step 6).
  - Otherwise the group's blocks would stay on disk, new records would follow them after the
    restart, and the next replay would find a group that never closes.
- **Interleaved records.** Records of unrelated transactions on the same shard can land inside an
  open group, for example between the hops of a lock-ahead MULTI. They share the group's
  durability: they are applied with it, or dropped with it. Per-key order is unchanged.
- **Durability.** `durable_lsn` only advances to the end of a block without `GROUP_CONT`.
  Otherwise, under `always`, a write could be acknowledged and then dropped with an incomplete
  group.

With this stage, single-shard transactions and scripts become atomic across a crash, which
matches what Valkey's AOF provides for MULTI/EXEC. The change is small (a guard, one flag bit,
and buffering in replay), so it is a good candidate for the first stage after the MVP.

**Scope: durability, not execution.** Groups make a transaction's *durability* all-or-nothing,
not its *execution*.
- If a command inside an EXEC fails at run time, the commands before it have already been
  applied and journaled. OOM is one example.
- The group then records exactly what ran, and replay reproduces the same partial result.
- That is correct: the log reflects what the node executed. The same holds for replication
  today.
- Groups matter only when loading the AOF after a crash that takes down the whole replication
  group. A live replica already has the same prefix the master executed. Replay does not apply
  a group whose end did not reach the disk.
- Partial transactions are a general Dragonfly issue, not an AOF-specific one. A replica can end
  up with a partial transaction when the master crashes mid-way. The group boundaries this stage
  adds to the journal could later let replicas hold back an incomplete transaction too. That
  would be a separate change to replication.

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
- **Other `RdbLoader`-driven loads:** `DFLY LOAD`, `DEBUG RELOAD`.

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
- During replay, hold back multi-shard transactions at the tail until every other chain has
  shown its part or reached end of log.
- This needs transaction ids that are unique across restarts, because tails are not in lockstep
  (see [Corruption Scenarios Not Covered](#corruption-scenarios-not-covered)).
- Drop the incomplete ones. Only the tail of the log can contain them.
- Dropping a transaction's part leaves that part on disk in the middle of a chain. As with a
  global command completed by an exhausted chain, the files no longer match the replayed state.
  So this case also forces a checkpoint before ACTIVE (see [Replay](#replay-at-startup),
  step 6).

---

## Other Extensions

- **`--aof_direct_io`** for very high-throughput workloads. The writer would keep the partial
  tail page in memory and pad only at sync points. With O_DIRECT, parallel writes also map
  directly to device queue depth. This is worth doing only if benchmarks show that double
  buffering in the page cache hurts.
- **Size-based rotation** (`--aof_segment_max_bytes`), if a later feature needs fixed-size
  segments. For example, partial sync of replicas from AOF segments may want them. It does not
  affect disk usage (see [Checkpoints](#checkpoints-bounding-file-growth)).
- **Partial sync for replicas from AOF segments, replacing the ring buffer.** AOF LSNs are
  journal LSNs, so a reconnecting replica's LSN points directly into the AOF chain.
  - With AOF on, partial sync can read the missing records from the segments instead of from the
    in-memory replication ring buffer. The ring buffer can then be dropped, which saves its
    memory (by default about maxmemory / shard count / 200 per shard).
  - It also extends the partial-sync window from seconds of retention to everything since the
    last checkpoint.
  - The cost is reading from disk on reconnect instead of from memory. Serving a replica could
    also delay deleting segments that the replica still needs; garbage collection would have to
    account for connected replicas.
- **`dfly-aof-check --fix`**, a repair tool for corrupted segments.
- **Corruption detection** for data that was already synced; see
  [Corruption Scenarios Not Covered](#corruption-scenarios-not-covered).
- **S3 saves as checkpoints.** A completed save to S3 commits the manifest with the base
  object's URI, and resets the AOF like a local save.
  - **Replay:** startup loads that base through the existing S3 snapshot loader, then replays the
    local segments as usual. The save path already finishes the upload before it returns, so the
    manifest never references an incomplete object.
  - **Retention requirement:** the AOF cannot own an S3 object the way it owns a hard link. The S3
    object that the manifest references must therefore be kept until the next checkpoint
    replaces it. Garbage collection never deletes S3 objects, and bucket lifecycle rules or
    retention scripts must not remove that object.
  - **Failure mode:** recovery depends on reaching S3 at startup. If the referenced base cannot
    be fetched (network, credentials, or a deleted object), startup fails loudly and names the
    missing object, instead of starting with partial data.
  - **Alternative:** tee the save stream into a local base file in `--aof_dir` as well. That is
    one serialization to two destinations. Recovery stays local and the AOF owns its base, at
    the cost of local disk space and write bandwidth for a second copy.
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
| `--aof_rewrite_percentage` | 100 | MVP | Auto-checkpoint growth factor |
| `--aof_rewrite_min_size` | 64MB | MVP | Auto-checkpoint minimum size |
| `--aof_write_batch_bytes` | TBD | MVP | Maximum size of one submitted batch of sealed blocks |
| `--aof_max_buffered_bytes` | TBD | MVP | Backpressure threshold per shard: queued, retrying and in-flight write buffers |
| `--aof_fsync` | `everysec` | Fsync Policy | `always` / `everysec` / `no`; alias `appendfsync` |
| `--aof_exit_on_sync_error` | `false` | Fsync Policy | Exit on a sync failure under `always`, as Valkey does |

Commands:
- No new command in the MVP. SAVE and BGSAVE trigger checkpoints.
- `CONFIG SET appendfsync` ([Fsync Policy](#fsync-policy))
- `CONFIG SET appendonly` ([Re-base](#re-base-paths-that-bypass-the-journal))

`INFO persistence` gains these fields:
- **MVP, global:**
  - `aof_enabled`, `aof_rewrite_in_progress`, `aof_last_bgrewrite_status`,
    `aof_checkpoint_failures`
  - `aof_current_size`, `aof_base_size`, `aof_rewrite_trigger_size`
  - `aof_last_write_status`
- **MVP, per shard:** `aof_buffered_bytes`, `aof_unsynced_bytes`, `aof_throttle_usec`,
  `aof_fsync_latency`, `aof_load_discarded_bytes` (bytes discarded at the last load)
- **Fsync Policy stage:** `aof_delayed_fsync`

Base files store each shard's cut LSN in an `aof-cut-lsn` aux field. This field replaces
`aof-preamble`, which currently always contains 0. The loader uses the manifest as the
authoritative source; the aux field provides diagnostic information only.

---

## Implementation Map

| Area | Files | Stage |
|---|---|---|
| `AofStreamer`, `AofSegmentWriter`, segment reader, manifest | `src/server/journal/aof.{h,cc}` (new) | MVP |
| Async fsync, `rw_flags` on async writes, fallback `sync_file_range` and `fadvise` (option A) | `helio/util/fibers/uring_file.{h,cc}` | MVP |
| Shared apply logic for replica and AOF replay | `src/server/journal/journal_applier.{h,cc}` (new, extracted from `replica.cc`) | MVP |
| End-of-log-aware global-command barrier | [tx_executor.cc](../src/server/journal/tx_executor.cc) (`MultiShardExecution`) | MVP |
| Always-on journal user; seal and rotate at the cut | [journal_slice.cc](../src/server/journal/journal_slice.cc), [journal.cc](../src/server/journal/journal.cc) | MVP |
| Atomic cut capture on every save, fsync, hard links for qualifying saves, manifest commit | [snapshot.cc](../src/server/snapshot.cc), [save_stages_controller.cc](../src/server/detail/save_stages_controller.cc) | MVP |
| Startup precedence, INFO, `-MISCONF` gating | [server_family.cc](../src/server/server_family.cc), [main_service.cc](../src/server/main_service.cc) | MVP |
| Durability wait for `always` | [main_service.cc](../src/server/main_service.cc), connection reply flush | Fsync Policy |
| `AtomicGroupGuard` around multi-command transactions | [journal.h](../src/server/journal/journal.h), [transaction.cc](../src/server/transaction.cc) | Atomic Groups |
| Switch to `JournalApplier`, re-base after full sync | [replica.cc](../src/server/replica.cc) | Re-base |

---

## Phasing

1. **MVP: log, checkpoints and replay.** The log is only useful together with checkpoints, so
   they ship as one stage:
   - `AofStreamer` and `AofSegmentWriter`: parallel async writes, periodic sync, spare segments,
     rotation at the cut, `-MISCONF`.
   - Segment format and manifest.
   - Checkpoints: the cut on qualifying saves, hard links, garbage collection, the automatic
     trigger, and checkpoint-health reporting.
   - Replay, including the global-command barrier, the torn-tail rules, and restart with a
     changed shard count.
2. **Fsync policy:** `always` with group commit and reply gating coalesced across a pipeline,
   and `no`.
3. **Atomic groups:** single-shard transaction and script atomicity.
4. **Re-base:** `CONFIG SET appendonly`, AOF on replicas, `DFLY LOAD` and `DEBUG RELOAD`.
5. **Optional:** multi-shard tail atomicity, size-based rotation, direct I/O, partial sync from
   AOF, repair tool.

---

## Testing

- **MVP unit tests** in `aof_test.cc`, alongside `journal_test.cc`:
  - block framing and CRC checks, including the exact CRC byte range
  - a `len` below 25, or one that does not fit in the remaining file, makes the block invalid,
    with no large allocation
  - during a rotation with writes outstanding on both segments, `durable_lsn` does not advance
    into the new segment before the old segment's final sync
  - buffers queued while waiting for a spare count against `--aof_max_buffered_bytes`
  - a crash right after the replay truncation and renames does not bring discarded segments back,
    and the resumed segment keeps seqs contiguous
  - global commands from different runs that reuse a txid are paired correctly, because chains
    pass global commands in lockstep
  - out-of-order write completions advance `written_lsn` only over the contiguous prefix
  - blocks sealed in one proactor loop iteration are submitted as one write
  - a torn tail gets truncated at the first invalid block, including valid blocks after a hole
  - a crash during rotation that tears the old segment's tail, while the new segment already has
    blocks, ends the chain at the tear; the later segment is renamed `.discarded`
  - a clean shutdown leaves nothing to truncate
  - `aof_load_discarded_bytes` reports what each shard discarded
  - empty segments (valid header, no valid first block) are skipped anywhere in a chain,
    including an empty active segment followed by an unused spare right after a checkpoint cut
  - LSN continuity is checked across empty segments, starting from the manifest's `cut_lsn`
  - a crash during spare preparation leaves only a `.spare.tmp` file, which replay ignores and
    startup cleans up
  - a failed write is retried at its offset; `-MISCONF` clears only after the range is written
    and synced
  - a failed sync is never retried as proof of durability; the shard stays failed until a
    checkpoint starts a new chain
  - rotation at a checkpoint cut and at restart keeps LSNs continuous
  - a failed checkpoint leaves a valid chain from the old cut, across the extra segment it
    started
  - atomic manifest replacement
  - garbage collection of unreferenced files
- **MVP integration tests** in `tests/dragonfly/aof_test.py`, using the seeder and `capture()`:
  - Seeder load, then `kill -9`, then restart: the captures match, up to the one-second loss
    window.
  - Checkpoint under load, with a crash injected at each checkpoint step: after restart, the
    data matches.
  - A BGSAVE to a local path on the same filesystem becomes the checkpoint. Deleting the user's
    dump file afterwards does not break replay.
  - A save to S3 (in the MVP), or to another filesystem, stays a plain save: no cut, no rotation,
    no manifest commit, and the automatic trigger is not reset. The shard keeps a single segment.
  - An automatic checkpoint that comes due during a qualifying BGSAVE is dropped, and that
    BGSAVE commits as the checkpoint.
  - With `--snapshot_cron`, the AOF stays bounded with no extra snapshots.
  - Checkpoints forced to fail repeatedly: the log keeps growing, `aof_checkpoint_failures`
    counts up, and the next successful checkpoint reclaims the space.
  - A FLUSHALL in the middle of the log replays correctly through the barrier.
  - A FLUSHALL present in only some chains (the others truncated before it) replays without
    deadlock, and forces a checkpoint before ACTIVE.
  - Crash twice in a row, with a FLUSHALL torn on some shards the first time and new writes in
    between: after the second restart, the writes made after the first restart survive.
  - Restart with a different `--proactor_threads`, both larger and smaller: the data matches and
    a checkpoint happened.
  - A missing manifest with AOF files still present in `--aof_dir` makes startup fail, instead
    of loading `--dbfilename`.
  - A key set with `PXAT` and then modified, replayed after its deadline: the key comes back
    with its TTL and then expires, instead of being recreated without a TTL.
  - First start with `--aof` and an existing `--dbfilename` snapshot: the initial checkpoint
    makes the snapshot data the first base, and a later restart from the AOF alone matches.
  - `--aof` on a standalone node with no replicas: records reach the AOF, i.e. the journal is
    started.
  - Disk full (simulated with a small tmpfs) produces `-MISCONF`.
- **Later stages:**
  - `always`: after `kill -9`, every acknowledged write survives.
  - `always` with an injected sync failure: held replies are woken and answered with an error,
    and no connection hangs.
  - Atomic groups: `kill -9` in the middle of a large EVAL or MULTI/EXEC leaves the transaction
    applied either entirely or not at all. A dropped group is truncated away, so a second crash
    after new writes does not find an unclosed group. A group cut off by the tail is dropped whole.
    A group that is the last write before an idle period still closes, and an `always` reply to it
    returns.
  - Re-base: a replica with AOF enabled goes through full sync, re-base, then stable sync, and is
    killed. After restart it matches the master.
- **Benchmarks:**
  - memtier SET throughput and p99 latency, MVP sync against AOF off. Target: less than 10%
    regression.
  - Parallel vs. serialized write submission, on ext4 and on XFS.
  - `RWF_DONTCACHE` vs. the `sync_file_range` + `fadvise` fallback: throughput, `fdatasync`
    latency, and page-cache footprint.
  - `always` with pipelining, to show that group commit amortizes the sync cost.

---

## Open Questions

- **Defaults.** What should the defaults for `--aof_write_batch_bytes` and
  `--aof_max_buffered_bytes` be?
- **Blocking vs. dropping under backpressure.** Should `ThrottleIfNeeded` ever give up on AOF
  instead of stalling the shard, for example after a timeout? Or should it always block?
- **Group latency coupling** (atomic groups stage). Under `always`, an open atomic group holds
  back `durable_lsn`. Replies to unrelated writes on that shard then wait until the group closes,
  which can be milliseconds for a multi-hop MULTI/EXEC. Is that acceptable, or should
  interleaved records be kept out of open groups?
- **Multi-shard tail atomicity.** Should it be on by default, or behind a flag?
