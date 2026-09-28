# Sharded Append-Only File (AOF) Design

> **Status:** design proposal. Nothing described here is implemented yet.

This document proposes durable, log-based persistence for Dragonfly, built on the per-shard
journal that replication already uses. Each shard writes its own append-only log, and periodic
snapshots (checkpoints) keep the logs bounded.

The document has two parts:
- **[Part 1: High-Level Design](#part-1-high-level-design)** describes the approach, the
  guarantees and the staging. It is the part to agree on first.
- **[Part 2: Detailed Design](#part-2-detailed-design)** collects the low-level mechanics:
  formats, ordering rules and crash handling. It is intentionally incomplete. Each section ends
  with the edge cases that are known but not yet resolved.

## Table of Contents

**Part 1: High-Level Design**

- [Motivation](#motivation)
- [Goals and Non-Goals](#goals-and-non-goals)
- [Definitions](#definitions)
- [Building Blocks We Reuse](#building-blocks-we-reuse)
- [Architecture](#architecture)
- [Write Path](#write-path)
- [Checkpoints](#checkpoints)
- [Replay](#replay)
- [MVP Guarantees and Restrictions](#mvp-guarantees-and-restrictions)
- [Later Stages](#later-stages)
- [Phasing](#phasing)

**Part 2: Detailed Design**

- [On-Disk Format](#on-disk-format)
- [Writer](#writer)
- [I/O Mode](#io-mode)
- [Checkpoint Protocol](#checkpoint-protocol)
- [Replay Protocol](#replay-protocol)
- [Corruption Scenarios Not Covered](#corruption-scenarios-not-covered)
- [Later Stages in Detail](#later-stages-in-detail)
- [Configuration and Observability](#configuration-and-observability)
- [Implementation Map](#implementation-map)
- [Testing](#testing)
- [Open Questions](#open-questions)

---

# Part 1: High-Level Design

## Motivation

Today Dragonfly persists data only through point-in-time snapshots (RDB/DFS). A crash loses
every write made since the last snapshot. Valkey solves this with AOF, but its AOF is a single
global log. Dragonfly's shared-nothing architecture ([df-share-nothing.md](df-share-nothing.md))
lets each shard write its own log, so there is no global serialization point. Writing into
multiple files also maps well onto modern disks and filesystems that keep several I/O queues.

## Goals and Non-Goals

Goals:
- **Configurable durability.** The MVP loses at most a short window of writes on a crash. A later
  stage adds an `always` mode in which every acknowledged write survives a crash.
- **No cross-shard state on the write hot path.**
- **Bounded log size without operator intervention.** The log is reduced periodically subject to
  implementation policies.
- **Fast recovery.** Shards replay their logs in parallel.

Non-goals:
- The epoll backend. AOF requires io_uring to allow async writes without throttling the request flow.
- Loading or producing Valkey-format AOF files.
- Remote or tiered AOF storage (S3). The AOF is local only.
- A hard bound on disk usage. A full disk surfaces through metrics and the error policy.
- Detecting media corruption of data that was already synced (see
  [Corruption Scenarios Not Covered](#corruption-scenarios-not-covered)).

## Definitions

Terms are per shard unless stated otherwise.

- **Journal record:** one serialized write as produced by `JournalSlice`. The AOF stores these
  bytes unchanged.
- **LSN:** the journal's per-shard sequence number of a record.
- **Block:** a header with a CRC followed by one or more journal records. The unit of writing
  and of validation.
- **Segment:** one AOF file of one shard: a header followed by blocks.
- **Chain:** the segments of one shard that replay reads, starting at the checkpoint's cut.
  Normally a single segment.
- **Spare segment:** a segment prepared in the background, so that rotation never waits on file
  creation.
- **`durable_lsn`:** the end of the prefix of records covered by a completed `fdatasync`.
- **Base:** a full snapshot of the dataset, taken at a checkpoint's cut.
- **Cut LSN (`L_i`):** shard i's first record after the base's point-in-time cut.
- **Checkpoint:** a committed base plus its per-shard cut LSNs.
- **Logical time:** the clock a record's transaction originally ran with
  (`Transaction::time_now_ms_`, the time its deadline checks used). Replay runs each record at its
  logical time.
- **Manifest:** the file naming the current base and each shard's cut. Replaced atomically.
- **Global command:** a journaled global transaction (`CO::GLOBAL_TRANS`) that writes a record
  on every shard: FLUSHALL, FLUSHDB, FLUSHSLOTS, FT.CREATE, FT.ALTER, FT.DROPINDEX and
  FT.SYNUPDATE. A `CO::NO_AUTOJOURNAL` global transaction that records itself on one shard only,
  such as MOVE, is *not* a global command for replay.

## Building Blocks We Reuse

- **The journal serializes each record once and fans it out.** `JournalSlice::AddLogRecord`
  ([journal_slice.cc](../src/server/journal/journal_slice.cc)) assigns each record a per-shard
  LSN and hands it to every registered `JournalConsumerInterface`. AOF becomes a third consumer,
  after `ReplicaStreamer` and `SlotMigrationStreamer`.
- **Journal records replay independently per shard.** Global commands (FLUSHALL, FT.CREATE, ...)
  are the exception. Replicas already pair them across shards with a barrier
  (`DflyShardReplica::ExecuteTx` in [replica.cc](../src/server/replica.cc)).
- **Journal records are almost deterministic.** TTLs are journaled as absolute times, and expiry
  and eviction are journaled as `DEL`. Commands still compare deadlines with their transaction's
  clock, though: `SET ... PXAT T` or `PEXPIREAT` with an elapsed `T` deletes the key. A log
  replayed hours later reproduces the original state only if each record runs at its original
  time (see [Replay](#replay)).
- **Snapshots are point-in-time per shard.** Replication full sync already starts a journal
  consumer exactly at the snapshot's cut ([snapshot.cc](../src/server/snapshot.cc)). That is
  the "base + log from LSN L" model AOF checkpoints need.

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

- **`AofStreamer`** is a new journal consumer, one per shard. It batches journal records into
  blocks.
- **`AofSegmentWriter`** owns the shard's segment files. It issues writes and syncs through
  io_uring on the shard's own proactor.
- **AOF LSN = journal LSN.** There is no second sequence number.
- **`JournalApplier`** is the apply logic, extracted from `DflyShardReplica::ExecuteTx`. Replica
  stable sync and AOF replay both use it.

All files live in `--dir`, next to the regular snapshot files:

```
appendonly.manifest                   # source of truth: path of the base and per-shard cuts
dump-*.dfs                            # base: a regular snapshot, referenced by path
appendonly-base-<ckpt>-*.dfs          # base written by an automatic checkpoint (AOF-owned)
appendonly-<shard>-<seq>.aof          # segment files, one chain per shard
```

## Write Path

1. The shard appends each journal record to an in-memory **block**.
2. The block is **sealed** when it reaches about 8KB, or at the latest on the shard's heartbeat
   (10ms by default). Each sealed block becomes one async write at a fixed file offset. Writes
   go through the page cache and may be in flight in parallel.
3. Every shard runs `fdatasync` on its log periodically (every second in the MVP). Replies are
   never delayed.
4. **Backpressure.** If the shard's unwritten AOF buffers exceed `--aof_max_buffered_bytes`,
   the shard throttles, as it does for a slow replica.
5. **Errors.** A failed write or sync puts the shard into a reduced-durability state. How that
   state is surfaced depends on the error policy. By default, the server keeps serving, and
   metrics report the reduced durability until a successful checkpoint fixes it.

Details: [Writer](#writer) and [I/O Mode](#io-mode).

## Checkpoints

A log only stays bounded if old records can be deleted, and they can be deleted only once a newer
snapshot contains their effects. A **checkpoint** is such a snapshot (the **base**) together with
the LSN at which it was cut on each shard.

- **Every local save is a checkpoint.** While AOF is on, SAVE, BGSAVE, `--snapshot_cron` and the
  automatic trigger all produce a checkpoint. There is no separate rewrite command. Saves to S3
  or other cloud storage stay plain saves in the MVP.
- **The manifest references the base by path.** A user's save stays an ordinary dump file,
  under the user's own retention policy. Automatic checkpoints write AOF-owned bases instead.
- **Rotation at the cut.** Each shard starts a new segment exactly at the checkpoint's cut. Once
  the new manifest is committed, garbage collection deletes every older segment as whole files,
  and the old base if the AOF created it.
- **Automatic trigger.** A checkpoint runs when the log outgrows
  `max(--aof_rewrite_min_size, base_size * --aof_rewrite_percentage / 100)`, as in Valkey.

This plays the role of Valkey's `BGREWRITEAOF`: the new base comes from memory, not from the old
log, and the snapshot's point-in-time cut replaces `fork()`.

Details: [Checkpoint Protocol](#checkpoint-protocol).

## Replay

When AOF is enabled and a manifest exists, startup loads the base that the manifest names, and
ignores `--dbfilename` for loading. (`--dbfilename` still names the files that saves write, so
bases are usually regular dumps.) Startup then proceeds as follows:

1. Enter `LOADING`.
2. Load the base.
3. Replay each shard's segment chain from its cut LSN, all shards in parallel, through
   `JournalApplier`. Keys are routed to their current owner, so the shard count can change
   between runs.
4. Global commands synchronize across the chains with a barrier.
5. Each chain recovers its **longest valid prefix**. Whatever follows the first invalid block is
   discarded and truncated.
6. Resume logging, or run a fresh checkpoint when the old chains cannot simply be continued (for
   example, after a shard-count change).
7. Enter `ACTIVE`.

Replay runs each record at its original **logical time**, persisted with the record, and
background expiry stays off until replay ends. A replayed command therefore sees the state it
originally saw. Suspending expiry alone is not enough, because commands such as `SET ... PXAT`
delete a key whose deadline has passed.

On the first start with `--aof` there is no manifest yet. The server loads `--dbfilename` as
usual, and that dump becomes the first base (adopted directly, or through an initial
checkpoint), because the load bypasses the journal.

Details: [Replay Protocol](#replay-protocol).

## MVP Guarantees and Restrictions

**Loss window.** Replies do not wait for the disk, so the window is best effort, not a hard bound:
- A **process crash** (segfault, OOM kill, `kill -9`) loses the open block (about one heartbeat
  of writes) and sealed blocks whose writes had not completed. Completed writes are in the page
  cache and survive.
- An **OS crash or power loss** loses about the last second of writes, plus anything still queued
  or in flight.
- A slow device can stretch both windows. Metrics show when that happens.

**Consistency after a crash:**
- Each shard recovers a prefix of what it wrote. Global commands are the exception: one that
  survives on any shard is applied on all of them.
- Transactions are not atomic across a crash. A MULTI/EXEC or script can be applied partially,
  and a multi-shard transaction can be applied on some shards only. Replication gives the same
  guarantee today. Later stages close these gaps.

**Restrictions,** lifted by later stages:
- AOF can be enabled only at startup (`--aof`), not through `CONFIG SET`.
- Not supported on replicas. `DFLY LOAD` and `DEBUG RELOAD` are rejected.
- Default namespace only.

## Later Stages

- **[Fsync policy](#fsync-policy).** Configurable `--aof_fsync`: `always` (group commit;
  replies wait until durable), `everysec` (the MVP behavior) and `no`.
- **[Atomic groups](#atomic-groups).** Mark single-shard transaction boundaries in the log, so
  replay applies a MULTI/EXEC or script either whole or not at all.
- **[Re-base](#re-base).** Support the paths that change data without going through the journal
  (enabling AOF at runtime, replicas, `DFLY LOAD`) by taking a checkpoint after them.
- **[Multi-shard tail atomicity](#multi-shard-tail-atomicity).** Hold back a multi-shard
  transaction at the tail until every participating shard's part is known to be present.
- **[Other extensions](#other-extensions).** Direct I/O, partial replica sync from AOF segments,
  S3 saves as checkpoints, a repair tool.

## Phasing

1. **MVP:** log, checkpoints and replay. The log is useless without checkpoints, so they ship
   together.
2. **Fsync policy:** `always` with group commit, and `no`.
3. **Atomic groups.**
4. **Re-base:** `CONFIG SET appendonly`, AOF on replicas, `DFLY LOAD` and `DEBUG RELOAD`.
5. **Optional:** multi-shard tail atomicity, size-based rotation, direct I/O, partial sync from
   AOF, repair tool.

---

# Part 2: Detailed Design

This part records the mechanics worked out so far. It is a working draft: any of it can change
during implementation, and it does not try to resolve every case. Each section ends with an
**Open edge cases** list. A newly found edge case belongs on that list; it does not need a full
solution in this document.

## On-Disk Format

### Directory

AOF files live in `--dir`, next to the regular snapshot files. Local saves are written to `--dir`
as well (`SaveStagesController::BuildFullPath`), so every local save can serve as a base. AOF
requires a local `--dir`; with a remote one (`s3://...`) it refuses to start. A separate
`--aof_dir` is a later extension (see [Other extensions](#other-extensions)).

### Segment header

Written and synced when the segment is prepared as a spare, before any block:
- magic `DFAOF1`, format version
- `shard_id`, `shard_count`, segment `seq`
- `segment_uid`: a random 64-bit id per prepared file. It seeds the block CRCs, so a block
  validates only in the segment it was written to.
- `reserved` (64 bits, always 0), for a later field such as a per-run id
- header CRC32C

The start LSN is not in the header, because it is unknown when the spare is prepared. The first
block's `first_lsn` gives it.

### Blocks

```
[u64 len][u32 crc32c][u64 first_lsn][u32 n_records][u8 flags][payload]
```

- The header is 25 bytes, fixed-width little-endian. `len` covers the whole block, header
  included.
- `payload` is the concatenated `JournalItem::data` of the block's records. Each record is
  self-contained (its own `SELECT` prefix and opcode, even for PING), so replay decodes records
  one by one and counts LSNs from `first_lsn`.
- Each record also needs its logical time: the transaction's `time_now_ms_`, carried into the
  journal entry. `JournalItem::time_ms` is not a substitute: `JournalSlice::CallOnChange` sets it
  from the wall clock, later than the transaction's clock. How the block carries the time is an
  open edge case below.
- `crc32c` covers `segment_uid`, then `len`, `first_lsn`, `n_records`, `flags` and `payload`.
- `flags` is 0 in the MVP. Atomic groups use one bit later.
- A block is valid only if `len >= 25`, it fits in the rest of the file, its CRC matches, and
  `first_lsn` is the expected next LSN. A zero `len` (a hole or unwritten space) is invalid.
  Replay checks the bounds before reading or allocating.

### Manifest

A small text file, replaced atomically (write temp file, fsync, rename, fsync the directory). It
contains:
- format version and `shard_count`
- current checkpoint id, the base's path (relative to `--dir`) and format (DFS or RDB), and
  whether the base is AOF-owned
- the cut's logical time, `cut_time_ms`
- per shard: `cut_seq` (the first segment to read) and `cut_lsn`
- a `bootstrapping` state for the very first start (see [Bootstrap](#bootstrap))

Segments describe themselves, so rotation never touches the manifest. Only a checkpoint commit
does.

Base files store each shard's cut LSN in an `aof-cut-lsn` aux field, replacing the always-zero
`aof-preamble`, and the cut's logical time in an `aof-cut-time` aux field. Replay takes the cuts
from the base itself, so a base is never paired with the wrong cuts:
- A save that reuses a file name renames its new dump over the old base before it commits the
  manifest. After a crash in between, the old manifest points at the new dump. Replay still uses
  that dump's own cuts, and the chain covers them, because nothing was deleted yet.
- A dump written before AOF was enabled (see [Bootstrap](#bootstrap)) has no aux fields. The
  manifest's `cut_lsn` and `cut_time_ms` apply to it.

**Open edge cases.**
- **Encoding logical time.** Options: a `base_time_ms` in the block header plus a varint delta
  per record (which requires per-record framing in the payload), or a time field in the journal
  record itself (which also changes the replication format).
- Very large records. A single journal record can be gigabytes (`max_bulk_len` bounds each
  argument, not the record). Decide whether replay streams a block or the AOF caps record size.
- Format versioning and upgrade rules, for both segments and the manifest.
- The exact manifest grammar.
- The block size (8KB `kAofBlockBytes`) needs benchmarking.

---

## Writer

### Appending and sealing

- `ConsumeJournalChange(item)` runs on the shard thread, possibly inside an atomic section, and
  must not preempt. It appends the record's bytes to the open block.
- `ThrottleIfNeeded()` runs after records, where the journal allows a flush (outside
  `DisableFlushGuard`). It seals the open block if it reached `kAofBlockBytes`, and applies
  backpressure. It does not seal on every call; that would produce one block per record.
- **The heartbeat seals small blocks.** `EngineShard::Heartbeat()` seals whatever is open, at
  its very top, before the checks that make it skip its other work. A small block therefore
  waits at most one heartbeat (`1000 / --hz` ms). Sealing on every loop iteration would instead
  issue tiny writes that rewrite the same page over and over.
- **Sealing** assigns the block the next file offset and submits it as one async write, without
  waiting for earlier writes. The buffer lives until the write completes.

Parallel writes are safe because a file write carries its own offset; order is encoded in the
offset, not in submission order. They also let the kernel merge adjacent writes.

This mirrors Valkey's `everysec`, which writes its buffer on every event-loop iteration and runs
`fsync` once per second: process crashes are far more common than machine crashes.

### Completion tracking and backpressure

- In-flight writes sit in a FIFO ordered by offset. Completions can arrive out of order;
  `written_lsn` advances over the contiguous prefix of completed writes, and their buffers are
  released.
- A short write is resubmitted for the remaining bytes.
- Backpressure counts every AOF buffer that is not yet written: queued (for example while waiting
  for a spare), being retried, or in flight. Above `--aof_max_buffered_bytes`, the shard waits on
  an `EventCount` that completions notify, like replication throttling in
  `BufferedSocketWriter`.

### Sync

Every `kFSyncMs` (1s), if the active segment has completed but unsynced writes:
1. Set `sync_target` to the end of the contiguous completed prefix *within the active segment*.
2. Issue an async `fdatasync`. At most one is in flight per shard.
3. On completion, advance `durable_lsn` (atomic, readable from any thread) and wake waiters.

An `fdatasync` covers one file, and only writes completed before it was issued. So during a
rotation, `durable_lsn` enters the new segment only after the old segment's final sync.

### Rotation and spare segments

A shard rotates only at a checkpoint cut and when the log resumes after a restart. There is no
size-based rotation.

- The next block goes into the spare, right after its header.
- Once all writes to the old segment have completed, the writer issues its final `fdatasync` and
  closes it.
- **Spare preparation**, in the background right after each rotation: create
  `<name>-<shard>.spare.tmp`, write and `fdatasync` the header, rename it to its final segment
  name, and `fsync` the directory. A final name thus only ever points at a file whose header is
  durable, and no directory fsync sits on the write path.
- No zero-filling and no `fallocate` (see [I/O Mode](#io-mode)).
- If rotation finds the spare not ready, sealed blocks queue up under normal backpressure.
- A header-only file is an **empty segment**. A crash can leave several in a row (an unused spare,
  an active segment that never got a block). Replay skips them.

### Clean shutdown

Seal and submit the open block, wait for in-flight writes, sync once. The log ends at a block
boundary, and replay has nothing to discard.

### Error handling

A failed write or sync puts the shard into a **reduced-durability state**: `durable_lsn` stops
advancing, and the shard's log can no longer be trusted to hold everything after that point.
- **Write failure.** The writer keeps the block buffer and retries at the same offset with
  backoff. A later successful write does not repair the hole, so the state lasts until the
  failed range is written and a sync covers it, or until a checkpoint.
- **Sync failure.** A failed `fsync` cannot be retried: Linux may already have dropped the dirty
  pages, so a later sync can succeed without the data ("fsyncgate", PostgreSQL 2018). Only a
  successful checkpoint whose cut is *after* the failure fixes it, because its base comes from
  memory. A new checkpoint is scheduled immediately, or right after the one in progress.
- **Error policy.** How the state is surfaced to clients is a policy choice. By default the
  server keeps serving reads and writes, and reports the state through metrics and
  `INFO persistence` (`aof_last_write_status`). Stricter policies are possible, for example
  rejecting writes until the state clears.

**Open edge cases.**
- Which error policies to offer (keep serving, reject writes, exit), and which one is the
  default under `always`, where serving writes that cannot become durable breaks the contract.
- Should backpressure ever give up (for example after a timeout) instead of stalling the shard?
- Unsynced data that already left AOF's buffers sits in the page cache, and is bounded only by
  the kernel's dirty-page throttling (`vm.dirty_*`, cgroup writeback). Decide whether that is
  enough.
- Write retries have no deadline. Define when a persistent error (ENOSPC, EIO) stops retrying,
  and what happens next.
- Disk full: a checkpoint needs space too, so it may not be able to recover the shard.
- `--hz <= 0` disables the heartbeat (`GetPeriodicCycleMs`), and with it the sealing of small
  blocks. Either require a positive `--hz` with AOF, or give AOF its own flush timer.
- Verify that no `DisableFlushGuard` section can span a heartbeat, so the heartbeat never seals
  inside one.
- Cap io-wq workers (`IORING_REGISTER_IOWQ_MAX_WORKERS`) so that a stalled disk does not spawn
  many kernel threads.

---

## I/O Mode

Segments use page-cache I/O (`O_WRONLY | O_CLOEXEC`, no `O_DIRECT`).

- **Why not O_DIRECT.** Blocks are often a few hundred bytes. With 4K alignment, every write
  would either rewrite the partial tail page (write amplification, and a torn rewrite can destroy
  durable bytes) or pad to 4K (wasted space). O_DIRECT also still needs `fdatasync`. A direct I/O
  mode remains a later option.
- **No preallocation.** Zero-filling halves effective bandwidth. `fallocate` has latency and
  leaves unwritten extents whose conversion is a metadata change anyway. Each `fdatasync` then
  also commits the new file size, which is negligible at one sync per second.
- **Page cache hygiene.** AOF data is written once and almost never read. Writes use
  `RWF_DONTCACHE` (Linux 6.14+,
  [LWN](https://lwn.net/ml/all/20241203153232.92224-2-axboe@kernel.dk/)): writeback starts at once
  and the pages are dropped after it completes. It is not a durability flag. If a write returns
  `-EOPNOTSUPP`, retry it without the flag, disable the flag for later writes, and after each sync
  use `sync_file_range` followed by `fadvise(DONTNEED)` on the synced range.
- **Base files** keep the existing O_DIRECT snapshot path, plus a final `FSync()` before the
  rename. Snapshot files are not fsynced today.

### Buffered writes in io_uring

- Buffered writes complete inline on filesystems with `FOP_BUFFER_WASYNC` (XFS, btrfs). On ext4
  they are punted to io-wq, which serializes writes to one inode. The submitting thread never
  blocks either way, but the achievable parallelism depends on the filesystem.
- `fsync`, `sync_file_range` and `fallocate` always run in io-wq.
- For buffered I/O, the device queue depth is set by writeback. Parallel submission mainly cuts
  per-write latency.

### Async I/O vs. writer fiber

Both options submit writes the same way and share format and semantics. They differ only in where
the control logic (completion tracking, syncs, rotation) lives.

| | Option A: pure async (recommended) | Option B: writer fiber |
|---|---|---|
| Control logic | io_uring completion callbacks, as in tiering's `DiskStorage` | A permanent fiber per shard |
| Sync | `AddPeriodic` issues an async `fdatasync` | Fiber-blocking `FSync()` |
| Spare preparation | A short-lived fiber | Inline in the fiber |
| helio changes | Async fsync, `rw_flags` on async writes, async `sync_file_range` / `fadvise` | None |
| Cost | Logic split across callbacks, which must never block | A fiber stack per shard, and a wakeup per completion batch |

Option A avoids a permanent fiber and matches existing code. If the helio additions would delay
the MVP, option B is an acceptable interim step: moving from B to A changes only
`AofSegmentWriter`.

**Open edge cases.**
- Benchmark `RWF_DONTCACHE` against the fallback: throughput, sync latency and page-cache
  footprint.
- Benchmark parallel against serialized submission on ext4 and XFS.

---

## Checkpoint Protocol

### Qualifying saves

A save becomes the checkpoint depending only on where its output lives:

| Save | Checkpoint? |
|---|---|
| Automatic trigger, shard-count change at restart | Yes: an AOF-owned base, `<name>-base-<ckpt>-*` |
| SAVE, BGSAVE, `--snapshot_cron` to a local file (in `--dir`) | Yes: the manifest references the dump by path |
| S3 or other cloud storage | No, it stays a plain save |

- **User dumps stay the user's.** The AOF never deletes or renames a user's dump. It only
  requires that the dump the manifest references is kept until the next checkpoint. If it is
  missing at startup, replay fails loudly and names the file. `INFO persistence` shows the
  current base (`aof_base_file`), so operators know which dump to keep.
- **AOF-owned bases** come from automatic checkpoints. Writing them under the AOF's own name
  keeps automatic checkpoints from piling up dumps under a timestamped `--dbfilename`. Garbage
  collection deletes an AOF-owned base once a newer checkpoint commits.
- **Plain saves leave the AOF alone.** No cut, no rotation, no manifest commit, and no reset of
  the automatic trigger.
- **Fsync.** A qualifying save fsyncs its files, since a base must be durable.
- **Formats.** DFS and RDB both work as a base.
- **One save at a time** (the existing `SaveStagesController` rule). An automatic checkpoint that
  comes due during a qualifying save is dropped; the running save becomes the checkpoint.
- **Result reporting.** If the save succeeds but its manifest commit fails, SAVE still reports
  success. The failure shows up in [checkpoint health](#checkpoint-health).

### Flow for checkpoint C

1. Take the cut as a *global* transaction, like SAVE, so every shard orders global commands the
   same way relative to it. On each shard, with no preemption in between:
   `RegisterChangeListener`, `L_i = journal::GetLsn()`, and
   `aof_streamer.SealAndRotate(start_lsn = L_i, ckpt = C)`. Every record below `L_i` is in the
   base; every record from `L_i` on is in the new segment or later ones.
2. Serialize with the existing `RdbSaver` / `SliceSnapshot` (`stream_journal = false`). Logging
   continues meanwhile.
3. `fsync` each file, rename it into place, `fsync` the directory. A failed `fsync` fails the
   save before its rename, so an existing dump with the same name, possibly the current base,
   stays intact.
4. Commit the new manifest atomically.
5. Delete every segment of shard i with `seq < cut_seq_i`, and base C-1 if it is AOF-owned. User
   dumps are never deleted.

### Crash safety

- **Before the commit (step 4):** the old manifest stays valid, and its chain continues through
  the cut without a gap, because nothing was deleted.
- **After the commit:** startup deletes AOF files the manifest does not reference (segments and
  AOF-owned bases), never user dumps.
- **A failed checkpoint** deletes its temp files; logging simply continues.

Disk usage is roughly `base + trigger threshold + one base in progress`, plus the log written
while the checkpoint runs. A slow snapshot under heavy writes can push usage well past the
threshold.

### Checkpoint health

Checkpoints bound the log only while they succeed. `INFO persistence` reports
`aof_last_bgrewrite_status`, `aof_checkpoint_failures` (consecutive), `aof_current_size` and
`aof_rewrite_trigger_size`. Each failure is logged, and retried with backoff.

**Open edge cases.**
- The user deletes or moves the dump that is the current base. Replay fails at the next start.
  Decide whether the server should also detect this at runtime (for example with a periodic
  `stat`) and take a new checkpoint.
- A save that reuses a DFS name renames the summary and the shard files one by one. A crash in
  between can leave a mix of old and new files, which the loader rejects on a snapshot-id
  mismatch. Reused DFS names need generation-specific file names, or an atomic commit of the
  whole file set, before such a save can become the base.
- An older dump restored over the base's path has cuts below the chain's first segment. Replay
  must reject it as a gap, not replay a partial log onto it.
- Frequent plain saves (for example S3-only backups) can keep delaying the automatic checkpoint.
- A sync failure while a checkpoint is in progress: that checkpoint's cut is too early to clear
  the failed state, so a follow-up checkpoint is needed. The scheduling needs a precise rule.
- The cost of the global cut transaction on a busy server.

---

## Replay Protocol

### Bootstrap

A missing manifest does not prove this is the first start, because a manifest can be lost.
- Bootstrap runs only when `--dir` has no AOF files at all. AOF files without a manifest
  make startup fail, since loading `--dbfilename` could silently replace a newer AOF.
- Bootstrap first writes a manifest in the `bootstrapping` state. Finding that state at startup
  means an interrupted first start: delete the other AOF files and bootstrap again.
- The server loads `--dbfilename` (if present). That load bypasses the journal, so without a
  base the next restart would replay only later writes onto an empty dataset. The loaded
  snapshot therefore becomes the first base, in one of two ways:
  - **Adopt the dump (preferred).** Commit a manifest that references the loaded dump by path.
    No write happens before the load finishes, so the cut is the journal's first LSN on every
    shard, recorded as the manifest's `cut_lsn`. The load's time is recorded as `cut_time_ms`,
    so a later replay loads the dump as of that time and drops exactly the keys this load
    dropped. This avoids a full snapshot at startup.
  - **Initial checkpoint (alternative).** Take a checkpoint while still in `LOADING`, if adoption
    turns out to be impractical.
  - **No dump:** the base is empty, and the manifest simply has none.
- Either way, the `AofStreamer`s register before any write, and the server switches to `ACTIVE`
  only after the manifest commits.
- After bootstrap, `--dbfilename` is never used for loading again. The manifest names the base.

### Steps

1. **Enter LOADING** through the existing `ServerFamily::Load` machinery.
2. **Load base C as of its cut time.** The loader decides what is expired against the base's
   `aof-cut-time`, not the wall clock, so it keeps exactly the keys and members that were live at
   the cut. Background expiry stays off until the tail replay ends. Base keys go to whichever shard
   owns them now.
3. **Validate each chain.** Starting at `cut_seq_i`, segment seqs must be contiguous; a gap is a
   hard error. Empty segments are skipped wherever they appear. The first non-empty segment must
   start at or before the base's cut `L_i`, and each later one where the previous one's valid
   records end. An LSN discontinuity ends the chain there (step 5).
4. **Replay each chain in its own fiber,** all in parallel. Chain i runs on proactor
   `i % pool size`, as replica flows do. Records below `L_i` are skipped. Records are applied
   through `JournalApplier` (`JournalExecutor` → `Service::DispatchCommand`).
   - Per-key order is preserved, because all of a key's records live in one chain. The same
     argument makes replication across different shard counts correct.
   - **Each record runs at its logical time.** The replayer pins the transaction clock
     (`Transaction::time_now_ms_`, which `DbContext::time_now_ms` exposes) to the record's
     persisted time, instead of the wall clock.
     - Suspending expiry is not enough. `SET ... PXAT` with an elapsed deadline deletes the key
       (`string_family.cc`), and so does `PEXPIREAT` (`DbSlice::UpdateExpire`), regardless of
       `expire_allowed_`. Replayed after `T` on the wall clock, `SET k 0 PXAT T; INCR k` would
       leave `k` without a TTL.
     - With the pinned clock, lazy expiry during replay also happens exactly where it could have
       happened originally. Real expirations are in the log as `DEL` anyway.
     - Active (background) expiry runs on the wall clock, so it stays off until replay ends.
5. **Find each chain's end of log** (see [End of log](#end-of-log)).
6. **Resume the log** (see [Resuming the log](#resuming-the-log)).
7. **Finish.** Run `PerformPostLoad` and `ForceReplicasToFullSync()`, then switch to `ACTIVE`.

### Global-command barrier

Global commands meet at the `MultiShardExecution` barrier, keyed by txid.
- One-copy records such as MOVE bypass the barrier. Waiting for copies that never come would
  deadlock.
- The participant count is the number of chains that have not reached end of log, not
  `shard_count`. A chain at end of log counts as arrived at every pending and future barrier,
  and reaching end of log wakes fibers already waiting. Without this, a FLUSHALL present in chain
  A but torn off chain B would block replay forever.
- Applying it is correct: the command did run on every shard, and all of B's surviving records
  precede it.
- Txids restart in every process, but pairing by txid is still safe: all chains hold the same
  sequence of global commands and pass them in lockstep.
- The checkpoint cut is itself a global transaction, so no chain starts after a global command
  that another chain has before its cut.

### End of log

Within a segment, replay reads blocks up to the first invalid one. It moves on to the next
non-empty segment only if the current segment ended cleanly, at end of file, and the next one
starts at the expected LSN. An invalid block anywhere ends the chain.
- Data written after the last sync can reach the disk in any order (parallel writes, writeback),
  so a crashed tail can hold holes and torn blocks followed by valid ones.
- Replay keeps every valid record before the end, and discards everything after it, including
  valid blocks after a hole and later segments.
- It truncates the segment at the end and fsyncs the file. Later segments are renamed to
  `*.discarded`, from highest seq to lowest, with the directory fsynced after each rename. Thus an
  interrupted cleanup leaves the remaining `*.aof` files contiguous and can safely be retried.
  Discarded files are kept until the next checkpoint, and the resumed segment reuses the first
  discarded seq.
- `aof_load_discarded_bytes` and a warning report what each shard discarded.
- Replay does not tell torn tails from corruption of synced data (see
  [Corruption Scenarios Not Covered](#corruption-scenarios-not-covered)).

### Resuming the log

The files must describe exactly the state replay produced, or a second crash replays them
differently.
- **Every chain replayed a prefix of its own log:** truncation is enough. On each shard, call
  `journal::StartInThreadAtLsn(last_lsn_i + 1)`, open a new segment, and only then register the
  `AofStreamer`, so replayed records are not logged again.
- **A global command was completed by an exhausted chain:** checkpoint in `LOADING`. Otherwise
  the command would exist only in the longer chains: after new writes and a second crash, the
  shorter chain would replay those writes first, and the old FLUSH would then erase them.
  (Cheaper alternative, not chosen: append the missing command to the shorter chains.)
- **Shard count changed:** the old chains cannot continue. Checkpoint in `LOADING`; its cut
  registers the new `AofStreamer`s.

**Open edge cases.**
- **`PerformPostLoad` ordering.** RDB loading defers the base's synonym aux commands until
  `PerformPostLoad`. A tail `FT.SYNUPDATE` (or a DROP and CREATE of the same index) is then
  overwritten by older base metadata. The base must be finalized before tail replay, which
  probably means splitting `PerformPostLoad`.
- **Global commands other than FLUSH completed by an exhausted chain.** For example,
  `FT.DROPINDEX ... DD` deletes documents on that shard. The "prefix except global commands"
  guarantee, and the forced checkpoint, must be defined for every barriered command.
- **Loading as of the cut time.** `RdbLoader` drops expired keys, hash fields and set members
  against the wall clock. It needs a pinned time instead. Also check whether a snapshot
  serializes a key whose deadline passes between the cut and its serialization.
- **Clock coverage.** Every time-dependent path in command execution must read the transaction
  clock, not `GetCurrentTimeMs()` directly. That needs an audit (hash field expiry, `GETEX`,
  streams, scripts), plus a hook for `JournalExecutor` to set the time of the transaction that
  `DispatchCommand` creates.
- **An adopted dump replaced during bootstrap.** An external tool could replace the dump between
  the load and the manifest commit, and the manifest would then reference a file that was never
  loaded. Decide whether the manifest also records the file's identity (inode, size, a checksum).
- **Txid lockstep.** The lockstep argument needs to be verified in code, or txids made unique
  across restarts.
- Replay throughput when the shard count changed, since most records are dispatched to another
  thread.
- Search index build time during replay.

---

## Corruption Scenarios Not Covered

At the first invalid block, replay ends the shard's log. It uses the same rule for torn tails
and for corruption of synced data. This leaves the following unprotected:

- **Media corruption of synced data** (bit rot, bad sectors, firmware bugs). Every later record
  of that shard is lost, including writes acknowledged under `always`. It is reported only
  through the load warning and `aof_load_discarded_bytes`.
- **Damage in the middle of the log** discards the rest of the chain. There is no skipping ahead.
- **Cross-shard inconsistency.** The damaged shard recovers a shorter, possibly much older,
  prefix than the others.
- **Corruption that keeps a valid CRC** (about 1 in 2^32 per damaged block).
- **Devices that do not honor flushes.** Synced data is lost without an error.
- **Lost or misdirected writes** leave a hole in synced data.
- **Corruption outside segments.** Base and manifest damage fails the load through the existing
  checks. A lost segment file is a seq gap, which is a hard error.
- **Operator errors.** Files mixed in from elsewhere are caught only in part (by `shard_id`,
  `shard_count`, `segment_uid`, seq and LSN checks). A whole chain restored from an older backup
  of the same node passes.

Possible later additions: a durable-offset record in the segment header, so damage before it is
reported as corruption instead of truncated; fixed framing, as in the LevelDB/RocksDB WAL, so
replay can skip a damaged frame; a strict load mode and a `dfly-aof-check` tool. At the
infrastructure level: checksumming filesystems, storage with power-loss protection, replicas.

---

## Later Stages in Detail

### Fsync policy

Replaces the fixed `kFSyncMs` with `--aof_fsync` (alias `appendfsync`):

| mode | writer | replies | loss window |
|---|---|---|---|
| `always` | Group commit: one `fdatasync` covers every write completed before it; new writes keep flowing meanwhile | Held until `durable_lsn >= L_s` on every shard the transaction touched | No acknowledged writes |
| `everysec` (default) | The MVP behavior | Not delayed | About 1s plus in-flight data |
| `no` | No periodic sync; syncs only at cuts, rotation and shutdown | Not delayed | Up to OS writeback |

`always` does not use `RWF_DSYNC`: with parallel writes, each write would pay for its own sync.

**Reply gating.** When a write transaction concludes, the coordinator records each active shard's
`last_appended_lsn` in `ConnectionContext` (conservative, and free under group commit). The
connection waits right before the reply builder flushes to the socket, so a pipeline or a
squashed EXEC pays one wait. Read-only commands never wait.

**Sync failure under `always`.** The failure wakes every waiter on that shard. A connection whose
target was not reached replaces its buffered success replies with an error saying the write was
applied but not persisted, or closes the connection if it cannot. Valkey exits the process
instead; a `--aof_exit_on_sync_error` flag can offer that.

**Segment recycling.** Frequent syncs make each append also commit size metadata. Reusing
obsolete segments (a new header with a new `segment_uid`) avoids that, as PostgreSQL does with
WAL segments. Benchmark first.

**Open edge cases.**
- A write failure (not only a sync failure) also leaves held replies waiting; the same wake-up
  rule is needed.
- A recycled file's stale blocks fail their CRC only probabilistically (about 1 in 2^32, plus
  `segment_uid` collisions). Decide whether that is acceptable, or scrub them.
- Pipelines mixing shards: a connection only keeps a per-shard maximum LSN, so on failure it
  cannot tell which earlier replies did become durable.

### Atomic groups

A single-shard MULTI/EXEC, EVAL or FCALL writes several records, and today a block boundary can
fall between them (`DisableFlushGuard` covers only expiry and eviction).
- A `journal::AtomicGroupGuard` opens when the transaction starts on a shard and closes after its
  last hop there. It nests.
- Blocks sealed while a group is open carry a `GROUP_CONT` flag. Sealing continues inside a group,
  so a large script neither pins memory nor deadlocks throttling.
- Closing the guard seals the open block without the flag. If that block is empty, it writes a
  header-only group-end block (`len == 25`, which replay does not confuse with a hole).
- Replay applies a group only after it sees its closing block, and otherwise truncates the chain
  at the group's first block. For large groups, it validates forward to the closing block, then
  seeks back and applies block by block.
- `durable_lsn` advances only to the end of a block without `GROUP_CONT`.
- Groups make *durability* all-or-nothing, not execution. An EXEC in which one command fails with
  OOM is recorded, and replayed, exactly as it ran. Groups matter when every process in the
  replication group crashes and the AOF is loaded; a live replica already has what the master
  executed.

**Open edge cases.**
- Seal the open block when the outermost guard opens. Otherwise the group's first block also holds
  unrelated earlier records, and dropping a truncated group drops them too.
- Records of unrelated transactions land inside an open group (for example between the hops of
  a lock-ahead MULTI) and share its fate.
- Under `always`, an open group holds back `durable_lsn`, so unrelated replies on that shard wait
  until it closes.

### Re-base

Invariant: **whatever reaches this node's replicas also reaches its AOF.** A path that changes
data without the journal must trigger a **re-base**, a checkpoint whose cut also registers the
`AofStreamer` if needed.
- `CONFIG SET appendonly yes` (first enable at runtime).
- Replica full sync (`RdbLoader` bypasses the journal): stop appending and mark the manifest
  invalid when it starts, re-base when stable sync starts. Stable sync already journals applied
  entries on the replica.
- `DFLY LOAD` and `DEBUG RELOAD`.

Incoming slot migrations need nothing: the target applies them through the journal executor.

**Open edge cases.**
- What a crash between "manifest invalid" and the re-base means for the replica at startup.
- Non-default namespaces (journal records carry no namespace).

### Multi-shard tail atomicity

A multi-shard transaction can be durable on shard A but not on B. Under `always` this affects
only unacknowledged transactions; replicas have the same semantics today.

Possible fix: carry the participant count in the deprecated per-entry field (always `1u` today).
At the tail, hold back a multi-shard transaction until every other chain has shown its part or
reached end of log. For an incomplete one, end that chain right before its part (later records
may depend on it) and force a checkpoint before `ACTIVE`.

**Open edge cases.**
- Needs txids that are unique across restarts, because tails are not in lockstep. A per-run id
  in the segment header's `reserved` field helps only at segment granularity.
- On by default, or behind a flag?

### Other extensions

- **`--aof_direct_io`**, keeping the partial tail page in memory. Only if benchmarks show that
  double buffering hurts.
- **`--aof_dir`**, a separate AOF directory, for example to put the log on its own device. Bases
  are referenced by path, so this does not change the checkpoint rules.
- **Size-based rotation**, if a later feature needs fixed-size segments. It does not reduce disk
  usage.
- **Partial replica sync from AOF segments,** replacing the in-memory ring buffer (by default
  about maxmemory / shard count / 200 per shard) and extending the window to everything since the
  last checkpoint. Garbage collection would have to respect connected replicas.
- **`dfly-aof-check --fix`**, a repair tool.
- **AOF-owned copies of user bases** (a hard link, a reflink or a copy), so that deleting or
  moving the user's dump cannot break recovery.
- **S3 saves as checkpoints.** The manifest references the base object; startup loads it through
  the S3 snapshot loader and fails loudly if it is missing. The object must be retained until the
  next checkpoint. Alternative: tee the save into a local base as well.
- **`BGREWRITEAOF`** as an alias for a checkpoint, for Valkey tooling.

---

## Configuration and Observability

| Flag | Default | Stage | Notes |
|---|---|---|---|
| `--aof` | `false` | MVP | Alias: `appendonly` |
| `--aof_name` | `appendonly` | MVP | File name prefix |
| `--aof_rewrite_percentage` | 100 | MVP | Auto-checkpoint growth factor |
| `--aof_rewrite_min_size` | 64MB | MVP | Auto-checkpoint minimum size |
| `--aof_max_buffered_bytes` | TBD | MVP | Per-shard backpressure threshold |
| `--aof_fsync` | `everysec` | Fsync policy | `always` / `everysec` / `no`; alias `appendfsync` |
| `--aof_exit_on_sync_error` | `false` | Fsync policy | Exit on a sync failure under `always` |

No new commands in the MVP; SAVE and BGSAVE trigger checkpoints. Later: `CONFIG SET appendfsync`
and `CONFIG SET appendonly`.

`INFO persistence`:
- **MVP, global:** `aof_enabled`, `aof_rewrite_in_progress`, `aof_last_bgrewrite_status`,
  `aof_checkpoint_failures`, `aof_base_file`, `aof_current_size`, `aof_base_size`,
  `aof_rewrite_trigger_size`, `aof_last_write_status`
- **MVP, per shard:** `aof_buffered_bytes`, `aof_unsynced_bytes`, `aof_throttle_usec`,
  `aof_fsync_latency`, `aof_load_discarded_bytes`
- **Fsync policy:** `aof_delayed_fsync`

---

## Implementation Map

| Area | Files | Stage |
|---|---|---|
| `AofStreamer`, `AofSegmentWriter`, segment reader, manifest | `src/server/journal/aof.{h,cc}` (new) | MVP |
| Async fsync, `rw_flags`, fallback `sync_file_range` / `fadvise` (option A) | `helio/util/fibers/uring_file.{h,cc}` | MVP |
| Shared apply logic for replica and replay | `src/server/journal/journal_applier.{h,cc}` (new, from `replica.cc`) | MVP |
| End-of-log-aware barrier for all global commands; MOVE bypasses it (`IsGlobalCmd` only knows FLUSH\* today) | [tx_executor.cc](../src/server/journal/tx_executor.cc) | MVP |
| Persist each record's logical time; pin the transaction clock during replay | `src/server/journal/aof.{h,cc}`, [executor.cc](../src/server/journal/executor.cc), [transaction.cc](../src/server/transaction.cc) | MVP |
| Seal the open block at the top of `Heartbeat()` | [engine_shard.cc](../src/server/engine_shard.cc) | MVP |
| Always-on journal user; seal and rotate at the cut | [journal_slice.cc](../src/server/journal/journal_slice.cc), [journal.cc](../src/server/journal/journal.cc) | MVP |
| Cut capture, fsync, `aof-cut-lsn` aux field, manifest commit | [snapshot.cc](../src/server/snapshot.cc), [save_stages_controller.cc](../src/server/detail/save_stages_controller.cc) | MVP |
| Startup precedence, base load as of the cut time, INFO, error policy | [server_family.cc](../src/server/server_family.cc), [main_service.cc](../src/server/main_service.cc), `rdb_load.cc` | MVP |
| Durability wait for `always` | [main_service.cc](../src/server/main_service.cc), connection reply flush | Fsync policy |
| `AtomicGroupGuard` | [journal.h](../src/server/journal/journal.h), [transaction.cc](../src/server/transaction.cc) | Atomic groups |
| `JournalApplier` in replica, re-base after full sync | [replica.cc](../src/server/replica.cc) | Re-base |

`AofStreamer` must start the journal itself (`journal::StartInThread()`) before registering:
`RegisterConsumer` only counts users, and a standalone node has no journal otherwise.

---

## Testing

**MVP unit tests** (`aof_test.cc`, next to `journal_test.cc`):
- Block format: CRC byte range, bounds checks on `len`, no large allocation from a torn `len`.
- Writer: out-of-order completions advance `written_lsn` only over the contiguous prefix; small
  records share a block until the size limit or heartbeat; the heartbeat seals even when it
  skips its other work; queued buffers count against backpressure.
- Rotation: `durable_lsn` does not enter the new segment before the old one's final sync; LSNs
  stay continuous across rotation.
- Errors: a failed write is retried at its offset; a failed sync keeps the shard failed until a
  checkpoint.
- Replay: truncation at the first invalid block, including valid blocks after a hole; empty
  segments skipped anywhere, with LSN continuity checked across them; `.spare.tmp` ignored;
  truncation is crash-safe and keeps seqs contiguous; a clean shutdown leaves nothing to
  truncate.
- Manifest replacement and garbage collection.

**MVP integration tests** (`tests/dragonfly/aof_test.py`, seeder and `capture()`):
- `kill -9` under load, then restart: data matches up to the loss window.
- A crash injected at each checkpoint step.
- Qualifying and non-qualifying saves; a crash between a same-name save's rename and its
  manifest commit replays with the new dump's own cuts; a deleted base makes startup fail and
  name the file.
- `--snapshot_cron` keeps the AOF bounded; repeated checkpoint failures are reported and later
  recovered.
- Global commands: FLUSHALL mid-log; FT.CREATE / DROPINDEX / CREATE in order; MOVE followed by
  FLUSHALL without deadlock; a FLUSHALL torn off some chains, then a second crash, loses no
  writes made after the first restart.
- Replay after the deadline: `SET k 0 PXAT T; INCR k` restores `k` with its TTL, and a
  `PEXPIREAT` issued before its deadline does not delete the key during replay.
- Restart with more and with fewer proactor threads.
- Bootstrap: first start with an existing dump adopted as the base; a crash during bootstrap; a
  missing manifest with AOF files present fails startup.
- Standalone node with no replicas still logs; non-default namespaces are rejected; a full disk
  is reported as reduced durability, and a checkpoint clears it once space is freed.

**Later stages:** `always` survives `kill -9` with every acknowledged write, and does not hang on
an injected sync failure; atomic groups are applied whole or not at all; a replica with AOF
survives full sync, re-base and a kill.

**Benchmarks:** memtier SET throughput and p99 against AOF off (target: under 10% regression);
parallel vs. serialized submission on ext4 and XFS; `RWF_DONTCACHE` vs. the fallback; `always`
with pipelining.

---

## Open Questions

- Defaults for `--aof_max_buffered_bytes` and `kAofBlockBytes`.
- Should `ThrottleIfNeeded` ever drop AOF instead of stalling the shard?
- Group latency coupling under `always` (see [Atomic groups](#atomic-groups)).
- Multi-shard tail atomicity: default on, or behind a flag?
