# Sharded Append-Only File (AOF) Design

> **Status:** design proposal. Nothing described here is implemented yet.

This document proposes durable, log-based persistence for Dragonfly. It is built on the per-shard
journal that replication already uses. Every shard writes its own append-only log with no global
serialization point. Periodic DFS checkpoints keep the logs bounded.

## Table of Contents

1. [Motivation](#motivation)
2. [Building Blocks We Reuse](#building-blocks-we-reuse)
3. [Architecture](#architecture)
4. [On-Disk Layout](#on-disk-layout)
5. [Write Path](#write-path)
6. [I/O Mode](#io-mode)
7. [Fsync Policy](#fsync-policy)
8. [Checkpoints: Bounding File Growth](#checkpoints-bounding-file-growth)
9. [Replay at Startup](#replay-at-startup)
10. [Re-base: Paths That Bypass the Journal](#re-base-paths-that-bypass-the-journal)
11. [Atomicity Semantics](#atomicity-semantics)
12. [Configuration and Observability](#configuration-and-observability)
13. [Implementation Map](#implementation-map)
14. [Phasing](#phasing)
15. [Testing](#testing)
16. [Open Questions](#open-questions)

---

## Motivation

Today Dragonfly persists data only through point-in-time snapshots (RDB/DFS). A crash loses
every write made since the last snapshot. Valkey solves this with AOF, but its AOF is a single
global log. Adopting that design would reintroduce a global serialization point that
Dragonfly's shared-nothing architecture avoids ([df-share-nothing.md](df-share-nothing.md)).

Goals:
- Configurable durability, from "every acknowledged write survives a crash" (`always`) to "up to
  about one second of writes may be lost" (`everysec`).
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

- The naming follows the pattern of the other streamers: a consumer named after its purpose
  (`AofStreamer`) owns a writer named after its destination (`AofSegmentWriter`), just as
  `ReplicaStreamer` owns a `BufferedSocketWriter`.
- **`AofStreamer`** is a new `JournalConsumerInterface`, one per shard (thread-local, like the
  journal slice). It turns journal records into blocks and keeps track of atomic groups.
  - It registers with `journal::RegisterConsumer`. That permanently acquires a journal user, so
    the journal never auto-stops (`MaybeStop()`) while AOF is on.
  - The replication ring buffer and its retention policy don't change, and AOF does not depend
    on them.
- **`AofSegmentWriter`** runs one writer fiber per shard, on the shard's own proactor. It uses
  `fb2::LinuxFile` over io_uring and owns segment files, spare segments, rotation, and fsync.
  - It follows the buffering and in-flight model of `BufferedSocketWriter`, with a file as the
    destination instead of a socket. It is a separate class, because file writes need offsets,
    fsync, block framing, and rotation, none of which applies to a socket.
  - It reuses `PendingBuf` ([pending_buf.h](../src/server/journal/pending_buf.h)) for iovec
    batching.
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
  - header CRC
- **Body:** a sequence of blocks, each laid out as:

  ```
  [u32 len][u32 crc32c][u64 first_lsn][u32 n_records][u8 flags][payload]
  ```

  - `payload` is the concatenated `JournalWriter` bytes, i.e. exactly what is already in
    `JournalItem::data`. Nothing is serialized a second time.
  - `flags` has one bit, `GROUP_CONT`. It means the block ends inside an atomic group that
    continues in the next block (see [Atomic groups](#atomic-groups)).
  - The end of data is either a block with `len == 0` or a block whose `first_lsn` is not the
    expected next LSN. That rule is what allows segments to be pre-zeroed or recycled (see
    [I/O Mode](#io-mode)).

### Atomic groups

A multi-command transaction on one shard (MULTI/EXEC, EVAL, FCALL) writes several journal
records. Today nothing prevents a flush boundary from falling between them. `DisableFlushGuard`
is used only around expiry and eviction, not around EXEC or script execution. So the AOF marks
transaction boundaries explicitly:

- **Guard.** A new `journal::AtomicGroupGuard` is opened by the transaction when a multi-command
  transaction starts executing on a shard, and closed when it finishes there (its last hop). The
  count is kept per shard, so guards can nest.
- **Sealing inside a group.** `AofStreamer` still seals blocks while a group is open. Otherwise a
  large script would pin memory, and throttling would deadlock, because the writer can only drain
  sealed blocks. Blocks sealed inside a group carry `GROUP_CONT`. The block that closes the group
  has the flag cleared.
- **Replay.** Records from `GROUP_CONT` blocks are buffered, and applied only when the closing
  block arrives. A group cut off by a torn tail is dropped whole.
- **Interleaved records.** Records of unrelated transactions on the same shard can land inside an
  open group, for example between the hops of a lock-ahead MULTI. They share the group's
  durability: they are applied with it, or dropped with it. Per-key order is unchanged.
- **Durability.** `durable_lsn` therefore only advances to the end of a block without
  `GROUP_CONT`. Otherwise, under `always`, a write could be acknowledged and then dropped with an
  incomplete group.

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

`ThrottleIfNeeded()`:
- Seals the open block and hands it to the writer by notifying an `EventCount`.
- Applies disk backpressure: if `pending_bytes > --aof_max_pending_bytes`, it blocks until the
  writer catches up. This stalls the shard, the same way replication throttling in
  `BufferedSocketWriter` does today.

The writer fiber loops:
1. Take every sealed block and write them all with one `pwritev` at the segment offset.
2. Publish `written_lsn`.
3. Sync according to the [fsync policy](#fsync-policy), then publish `durable_lsn` (an atomic
   readable from any thread) and wake waiters. `durable_lsn` never passes an open
   [atomic group](#atomic-groups).
4. When `offset >= --aof_segment_max_bytes`, rotate at a block boundary:
   - `fdatasync` and close the old segment.
   - Switch to the spare segment, and write its header as part of its first batch.

**Spare segments.** A new segment's directory entry must be durable before any record in it is
reported durable. Otherwise, after a power loss, the file could vanish together with
acknowledged records. So each shard always keeps one spare segment fully prepared in the
background:
- create it (or rename a recycled one into place), and zero-fill it;
- `fdatasync` the file, then `fsync` the directory.

Rotation, and the switch at a checkpoint cut, only ever move to a spare that is already durable,
so no directory fsync sits on the write path. If the spare is not ready yet (a slow disk), the
writer waits for it. Sealed blocks keep accumulating, and normal backpressure applies.

---

## I/O Mode

Segments are written through the page cache (`O_WRONLY | O_CLOEXEC`, no `O_DIRECT`).

**Why not O_DIRECT.** O_DIRECT needs the offset, length and buffer aligned to 4K, and a
group-commit batch is often only a few hundred bytes. Every flush would then have to do one of
two things:
- Rewrite the partial tail page. That costs 4K of write amplification per commit. Worse, if the
  rewrite tears on power loss, it can destroy bytes in that page that were already acknowledged.
- Pad every flush out to a fresh 4K block. At low concurrency that wastes a lot of space.

O_DIRECT also does not make anything durable. We would still need `fdatasync` or `RWF_DSYNC` to
flush the device cache.

**Durability.** Durability comes only from the fsync policy. In `always` mode, each write is
submitted as an io_uring `WRITEV` with `rw_flags = RWF_DSYNC`, so the write and the datasync are
a single operation per group commit.

**Keeping fdatasync cheap.**
- The problem: `fallocate` alone leaves *unwritten* extents on ext4 and xfs. The first write into
  one converts it, which is a metadata change and forces a filesystem journal commit on every
  `fdatasync`.
- The fix: the next segment is prepared in the background in one of two ways:
  - write zeros into it, or
  - **recycle** a garbage-collected segment: rename it and overwrite it in place, similar to how
    PostgreSQL recycles WAL segments.
- Why recycling is safe: every block header carries `first_lsn`. Replay stops at the first block
  whose `first_lsn` is not the expected next LSN, so stale data left in a recycled file is read
  as end of log.

**Page cache.** AOF data is written once and almost never read, so it should not push other data
out of the page cache.
- Once a region of a segment is durable, the writer issues `IORING_OP_SYNC_FILE_RANGE` (WRITE)
  followed by `IORING_OP_FADVISE` (DONTNEED) on that region.
- This also caps the dirty pages that count against a container's cgroup memory limit.

**Checkpoint base files** keep using the existing O_DIRECT snapshot path. Those are large
sequential writes that are already aligned. The only change is a final `FSync()` before the
rename, since snapshot files are not fsynced today.

**Later option.** A `--aof_direct_io` flag could serve very high-throughput `everysec`
workloads. The writer would keep the partial tail page in memory and pad only at fsync points.
This is worth doing only if benchmarks show that double buffering in the page cache hurts.

---

## Fsync Policy

`--aof_fsync` takes three values. The Valkey name `appendfsync` is also accepted for CONFIG
compatibility.

| mode | writer behavior | client visibility | loss window |
|---|---|---|---|
| `always` | Datasync every write batch. Group commit: whatever accumulates during one sync goes into the next batch. | The reply is held until `durable_lsn[s] >= L_s` on every shard the transaction touched. | No acknowledged writes |
| `everysec` (default) | Write each block once it is sealed (or after about 1ms idle). `fdatasync` at most once per second when dirty. | Replies are never delayed. | About 1s plus in-flight data |
| `no` | Write only. The kernel decides when to flush, optionally smoothed with `sync_file_range`. | Replies are never delayed. | Up to OS writeback |

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

### Error policy

Errors are handled the same way as in Valkey:
- A failed write or fsync moves the slice into an error state.
- While in that state, write commands are rejected with `-MISCONF` until a later write succeeds.
  This applies in `always` mode, and in `everysec` mode after a failed fsync.
- The state is exposed in `INFO persistence` as `aof_last_write_status` and `aof_delayed_fsync`.

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
  implemented. A thin alias can be added later if Valkey tooling compatibility calls for it.
- **Forced re-base:** whenever data changed without going through the journal (see
  [Re-base](#re-base-paths-that-bypass-the-journal)).

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
   - Records inside [atomic groups](#atomic-groups) are buffered until the group's closing
     block, then applied together.
   - **Global commands** (FLUSHALL, FLUSHDB, FLUSHSLOTS) meet at the `MultiShardExecution`
     barrier, keyed by txid. The barrier's participant count is not fixed at `shard_count`. It
     is the number of chains that have not reached end of log.
     - A chain that reaches end of log counts as having arrived at every pending and future
       barrier. Reaching end of log also wakes fibers already waiting at a barrier.
     - Without this, a crash tail can deadlock replay. In `everysec` mode, chain A may have a
       FLUSHALL durable while chain B's log ends before it, so the fibers holding the FLUSHALL
       would wait forever for B.
     - **Why applying it is correct:** the global command did run on every shard. If B's log ends
       before it, every surviving record of B comes before it in B's order. So running it after
       B is exhausted matches what happened.
     - The global cut at checkpoints still guarantees that no chain starts past a global command
       that another chain has before its cut.
5. **Handle a damaged tail.** A CRC mismatch alone cannot tell a torn write from corruption of a
   block that was written completely. So the default truncation applies only when the tail is
   demonstrably incomplete. In the chain's **last** segment, that means one of:
   - a short read: the block extends past the end of the file;
   - `len == 0`;
   - a bad block (CRC mismatch or unexpected `first_lsn`) with no CRC-valid block after it in the
     segment whose `first_lsn` is greater than the expected LSN. Stale blocks in a recycled
     segment have lower LSNs, so they do not count.

   With `--aof_load_truncated=true` (the default), log a warning and truncate the file at that
   point. Otherwise, fail.

   Everything else is corruption, and is fatal:
   - a CRC failure followed by later valid blocks;
   - any damage in an earlier segment.

   Loading past corruption needs an explicit repair step, such as a future
   `dfly-aof-check --fix`.

   In `always` mode, a reply is sent only after its block was fsynced in full. So a torn write
   can only affect the final, unacknowledged batch.
6. **Resume the log.**
   - **Same shard count:** on each shard, call `journal::StartInThreadAtLsn(last_lsn_i + 1)`,
     open a new segment at that LSN, and only then register `AofStreamer`. Registering it this late
     keeps replayed records from being appended to the log again.
   - **Shard count changed** (the manifest's `shard_count` differs from the current one): the
     old chains cannot be continued. Run a re-base while still in LOADING. Its cut registers the
     new `AofStreamer`s, and the server switches to ACTIVE only after that.
7. **Finish.** Run `PerformPostLoad` and `ForceReplicasToFullSync()`, then switch to ACTIVE.

---

## Re-base: Paths That Bypass the Journal

The invariant is: **whatever reaches this node's replicas also reaches its AOF.** A path that
changes the dataset without going through the journal breaks the chain. Every such path must
trigger a **re-base**: a checkpoint whose cut also registers `AofStreamer` if it is not registered
yet.

Paths that need a re-base:
- **First enable** with `CONFIG SET appendonly yes`.
- **Replica full sync.** `RdbLoader` bypasses the journal.
  - When a full sync starts, stop appending and mark the manifest invalid.
  - When it finishes, re-base at the start of stable sync.
  - From then on AOF works on the replica with no further special handling. Stable sync already
    records applied entries into the replica's own journal (see the PING handling in
    `replica.cc`).
- **Other `RdbLoader`-driven loads:** `DFLY LOAD`, `DEBUG RELOAD`, `DEBUG LOAD`.
- **Shard count change at restart** (see [Replay](#replay-at-startup), step 6).

Incoming slot-migration data needs nothing extra. The target applies it through the journal
executor and journals it like any other write.

---

## Atomicity Semantics

- **Single-shard transactions and scripts** are atomic in the AOF. Their records form an
  [atomic group](#atomic-groups), and replay applies a group entirely or not at all.
- **Multi-shard transactions** (an MSET across shards, a cross-shard EXEC or EVAL) are not
  atomic across a crash. Shard A's part can be durable while shard B's is not.
  - Under `always`, this can only affect transactions that were never acknowledged.
  - Replicas have the same semantics today.
- **Possible fix for a later phase:**
  - Reuse the deprecated per-entry field (currently always written as `1u`) to carry the number
    of shards that participate in the transaction.
  - During replay, hold back multi-shard txids at the tail until every other chain has shown its
    part or reached EOF.
  - Drop the incomplete ones. Only the tail of the log can contain them.

---

## Configuration and Observability

| Flag | Default | Notes |
|---|---|---|
| `--aof` | `false` | Alias: `appendonly` |
| `--aof_dir` | `--dir` | |
| `--aof_name` | `appendonly` | File name prefix |
| `--aof_fsync` | `everysec` | `always` / `everysec` / `no`; alias `appendfsync` |
| `--aof_segment_max_bytes` | 256MB | Segment rotation size |
| `--aof_rewrite_percentage` | 100 | Auto-checkpoint growth factor |
| `--aof_rewrite_min_size` | 64MB | Auto-checkpoint minimum size |
| `--aof_load_truncated` | `true` | Truncate a demonstrably incomplete tail instead of failing |
| `--aof_max_pending_bytes` | TBD | Backpressure threshold per shard |

Commands:
- `CONFIG SET appendonly`
- `CONFIG SET appendfsync`
- `DEBUG AOF CHECKPOINT`

`INFO persistence` gains these fields:
- **Global:**
  - `aof_enabled`, `aof_rewrite_in_progress`, `aof_last_bgrewrite_status`
  - `aof_current_size`, `aof_base_size`
  - `aof_last_write_status`, `aof_delayed_fsync`
- **Per shard:** `aof_pending_bytes`, `aof_throttle_usec`, `aof_fsync_latency`

Base files carry a per-shard `aof-cut-lsn` aux field. It replaces the `aof-preamble` aux field,
which is currently hardcoded to 0. It is informational only; the manifest is authoritative.

---

## Implementation Map

| Area | Files |
|---|---|
| `AofStreamer`, `AofSegmentWriter`, segment reader, manifest | `src/server/journal/aof.{h,cc}` (new) |
| Shared apply logic for replica and AOF replay | `src/server/journal/journal_applier.{h,cc}` (new, extracted from `replica.cc`) |
| End-of-log-aware global-command barrier | [tx_executor.cc](../src/server/journal/tx_executor.cc) (`MultiShardExecution`) |
| `AtomicGroupGuard` around multi-command transactions | [journal.h](../src/server/journal/journal.h), [transaction.cc](../src/server/transaction.cc) |
| Always-on journal user; seal and rotate at the cut | [journal_slice.cc](../src/server/journal/journal_slice.cc), [journal.cc](../src/server/journal/journal.cc) |
| Atomic cut capture, fsync of base files, checkpoint mode | [snapshot.cc](../src/server/snapshot.cc), [save_stages_controller.cc](../src/server/detail/save_stages_controller.cc) |
| Startup precedence, INFO, re-base triggers | [server_family.cc](../src/server/server_family.cc) |
| Durability wait for `always`, `-MISCONF` gating | [main_service.cc](../src/server/main_service.cc), connection reply flush |
| Switch to `JournalApplier`, re-base after full sync | [replica.cc](../src/server/replica.cc) |

---

## Phasing

1. `AofStreamer` and `AofSegmentWriter`, segment format, and rotation; `everysec` and `no`
   modes; replay without a base, including the global-command barrier.
2. Checkpoints, manifest, garbage collection, and the automatic checkpoint trigger. File growth
   becomes bounded at this phase.
3. `always` mode, with group commit and reply gating coalesced across a pipeline.
4. Re-base triggers (CONFIG SET, replica full sync, DFLY LOAD, shard count change) and INFO
   metrics.
5. Optional work:
   - Tail atomicity for multi-shard transactions.
   - Partial sync for replicas served from AOF segments. This is cheap because AOF LSN equals
     journal LSN.

---

## Testing

- **Unit tests** in `aof_test.cc`, alongside `journal_test.cc`:
  - block framing and CRC checks
  - a torn tail gets truncated
  - a CRC failure followed by valid blocks is fatal, not truncated
  - corruption in the middle of a file is an error
  - an atomic group cut off by the tail is dropped whole
  - stale blocks in a recycled segment are read as end of log
  - rotation keeps LSNs continuous
  - atomic manifest replacement
  - garbage collection of unreferenced files
- **Integration tests** in `tests/dragonfly/aof_test.py`, using the seeder and `capture()`:
  - Seeder load, then `kill -9`, then restart: the captures match. Under `always`, every
    acknowledged write survives.
  - Checkpoint under load, with a crash injected at each checkpoint step: after restart, the
    data matches.
  - A FLUSHALL in the middle of the log replays correctly through the barrier.
  - A FLUSHALL present in only some chains (the others truncated before it) replays without
    deadlock.
  - `kill -9` in the middle of a large EVAL or MULTI/EXEC: after restart, the transaction is
    applied either entirely or not at all.
  - Restart with a different `--proactor_threads`: the data matches and a re-base happened.
  - A replica with AOF enabled goes through full sync, re-base, then stable sync, and is killed.
    After restart it matches the master.
  - Disk full (simulated with a small tmpfs) produces `-MISCONF` in `always` mode.
- **Benchmarks:**
  - memtier SET throughput and p99 latency, `everysec` against AOF off. Target: less than 10%
    regression.
  - `always` with pipelining, to show that group commit amortizes the fsync cost.

---

## Open Questions

- **Group latency coupling.** Under `always`, an open atomic group holds back `durable_lsn`.
  Replies to unrelated writes on that shard then wait until the group closes, which can be
  milliseconds for a multi-hop MULTI/EXEC. Is that acceptable, or should interleaved records be
  kept out of open groups?
- **Blocking vs. dropping under backpressure.** Should `ThrottleIfNeeded` ever give up on AOF
  instead of stalling the shard, for example after a timeout in `everysec` mode? Or should it
  always block?
- **Multi-shard tail atomicity.** Should it be on by default, or behind a flag?
