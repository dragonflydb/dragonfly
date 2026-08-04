# Replication design

This document describes how Dragonfly-to-Dragonfly replication works: the handshake, full sync,
partial sync, stable-state streaming, and failover/takeover. It does not cover Dragonfly acting
as a *replica of Redis* in detail (see "Replicating from a Redis master" at the end) or cluster
slot migration (`RestoreStreamer`), which is a related but separate mechanism built on top of the
same `JournalStreamer` base class.

## Key source files

| Concern | File |
|---|---|
| Master-side replication protocol (`DFLY ...` subcommands) | `src/server/dflycmd.{h,cc}` |
| Replica-side state machine | `src/server/replica.{h,cc}` |
| Common client transport (connect/handshake I/O helpers) | `src/server/protocol_client.{h,cc}` |
| Per-shard journal (ring buffer, LSN, change notifications) | `src/server/journal/journal.{h,cc}`, `journal_slice.{h,cc}` |
| Journal wire format / entries | `src/server/journal/types.h`, `src/server/journal/serializer.h` |
| Streaming journal changes to a replica flow socket | `src/server/journal/streamer.{h,cc}` |
| Grouping journal entries back into transactions on the replica | `src/server/journal/tx_executor.{h,cc}` |
| Bucket-granular, copy-on-write serialization used by both full sync and cluster migration | `src/server/serializer_base.{h,cc}` |
| Full-sync snapshot producer (per shard) | `src/server/snapshot.{h,cc}`, `rdb_save.{h,cc}` |
| Full-sync consumer (RDB loader) | `src/server/rdb_load.{h,cc}` |
| Egress rate limiting shared by the snapshot traversal and journal writers | `src/server/detail/egress_throttle.{h,cc}` |
| `REPLICAOF`/`SLAVEOF`/`ADDREPLICAOF`/`REPLCONF`/`REPLTAKEOVER` commands, master identity | `src/server/server_family.cc` |

## Two roles, two state machines

Replication has two independent state machines that talk to each other over one control
connection plus one connection *per shard* ("flow"):

- **Master side**: `DflyCmd::ReplicaInfo` per connected replica, state
  `PREPARATION -> FULL_SYNC -> STABLE_SYNC`, with `CANCELLED` reachable from any state
  (`src/server/dflycmd.h:112`, enum `SyncState`).
- **Replica side**: `Replica` holds a bitmask `state_mask_` of `R_ENABLED`, `R_TCP_CONNECTED`,
  `R_GREETED`, `R_SYNCING`, `R_SYNC_OK` (`src/server/replica.h:48-54`), advanced by
  `Replica::MainReplicationFb` (`src/server/replica.cc:235-328`).

Both machines are per session: a master shards each replica's sync session (`sync_id`) into
`shard_set->size()` flows, one TCP connection per master shard (`DflyCmd::Flow`,
`src/server/dflycmd.cc:286-383`). The replica mirrors this with one `DflyShardReplica` per flow
(`src/server/replica.cc:558-571`).

## Terminology used below

- **LSN** (Log Sequence Number): a per-shard, monotonically increasing counter for journal
  entries. `journal::GetLsn()` returns the LSN of the *next* entry that will be written
  (`JournalSlice::lsn_` starts at 1, `src/server/journal/journal_slice.h:84`, incremented in
  `AddLogRecord`, `src/server/journal/journal_slice.cc:86`). LSNs are **not** comparable across
  shards - each shard has an independent sequence.
- **Flow**: one shard's replication connection. A replica with `N` master-side shards has `N`
  flows, mapped by flow-id to shard-id (`flow_id >= shard_set->size()` is rejected,
  `src/server/dflycmd.cc:312`).
- **`master-replid`** (`master_replid` in code): 40-hex-char random ID generated once per master
  process lifetime (`ServerFamily::Init`, `src/server/server_family.cc:1166`). Used by replicas
  to detect that they reconnected to a *different* master (e.g. after a restart) versus the same
  one.

---

## 1. Handshake (per-connection greeting)

A replica connects with a plain RESP client and issues, in order
(`Replica::Greet`, `src/server/replica.cc:330-373`):

1. `PING` - expects `+PONG`.
2. `REPLCONF listening-port <port>` - `+OK`.
3. `REPLCONF ip-address <ip>` (only if `--replica_announce_ip` is set) - best-effort, a bad
   response is only logged as a warning (older masters may not support it).
4. `REPLCONF capa eof capa psync2` - `+OK`.
5. `REPLCONF capa dragonfly` - the master's reply distinguishes a Redis master (single-element
   `+OK`) from a Dragonfly master (a multi-bulk array).

On the master side, `REPLCONF CAPA dragonfly` is handled specially in
`ServerFamily::ReplConf` (`src/server/server_family.cc:3546-3568`): it calls
`DflyCmd::CreateSyncSession` to allocate a `sync_id` and reserve one `FlowInfo` slot per shard
(`src/server/dflycmd.cc:771-796`), then replies with a 5-element array:

```
<master_replid> <sync_id "SYNCn"> <num_flows = shard_set->size()> <DflyVersion::CURRENT_VER> <lineage_id>
```

The replica parses this in `Replica::HandleCapaDflyResp`
(`src/server/replica.cc:375-435`). Notable behavior:

- If the advertised `master_replid` equals the replica's own client id, it refuses (protects
  against accidentally replicating from itself).
- If the master's `master_replid` differs from the one this `Replica` object last saw, and
  `--break_replication_on_master_restart` is set, replication is aborted outright (protects a
  replica from silently flushing its dataset because the same-address master process restarted
  with fresh, unrelated data). Otherwise the previously-remembered `last_journal_LSNs_` are
  dropped, which forces a full resync (partial sync is only attempted against the same
  `master_replid` the replica last saw, or against the specific "different master" case described
  in [Partial sync after promotion / failover](#partial-sync-after-promotion--failover)).
- The 5th field, `lineage_id`, is stored but only consulted by an experimental feature that is
  out of scope for this document.

Then, only for a Dragonfly master, `Replica::ConfigureDflyMaster`
(`src/server/replica.cc:437-452`) sends:

- `REPLCONF CLIENT-ID <cluster_family().MyID()>` - lets the master tag the `ReplicaInfo` with the
  replica's stable cluster node ID (`DflyCmd::ReplicaInfo::SetId`).
- `REPLCONF CLIENT-VERSION <DflyVersion::CURRENT_VER>` - the replica's own protocol version,
  stored on `ReplicaInfo` and later used to gate partial-sync behavior and RDB feature framing
  (search index blobs etc.).

After this, `state_mask_ |= R_GREETED`.

## 2. Per-flow negotiation (`DFLY FLOW`)

For each of the `num_flows` shards, the replica opens a **new** TCP connection
(`DflyShardReplica::StartSyncFlow`, `src/server/replica.cc:968-1031`) and sends:

```
DFLY FLOW <master_repl_id> <dfly_session_id> <flow_id> [<lsn>] [<last_master_id> <lsn-vec>]
```

- `<lsn>` is appended only if the replica remembers a *same-master* resumable LSN for this flow
  (from a previous disconnect) and the master's advertised version is `> VER1` and
  `--replica_partial_sync` is enabled.
- `<last_master_id> <lsn-vec>` (the previous master's id and a `-`-joined vector of per-shard
  LSNs) is appended only when the replica has `LastMasterSyncData` from a *different* master it
  used to follow (see [Partial sync after promotion / failover](#partial-sync-after-promotion--failover))
  and the master's version is `>= VER5`.

On the master, `DflyCmd::Flow` (`src/server/dflycmd.cc:286-383`) validates `master_id`, resolves
`flow_id` and the session's `ReplicaInfo`, migrates the connection's fiber to the shard's own
proactor thread (`conn->Migrate`), and calls `journal::StartInThread()` - this lazily
initializes that shard's journal ring buffer if not already active
(`journal::StartInThread`, `src/server/journal/journal.cc:24-29`; buffer capacity is
`--shard_repl_backlog_len`, default 8192 entries, `src/server/journal/journal_slice.cc:20-21,39`).

It then decides full vs. partial sync (all under the `ReplicaInfo` lock, and only while state is
still `PREPARATION`):

1. **Failover match**: this master itself descends from a promoted replica
   (`ServerFamily::GetLastMasterData()` is non-empty, i.e. this process ran `REPLICAOF NO ONE` or
   `REPLTAKEOVER` and remembers the old master's id plus per-shard LSNs it had reached), and the
   requesting replica's `last_master_id` matches that remembered id. In that case the LSN to
   resume from is taken from the replica's supplied `lsn-vec` for this `flow_id`, not from the
   `<lsn>` argument.
2. Otherwise, if a bare `<lsn>` was sent, that is the candidate resume point (reconnect to the
   *same* master/flow).

The candidate LSN is only honored if `DflyCmd::IsLSNInPartialSyncBuffer`
(`src/server/dflycmd.cc:487-497`) confirms it is still retrievable: either it equals the current
`journal::GetLsn()` (nothing missed) or it's inside the shard's ring buffer
(`journal::IsLSNInBuffer`, backed by `JournalSlice::IsLSNInBuffer`,
`src/server/journal/journal_slice.cc:43-55`, which just checks the LSN falls within
`[front.lsn, back.lsn]` of the `boost::circular_buffer`). If the buffer has already evicted that
LSN (replica was disconnected too long, or the buffer is too small), the master logs it and falls
back to full sync for that flow - silently, no error is sent for this specific failure.

The master replies to `DFLY FLOW` with `(sync_type, eof_token)` where `sync_type` is `"FULL"` or
`"PARTIAL"`, and `eof_token` is a fresh random 40-hex string
(`SetupFlowConnection`, `src/server/dflycmd.cc:101-120`) used later only in the full-sync path to
mark the end of the RDB stream out-of-band from the RDB format itself.

**Important asymmetry**: partial/full sync is decided *per flow*, independently, at `DFLY FLOW`
time - before the replica has even sent `DFLY SYNC`. `DFLY SYNC` (below) fans out to shards and,
for any flow that was negotiated as `PARTIAL`, skips starting a full-sync saver for that flow
entirely.

## 3. Full sync (`DFLY SYNC`)

Once *all* flows have replied to `DFLY FLOW`, the replica sends `DFLY SYNC <sync_id>` on the
*first* (control) connection (`Replica::InitiateDflySync`, `src/server/replica.cc:684-688`, via
`SendNextPhaseRequest`). `DflyCmd::Sync` (`src/server/dflycmd.cc:385-432`) requires state
`PREPARATION`, and - under a `Transaction::Guard` so no write transaction is mid-flight - runs
`StartFullSyncInThread` on every shard whose flow was **not** already resolved to `PARTIAL`
(a flow with `start_partial_sync_at` set makes `StartFullSyncInThread` a no-op / error path;
see `src/server/dflycmd.cc:404-417`). It then transitions `ReplicaInfo` to `FULL_SYNC` and
replies `+OK` - **without waiting for the snapshot itself to finish**; the RDB bytes stream
asynchronously over the already-open flow sockets.

`DflyCmd::StartFullSyncInThread` (`src/server/dflycmd.cc:688-727`) per shard:

- Creates an `RdbSaver` writing directly to the flow's socket. Shard 0 additionally saves the
  "summary" (Lua scripts, global metadata, search index defs) - `SaveMode::SINGLE_SHARD_WITH_SUMMARY`
  vs. plain `SaveMode::SINGLE_SHARD` for the rest.
- Calls `SaveHeader` then `saver->StartSnapshotInShard(true, ...)`, which spins up a
  `SliceSnapshot` (`src/server/snapshot.cc:80-125`). Crucially, when `stream_journal=true` the
  `SliceSnapshot` **registers itself as a journal consumer before it starts iterating the hash
  table** (`journal::RegisterConsumer(this)` at `snapshot.cc:87`, ahead of the
  `IterateBucketsFb` fiber launch). Any write that lands on a key/bucket *after* the snapshot
  cursor already passed it is captured as a journal entry and appended into the very same RDB
  byte stream, interleaved with bucket data (`SliceSnapshot::ConsumeJournalChange`,
  `snapshot.cc:351-355`). This is what makes it safe for clients to keep writing during full
  sync: there is no snapshot-to-journal handoff gap, because the journal listener is live for the
  entire duration of the snapshot, not just after it.
- Once all buckets are traversed, the snapshot fiber writes a "full sync cut" marker into the RDB
  stream (`SliceSnapshot::IterateBucketsFb`, `snapshot.cc:216-218`, `serializer_->SendFullSyncCut()`).

On the replica, each flow's `DflyShardReplica::FullSyncDflyFb`
(`src/server/replica.cc:1048-1104`) feeds the socket into an `RdbLoader`. The loader is given a
callback (`SetFullSyncCutCb`) that decrements a `BlockingCounter` shared across all flows the
first time the cut marker is observed. `Replica::InitiateDflySync` blocks on
`sync_block->Wait()` (`src/server/replica.cc:690-693`) until every flow has hit its cut - this is
how the replica knows the RDB "photo" portion is done on all shards simultaneously, even though
each shard streams independently and at its own pace.

After the cut, the master's snapshot fiber is *not* done: journal changes are still being
forwarded into the RDB stream. The transition out of that is driven separately by
`DFLY STARTSTABLE` (next section) - `StopFullSyncInThread` finalizes the journal hookup, sends the
journal offset as an RDB opcode (`RDB_OPCODE_JOURNAL_OFFSET`, read back on the replica as
`RdbLoader::journal_offset_`), flushes, and only then writes the raw `eof_token` bytes to the
socket. The replica's full-sync fiber reads and verifies that `eof_token` off the wire
(`src/server/replica.cc:1073-1086`) before considering the flow's full sync complete, and stashes
any bytes read past the token as `leftover_buf_` to be replayed as the start of the stable-sync
stream (the token can arrive already followed by live journal bytes on a fast enough connection).

If every flow negotiated `PARTIAL`, none of this happens: `num_full_flows == 0` short-circuits to
`sync_type = "partial"` (`src/server/replica.cc:671-673`) and the replica skips straight to
sending `DFLY STARTSTABLE`. Mixed full/partial across flows of the same session is treated as
unrecoverable (`src/server/replica.cc:673-676`, "Won't do a partial sync: some flows must fully
resync") - replication for that session errors out and reconnects from scratch.

`FlushAll`/`FlushSlots` on the replica happens once, before starting the full-sync flows, only
if *all* flows resolved to full (`src/server/replica.cc:654-670`) - a purely-partial resync never
touches existing data.

### Bucket serialization, locking, and concurrent writes

The snapshot traversal and live client writes run concurrently against the same `DashTable`
without a global lock. The mechanism (`src/server/serializer_base.{h,cc}`, shared by full sync,
`SAVE`/`BGSAVE`, and cluster slot migration's `RestoreStreamer`) is a per-bucket copy-on-write
scheme:

- The traversal fiber (`IterateBucketsFb`) walks physical DashTable buckets and calls
  `SerializerBase::ProcessBucket` on each one (`serializer_base.cc:177-223`). Every bucket has a
  version number; the snapshot remembers a `snapshot_version_` taken at start. `ProcessBucket`
  only serializes a bucket if its version is still older than `snapshot_version_`, then stamps it
  with `snapshot_version_` *before* serializing (so a concurrent second visit — from either the
  traversal continuing or a write racing it — is a guaranteed no-op, `serializer_base.cc:180,210`).
- A write is *not* blocked by the snapshot. `DbSlice` invokes `SerializerBase::OnChange` on the
  mutating fiber for every bucket about to be touched, *before* the mutation is applied
  (`serializer_base.cc:229-242`). `OnChange` calls the same `ProcessBucket`, this time with
  `on_update=true`: if the bucket hasn't been visited by the traversal yet, it gets serialized
  right there, inline, on the writer's own fiber ("side-saved") — capturing the pre-mutation value
  — and is marked as done so the traversal skips it later. If the bucket was already serialized,
  this is just a version check with no extra work. Either way, the calling write's own fiber does
  this work and then proceeds with its mutation; other client fibers touching *other* buckets are
  never blocked by this, because each bucket's version/latch state is independent — there is no
  table-wide lock anywhere in this path.
- The only genuine mutex, `stream_mu_`, protects the *output stream* (the shared `RdbSerializer`
  buffer/socket-writer), not bucket access: it stops the traversal fiber's in-progress write of a
  large value from interleaving, mid-value, with a side-saved change from another fiber onto the
  same output stream (`serializer_base.h:224-226`, `snapshot.cc:337-350`). It is only actually
  held (`detail::OptionalMutex`) when `--serialization_tagged_chunks` is disabled; the tagged-chunk
  wire format normally makes this unnecessary. It is never used to gate access to a bucket.
- For values offloaded to tiered storage, `BucketDependencies` (`serializer_base.h:62-84`) tracks
  an async fetch-in-flight per bucket via a small per-bucket latch, so a second touch of that
  *specific* bucket (e.g. the traversal reaching it while an `OnChange` side-save is still waiting
  on a tiered read) blocks only until that one bucket's pending read resolves
  (`BucketDependencies::Wait`, `serializer_base.cc:48-55`) - again bucket-scoped, not global.

This is what the ordering-invariant comment in the code states directly: for any key, the
replica/snapshot must observe the pre-mutation value strictly before the journal entry that
mutates it, and the mechanism above guarantees that without ever stalling writes to unrelated
keys.

### Throttling the snapshot

Full-sync (and plain `SAVE`/`BGSAVE`) egress is rate-limited per shard thread by
`ServerState::tlocal()->GetEgressThrottler()`, a `detail::EgressThrottler`
(`src/server/detail/egress_throttle.{h,cc}`) configured from `--snapshot_egress_limit_bytes`
(bytes/second; `0` disables throttling, which is the default,
`src/server/server_state.cc:47-50,263`). It uses a GCRA (generic cell rate algorithm) to decide
how long the traversal fiber must sleep to keep the observed byte rate under the limit.

Two call sites cooperate: `IterateBucketsFb` calls `Throttle()` once per bucket-traversal
iteration to block if the shard is currently over budget
(`snapshot.cc:206`), and the actual write path calls `Record(bytes, high_prio)` when data is
pushed out (`snapshot.cc:291`). The throttler distinguishes high- and low-priority egress:
regular bulk snapshot data pushed from the traversal fiber itself is recorded as low priority,
while data pushed from any other fiber (`!snapshot_fb_.IsActive()`, e.g. an inline `OnChange`
side-save reacting to a live write) is recorded as high priority and is not throttled until
low-priority egress has already claimed its own baseline share of the budget. This keeps the
bulk snapshot from starving the live journal traffic riding along the same connection, and vice
versa - a saturated snapshot egress budget slows the traversal down, not ordinary writes.

The same per-thread throttler instance is also used by the stable-sync `JournalStreamer`
(`AsyncWrite`, `src/server/journal/streamer.cc:294`) and by `RestoreStreamer`'s migration bucket
loop (`snapshot.cc`/`streamer.cc`), so `--snapshot_egress_limit_bytes` effectively caps total
replication/migration egress bandwidth per shard thread, not just the initial full-sync photo.

## 4. Stable sync (`DFLY STARTSTABLE`)

The replica sends `DFLY STARTSTABLE <sync_id>` once full sync's cut is observed on every flow
(or immediately, for an all-partial session). `DflyCmd::StartStable`
(`src/server/dflycmd.cc:434-485`) requires state `FULL_SYNC` or `PREPARATION` (the latter covers
the all-partial case, where `DFLY SYNC` was never sent) and that every flow's connection is still
alive. For each shard:

- If the flow was full: `StopFullSyncInThread` (finalize snapshot, send journal offset + EOF
  token, described above).
- If the flow was partial: nothing to stop (no saver was ever started).
- Either way: `StartStableSyncInThread` creates a `JournalStreamer` and calls `Start()` on the
  flow socket (`src/server/dflycmd.cc:751-769`).

`JournalStreamer::Start` (`src/server/journal/streamer.cc:147-155`): if
`config_.start_partial_sync_at == 0` (i.e. a full-sync flow, which always starts stable sync from
"now"), it registers as a live journal consumer immediately. If it's a partial-sync flow with a
nonzero starting LSN, it does **not** register yet - a background "stalled data writer" fiber
first runs `MaybePartialStreamLSNs` (`streamer.cc:206-246`), which walks the ring buffer entry by
entry from the requested LSN forward (`journal::IsLSNInBuffer` / `journal::GetEntry`) and writes
each one to the socket directly, *then* registers as a live consumer once it has caught up to
`journal::GetLsn()`. If the buffer entries get evicted out from under it while it's replaying
(shouldn't normally happen since the eviction check already passed at `DFLY FLOW` time, but the
buffer keeps advancing concurrently), it reports an unrecoverable error instead of silently
resyncing.

`ReplicaInfo` transitions to `STABLE_SYNC` and the master replies `+OK`. On the replica,
`Replica::ConsumeDflyStream` (`src/server/replica.cc:889-944`) starts a
`StableSyncDflyReadFb`/`StableSyncDflyAcksFb` pair per flow and blocks until an error/cancel
tears it all down - there's no clean way to leave stable sync; the *only* way `ConsumeDflyStream`
returns is via `exec_st_.GetError()` after cancellation (`src/server/replica.cc:940-943`,
`CHECK(exec_st_.GetError())`).

### Wire format & command grouping

Each journal entry (`journal::Entry`/`ParsedEntry`, `src/server/journal/types.h`) carries an
opcode (`SELECT`, `EXPIRED`, `COMMAND`, `PING`, `LSN`) plus, for `COMMAND`/`EXPIRED`, a `TxId`,
`DbIndex`, and the backed command arguments. `TransactionReader::NextTxData`
(`src/server/journal/tx_executor.cc:96-124`) turns the raw stream into `TransactionData` records
one at a time and (when tracking LSNs) cross-checks the LSN opcode's embedded value against its
own running counter, logging (not failing) on mismatch.

Multi-shard transactions (global commands `FLUSHALL`/`FLUSHDB`/`DFLYCLUSTER FLUSHSLOTS`, and
ordinary cross-shard multi-commands identified by a shared `TxId`) are re-synchronized across
flow fibers via `MultiShardExecution` (`src/server/journal/tx_executor.h:16-38`): each flow
inserts its `TxId` into a shared map and waits on a `BlockingCounter` until all participating
shards have received their portion, then a `Barrier` ensures exactly one flow (the one that
inserted first) actually executes a *global* command exactly once
(`DflyShardReplica::ExecuteTx`, `src/server/replica.cc:1242-1302`), while all flows wait again on
the barrier for it to finish before continuing. This is necessary because each flow fiber reads
and executes independently and out of lockstep with other flows' fibers.

`journal::Op::PING` entries (used by the master to force an ACK, see
[Backpressure & ACKs](#backpressure--acks) and [Takeover](#5-takeover-repltakeover--dfly-takeover))
are counted toward `journal_rec_executed_` on the replica but do not touch the executor, and - if
this replica shard's own journal happens to be active - are re-recorded into it as well
(`src/server/replica.cc:1129-1136`).

### Backpressure & ACKs

Each flow runs an acks fiber (`DflyShardReplica::StableSyncDflyAcksFb`,
`src/server/replica.cc:1188-1220`) that periodically (`--replication_acks_interval`, default
1000ms) or immediately after `>= 1024` newly-executed records sends
`REPLCONF ACK <journal_rec_executed_>` back to the master on the same flow socket. The master
records it in `FlowInfo::last_acked_lsn` (`ServerFamily::ReplConf`, ACK branch,
`src/server/server_family.cc:3594-3610`) - note this write happens on whichever thread owns that
shard's flow connection, matching the comment in `dflycmd.h` that `last_acked_lsn` is only
touched by the flow's owner-shard proactor.

On the master side, `JournalStreamer::ThrottleIfNeeded` (`streamer.cc:337-383`) blocks the
*journal-writing* fiber (not the replica connection) whenever the pending write buffer reaches
`--replication_stream_output_limit` (default 1MB) until the in-flight socket
write drains, up to `--replication_timeout` (default 30000ms) before giving up and cancelling the
whole replication session with an error. This is the master's push-back mechanism against a slow
or stuck replica - it is asymmetric: a replica falling behind stalls *all writers on that shard*
on the master, not just the one connection.

### Detecting a stuck full sync

Independently of ACKs, the master's periodic per-shard heartbeat calls
`DflyCmd::BreakStalledFlowsInShard` (`src/server/dflycmd.cc:831-869`, wired from
`shard_set->Init`'s heartbeat callback, `src/server/main_service.cc:1147-1148`). For every replica
still holding an active full-sync `saver` on this shard, if `RdbSaver::GetLastWriteTime()` is
older than `--replication_timeout` it force-cancels that whole replica session. This only
applies to the full-sync phase (`flow->saver` is reset once `StopFullSyncInThread` runs); stable
sync relies on `JournalStreamer::ThrottleIfNeeded`'s own timeout instead.

## Partial sync buffer & its limits

The partial-sync "recent history" is a per-shard `boost::circular_buffer<JournalItem>` sized by
`--shard_repl_backlog_len` (default 8192 entries - a count of entries, not bytes;
`src/server/journal/journal_slice.cc:20-21,39`). It is populated by every `AddLogRecord` call
regardless of whether any replica is currently connected (`journal::StartInThread` is called
once a shard starts journaling - which happens both when a replica first attaches and, notably,
also when *this* node was demoted from master to replica-of-someone-else and later resumes acting
as a source, see below). A replica reconnecting after the requested LSN has been evicted from the
buffer, or after `journal::ClearBuffer()` was called
(`src/server/journal/journal.cc:37-44`, invoked by `ServerFamily::ForceReplicasToFullSync`,
`src/server/server_family.cc:3352-3358`), always falls back to full sync for the affected flow(s)
- there is no partial partial-sync; a flow is either fully resumable or must fully resync.

`ClearBuffer` deliberately advances the LSN counter past whatever is currently buffered
(`journal_slice.SetStartingLSN(journal_slice.cur_lsn() + 1)`) specifically so that a stale
replica-held LSN can never accidentally alias a *future* LSN in an emptied buffer and be granted
a bogus "partial sync" over data it never actually saw.

## Partial sync after promotion / failover

When a replica is promoted to master, other replicas that were following the *old* master can
still resume via partial sync instead of a full resync, using the "different master" path of
`DflyCmd::Flow`'s `failover_match` check (`src/server/dflycmd.cc:344-368`).

Promotion happens via `REPLICAOF NO ONE`
(`ServerFamily::ReplicaOfNoOne`, `src/server/server_family.cc:3360-3383`) or via
`REPLTAKEOVER`/`DFLY TAKEOVER` (`src/server/server_family.cc:3460-3513`,
`src/server/dflycmd.cc:502-638`). Both eventually call `replica_->Stop()`, which - if the replica
had reached stable sync - returns a `LastMasterSyncData{id, last_journal_LSNs}` snapshot of the
old master's id and the per-flow LSNs this node had executed (`src/server/replica.cc:171-197`,
`Replica::Stop`). This is stashed as `ServerFamily::last_master_data_`. Other replicas that were
following the old master send their own remembered `last_master_id`/`lsn-vec` when they reconnect
(from their own `Stop()`/reconnect path); if it matches, they can resume from their own
last-seen LSN in the new master's *same* journal sequence, because a promoted node continues its
journal LSN numbering from where it left off as a replica, rather than resetting to 1 or to a new
epoch.

That LSN continuity is what `Replica::StartJournalAtOwnLSN` (`src/server/replica.cc:1521-1529`)
provides: each shard's journal is (re)started at `GetRecCountExecutedPerShard`, i.e. the number
of journal records this node has actually executed for the flows mapped to that shard (at least
1, since journal numbering starts at 1) - not at 0. It is invoked in two promotion paths:

- `ServerFamily::ReplicaOfNoOne`, gated by `--replicaof_no_one_start_journal` (default `true`) -
  "preserves journal offsets after `REPLICAOF NO ONE`".
- `ServerFamily::ReplTakeOver`, unconditionally, before issuing the `TAKEOVER` RPC to the old
  master - so this node's journal is already warm and partial-sync-ready the moment the takeover
  completes and it becomes master.

## 5. Takeover (`REPLTAKEOVER` / `DFLY TAKEOVER`)

`REPLTAKEOVER <seconds> [SAVE]` is issued on the replica; it forwards to the master as
`DFLY TAKEOVER <seconds> [SAVE] <sync_id>` (`Replica::TakeOver`, `src/server/replica.cc:215-233`).
On the master (`DflyCmd::TakeOver`, `src/server/dflycmd.cc:502-638`), while holding a shared lock
on the requesting `ReplicaInfo` and requiring it already be in `STABLE_SYNC`:

1. Atomically flips the master's global state `ACTIVE -> TAKEN_OVER`
   (`Service::SwitchState`) - this is the actual "lockdown"; a concurrent second takeover attempt
   fails here.
2. Waits (bounded by the given timeout) for all in-flight command dispatches across all listeners
   to finish (`DispatchTracker`), and disables key expiration
   (`SetExpireAllowed(false)`) so nothing mutates state during the handoff window.
3. For the *requesting* replica only, sends a `journal::Op::PING` (forces every stable-sync flow
   to send a fresh ACK immediately instead of waiting for its normal interval) and busy-waits
   (`WaitReplicaFlowToCatchup`, `src/server/dflycmd.cc:122-155`) until `last_acked_lsn` on every
   shard equals the master's current LSN, i.e. the replica has now applied everything.
4. If that succeeds, replies `+OK` to the takeover request, then - best-effort, without forcing a
   PING (which would itself advance the LSN and defeat partial sync for those nodes) - waits for
   *every other* connected replica to catch up too, so they don't miss data or need a full resync
   against the replica that is about to become the new master.
5. Optionally does a synchronous `SAVE` (test-only knob), then shuts the process down
   (non-cluster mode) or, in cluster mode, reconciles cluster slot ownership onto the new master
   (`cluster_family().ReconcileMasterSlots`).

The replica side of the handoff (turning the confirmed OK into "I'm now master") happens in
`ServerFamily::ReplTakeOver` (`src/server/server_family.cc:3460-3513`): it calls
`StartJournalAtOwnLSN()` *before* issuing the `TakeOver()` RPC (so its own journal is warm and
partial-sync-ready the moment it becomes master), and only flips `SetMasterFlagOnAllThreads(true)`
after the master confirms `+OK` and the old `replica_` is torn down.

## 6. Cancellation, disconnects, and cleanup

Any error on any flow (socket error, protocol error, timeout) invokes `ReplicaInfo`'s error
handler, installed at session creation (`DflyCmd::CreateSyncSession`,
`src/server/dflycmd.cc:776-782`): it detaches a fiber that calls `DflyCmd::StopReplication`
for that `sync_id`. `StopReplication` calls `ReplicaInfo::Cancel()`
(`src/server/dflycmd.cc:159-184`) which performs, in order:

1. Flip `SyncState` to `CANCELLED` and cancel the shared `ExecutionState` - any fiber blocked on
   `cntx_->IsRunning()` unblocks and starts unwinding.
2. Fan out to every shard and run that flow's `cleanup` closure - for a full-sync flow this
   shuts the socket and cancels the `RdbSaver` (`StartFullSyncInThread`'s cleanup,
   `src/server/dflycmd.cc:699-708`); for a stable-sync flow it shuts the socket and cancels the
   `JournalStreamer` (`StartStableSyncInThread`'s cleanup, `src/server/dflycmd.cc:762-768`).
3. Join the error handler fiber, then (back in `StopReplication`) erase the session from
   `replica_infos_` and republish the lock-free snapshot used by `INFO`/metrics readers.

The replica's own error handling is layered similarly: `Replica::MainReplicationFb`'s outer loop
(`src/server/replica.cc:241-319`) resets `state_mask_` back down to just `R_ENABLED` on any
failure at any phase and loops, waiting `--master_reconnect_timeout_ms` (default 1000ms) before
retrying the whole handshake from DNS resolution onward - a disconnect at any point restarts from
`Greet()`, it never resumes a handshake mid-way. Only `passed_full_sync_` and
`last_journal_LSNs_` (captured at the top of `Replica::Stop`/before a reconnect attempt) survive
across the reconnect, and only the latter is what makes partial sync on reconnect possible.

## Observability

- `SyncStateName` (`src/server/dflycmd.cc:53-66`) maps `SyncState` to the strings reported by
  `INFO REPLICATION` / `DFLYCLUSTER`-style tooling: `preparation`, `full_sync`, `stable_sync` (or
  `online` if `--info_replication_valkey_compatible`), `cancelled`.
- `DflyCmd::GetReplicasRoleInfo` (`src/server/dflycmd.cc:890-908`) is the master-side per-replica
  summary (id, address, port, state, lag) built from the thread-local
  `tl_replica_infos` snapshot (updated under `mu_` via `UpdateReplicaInfoCacheLocked`, published
  to every proactor so readers never take a lock).
- Replication lag is only meaningful (and only computed) for replicas in `STABLE_SYNC`
  (`DflyCmd::ReplicationLags`, `src/server/dflycmd.cc:958-992`): per shard,
  `journal::GetLsn() - flow.last_acked_lsn`, maximized across shards.
- `DFLY REPLICAOFFSET` (`src/server/dflycmd.cc:649-657`) returns the master's current per-shard
  LSN vector - used by `WAIT` (`ServerFamily::Wait`, `src/server/server_family.cc:3620` onward) to
  know what LSN replicas must reach.
- `Replica::GetSummary`/`GetClientInfo` (`src/server/replica.cc:1383-1502`) expose the replica-side
  view for `INFO`/`CLIENT LIST`: `GetCurrentPhase()` derives a human string
  (`DISABLED`/`TCP_CONNECTING`/`GREETING`/`FULL_SYNC_IN_PROGRESS`/`INITIAL_SYNC`/`STABLE_SYNC`)
  purely from `state_mask_`.

## Replicating from a Redis master

If the handshake's `REPLCONF capa dragonfly` reply is a single-element `+OK` (not a Dragonfly
master), the replica falls back to standard Redis replication:
`Replica::InitiatePSync` (`src/server/replica.cc:454-546`) issues `PSYNC ? -1`, parses either a
`+FULLRESYNC <replid> <offset>` (disk-based, size-prefixed RDB) or a diskless `EOF:<40-byte-token>`
stream, loads it through the same `RdbLoader` used for Dragonfly full sync (single-shard, no
sharded flows - everything arrives on one connection), then hands off to
`Replica::ConsumeRedisStream` (`src/server/replica.cc:725-887`), which parses the plain RESP
command stream Redis sends during stable sync, batches/squashes commands, and periodically sends
`REPLCONF ACK <offset>` via `Replica::RedisStreamAcksFb`. Partial resync (`PSYNC` `CONTINUE`) is
explicitly **not implemented** - `ParseReplicationHeader` treats a `+CONTINUE` reply as an error
(`src/server/replica.cc:1368-1374`, "Partial replication not supported yet"), so every reconnect
to a Redis master is a full resync. Dragonfly does not support the reverse direction (a Redis
instance replicating from a Dragonfly master).
