# Cluster Mode

## 1. Overview

A Dragonfly cluster is a set of Dragonfly nodes that collectively own the 16384-slot
hash space. Slot ownership is partitioned across master nodes; each master may have zero
or more replicas. Nodes do not gossip with each other and share no state: the topology
is authored by an external **cluster manager** and pushed to every node via a single
command (`DFLYCLUSTER CONFIG`). The cluster manager is the only source of truth for
cluster state. Each node accepts client requests only for keys whose slot it owns,
redirecting all others with `-MOVED`.

The cluster state explicitly includes any in-progress slot transfers between nodes:
migrations are not a side channel but a declarative field of the configuration JSON, and
a node starts, finishes, or cancels a migration purely as a consequence of receiving a
config in which the corresponding `migrations[]` entry has appeared, advanced, or
disappeared.

The intra-node sharding boundary (one thread-shard per core, shared-nothing) is
orthogonal to slot ownership. A node's owned slots are spread across its thread-shards
by a tag-derived hash (or, optionally, by slot id), so a hot slot's load is distributed
across cores regardless of cluster topology.

This document specifies the cluster command surface, the configuration JSON, the slot
routing rules, the migration protocol and state machine, and the invariants that make
the scheme safe under concurrent writes.

### 1.1 Terminology

- **Cluster manager** — the external operator that authors the topology, holds the only
  authoritative copy of cluster state, monitors node health, performs failovers, and
  pushes configuration to every node. Dragonfly ships only the data plane; the cluster
  manager is supplied separately (e.g. Dragonfly Swarm in Dragonfly Cloud, or a
  user-written orchestrator).
- **Slot** — one of the 16384 buckets `[0, 16383]` of the hash space.
- **Hash tag** — the substring of a key enclosed in `{}`. When present, only the tag is
  hashed; otherwise the whole key is hashed. Multi-key commands must resolve to a single
  slot, so applications use a shared tag (`user:{u42}:name`, `user:{u42}:cart`) to
  co-locate keys.
- **Thread-shard** — Dragonfly's intra-node partition. Each thread owns disjoint
  hash-table partitions and processes commands for them serially.
- **Source / target** — the master that currently owns a slot range being migrated, and
  the master that will own it after the migration finalizes.
- **Flow** — the per-thread-shard outbound migration stream. One flow per source shard.

## 2. Command Surface

Five top-level commands are registered. `CLUSTER` is exposed on every listener;
`DFLYCLUSTER` and `DFLYMIGRATE` are hidden and admin-only and must be reached through
the admin listener configured by `--admin_port`. `READONLY` and `READWRITE` are stubs
provided for client-library compatibility.

| Top-level | Subcommand | ACL flags | Scope |
|-----------|------------|-----------|-------|
| `CLUSTER` | `HELP`, `MYID`, `INFO`, `NODES`, `SHARDS`, `SLOTS`, `KEYSLOT <key>` | `SLOW` | Read-only introspection. Available on any listener. |
| `READONLY`, `READWRITE` | — | `FAST \| CONNECTION` | Stubs in real-cluster mode. No replica-read routing today. |
| `DFLYCLUSTER` | `CONFIG <json>` | `ADMIN \| GLOBAL_TRANS \| HIDDEN`, ACL `ADMIN \| SLOW` | Installs new cluster state. Runs as a global transaction. |
| `DFLYCLUSTER` | `GETSLOTINFO SLOTS <id\|a-b>...` | as above | Per-slot stats: `key_count`, `total_reads`, `total_writes`, `memory_bytes`. |
| `DFLYCLUSTER` | `FLUSHSLOTS <start> <end> [...]` | as above | Deletes data in slot ranges. Journaled to replicas. |
| `DFLYCLUSTER` | `SLOT-MIGRATION-STATUS [<peer_id>]` | as above | Per-migration status: `[direction, peer_id, state, keys_migrated, error]`. |
| `DFLYMIGRATE` | `INIT <source_id> <flows_num> [start end]...` | `ADMIN \| HIDDEN`, ACL `ADMIN \| SLOW \| DANGEROUS` | Source → target: announce migration. |
| `DFLYMIGRATE` | `FLOW <source_id> <shard_id>` | as above | Source → target: open one per-shard data stream. |
| `DFLYMIGRATE` | `ACK <source_id> <attempt>` | as above | Source → target: finalize attempt. |

A few invariants on the surface:

- **All `DFLYCLUSTER` commands require `--cluster_mode=yes`**, except that emulated mode
  silently ignores them. They are the cluster manager's interface; clients should never
  call them.
- **`DFLYMIGRATE` is internal**, spoken only between Dragonfly masters during a
  migration. It is not part of the client-facing contract.
- **The cluster manager must push to every node** — masters and replicas — before
  authoring a successor config. There is no in-cluster gossip; a node that has not
  received the new config will reply with stale slot ownership.

### 2.1 Reply shapes that matter

- **`-MOVED <slot> <ip>:<port>`** — returned for any key whose slot this node does not
  own. The endpoint is the current owner per the local config, which during a migration
  may already be the target (§5).
- **`-CROSSSLOT Keys in request don't hash to the same slot`** — returned for any
  command, transaction, or Lua script whose keys span more than one slot.
- **`-ERR Invalid cluster configuration.`** — `DFLYCLUSTER CONFIG` rejection. The
  previous config is preserved untouched.
- **`-ERR Cluster is disabled. Use --cluster_mode=yes to enable.`** — sent when
  `DFLYCLUSTER` is invoked on a node that is not in real-cluster mode.

### 2.2 Operational sequence for the cluster manager

A cluster manager typically:

1. Starts each node with `--cluster_mode=yes --admin_port=<p> [--cluster_node_id=<id>] [--cluster_announce_ip=<ip>]`.
2. Calls `DFLYCLUSTER MYID` on each node's admin port to discover its identity.
3. Issues `REPLICAOF <master_ip> <master_port>` to each intended replica. Replication is
   set up out-of-band from cluster state: the `replicas[]` field of the cluster config
   is descriptive (used by `CLUSTER NODES/SHARDS/SLOTS` to advertise topology and by
   client-facing health filtering) and does **not** by itself cause any node to start
   replicating.
4. Pushes the cluster JSON to every node via `DFLYCLUSTER CONFIG <json>`.
5. To migrate slots, edits the source shard's `migrations[]`, pushes the new JSON to
   every node, polls `DFLYCLUSTER SLOT-MIGRATION-STATUS` until each migration reports
   `FINISHED`, and then pushes a successor JSON in which the migration entry is dropped
   and the slots are reassigned to the target.
6. On any node restart, re-pushes the current cluster JSON; a freshly started node owns
   no slots and rejects user traffic until a config arrives.

## 3. Data Model

### 3.1 Slot identifiers

`SlotId` is a 16-bit value in `[0, kMaxSlotNum]` where `kMaxSlotNum = 0x3FFF`. The
mapping from key to slot is

```
SlotId KeySlot(key) = crc16(tag(key)) & 0x3FFF
```

where `tag(key)` returns the content of the first balanced `{...}` substring if both
braces are present and the content is non-empty, otherwise `key` itself.

`SlotRange { start, end }` is the **closed** interval `[start, end]`. `SlotRanges` holds
a sorted vector of disjoint closed ranges. Together they describe slot ownership and
the per-shard migration scope. `SlotSet` is a 16384-bit bitset built from `SlotRanges`
for O(1) ownership tests on the hot path.

### 3.2 Hash tags and intra-node sharding

The key tag is used independently in two places:

1. **Cluster slot computation.** `KeySlot(key)` always uses the tag and always returns
   a 14-bit value, regardless of whether cluster mode is enabled.
2. **Thread-shard selection.** By default, a key's owning thread-shard is
   `XXH64(tag(key)) % shard_count`. Under `--experimental_cluster_shard_by_slot`, it is
   `KeySlot(key) % shard_count` instead.

Default tag-hash mode spreads any individual slot's keys across multiple thread shards
on the owning node, so a hot slot does not become a hot core. Slot-modulo mode pins each
slot to one shard and is reserved for future migration optimizations where bucket-level
traversal is cheaper when slots are contiguous.

### 3.3 Cluster topology

The topology is an array of `ClusterShardInfo`. Each shard is one master plus zero or
more replicas, owning a disjoint set of slot ranges, with zero or more outgoing
migrations attached.

```
ClusterShardInfo {
  SlotRanges          slot_ranges;     // slots owned by this master, sorted, disjoint
  ClusterExtendedNodeInfo master;       // node + health
  vector<ClusterExtendedNodeInfo>  replicas;
  vector<MigrationInfo>            migrations;  // outgoing from this master
}

ClusterNodeInfo         { string id; string ip; uint16 port; }
ClusterExtendedNodeInfo { ClusterNodeInfo; NodeHealth health = ONLINE; }
MigrationInfo           { SlotRanges slot_ranges; ClusterNodeInfo node_info; }
enum NodeHealth         { FAIL, LOADING, ONLINE, HIDDEN }
```

`ClusterShardInfo`s are kept sorted by `master.id`, so a config that is byte-different
but semantically identical compares equal under `operator==`.

`MigrationInfo` lives only on the source shard; the target shard learns of an incoming
migration by inspecting `migrations` arrays in other shards whose `node_info.id` matches
its own node id. **The migration is part of the cluster state**, not a control message:
the act of receiving a config whose `migrations[]` differs from the previous config is
what causes a source to start streaming or a finished migration to be cleaned up.

![Cluster topology](./cluster-topology.svg)

## 4. Cluster Configuration

`DFLYCLUSTER CONFIG <json>` is the single mechanism by which a node learns the cluster
state. The same JSON is pushed to every node — masters and replicas alike. Replicas
need the state to redirect clients via `-MOVED` and to participate in migration cleanup.

### 4.1 JSON wire format

```jsonc
[
  {
    "slot_ranges": [ { "start": 0, "end": 8191 } ],
    "master": {
      "id":   "node-A",
      "ip":   "10.0.0.1",
      "port": 7000,
      "health": "online"           // optional, default "online"
    },
    "replicas": [
      { "id": "node-A-r1", "ip": "10.0.0.2", "port": 7000, "health": "loading" }
    ],
    "migrations": [                  // optional; presence drives the migration FSM
      {
        "node_id":     "node-B",
        "ip":          "10.0.0.3",
        "port":        7000,         // admin port of the target
        "slot_ranges": [ { "start": 4096, "end": 8191 } ]
      }
    ]
  },
  {
    "slot_ranges": [ { "start": 8192, "end": 16383 } ],
    "master":   { "id": "node-B", "ip": "10.0.0.3", "port": 7000 },
    "replicas": []
  }
]
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `slot_ranges[].start` / `.end` | uint16 | yes | Closed interval, `0..16383`, `start <= end`, ranges disjoint. |
| `master.id` | string | yes | Matches the target node's `DFLYCLUSTER MYID`. |
| `master.ip`, `.port` | string, uint16 | yes | Endpoint advertised to clients via `CLUSTER SLOTS/SHARDS/NODES` and embedded in MOVED replies. |
| `master.health` | enum string | no | `online` \| `loading` \| `fail` \| `hidden`; default `online`. See [cluster-node-health.md](cluster-node-health.md) for filtering rules. |
| `replicas[]` | array | yes (may be empty) | Descriptive only; replicas are wired up by `REPLICAOF` separately. |
| `migrations[]` | array | no | Outgoing slot migrations from this master. |
| `migrations[].node_id`, `.ip`, `.port` | | yes | The **target** master node. Port is the target's admin port. |
| `migrations[].slot_ranges` | | yes | Non-empty; must lie entirely inside this shard's `slot_ranges`. |

### 4.2 Validation

A config is rejected with `-ERR Invalid cluster configuration.` if any of the following
hold:

- A slot in `[0, 16383]` is owned by zero or by more than one shard.
- A node id appears twice as `master`, or twice as a replica of the same master.
- A `migrations[]` entry targets the migration's own source shard.
- A `migrations[]` target id is not a master in the same config.
- A source shard has two `migrations[]` entries with the same target id (the "one
  migration per node pair" invariant).
- A `migrations[]` slot range is empty, invalid, not contained in the source shard's
  ranges, or overlaps another `migrations[]` entry on the same source.

Validation runs before installation; on failure the node continues with its previous
config unchanged.

### 4.3 Installation and atomicity

A new config replaces the previous one atomically on each receiving node:

1. Parse and validate the JSON into a new `ClusterConfig`.
2. Diff against the previous config to compute four sets: `enable_slots`,
   `disable_slots`, `new outgoing migrations`, `removed migrations`.
3. `AwaitFiberOnAll(SetCurrent)` installs the new config on every proactor in lockstep
   under a process-wide `set_config_mu` mutex.
4. For each slot in `disable_slots`, start a background fiber that traverses every
   shard and deletes entries belonging to those slots (§5.3). On the source side, "data
   from migrated slots is permanently erased" once the slots leave its ownership.
5. Start outgoing-migration fibers for newly added `migrations[]` entries and stop
   fibers for removed ones.

Steps 3-5 happen on the receiving node. Cross-node consistency is the cluster manager's
responsibility: it must apply a new config to every node before authoring the next one,
or clients may MOVED-bounce between disagreeing masters.

A node owns no slots before the first config arrives, so user traffic to a freshly
started node is rejected with the cluster-not-configured error. The cluster manager must
also re-push the current config after any node restart and after every topology change.

The config itself is **not** carried over the replication stream; replicas receive their
own `DFLYCLUSTER CONFIG` directly. The journal does carry slot-flush side effects
(§5.3) so that replicas drop the same data their master does.

## 5. Slot Routing

### 5.1 Owned-slot check

Every command that touches a key fans its keys through `KeySlot()` and aggregates them
into a `UniqueSlotChecker`. The transaction layer then enforces two properties before
dispatch:

1. **Single-slot transaction invariant.** If keys map to more than one distinct slot,
   the command is rejected with `-CROSSSLOT`.
2. **Ownership check.** For every key's slot, `SlotOwnershipError(slot)` runs:
   - If no cluster config is installed, the command is rejected with the
     cluster-not-configured error.
   - If the slot is not in this node's `SlotSet`, the command is rejected with
     `-MOVED <slot> <master.ip>:<master.port>`, where `master` is the current owner per
     the local config (which during a migration may already be the target).

These checks are O(1) per key after the bitset lookup; they run on every command in
cluster mode.

### 5.2 MOVED redirection

A `-MOVED` reply carries the integer slot id and the endpoint of the master that the
local config says owns that slot. Clients are expected to refresh their slot map via
`CLUSTER SHARDS` or `CLUSTER SLOTS` and retry. During migration, the source returns
MOVED for the migrating slots only after the finalizing config has been applied; until
then the source continues to serve the slots from its own copy of the data while
streaming updates to the target (§6).

### 5.3 Slot deletion

A slot transition from "owned" to "not owned" triggers an incremental background
deletion via `DbSlice::FlushSlots(slot_ranges)`:

- The DbSlice captures a monotonically increasing `next_version` value and installs an
  on-change callback that deletes any newly inserted entry with `version < next_version`
  in the flushed ranges. This catches inserts that race the traversal.
- A detached fiber walks every hash-table bucket of the prime table on the owning
  thread-shard, deleting entries whose `KeySlot()` falls in the flushed ranges and
  whose version predates `next_version`. The fiber yields on every bucket so other
  work proceeds.
- After traversal, the channel store drops sharded-pub/sub subscriptions for the
  flushed slots.

The same path is invoked by `DFLYCLUSTER FLUSHSLOTS <start> <end> ...` and is replicated
to replicas as a `DFLYCLUSTER FLUSHSLOTS` journal entry, so master and replicas drop the
same data.

### 5.4 Cluster-mode command restrictions

In real cluster mode (`--cluster_mode=yes`):

- `SELECT` to any DB index other than the default is rejected; cluster-mode storage is
  single-DB.
- Global-keyspace pub/sub (`PUBLISH`, `SUBSCRIBE`, `PSUBSCRIBE`) is rejected. Sharded
  pub/sub (`SPUBLISH`, `SSUBSCRIBE`, `SUNSUBSCRIBE`) routes by slot and is supported.
- Multi-key commands and `MULTI/EXEC` blocks that span multiple slots return
  `-CROSSSLOT`. Lua scripts that touch multiple slots are likewise rejected (global-lock
  Lua scripts are not supported in cluster mode).

`--cluster_mode=emulated` exposes the cluster commands on a single node that owns all
16384 slots. In emulated mode `DFLYCLUSTER` is not available, `SELECT` and global
pub/sub still work, and the above restrictions do not apply. Emulated mode is intended
for development, migration phases, and resource-constrained environments where one node
stands in for an entire cluster.

## 6. Migration Protocol

A slot migration is initiated by adding a `migrations[]` entry to the source shard in
the next `DFLYCLUSTER CONFIG`. Because migration is part of cluster state rather than a
distinct command, the protocol becomes the reactive consequence of two configs landing
on every node: the **opening** config introduces the entry, and the **closing** config
removes the entry and reassigns the slots to the target. Adding the same entry twice is
idempotent; the source's state machine ignores configs that do not change its
outgoing-migration set. At most one migration may be in flight between any given source
/ target pair — enforced by config validation (§4.2).

The data path reuses the replication wire: a `RestoreStreamer` serializes the migrating
slots as `RESTORE` commands and forwards subsequent journal mutations on the same slots
as ordinary write commands, all over a single TCP connection per source thread-shard.
`RESTORE` semantics give the target the same shard-local execution path as any other
write, so updates flow through the target's transaction framework and onward to the
target's replicas without any migration-specific code on the replica side.

![Slot migration](./cluster-slot-migration.svg)

### 6.1 States

Each source-side `OutgoingMigration` and each target-side `IncomingSlotMigration`
carries a `MigrationState` from the ordered enum

```
C_CONNECTING → C_SYNC → C_FINISHED        (happy path)
              ↘ C_ERROR ↗                  (transient, retried)
              ↘ C_FATAL                    (terminal, no retry)
```

| State | Source meaning | Target meaning |
|-------|----------------|----------------|
| `C_CONNECTING` | Opening TCP, authenticating, waiting on INIT reply. | `INIT` received; flows not yet open. |
| `C_SYNC` | Streaming snapshot + journal over all flows. | Reading and applying `RESTORE` and subsequent write commands. |
| `C_ERROR` | Transient failure (peer disconnect, INIT rejected, ACK timeout). Retried after 500 ms. | Same; recoverable. |
| `C_FINISHED` | ACK acknowledged; slots disabled locally via config diff. | LSN matched on every flow; `Join()` returned. |
| `C_FATAL` | Target reported `INCOMING_MIGRATION_OOM`; will not retry. | Local apply hit `OOM`; flushed migrated keys. |

There is no `C_FATAL → C_ERROR` transition; recovery from `C_FATAL` requires the
cluster manager to remove and re-add the migration after addressing the memory
shortage.

`DFLYCLUSTER SLOT-MIGRATION-STATUS` reports these states verbatim as
`CONNECTING | SYNC | ERROR | FINISHED | FATAL` strings, alongside direction
(`in`/`out`), peer id, count of migrated keys, and the last error message.

### 6.2 Source flow

The source spawns one `OutgoingMigration` per `migrations[]` entry, owning a single
fiber `SyncFb`:

1. **CONNECTING.** Open one TCP connection to the target's admin port; authenticate via
   the same path used by replication. Timeout is
   `--slot_migration_connection_timeout_ms` (default 2000).
2. **INIT.** Send `DFLYMIGRATE INIT <my_id> <flows_num> <slot_start> <slot_end> ...`
   over that socket, where `flows_num` is the source's thread-shard count. The target
   replies `+OK` once it has created an `IncomingSlotMigration` keyed by `<my_id>` with
   the announced slot ranges. Two error replies are special:
   - `INCOMING_MIGRATION_OOM` → source moves to `C_FATAL` and stops retrying.
   - `UNKNOWN_MIGRATION` (target has not yet applied the matching config) → sleep
     500 ms and retry. After 30 s the source escalates to `C_ERROR` to surface a
     cluster-manager misorder.
3. **Per-shard FLOW.** On every source thread-shard, open a fresh connection and send
   `DFLYMIGRATE FLOW <my_id> <shard_id>`. The target replies `+OK` and detaches the
   socket from the command dispatch loop, handing it to the flow consumer (§6.3).
4. **SYNC.** Under a single global transactional cut, every flow's `RestoreStreamer`
   registers as a journal listener and a db_slice change listener for its subset of the
   migrating slot ranges. The streamer then walks the prime table emitting `RESTORE`
   for each entry in the migrating slots while concurrent mutations are captured by the
   journal listener and sent through the same socket. Once the bucket walk completes,
   the streamer continues forwarding journal entries.
5. **Finalization.** With `attempt = 1`, the source issues a node-wide client pause
   (non-admin connections only), runs `Finalize(attempt)` on every flow to push an
   `LSN(attempt)` opcode, then sends `DFLYMIGRATE ACK <my_id> <attempt>` and waits up
   to `--migration_finalization_timeout_ms` (default 30000) for `+OK`. If the target
   reports any flow as not yet quiesced, the source increments `attempt`, reopens the
   ACK connection if needed, and retries.
6. **C_FINISHED.** On a successful ACK, the source `CloneWithChanges(disable=migrating)`
   the local config so the migrated slots immediately MOVED-redirect to the target.
   The migration entry itself is not removed yet; that requires the closing
   `DFLYCLUSTER CONFIG` from the cluster manager. On that config, the source's slot
   deletion path (§5.3) runs and the migration object is destroyed.

### 6.3 Target flow

The target maintains one `IncomingSlotMigration` per source node id, holding
`flows_num` independent flow consumers:

- On `DFLYMIGRATE INIT`: create the `IncomingSlotMigration` (or reuse the existing one
  for this source if its slot ranges are unchanged), set state to `C_CONNECTING`, and
  arm a `BlockingCounter` of size `flows_num` so `Join()` only succeeds when every
  flow has matched the same `attempt`.
- On each `DFLYMIGRATE FLOW`: validate the source id, look up the matching
  `ClusterShardMigration`, hand it the socket, and reply `+OK`. The thread-shard
  consumer then runs a `JournalReader` over the socket, decoding journal entries and
  executing them with a `JournalExecutor` that applies `RESTORE` and subsequent writes
  to the target's local state. The migrating slots are **not** marked as locally owned
  yet, so client traffic for them still receives MOVED back to the source.
- On `--slot_migration_throttle_us > 0`, the consumer sleeps `throttle_us` microseconds
  every 100 µs of accumulated processing time to cap migration's CPU share.
- An applied command that returns `OpStatus::OUT_OF_MEMORY` is reported as a fatal
  error: the migration transitions to `C_FATAL`, the existing migrated keys are
  flushed, and the source is sent `INCOMING_MIGRATION_OOM` on its next INIT/ACK.
- On `DFLYMIGRATE ACK <source_id> <attempt>`: call `Join(attempt)` to wait, up to
  `--migration_finalization_timeout_ms`, for every flow to receive an `LSN(attempt)`
  opcode and decrement the counter. If any flow has consumed further journal entries
  past that `LSN`, the attempt is treated as failed and the source must retry with
  `attempt + 1`. On success, reply `+OK`.

The target does not enable the migrated slots on ACK. It enables them only when the
closing `DFLYCLUSTER CONFIG` arrives with the slots assigned to it. Between ACK and that
config, the slots are present in storage but routed to the source by the target's own
`SlotOwnershipError`. This gap is what makes the protocol safe under cluster-manager
asymmetry: a target that has ACKed but not yet been informed of its new ownership
behaves as if the migration never happened.

### 6.4 Replicas and migration

Migration data does not flow over the replication channel. Source and target replicas
receive the migrated keys via their masters' normal replication stream — because the
incoming side applies `RESTORE` and subsequent writes through its transaction
framework, those mutations are journaled and replicated to its replicas exactly like
client writes.

What replicas do see directly is the `DFLYCLUSTER CONFIG` itself, pushed to them by the
cluster manager. A replica uses the config only for:

- MOVED-target computation when its master is in `loading` or `fail`, or when a slot is
  migrated away from its master.
- Tracking that a migration is in progress, so if its master fails mid-migration the
  cluster manager can promote it and remove the stale migration entry in the next
  config.

`DFLYCLUSTER FLUSHSLOTS` is journaled (§5.3); replicas drop the same keys their master
drops when slot ownership shrinks.

## 7. Failure Modes

| Failure | Detection | Recovery |
|---------|-----------|----------|
| Source-to-target TCP loss during SYNC | `JournalReader` read error on target; `RestoreStreamer` write error on source. | Both sides transition to `C_ERROR`. Source's `SyncFb` retries from CONNECTING after 500 ms; target keeps the partial state until the source reconnects with a new INIT that reuses the same migration object. |
| Target rejects INIT with `UNKNOWN_MIGRATION` | Source parses error reply. | Source sleeps 500 ms and retries. Escalates to `C_ERROR` after 30 s to flag a cluster-manager misorder. |
| Target reaches OOM applying a streamed command | `DispatchResult::OOM` returned from `JournalExecutor`. | Target moves to `C_FATAL`, flushes the partially migrated keys, and on the source's next INIT/ACK replies `INCOMING_MIGRATION_OOM`. Source moves to `C_FATAL` and stops retrying; cluster manager must remove the migration. |
| ACK round-trip lost after target's `Join` succeeded | Source's ACK read times out; target is already in `C_FINISHED`. | Source increments `attempt` and re-runs finalization. Target's `Join(attempt+1)` returns immediately because every flow's `BlockingCounter` is already zero. |
| Source dies between ACK and closing config | Target's `IncomingSlotMigration` is `C_FINISHED` with migrated keys present; target does not own the slots in its config. | Cluster manager detects the source failure and pushes a config that assigns the slots to the target. Target enables the slots on that config; no replay needed. |
| Target dies during SYNC | Source's flow sockets close. | Source returns to `C_ERROR` and retries from CONNECTING. The replacement target (after cluster-manager promotion) starts a fresh migration from scratch. |
| Replica of target loses connection to its master during SYNC | Replica's replication path retries normally. | The replica resyncs from its master; the migrated keys arrive as part of the normal replication catch-up. |

## 8. Modes, Flags, Ports

`--cluster_mode` selects the operating mode:

| Value | Behavior |
|-------|----------|
| unset / `""` | No cluster commands; `DFLYCLUSTER` rejected. |
| `emulated` | Single-node owner of slots `0..16383`. `CLUSTER *` answers from a synthetic topology built from the local replication summary. `DFLYCLUSTER` is not exposed. |
| `yes` | Full cluster mode. Topology must be installed via `DFLYCLUSTER CONFIG` before the node serves any data-plane traffic. |

| Flag | Default | Effect |
|------|---------|--------|
| `--admin_port` | `0` (off) | Required in real-cluster mode. `DFLYCLUSTER` and `DFLYMIGRATE` only accept connections on this listener; client-facing listeners reject them. |
| `--cluster_node_id` | `""` (uses the master replication id) | Node identity in the config; must match what the cluster manager embeds in `master.id`, `replicas[].id`, and `migrations[].node_id`. Disallowed in `emulated` mode. |
| `--cluster_announce_ip` | `""` (uses the local bind) | IP returned to clients in `CLUSTER SLOTS/SHARDS/NODES` and embedded in MOVED replies. |
| `--experimental_cluster_shard_by_slot` | `false` | When `true`, thread-shard selection uses `KeySlot(key) % shard_count` instead of `XXH64(tag(key)) % shard_count`. |
| `--slot_migration_throttle_us` | `0` (off) | Target-side throttle: sleep this many microseconds after every 100 µs of migration processing. Recommended `20` if migrations starve user traffic. |
| `--slot_migration_connection_timeout_ms` | `2000` | Source-side TCP connect timeout for INIT, FLOW, and ACK reconnects. |
| `--migration_finalization_timeout_ms` | `30000` | Source-side wait for ACK; target-side wait inside `Join()` for all flows to reach the same `attempt`. |

The wire format of `DFLYMIGRATE` is internal and not versioned. A mixed-version cluster
during a rolling upgrade is unsupported for slot migrations: cluster topology may be
updated, but `migrations[]` entries should be drained before upgrading either source or
target.

## 9. What Dragonfly Does Not Provide

Dragonfly ships only the cluster data plane. The following are out of scope for the
server and must be supplied by the cluster manager (or by a cloud product such as
Dragonfly Swarm):

- Node health detection and `health` field maintenance in the config.
- Master failover and replica promotion.
- Slot allocation, rebalancing, and the choice of which slots to migrate.
- Driving the open/close config pair for a migration.
- Inter-cluster orchestration (joining or splitting clusters).

A correct cluster manager treats Dragonfly nodes as passive: it observes them via
`CLUSTER` and `DFLYCLUSTER` introspection, computes a new authoritative config, and
pushes it to every node before authoring the next one.

## 10. Open Issues

1. Source-side replicas are not aware of in-progress outgoing migrations and do not
   incrementally drop migrated keys on FLUSHSLOTS-equivalent journaled events until the
   closing config update. This is correct but causes a transient memory bump until the
   source's slot-flush journal entry catches up.
2. `READWRITE` / `READONLY` are stubs in real-cluster mode; replicas do not currently
   serve reads with client-driven routing.
3. Per-slot blocking on the source during finalization is a node-wide client pause; a
   future change should pause only the migrating slots' keys.
4. There is no in-band heartbeat between source and target during SYNC; failure
   detection relies on TCP errors and the finalization timeout.
