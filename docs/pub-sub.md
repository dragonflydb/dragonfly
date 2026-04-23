# Pub/Sub Architecture

This document describes how Dragonfly implements the Publish-Subscribe (Pub/Sub) messaging
paradigm within its shared-nothing, multi-threaded architecture. It covers the global
subscription registry, the Read-Copy-Update (RCU) mechanism used to prevent lock contention
on the publish path, the asynchronous message delivery pipeline, and the backpressure system
that protects the server from slow-subscriber OOM.

## Overview

In a shared-nothing architecture, handling `PUBLISH` and `SUBSCRIBE` commands presents a
unique challenge: subscriptions must be globally addressable across all threads, but taking a
global lock on every `PUBLISH` would create a severe bottleneck. A single popular channel
with thousands of subscribers could serialize all publish operations onto one shard thread.

Dragonfly solves this by using a **centralized `ChannelStore` updated via RCU
(Read-Copy-Update)**:

- **Reads (`PUBLISH` / `SPUBLISH`)** are lock-free and use a thread-local pointer to the
  most recent `ChannelStore` snapshot.
- **Writes (`SUBSCRIBE` / `UNSUBSCRIBE` / `PSUBSCRIBE` / `PUNSUBSCRIBE`)** are serialized
  by a single mutex, performed by copying the necessary routing maps, applying the mutation,
  and atomically swapping the global pointer.

This design avoids contention on a single shard thread for heavy throughput on a single
channel and seamlessly scales across multiple threads even with a small number of channels.
Publish latency is lower than a shard-routed design because no inter-thread hop is required
to look up subscribers — the caller reads its local copy directly.

Dragonfly supports three flavors of Pub/Sub:

| Flavor | Commands | Scope | Cluster mode |
|--------|----------|-------|--------------|
| **Standard** | `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE` | Global (all channels) | Blocked — returns error |
| **Pattern** | `PSUBSCRIBE`, `PUNSUBSCRIBE` | Global (glob-matched) | Blocked — returns error |
| **Sharded** | `SPUBLISH`, `SSUBSCRIBE`, `SUNSUBSCRIBE` | Per-slot (channel name determines slot) | Supported |

## Primary Data Structures

| Type | Location | Role |
|------|----------|------|
| `ChannelStore` | `src/server/channel_store.h` | Centralized registry mapping channels/patterns to subscribers. Updated via RCU. |
| `ChannelStoreUpdater` | `src/server/channel_store.h` | Orchestrates RCU mutations (add/remove) to the `ChannelStore`. |
| `ChannelStore::Subscriber` | `src/server/channel_store.h` | Represents a subscribed client. Wraps `facade::ConnectionRef` plus a pattern string. |
| `ChannelStore::ControlBlock` | `src/server/channel_store.h` | Holds the `most_recent` atomic pointer and `update_mu` mutex. Prevents overlapping structural updates. |
| `ChannelStore::ChannelMap` | `src/server/channel_store.h` | `flat_hash_map<string, UpdatablePointer>` — maps channel/pattern names to subscriber lists. |
| `ChannelStore::SubscribeMap` | `src/server/channel_store.h` | `flat_hash_map<ConnectionContext*, ThreadId>` — maps subscriber contexts to their owning thread. |
| `ChannelStore::UpdatablePointer` | `src/server/channel_store.h` | Atomic wrapper around `SubscribeMap*`. Supports lock-free reads (`acquire`) and RCU-style swaps (`release`). |
| `ConnectionState::SubscribeInfo` | `src/server/conn_context.h` | Per-connection set of subscribed channels and patterns. Created lazily on first subscription. |
| `Connection::PubMessage` | `src/facade/dragonfly_connection.h` | Carries a formatted pub/sub message through the async dispatch queue. |
| `Connection::MessageHandle` | `src/facade/dragonfly_connection.h` | Variant wrapper (`PubMessage`, `MonitorMessage`, `MigrationRequest`, etc.) for the control-path dispatch queue. |
| `QueueBackpressure` | `src/facade/dragonfly_connection.cc` | Per-thread struct tracking `subscriber_bytes` and enforcing the `publish_buffer_limit`. |
| `facade::ConnectionRef` | `src/facade/connection_ref.h` | Weak reference to a connection. Thread-safe expiry check; dereference only on owning thread. |

## Data Flow Overview

<div align="center">
  <img src="pubsub/pubsub_data_flow_overview.svg" alt="Pub/Sub Data Flow" width="1000"/>
</div>

## Subscription Management (RCU)

### Data Structure Layout

Each `ChannelStore` instance holds two `ChannelMap` pointers:

<div align="center">
  <img src="pubsub/pubsub_data_structure_layout.svg" alt="Data Structure Layout" width="700"/>
</div>

`UpdatablePointer` wraps a `std::atomic<SubscribeMap*>` with `memory_order_acquire` on read
and `memory_order_release` on write. This ensures that when a thread reads the pointer, it
also sees the fully constructed `SubscribeMap` that the writer published.

### Two Levels of RCU

The `ChannelStoreUpdater` implements two granularities of copy-on-write:

1. **ChannelMap-level copy** — triggered when a channel slot must be added (first subscriber)
   or removed (last subscriber leaves). The entire `ChannelMap` is shallow-copied, the slot is
   added/removed on the copy, a new `ChannelStore` is allocated pointing to the new map, and
   the global `control_block.most_recent` is swapped.

2. **SubscribeMap-level RCU** — triggered when adding/removing a subscriber to an existing
   channel (the map slot already exists). Only the `SubscribeMap` for that channel is copied,
   the mutation is applied, and the `UpdatablePointer` is atomically swapped. No new
   `ChannelStore` or `ChannelMap` is needed.

This two-level scheme is implemented in `ChannelStoreUpdater::GetTargetMap()`:

```cpp
pair<ChannelStore::ChannelMap*, bool> ChannelStoreUpdater::GetTargetMap(ChannelStore* store) {
  auto* target = pattern_ ? store->patterns_ : store->channels_;

  for (auto key : ops_) {
    auto it = target->find(key);
    DCHECK(it != target->end() || to_add_);
    // We need to make a copy, if we are going to add or delete a new map slot.
    if ((to_add_ && it == target->end()) || (!to_add_ && it->second->size() == 1))
      return {new ChannelStore::ChannelMap{*target}, true};
  }

  return {target, false};
}
```

### Apply() Flow

<div align="center">
  <img src="pubsub/pubsub_apply.svg" alt="Apply Flow" width="1000"/>
</div>

Step 8 uses `AwaitBrief` (non-preempting dispatch) to update each thread's local pointer.
The `seq_cst` load in the callback ensures the thread reads the latest pointer value _and_
the memory published behind it.

### Modify() — Per-Key Mutation

For each key in the pending operations:

```
Modify(target, key)
  it = target->find(key)

  Case 1: Adding, key not in map (new channel)
    → target->emplace(key, new SubscribeMap{{cntx_, thread_id_}})

  Case 2: Removing, last subscriber (channel disappears)
    → freelist_.push_back(it->second.Get())  // defer deletion
    → target->erase(it)

  Case 3: Existing channel, add/remove subscriber (RCU on SubscribeMap)
    → replacement = new SubscribeMap{*it->second}
    → if to_add_: replacement->emplace(cntx_, thread_id_)
      else:       replacement->erase(cntx_)
    → freelist_.push_back(it->second.Get())  // old map, defer deletion
    → it->second.Set(replacement)            // atomic release-store
```

Old `SubscribeMap` pointers are not immediately deleted because concurrent `PUBLISH`
operations on other threads may still be reading them. They are placed in a `freelist_` and
deleted only after `AwaitBrief` completes — at which point every thread has acknowledged the
new state and no reader can hold a reference to the old maps.

### Connection-Level Subscription State

Each connection tracks its own subscriptions in `ConnectionState::SubscribeInfo`:

```cpp
struct SubscribeInfo {
  absl::flat_hash_set<std::string> channels;
  absl::flat_hash_set<std::string> patterns;

  bool IsEmpty() const;
  unsigned SubscriptionCount() const;
};
```

- **Created lazily**: `subscribe_info` is allocated on the first `SUBSCRIBE` or `PSUBSCRIBE`.
  A `subscriptions` counter on `ConnectionContext` is incremented.
- **Destroyed on empty**: when all channels and patterns are removed, `subscribe_info` is
  reset and `subscriptions` is decremented.
- **Gates pub/sub mode**: `AsyncOperations::operator()(PubMessage)` checks
  `cntx()->subscriptions == 0` and discards stale messages that arrive after full
  unsubscription (possible due to inter-thread dispatch delays).
- **Cleaned up on disconnect**: `Service::OnConnectionClose` calls `UnsubscribeAll()` and
  `PUnsubscribeAll()` if `subscribe_info` is non-null, ensuring the `ChannelStore` is
  updated even if the client disconnects abruptly.

## Message Publishing and Dispatch

### `ChannelStore::SendMessages`

When a client issues `PUBLISH channel message` (or `SPUBLISH`):

```
SendMessages(channel, messages, sharded)
  1. subscribers = FetchSubscribers(channel)
     → exact match: channels_->find(channel)
     → pattern match: for each (pat, subs) in *patterns_:
         if GlobMatcher{pat}.Matches(channel): Fill(subs, pat, &result)
     → sort result by thread_id  (enables efficient per-thread dispatch)

  2. If subscribers empty → return 0

  3. Backpressure gate (per destination thread):
     For each unique subscriber thread:
       Connection::EnsureMemoryBudget(sub_thread)
       → blocks fiber if that thread's subscriber_bytes > publish_buffer_limit

  4. Build message payload:
     BuildSender copies channel + message into a single shared_ptr<char[]>
     Creates string_view references into the shared buffer

  5. Cross-thread dispatch:
     shard_set->pool()->DispatchBrief(cb)
       → cb runs on EVERY I/O thread
       → uses lower_bound on sorted subscribers to find this thread's connections
       → for each local subscriber: conn->SendPubMessageAsync(PubMessage{...})

  6. Return subscriber count
```

### `BuildSender` — Shared Buffer Optimization

The `BuildSender` helper (anonymous namespace in `channel_store.cc`) creates a functor that
captures the message payload in a single allocation:

```cpp
auto buf = shared_ptr<char[]>{new char[channel.size() + messages_size]};
memcpy(buf.get(), channel.data(), channel.size());
// ... copy each message contiguously after the channel
```

The `shared_ptr` is captured by value in the dispatch functor, so all subscribers share the
same underlying buffer. Each `PubMessage` holds a copy of the `shared_ptr` (incrementing the
reference count) along with `string_view`s pointing into it. This avoids per-subscriber
string allocations.

### `FetchSubscribers` — Routing and Pattern Matching

```
FetchSubscribers(channel)
  1. Exact match: channels_->find(channel)
     → if found, Fill() creates Subscriber entries from the SubscribeMap

  2. Pattern match: iterate ALL patterns
     → for each (pat, subs): GlobMatcher{pat, case_sensitive=true}.Matches(channel)
     → matching subscribers are added with their pattern string

  3. Sort by Subscriber::ByThread (thread_id ordering)
     → enables O(log n) per-thread lookup during dispatch
```

The `Fill` helper reads the `SubscribeMap` (via `UpdatablePointer::Get()` — acquire load)
and creates `Subscriber` structs that hold a `ConnectionRef` (weak reference) obtained via
`conn->Borrow()`.

## Delivery: I/O Loop and Dispatch

Once a message lands on the destination thread via `DispatchBrief`, the callback calls
`conn->SendPubMessageAsync(PubMessage{...})`. This wraps the message in a `MessageHandle`
and pushes it onto the connection's `dispatch_q_` via `SendAsync()`.

How that queue is drained depends on which I/O loop the connection uses. Dragonfly has two
I/O loop implementations, selected at connection setup time:

| Loop | Flag / Condition | Protocols | Architecture |
|------|-----------------|-----------|--------------|
| **v1** (`IoLoop` + `AsyncFiber`) | Default for Redis, TLS | Redis (RESP), TLS connections | Two-fiber: blocking recv loop (producer) + `AsyncFiber` (consumer) |
| **v2** (`IoLoopV2`) | `--experimental_io_loop_v2` (default: true), non-TLS Memcache only | Memcache | Single-fiber: event-driven via `RegisterOnRecv`, dispatch queue drained inline |

### v1: `IoLoop` + `AsyncFiber` (Redis connections)

This is the primary path for all Redis Pub/Sub traffic. Each connection manages two streams:

| Stream | Queue | Contents |
|--------|-------|----------|
| **Data Path** | `parsed_head_` (linked list) | Pipelined Redis commands parsed by `IoLoop` |
| **Control Path** | `dispatch_q_` (deque of `MessageHandle`) | PubSub messages, Monitor events, Migration requests, Checkpoints, Invalidations |

`IoLoop` runs in the connection's main fiber: it calls `recv()` (blocking the fiber, not the
thread), parses commands into the `parsed_head_` linked list, and notifies the consumer.

`AsyncFiber` runs as a **separate dedicated fiber** (`Connection::AsyncFiber()`) that
consumes both streams in a prioritized loop:

<div align="center">
  <img src="pubsub/pubsub_asyncfiber_loop_dispatch_and_pipeline_processing.svg" alt="AsyncFiber Loop: Pub/Sub Dispatch and Pipeline Processing" width="700"/>
</div>

**Key insight**: The dispatch queue (Control Path) takes **default priority** over the
pipeline (Data Path). The pipeline is processed only when `dispatch_q_` is empty or when the
`async_dispatch_quota` (default: 100) is reached. The quota prevents Control Path items from
starving the Data Path: after processing `async_dispatch_quota` dispatch queue items without
interleaving pipeline work, the fiber yields to let `IoLoop` parse pending socket data, then
switches to pipeline processing.

### v2: `IoLoopV2` (Memcache connections)

`IoLoopV2` is an event-driven, single-fiber loop that uses `socket->RegisterOnRecv()`
instead of blocking on `recv()`. There is no separate `AsyncFiber` — the dispatch queue is
drained **inline** at the top of each iteration (strict priority over command parsing), using
the same `AsyncOperations` handler as v1. Key differences: no `async_dispatch_quota`
interleaving, no pipeline squashing, and backpressure uses `notifyAll()` instead of
per-publisher `notify()`.

> **Note**: `IoLoopV2` is currently enabled only for non-TLS Memcache connections
> (`--experimental_io_loop_v2`, default: true). Since Pub/Sub is a Redis-only feature,
> **all Pub/Sub traffic flows through the v1 `AsyncFiber` path**.

### PubMessage Processing (shared by v1 and v2)

Both loops dispatch `PubMessage` through the same handler:
`AsyncOperations::operator()(const PubMessage& pub_msg)`.

```
operator()(const PubMessage& pub_msg)
  // Discard stale messages after client fully unsubscribed
  if cntx()->subscriptions == 0:
    return  // silently drop

  // Cluster migration: force-unsubscribe the client
  if pub_msg.force_unsubscribe:
    → send PUSH ["sunsubscribe", channel, 0]
    → cntx()->Unsubscribe(channel)
    return

  // Format RESP push message
  if pattern empty:
    type = is_sharded ? "smessage" : "message"
    → send PUSH [type, channel, message]
  else:
    → send PUSH ["pmessage", pattern, channel, message]
```

Messages are sent as RESP3 Push types (`CollectionType::PUSH`) via
`RedisReplyBuilder::SendBulkStrArr`.

## Backpressure

Fast publishers sending to slow subscribers can cause unbounded memory growth in the
dispatch queues. Dragonfly prevents this with a **per-thread memory budget** for subscriber
messages.

### Memory Accounting

Memory is tracked at two levels:

| Scope | Variable | Location |
|-------|----------|----------|
| **Per-thread** (all connections) | `QueueBackpressure::subscriber_bytes` | `atomic_size_t`, thread-local struct |
| **Per-connection** | `dispatch_q_subscriber_bytes_` | Connection member |
| **Per-thread stats** | `conn_stats.dispatch_queue_subscriber_bytes` | `ConnectionStats` |

`UpdateDispatchStats(msg, add)` is called:
- **On enqueue** (`add=true`): in `SendAsync()`, before pushing to `dispatch_q_`.
  Atomically increments `subscriber_bytes`.
- **On dequeue** (`add=false`): via `absl::Cleanup` in `ProcessAdminMessage()`, after the
  message handler returns. Atomically decrements `subscriber_bytes`.

### Throttling Publishers

Before dispatching messages, `ChannelStore::SendMessages` calls
`Connection::EnsureMemoryBudget(sub_thread)` for each unique destination thread:

```cpp
void Connection::EnsureMemoryBudget(unsigned tid) {
  thread_queue_backpressure[tid].EnsureBelowLimit();
}

void QueueBackpressure::EnsureBelowLimit() {
  pubsub_ec.await([this] {
    return subscriber_bytes.load(memory_order_relaxed) <= publish_buffer_limit;
  });
}
```

This blocks the publishing fiber (not the thread) until the destination thread's subscriber
memory drops below the `publish_buffer_limit` (default: 128MB, configurable via
`--publish_buffer_limit`).

### Wake-up Path

In the `AsyncFiber` loop, after processing a dispatch queue message:

```cpp
if (subscriber_over_limit &&
    conn_stats.dispatch_queue_subscriber_bytes < qbp.publish_buffer_limit)
  qbp.pubsub_ec.notify();  // wake ONE blocked publisher
```

The check snapshots the "over limit" state before processing and only notifies if the state
transitioned from over-limit to under-limit. This avoids spurious wake-ups.

## Cluster Mode Integration

### Standard Pub/Sub — Blocked

In cluster mode, standard `PUBLISH`, `SUBSCRIBE`, `PSUBSCRIBE`, `UNSUBSCRIBE`, and
`PUNSUBSCRIBE` commands return an error:

```
(error) PUBLISH is not supported in cluster mode yet
```

This is enforced in `Service::Publish`, `Service::Subscribe`, `Service::Unsubscribe`,
`Service::PSubscribe`, and `Service::PUnsubscribe` by checking `IsClusterEnabled()`.

### Sharded Pub/Sub — Supported

Sharded Pub/Sub (`SPUBLISH`, `SSUBSCRIBE`, `SUNSUBSCRIBE`) works in cluster mode. The
channel name is treated as a key for slot routing purposes:

```cpp
// In Service::FindKeys:
if (cid->PubSubKind() == CO::PubSubKind::SHARDED) {
  if (cid->name() == "SPUBLISH")
    return KeyIndex(0, 1);      // channel is the key
  return {KeyIndex(0, args.size())};  // all channels are keys
}
```

This ensures that `SPUBLISH` and `SSUBSCRIBE` for the same channel are routed to the same
slot, and cluster slot ownership checks apply.

### Slot Migration — Forced Unsubscription

When cluster slots migrate away from a node, `ChannelStore::UnsubscribeAfterClusterSlotMigration`
is called:

```
UnsubscribeAfterClusterSlotMigration(deleted_slots)
  for each (channel, _) in *channels_:
    if deleted_slots.Contains(KeySlot(channel)):
      csu.Record(channel)
  csu.ApplyAndUnsubscribe()
```

`ApplyAndUnsubscribe()` differs from `Apply()`:
1. It deep-copies the `ChannelMap` and removes the migrated channels.
2. It calls `FetchSubscribers` for each removed channel _before_ updating the store
   (since `FetchSubscribers` reads from the current active store).
3. It uses `AwaitFiberOnAll` (fiber-based, may preempt) instead of `AwaitBrief` to dispatch
   both the store update and unsubscription messages.
4. On each thread, `UnsubscribeConnectionsFromDeletedSlots` sends `PubMessage`s with
   `force_unsubscribe=true`, which triggers `sunsubscribe` push messages to affected clients.

## Keyspace Event Notifications

Dragonfly integrates a limited form of keyspace notifications through the Pub/Sub system.
Currently, only expired-key events (`Ex`) are supported, controlled by the
`--notify_keyspace_events` flag.

When enabled:
1. `DbSlice` sets `expired_keys_events_recording_ = true`.
2. As keys expire (via `ExpireIfNeeded`), their names are appended to
   `db->expired_keys_events_`.
3. At the end of `DeleteExpiredStep`, batched events are published:

```cpp
ChannelStore* store = ServerState::tlocal()->channel_store();
store->SendMessages(
    absl::StrCat("__keyevent@", cntx.db_index, "__:expired"),
    events, false);
events.clear();
```

This uses the standard `SendMessages` path, so clients subscribed to
`__keyevent@0__:expired` receive notifications through the same dispatch pipeline.

## Command Registration

All Pub/Sub commands are registered in `Service::Register` (`src/server/main_service.cc`):

| Command | Arity | Flags | ACL |
|---------|-------|-------|-----|
| `PUBLISH` | 3 | `CO::LOADING \| CO::FAST` | `PUBSUB \| FAST` |
| `SPUBLISH` | 3 | `CO::LOADING \| CO::FAST` | `PUBSUB \| FAST` |
| `SUBSCRIBE` | -2 | `CO::NOSCRIPT \| CO::LOADING` | `PUBSUB \| SLOW` |
| `SSUBSCRIBE` | -2 | `CO::NOSCRIPT \| CO::LOADING` | `PUBSUB \| SLOW` |
| `UNSUBSCRIBE` | -1 | `CO::NOSCRIPT \| CO::LOADING` | `PUBSUB \| SLOW` |
| `SUNSUBSCRIBE` | -1 | `CO::NOSCRIPT \| CO::LOADING` | `PUBSUB \| SLOW` |
| `PSUBSCRIBE` | -2 | `CO::NOSCRIPT \| CO::LOADING` | `PUBSUB \| SLOW` |
| `PUNSUBSCRIBE` | -1 | `CO::NOSCRIPT \| CO::LOADING` | `PUBSUB \| SLOW` |
| `PUBSUB` | -1 | `CO::LOADING \| CO::FAST` | `SLOW` |

Notable flags:
- `CO::FAST` on `PUBLISH`/`SPUBLISH` — these are non-transactional, lock-free reads.
- `CO::NOSCRIPT` on all subscribe/unsubscribe — cannot be called from Lua scripts.
- `CO::LOADING` — permitted during database loading.
- None of the Pub/Sub commands are transactional (`IsTransactional() == false`).

## Key Files Reference

| Purpose | File Path |
|---------|-----------|
| ChannelStore & ChannelStoreUpdater | `src/server/channel_store.h`, `src/server/channel_store.cc` |
| Pub/Sub command handlers | `src/server/main_service.cc` (`Publish`, `Subscribe`, `Unsubscribe`, `PSubscribe`, `PUnsubscribe`, `Pubsub`) |
| Connection-level subscription state | `src/server/conn_context.h`, `src/server/conn_context.cc` (`ChangeSubscriptions`, `UnsubscribeAll`, `PUnsubscribeAll`) |
| PubMessage, AsyncFiber, backpressure | `src/facade/dragonfly_connection.h`, `src/facade/dragonfly_connection.cc` |
| ConnectionRef (weak subscriber refs) | `src/facade/connection_ref.h` |
| ServerState channel_store_ pointer | `src/server/server_state.h`, `src/server/server_state.cc` |
| Keyspace event integration | `src/server/db_slice.cc` (`DeleteExpiredStep`) |
| Cluster slot migration unsub | `src/server/channel_store.cc` (`UnsubscribeAfterClusterSlotMigration`, `ApplyAndUnsubscribe`) |
| GlobMatcher for pattern matching | `src/core/glob_matcher.h` |
