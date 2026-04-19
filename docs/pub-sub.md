# Pub/Sub Architecture

This document describes how Dragonfly implements the Publish-Subscribe (Pub/Sub) messaging
paradigm within its shared-nothing, multi-threaded architecture. It covers the global
subscription registry, the thread-safe concurrent hashmap that provides fine-grained locking,
the asynchronous message delivery pipeline, and the backpressure system that protects the
server from slow-subscriber OOM.

## Overview

In a shared-nothing architecture, handling `PUBLISH` and `SUBSCRIBE` commands presents a
unique challenge: subscriptions must be globally addressable across all threads, but taking a
global lock on every `PUBLISH` would create a severe bottleneck. A single popular channel
with thousands of subscribers could serialize all publish operations onto one shard thread.

Dragonfly solves this with a **single global `ChannelStore`** backed by
`phmap::parallel_flat_hash_map` (from the
[parallel-hashmap](https://github.com/greg7mdp/parallel-hashmap) library):

- The parallel hashmap **shards the map into N submaps**, each protected by its own
  `std::mutex`. Concurrent readers (e.g. `PUBLISH`) and writers (e.g. `SUBSCRIBE`) only
  contend if they happen to hash to the same submap.
- **Reads (`PUBLISH` / `SPUBLISH`)** acquire only the per-submap lock for the channel being
  looked up, so publishes to different channels proceed fully in parallel.
- **Writes (`SUBSCRIBE` / `UNSUBSCRIBE` / `PSUBSCRIBE` / `PUNSUBSCRIBE`)** hold the
  relevant submap lock for the duration of the insert or erase.

This design avoids contention on a single shard thread for heavy throughput on a single
channel and seamlessly scales across multiple threads even with a small number of channels.
Publish latency is lower than a shard-routed design because no inter-thread hop is required
to look up subscribers — the caller accesses the global store directly.

Dragonfly supports three flavors of Pub/Sub:

| Flavor | Commands | Scope | Cluster mode |
|--------|----------|-------|--------------|
| **Standard** | `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE` | Global (all channels) | Blocked — returns error |
| **Pattern** | `PSUBSCRIBE`, `PUNSUBSCRIBE` | Global (glob-matched) | Blocked — returns error |
| **Sharded** | `SPUBLISH`, `SSUBSCRIBE`, `SUNSUBSCRIBE` | Per-slot (channel name determines slot) | Supported |

## Primary Data Structures

| Type | Location | Role |
|------|----------|------|
| `ChannelStore` | `src/server/channel_store.h` | Single global registry mapping channels/patterns to subscribers. Backed by `phmap::parallel_flat_hash_map`. |
| `ChannelStore::Subscriber` | `src/server/channel_store.h` | Represents a subscribed client. Wraps `facade::ConnectionRef` plus a pattern string. |
| `ChannelStore::ChannelMap` | `src/server/channel_store.h` | `phmap::parallel_flat_hash_map_m<string, SubscribeMap>` — maps channel/pattern names to subscriber maps. Internally sharded into N submaps, each with its own `std::mutex`. |
| `ChannelStore::SubscribeMap` | `src/server/channel_store.h` | `phmap::flat_hash_map<ConnectionContext*, ThreadId>` — maps subscriber contexts to their owning thread. |
| `channel_store` (global) | `src/server/channel_store.h` | `extern ChannelStore*` — the single global instance, created during `Service::Init` and destroyed during `Service::Shutdown`. |
| `ConnectionState::SubscribeInfo` | `src/server/conn_context.h` | Per-connection set of subscribed channels and patterns. Created lazily on first subscription. |
| `Connection::PubMessage` | `src/facade/dragonfly_connection.h` | Carries a formatted pub/sub message through the async dispatch queue. |
| `Connection::MessageHandle` | `src/facade/dragonfly_connection.h` | Variant wrapper (`PubMessage`, `MonitorMessage`, `MigrationRequest`, etc.) for the control-path dispatch queue. |
| `QueueBackpressure` | `src/facade/dragonfly_connection.cc` | Per-thread struct tracking `subscriber_bytes` and enforcing the `publish_buffer_limit`. |
| `facade::ConnectionRef` | `src/facade/connection_ref.h` | Weak reference to a connection. Thread-safe expiry check; dereference only on owning thread. |

## Data Flow Overview

<div align="center">
  <img src="pubsub/pubsub_data_flow_overview.svg" alt="Pub/Sub Data Flow" width="1000"/>
</div>

## Subscription Management

### Data Structure Layout

The single global `ChannelStore` holds two `ChannelMap` instances (one for exact channels,
one for glob patterns):

<div align="center">
  <img src="pubsub/pubsub_data_structure_layout.svg" alt="Data Structure Layout" width="700"/>
</div>

Each `ChannelMap` is a `phmap::parallel_flat_hash_map_m` that internally splits its keyspace
into N submaps. Each submap has its own `std::mutex`, so concurrent operations on different
channels (or patterns) that hash to different submaps proceed without contention.

### Add() — Subscribing to a Channel

When a client issues `SUBSCRIBE channel` (or `PSUBSCRIBE pattern`), the connection handler
calls `channel_store->Add(channel, cntx, thread_id, pattern)`:

```cpp
void ChannelStore::Add(string_view channel, ConnectionContext* cntx,
                       uint32_t thread_id, bool pattern) {
  auto& map = pattern ? patterns_ : channels_;
  // try_emplace_l holds the write lock for the duration of the lambda ensuring atomicity
  map.try_emplace_l(
      channel, [&](auto& kv) { kv.second.emplace(cntx, thread_id); },
      SubscribeMap{{cntx, thread_id}});
}
```

`try_emplace_l` is a `parallel_flat_hash_map` method that atomically either:
- Calls the lambda on an existing entry (adds the subscriber to the existing `SubscribeMap`), or
- Inserts a new entry with the provided default value (a new `SubscribeMap` containing the subscriber).

The per-submap write lock is held for the duration of the operation, ensuring atomicity.

### Remove() — Unsubscribing from a Channel

When a client issues `UNSUBSCRIBE channel` (or `PUNSUBSCRIBE pattern`), the connection
handler calls `channel_store->Remove(channel, cntx, pattern)`:

```cpp
void ChannelStore::Remove(string_view channel, ConnectionContext* cntx, bool pattern) {
  auto& map = pattern ? patterns_ : channels_;
  // erase_if holds the write lock for its duration, removing cntx from the SubscribeMap
  // and erasing the channel entry atomically if no subscribers remain.
  map.erase_if(string{channel}, [&](auto& kv) {
    kv.second.erase(cntx);
    return kv.second.empty();
  });
}
```

`erase_if` holds the per-submap write lock while executing the lambda. The lambda removes
the subscriber from the `SubscribeMap` and returns `true` if the map is now empty, which
causes the channel entry itself to be erased. This ensures that channel creation/deletion and
subscriber management are atomic with respect to concurrent readers.

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
     → exact match: channels_.if_contains(channel, ...)
     → pattern match: patterns_.for_each(...)
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
  1. Exact match: channels_.if_contains(channel, ...)
     → if found, Fill() creates Subscriber entries from the SubscribeMap
     → holds the per-submap read lock only for the duration of the lambda

  2. Pattern match: patterns_.for_each(...)
     → for each (pat, subs): GlobMatcher{pat, case_sensitive=true}.Matches(channel)
     → matching subscribers are added with their pattern string
     → each per-submap lock is held only while iterating that submap

  3. Sort by Subscriber::ByThread (thread_id ordering)
     → enables O(log n) per-thread lookup during dispatch
```

The `Fill` helper reads the `SubscribeMap` and creates `Subscriber` structs that hold a
`ConnectionRef` (weak reference) obtained via `conn->Borrow()`.

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
  channels_.for_each(...)
    for each (channel, subs):
      if deleted_slots.Contains(KeySlot(channel)):
        Fill(subs, "", &subscribers)
        channel_subs_map[channel] = subscribers

  pool->AwaitFiberOnAll(idx):
    channel_store->UnsubscribeConnectionsFromDeletedSlots(channel_subs_map, idx)
```

The migration flow collects subscribers for affected channels in a single pass (under
per-submap locks), then dispatches `AwaitFiberOnAll` to send forced-unsubscribe messages
on each thread. The `erase_if` calls that actually remove the channel entries happen
within `UnsubscribeConnectionsFromDeletedSlots`, which sends `PubMessage`s with
`force_unsubscribe=true` to trigger `sunsubscribe` push messages to affected clients.

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
channel_store->SendMessages(
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
- `CO::FAST` on `PUBLISH`/`SPUBLISH` — these are non-transactional and only acquire fine-grained per-submap locks.
- `CO::NOSCRIPT` on all subscribe/unsubscribe — cannot be called from Lua scripts.
- `CO::LOADING` — permitted during database loading.
- None of the Pub/Sub commands are transactional (`IsTransactional() == false`).

## Key Files Reference

| Purpose | File Path |
|---------|-----------|
| ChannelStore (global instance & API) | `src/server/channel_store.h`, `src/server/channel_store.cc` |
| Pub/Sub command handlers | `src/server/main_service.cc` (`Publish`, `Subscribe`, `Unsubscribe`, `PSubscribe`, `PUnsubscribe`, `Pubsub`) |
| Connection-level subscription state | `src/server/conn_context.h`, `src/server/conn_context.cc` (`ChangeSubscriptions`, `UnsubscribeAll`, `PUnsubscribeAll`) |
| PubMessage, AsyncFiber, backpressure | `src/facade/dragonfly_connection.h`, `src/facade/dragonfly_connection.cc` |
| ConnectionRef (weak subscriber refs) | `src/facade/connection_ref.h` |
| Keyspace event integration | `src/server/db_slice.cc` (`DeleteExpiredStep`) |
| Cluster slot migration unsub | `src/server/channel_store.cc` (`UnsubscribeAfterClusterSlotMigration`) |
| GlobMatcher for pattern matching | `src/core/glob_matcher.h` |
