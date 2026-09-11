# Full-Sync Fanout Design

## Goal

Support a serving Dragonfly node with multiple downstream replicas that require a full sync without
creating a separate snapshot for every replica. Today, each requesting replica starts its own full
sync. This design creates one snapshot stream for a batch of replicas and runs at most one snapshot
job at a time.

This design reduces snapshot CPU and memory pressure. It does not make network egress free: sending
the snapshot to multiple replicas still sends one copy over each network connection.

## Scope

This applies to full sync from one serving node to replicas that have already been assigned to it.

An external controller monitors the instances and creates or repairs the replication tree after a
node failure. Dragonfly does not manage that topology itself. This design also does not change
partial-sync policy.

## Requirements

- A short, configurable collection delay lets multiple requests join one batch.
- All members receive the same initial stream and the same per-shard journal handoff positions.
- A serving node creates at most one snapshot at a time.
- A slow or disconnected replica is removed without blocking the other members.
- Batch membership is fixed once snapshot streaming starts; later requests form a later batch.
- A completed snapshot is not retained or replayed for later requests.

## Overview

```text
                 full-sync requests during collection window

  R1 --------\
  R2 ---------+----> [ Full-sync batch ] ----> one snapshot stream + one LSN vector
  R3 --------/+              |
                              +--------------------------------------------+
                                                                           |
  Serving node                                                          fanout
  +----------------+                                                   |
  | snapshot maker |-------------------------------------------+-------+-------+
  +----------------+                                           |       |       |
                                                               v       v       v
                                                              R1      R2      R3

  R4 arrives after the snapshot starts --> queued for the next batch
```

Every replica has its own connection, output queue, acknowledgement state, and progress. The
snapshot data and journal records are produced once per source shard, then distributed to those
independent flows.

## Batching requests

At most one batch can create or send a snapshot. A request belongs either to the current batch or
to the next batch:

- Requests received during the current batch's collection window receive the same snapshot.
- Requests received after snapshot creation starts are collected for the next batch. They never
  join the snapshot already in progress.

```text
time ------------------------------------------------------------------------------>

batch 1:  R1 + R2 --> [ collection ] --> [ snapshot ] --> [ shared delta feed ]
batch 2:                               R3 --> [ collection ] --> [ wait ] --> [ snapshot ]
                                                               waits for batch 1's snapshot
```

The first request opens a collection window. Requests arriving before its delay expires join that
batch. Reaching the configured member limit closes the window early. The source then fixes the
membership and starts the snapshot.

While that snapshot is active, later requests can be collected for the next batch, but their
snapshot cannot start. Once all surviving members receive the end of the full sync, the next batch
may start its snapshot. The completed batch continues to send its shared delta feed to its members
until they disconnect.

## Snapshot stream and handoff

Writes must not be lost while a replica receives a snapshot. For each source shard, full sync is
one ordered stream with two stages:

1. The initial stream contains the snapshot and the journal writes made while that snapshot is
   being serialized.
2. The shared delta feed continues with later journal writes after the initial stream ends.

At the end of the initial stream, the source writes a final journal position for each shard. This
position says where the following delta feed begins. It is the **snapshot cut**. An LSN is simply a
position in a shard's ordered change log. Because Dragonfly is sharded, the cut is a list of LSNs,
one per shard, rather than one global LSN. Every member of the batch uses the same list.

```text
time ---------------------------------------------------------------------->

       initial full-sync stream                    shared delta feed
  +-------------------------------------+------------------------------->
  | snapshot + writes during            | later journal writes
  | serialization + final LSN           | shared delta feed
  +-------------------------------------+------------------------------->
                                        ^
                           snapshot cut: where the feed starts

                same ordered stream for every member: R1, R2, ...
```

The source creates this initial stream once and distributes it to every member. Before it closes
the initial stream, it starts collecting the next journal writes. It first sends the final LSN and
the end of the full sync, then releases those buffered writes into the shared delta feed. This order
prevents a gap or a reordering at the handoff.

The snapshot and each delta payload are stored once per source shard. Replicas still have
independent connections, read positions, and outbound queues. Therefore, one slow replica can be
disconnected without delaying the other replicas, and shared data never means a shared socket
buffer.

The normal five-second partial-sync retention is not enough for a full sync that takes longer. The
batch therefore uses its own shared delta feed during the handoff and while serving its members.
Output limits and timeouts bound its memory use: if a replica cannot keep up, the source drops that
replica rather than retaining data indefinitely.


## Limits and failures

| Situation | Required behavior |
| --- | --- |
| A replica disconnects, times out, or cannot make output progress | Drop only that replica. The remaining members continue. |
| Per-replica output or shared delta-feed memory reaches its limit | Drop the lagging replica before memory becomes unbounded. |
| No batch members remain | Stop the batch and release its snapshot and delta-feed resources. |
| Snapshot creation or the shared delta feed fails | Fail the batch cleanly, release resources, and allow a later retry. |
| Another batch is ready while one snapshot is active | It waits; a second snapshot job never starts. |

The active snapshot job, queued requests, delta-feed memory, member drops, and failures must be
visible to operators.

## Configuration

| Setting | First-release behavior |
| --- | --- |
| Full-sync batch delay | Configurable, measured in seconds. It trades the first request's startup latency for more members sharing one snapshot. |
| Maximum batch size | Configurable. Reaching the limit starts the batch. |
| Active full-sync jobs | Fixed at one per serving node. |
| Per-replica output limit and timeout | Bound how much a slow replica can delay or buffer. Exceeding either disconnects it. |
| Partial-sync journal retention | Five seconds in the best case. It is not the only store for a long active full sync. |
