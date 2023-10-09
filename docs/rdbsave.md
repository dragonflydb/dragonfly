# RDB Snapshot design

The following document describes Dragonfly's point in time, forkless snapshotting procedure,
including all its configurations.

## Redis-compatible RDB snapshot

This snapshot is serialized into a single file or into a network socket.
This configuration is used to create redis-compatible backup snapshots.

The algorithm utilizes the shared-nothing architecture of Dragonfly and makes sure that each shard-thread serializes only its own data. Below is the high description of the flow.

<img src="http://static.dragonflydb.io/repo-assets/rdbsave.svg" width="80%" border="0"/>


1. The `RdbSave` class instantiates a single blocking channel (in red).
   Its purpose is to gather all the blobs from all the shards.
2. In addition it creates thread-local snapshot instances in each DF shard.
TODO: to rename them in the codebase to another name (SnapshotShard?) since `snapshot` word creates ambiguity here.
3. Each SnapshotShard instantiates its own RdbSerializer that is used to serialize each K/V entry into a binary representation according to the Redis format spec. SnapshotShards combine multiple blobs from the same Dash bucket into a single blob. They always send blob data at bucket granularity, i.e. they never send blob into the channel that only partially covers the bucket. This is needed in order to guarantee snapshot isolation.
4. The RdbSerializer uses `io::Sink` to emit binary data. The SnapshotShard instance passes into it a `StringFile` which is just a memory-only based sink that wraps `std::string` object. Once `StringFile` instance becomes large, it's flushed into the channel (as long as it follows the rules above).
4. RdbSave also creates a fiber (SaveBody) that pull all the blobs from the channel. Blobs migh come in unspecified order though it's guaranteed that each blob is self sufficient but itself.
5. DF uses direct I/O, to improve i/o throughput, which, in turn requires properly aligned memory buffers to work. Unfortunately, blobs that come from the rdb channel come in different sizes and they are not aligned by OS page granularity. Therefore, DF passes all the data from rdb channel through AlignedBuffer transformation. The purpose of this class is to copy the incoming data into a properly aligned buffer. Once it accumulates enough data, it flushes it into the output file.

To summarize, this configuration employs a single sink to create one file or one stream of data that represents the whole database.

## Dragonfly Snapshot (TBD)

Required for replication. Creates several multiple files, one file per SnapshotShard. Does not require a central sink. Each SnapshotShard still uses RdbSerializer together with StringFile to guarantee bucket level granularity. We still need AlignedBuffer if we want to use direct I/O.
For a DF process with N shard, it will create N files. Will probably require additional metadata file to provide file-level consistency, but for now we can assume that only N files are created,
since our use-case will be network based replication.

How it's gonna be used? Replica (slave) will hand-shake with the master and find out how many shard it has.
Then it will open `N` sockets and each one of them will pull shard data. First, they will pull snapshot data,
and replay it by distributing entries among `K` replica shards. After all the snapshot data is replayed,
they will continue with replaying the change log (stable state replication), which is out of context
of this document.

## Relaxed point-in-time (TBD)
When DF saves its snapshot file on disk, it maintains snapshot isolation by applying a virtual cut
through all the process shards. Snapshotting may take time, during which, DF may process many write requests.
These mutations won't be part of the snapshot, because the cut captures data up to the point
**it has started**. This is perfect for backups. I call this variation - conservative snapshotting.

However, when we perform snapshotting for replication, we would like to produce a snapshot
that includes all the data upto point in time when the snapshotting **finishes**. I called
this *relaxed snapshotting*. The reason for relaxed snapshotting is to avoid keeping the changelog
of all mutations during the snapshot creation.

As a side comment - we could, in theory, support the same (relaxed)
semantics for file snapshots, but it's not necessary since it might increase the snapshot sizes.

The snapshotting phase (full-sync) can take up lots of time which add lots of memory pressure on the system.
Keeping the change-log aside during the full-sync phase will only add more pressure.
We achieve relaxed snapshotting by pushing the changes into the replication sockets without saving them aside.
Of course, we would still need a point-in-time consistency,
in order to know when the snapshotting finished and the stable state replication started.

## Conservative and relaxed snapshotting variations

Both algorithms maintain a scanning process (fiber) that iteratively goes over the main dictionary
and serializes its data. Before starting the process, the SnapshotShard captures
the change epoch of its shard (this epoch is increased with each write request).

```cpp
SnapshotShard.epoch = shard.epoch++;
```

For sake of simplicity, we can assume that each entry in the shard maintains its own version counter.
By capturing the epoch number we establish a cut: all entries with `version <= SnapshotShard.epoch`
have not been serialized yet and were not modified by the concurrent writes.

The DashTable iteration algorithm guarantees convergence and coverage ("at most once"),
but it does not guarantee that each entry is visited *exactly once*.
Therefore, we use entry versions for two things: 1) to avoid serialization of the same entry multiple times,
and 2) to correctly serialize entries that need to change due to concurrent writes.

Serialization Fiber:

```cpp
 for (entry : table) {
    if (entry.version <= cut.epoch) {
      entry.version = cut.epoch + 1;
      SendToSerializationSink(entry);
    }
 }
```

To allow concurrent writes during the snapshotting phase, we setup a hook that is triggered on each
entry mutation in the table:

OnWriteHook:
```cpp
....
if (entry.version <= cut.version) {
  SendToSerializationSink(entry);
}
...
entry = new_entry;
entry.version = shard.epoch++;  // guaranteed to become > cut.version
```

Please note that this hook maintains point-in-time semantics for the conservative variation by pushing
the previous value of the entry into the sink before changing it.

However, for the relaxed point-in-time, we do not have to store the old value.
Therefore, we can do the following:

OnWriteHook:

```cpp
if (entry.version <= cut.version) {
  SendToSerializationSink(new_entry);  // do not have to send the old value
} else {
  // Keep sending the changes.
  SendToSerializationSink(IncrementalDiff(entry, new_entry));
}

entry = new_entry;
entry.version = shard.epoch++;
```

The change data is sent along with the rest of the contents, and it requires to extend
the existing rdb format to support differential operations like (hset, append, etc).
The Serialization Fiber loop is the same for this variation.
