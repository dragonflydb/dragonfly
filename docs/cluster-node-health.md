# Cluster Node Health

**Node health is passive metadata provided by the cluster manager (control plane) via the
`DFLYCLUSTER CONFIG` command.** Dragonfly nodes do not actively determine their own health status;
instead, the cluster orchestrator monitors node states and communicates health information to each
node through the cluster configuration.

Dragonfly supports node health status reporting for cluster configurations, providing
Valkey-compatible behavior for cluster management commands. This feature allows the cluster
manager to track the health state of each node and communicate it to clients through various
cluster commands.

## Overview

The node health feature was introduced in [PR #4758](https://github.com/dragonflydb/dragonfly/pull/4758)
and [PR #4767](https://github.com/dragonflydb/dragonfly/pull/4767) to address
[issue #4741](https://github.com/dragonflydb/dragonfly/issues/4741).

The health status is part of the cluster configuration and can be set for both master and replica
nodes. Different cluster commands use this information to filter or display nodes based on their
health state.

## Health States

Dragonfly supports four health states for cluster nodes:

| State     | Description                                                                               | Visible in Commands |
|-----------|-------------------------------------------------------------------------------------------|---------------------|
| `online`  | Node is fully operational and ready to serve requests                                    | All commands        |
| `loading` | Node is still loading data (e.g., during initial sync or restart)                       | `CLUSTER SHARDS`, `CLUSTER NODES` |
| `fail`    | Node has failed or is unreachable                                                        | `CLUSTER SHARDS`, `CLUSTER NODES` |
| `hidden`  | Replica exists but should not be exposed to clients (internal use by cluster manager)   | Masters: all commands; Replicas: none |

### Default State

When no health status is specified in the configuration, nodes default to the `online` state.

## Configuration

Node health is specified in the cluster configuration JSON that is passed via the
`DFLYCLUSTER CONFIG` command. The health status is set using the `health` field for each node.

### Configuration Format

```json
[
  {
    "slot_ranges": [
      { "start": 0, "end": 16383 }
    ],
    "master": {
      "id": "node-master-1",
      "ip": "10.0.0.1",
      "port": 7000,
      "health": "online"
    },
    "replicas": [
      {
        "id": "node-replica-1",
        "ip": "10.0.0.2",
        "port": 7001,
        "health": "online"
      },
      {
        "id": "node-replica-2",
        "ip": "10.0.0.3",
        "port": 7002,
        "health": "loading"
      },
      {
        "id": "node-replica-3",
        "ip": "10.0.0.4",
        "port": 7003,
        "health": "fail"
      },
      {
        "id": "node-replica-4",
        "ip": "10.0.0.5",
        "port": 7004,
        "health": "hidden"
      }
    ]
  }
]
```

### Setting Configuration

Use the `DFLYCLUSTER CONFIG` command to set the cluster configuration with health information:

```bash
DFLYCLUSTER CONFIG <json_config>
```

The health field is optional and case-insensitive. Valid values are: `online`, `loading`, `fail`,
and `hidden`.

## Command Behavior

Different cluster commands handle node health status in different ways:

### CLUSTER SHARDS

The `CLUSTER SHARDS` command returns detailed information about cluster shards, including the
health status of all nodes except those marked as `hidden`.

**Example:**

```bash
127.0.0.1:6379> CLUSTER SHARDS
1) 1) "slots"
   2) 1) (integer) 0
      2) (integer) 16383
   3) "nodes"
   4) 1) 1) "id"
         2) "node-master-1"
         3) "endpoint"
         4) "10.0.0.1"
         5) "ip"
         6) "10.0.0.1"
         7) "port"
         8) (integer) 7000
         9) "role"
        10) "master"
        11) "replication-offset"
        12) (integer) 0
        13) "health"
        14) "online"
      2) 1) "id"
         2) "node-replica-1"
         3) "endpoint"
         4) "10.0.0.2"
         5) "ip"
         6) "10.0.0.2"
         7) "port"
         8) (integer) 7001
         9) "role"
        10) "replica"
        11) "replication-offset"
        12) (integer) 0
        13) "health"
        14) "online"
      3) 1) "id"
         2) "node-replica-2"
         3) "endpoint"
         4) "10.0.0.3"
         5) "ip"
         6) "10.0.0.3"
         7) "port"
         8) (integer) 7002
         9) "role"
        10) "replica"
        11) "replication-offset"
        12) (integer) 0
        13) "health"
        14) "loading"
      4) 1) "id"
         2) "node-replica-3"
         3) "endpoint"
         4) "10.0.0.4"
         5) "ip"
         6) "10.0.0.4"
         7) "port"
         8) (integer) 7003
         9) "role"
        10) "replica"
        11) "replication-offset"
        12) (integer) 0
        13) "health"
        14) "fail"
```

**Note:** Nodes with `hidden` health status are filtered out and do not appear in the output.

### CLUSTER SLOTS

The `CLUSTER SLOTS` command returns slot distribution information. This command filters out
replicas that are not ready to serve requests.

**Filtering behavior:**
- Includes replicas with `online` health status
- Excludes replicas with `loading`, `fail`, or `hidden` health status

**Example:**

```bash
127.0.0.1:6379> CLUSTER SLOTS
1) 1) (integer) 0
   2) (integer) 16383
   3) 1) "10.0.0.1"
      2) (integer) 7000
      3) "node-master-1"
   4) 1) "10.0.0.2"
      2) (integer) 7001
      3) "node-replica-1"
```

In this example, only the master and the `online` replica (`node-replica-1`) are shown. Replicas
with `loading`, `fail`, or `hidden` status are not included.

### CLUSTER NODES

The `CLUSTER NODES` command returns a list of all cluster nodes in a space-separated format. This
command shows nodes with most health states but excludes `hidden` nodes.

**Connection state mapping:**
- `online` and `loading` nodes: shown as `connected`
- `fail` nodes: shown as `disconnected`
- `hidden` nodes: not shown in output

**Example:**

```bash
127.0.0.1:6379> CLUSTER NODES
node-master-1 10.0.0.1:7000@7000 master - 0 0 0 connected 0-16383
node-replica-1 10.0.0.2:7001@7001 slave node-master-1 0 0 0 connected
node-replica-2 10.0.0.3:7002@7002 slave node-master-1 0 0 0 connected
node-replica-3 10.0.0.4:7003@7003 slave node-master-1 0 0 0 disconnected
```

**Note:**
- `node-replica-1` (online): appears as `connected`
- `node-replica-2` (loading): appears as `connected`
- `node-replica-3` (fail): appears as `disconnected`
- `node-replica-4` (hidden): not shown in output

## Use Cases

### 1. Gradual Node Addition

When adding a new replica to a cluster, you can set its health status to `loading` while it's
syncing data. This allows the cluster manager to track the node but prevents clients from
redirecting read requests to it via `CLUSTER SLOTS`.

### 2. Failed Node Handling

When a node fails or becomes unreachable, the cluster manager can mark it as `fail`. This
provides visibility in `CLUSTER SHARDS` and `CLUSTER NODES` while excluding it from
`CLUSTER SLOTS` responses.

### 3. Internal Replicas

The `hidden` health status is useful for replica nodes that are managed internally by the cluster
orchestrator but should not be visible to external clients. Hidden replicas are filtered out from
all cluster commands (`CLUSTER SHARDS`, `CLUSTER SLOTS`, and `CLUSTER NODES`). Note that masters
marked as `hidden` are still visible in all commands; the filtering only applies to replicas.

### 4. Valkey Compatibility

This feature provides Valkey-compatible behavior for cluster client APIs:
- `CLUSTER SHARDS` returns the health status of replica nodes
- `CLUSTER SLOTS` does not return replicas that have not finished loading

## Implementation Details

For developers interested in the implementation:

1. **Data Structure**: The `NodeHealth` enum is defined in `src/server/cluster/cluster_defs.h`
   with four values: `FAIL`, `LOADING`, `ONLINE`, and `HIDDEN`.

2. **Configuration Parsing**: Health status is parsed from JSON in
   `src/server/cluster/cluster_config.cc` in the `ParseClusterNode` function.

3. **Command Handlers**: The cluster commands in `src/server/cluster/cluster_family.cc` implement
   filtering logic based on health status:
   - `ClusterShards`: Filters out replicas with `HIDDEN` health before calling `ClusterShardsImpl`
     (masters are still included even if marked `HIDDEN`)
   - `ClusterSlotsImpl`: Filters out `HIDDEN`, `FAIL`, and `LOADING` replicas (masters are always
     included)
   - `ClusterNodesImpl`: Filters out replicas with `HIDDEN` health when listing replicas (masters
     with `HIDDEN` health are still included) and maps health to connection state

4. **Default Value**: When not specified in configuration, nodes default to `ONLINE` state as
   defined in `ClusterExtendedNodeInfo`.

## See Also

- [Dragonfly Cluster Mode Documentation](https://www.dragonflydb.io/docs/cluster)
- [CLUSTER SHARDS Command](https://redis.io/commands/cluster-shards/)
- [CLUSTER SLOTS Command](https://redis.io/commands/cluster-slots/)
- [CLUSTER NODES Command](https://redis.io/commands/cluster-nodes/)
