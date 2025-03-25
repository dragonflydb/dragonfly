#!/usr/bin/env python3

import argparse
from argparse import RawTextHelpFormatter
import json
import math
from typing import Iterable, List
import redis
import subprocess
import time

"""
To install: pip install -r requirements.txt
"""


def die_with_err(err):
    print("!!!", err)
    exit(-1)


class Node:
    def __init__(self, host, port):
        self.id = ""
        self.host = host
        self.port = port

    def update_id(node):
        node.id = send_command(node, ["cluster", "myid"])
        print(f"- ID {node.id}")

    def __repr__(self):
        return f"{self.host}:{self.port}/{self.id}"

    def to_dict(self):
        return {"id": self.id, "ip": self.host, "port": self.port}


class Master(Node):
    def __init__(self, host, port):
        Node.__init__(self, host, port)
        self.replicas = []


def start_node(node, dragonfly_bin, threads):
    f = open(f"/tmp/dfly.cluster.node.{node.port}.log", "w")
    print(f"- Log file for node {node.port}: {f.name}")
    subprocess.Popen(
        [
            f"{dragonfly_bin}",
            f"--port={node.port}",
            "--cluster_mode=yes",
            f"--proactor_threads={threads}",
            "--dbfilename=",
            f"--logtostderr",
            "--proactor_affinity_mode=off",
            "--omit_basic_usage",
        ],
        stderr=f,
    )


def send_command(node, command, print_errors=True):
    client = redis.Redis(decode_responses=True, host=node.host, port=node.port)

    for i in range(0, 5):
        try:
            result = client.execute_command(*command)
            return result
        except Exception as e:
            if print_errors:
                print(e)
            time.sleep(0.1 * i)
        finally:
            client.close()

    if print_errors:
        print(f"Unable to run command {command} against {node.host}:{node.port} after 5 attempts!")

    return Exception()


class SlotRange:
    def __init__(self, start, end):
        assert start <= end
        self.start = start
        self.end = end

    def to_dict(self):
        return {"start": self.start, "end": self.end}

    @classmethod
    def from_dict(cls, d):
        return cls(d["start"], d["end"])

    def __repr__(self):
        return f"({self.start}-{self.end})"

    def merge(self, other: "SlotRange"):
        if self.end + 1 == other.start:
            self.end = other.end
            return True
        elif other.end + 1 == self.start:
            self.start = other.start
            return True
        return False

    def contains(self, slot_id):
        return self.start <= slot_id <= self.end

    def remove(self, slot_id):
        assert self.contains(slot_id)

        if self.start < self.end:
            if slot_id == self.start:
                return None, SlotRange(self.start + 1, self.end)
            elif slot_id == self.end:
                return SlotRange(self.start, self.end - 1), None
            elif self.start < slot_id < self.end:
                return SlotRange(self.start, slot_id - 1), SlotRange(slot_id + 1, self.end)
        return None, None


# Custom JSON encoder to handle SlotRange objects
class ClusterConfigEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, SlotRange) or isinstance(obj, Node):
            return obj.to_dict()
        return super().default(obj)


def build_config_from_list(masters: List[Master]):
    total_slots = 16384
    slots_per_node = math.floor(total_slots / len(masters))

    config = []
    for i, master in enumerate(masters):
        slot_range = SlotRange(i * slots_per_node, (i + 1) * slots_per_node - 1)
        c = {
            "slot_ranges": [slot_range],
            "master": master,
            "replicas": master.replicas,
        }
        config.append(c)

    # Adjust the last slot range to include any remaining slots
    config[-1]["slot_ranges"][-1].end += total_slots % len(masters)
    return config


def get_nodes_from_config(config):
    nodes = []
    for shard in config:
        nodes.append(shard["master"])
        for replica in shard["replicas"]:
            nodes.append(replica)

    for node in nodes:
        node.update_id()
    return nodes


def push_config(config):
    def push_to_node(node, config):
        # Use the custom encoder to convert SlotRange objects during serialization
        config_str = json.dumps(config, indent=2, cls=ClusterConfigEncoder)
        response = send_command(node, ["dflycluster", "config", config_str])
        print(f"- Push to {node.port}: {response}")

    for node in get_nodes_from_config(config):
        push_to_node(node, config)


def create_locally(args):
    print(f"Setting up a Dragonfly cluster:")
    print(f"- Master nodes: {args.num_masters}")
    print(f"- Ports: {args.first_port}...{args.first_port + args.num_masters - 1}")
    print(f"- Replicas for each master: {args.replicas_per_master}")
    print()

    next_port = args.first_port
    masters = []
    for i in range(args.num_masters):
        master = Master("127.0.0.1", next_port)
        next_port += 1
        for j in range(args.replicas_per_master):
            replica = Node("127.0.0.1", next_port)
            master.replicas.append(replica)
            next_port += 1
        masters.append(master)

    nodes = []
    for master in masters:
        nodes.append(master)
        for replica in master.replicas:
            nodes.append(replica)

    print("Starting nodes...")
    for node in nodes:
        start_node(node, args.dragonfly_bin, args.threads)
    print()
    time.sleep(0.5)

    if args.replicas_per_master > 0:
        print("Configuring replication...")
        for master in masters:
            for replica in master.replicas:
                response = send_command(replica, ["replicaof", master.host, master.port])
                print(f"- {replica.port} replicating {master.port}: {response}")
        print()

    print(f"Getting IDs...")
    for n in nodes:
        n.update_id()
    print()

    config = build_config_from_list(masters)
    print(f"Pushing config:\n{config}\n")
    push_config(config)
    print()


def config_single_remote(args):
    print(
        f"Configuring remote Dragonfly {args.target_host}:{args.target_port} to be a single-server cluster"
    )

    master = Master(args.target_host, args.target_port)
    master.update_id()

    test = send_command(master, ["get", "x"], print_errors=False)
    if type(test) is not Exception:
        die_with_err("Node either not found or already configured")

    config = build_config_from_list([master])
    print(f"Pushing config:\n{config}\n")
    push_config(config)
    print()


def build_config_from_existing(args):
    def list_to_dict(l):
        return {l[i]: l[i + 1] for i in range(0, len(l), 2)}

    def build_node(node_list):
        d = list_to_dict(node_list)
        node = Node(d["endpoint"], d["port"])
        node.id = d["id"]
        return node

    def build_slots(slot_list):
        slots = []
        for i in range(0, len(slot_list), 2):
            slots.append(SlotRange(slot_list[i], slot_list[i + 1]))
        return slots

    client = redis.Redis(decode_responses=True, host=args.target_host, port=args.target_port)
    existing = client.execute_command("cluster", "shards")
    config = []
    for shard_list in existing:
        shard = list_to_dict(shard_list)
        config.append(
            {
                "slot_ranges": build_slots(shard["slots"]),
                "master": build_node(shard["nodes"][0]),
                "replicas": [build_node(replica) for replica in shard["nodes"][1::]],
            }
        )

    client.close()
    return config


def find_master(config, host, port, die_if_not_found=True):
    new_owner = None
    for shard in config:
        if shard["master"].host == host and shard["master"].port == port:
            new_owner = shard
            break

    if new_owner == None and die_if_not_found:
        die_with_err(f"Can't find master (hint: use flag --target_host / --target_port).")

    return new_owner


def find_replica(config, host, port):
    for shard in config:
        for replica in shard["replicas"]:
            if replica.host == host and replica.port == port:
                return replica, shard
    die_with_err("Can't find target node")


def attach(args):
    print(f"Attaching remote Dragonfly {args.attach_host}:{args.attach_port} to cluster")
    if args.attach_as_replica:
        newcomer = Node(args.attach_host, args.attach_port)
        replica_resp = send_command(newcomer, ["info", "replication"])
        if replica_resp["role"] != "slave":
            die_with_err("Node is not in replica mode")
        if (
            replica_resp["master_host"] != args.target_host
            or replica_resp["master_port"] != args.target_port
        ):
            die_with_err("Node is not a replica of target")

        newcomer.update_id()

        config = build_config_from_existing(args)
        master_node = find_master(config, args.target_host, args.target_port)

        master_node["replicas"].append(newcomer)
        print(f"Pushing config:\n{config}\n")
        push_config(config)
    else:
        newcomer = Master(args.attach_host, args.attach_port)
        replica_resp = send_command(newcomer, ["info", "replication"])
        if replica_resp["role"] != "master":
            die_with_err("Node is not in master mode")
        newcomer.update_id()

        newcomer_config = build_config_from_list([newcomer])
        newcomer_config[0]["slot_ranges"] = []
        config = build_config_from_existing(args)
        print(f"Pushing config:\n{config}\n")
        push_config([*config, newcomer_config[0]])
    print()


def detach(args):
    print(f"Detaching remote Dragonfly {args.target_host}:{args.target_port} from cluster")
    print(
        "Important: detached node will not receive a new config! This means that the detached node will still 'think' that it belongs to the cluster"
    )
    config = build_config_from_existing(args)
    node = find_master(config, args.target_host, args.target_port, die_if_not_found=False)
    if node == None:
        replica, master = find_replica(config, args.target_host, args.target_port)
        master["replicas"].remove(replica)
    else:
        if len(node["slot_ranges"]) != 0:
            die_with_err("Can't detach a master with assigned slots")
        if len(node["replicas"]) != 0:
            die_with_err("Can't detach a master with replicas")
        config = [m for m in config if m != node]
    push_config(config)


def takeover(args):
    print(f"Promoting Dragonfly {args.target_host}:{args.target_port} from replica to master")
    print(
        "Important: do not forget to send command REPLICAOF NO ONE to new master, and update "
        "           additional replicas if such exist"
    )
    print("Important: previous master will be detached from the cluster")

    config = build_config_from_existing(args)
    replica, master = find_replica(config, args.target_host, args.target_port)
    master["replicas"].remove(replica)
    master["master"] = replica

    push_config(config)


def move(args):
    config = build_config_from_existing(args)
    new_owner = find_master(config, args.target_host, args.target_port)

    def remove_slot(slot_id, from_range: SlotRange, slot_ranges: list):
        slot_ranges.remove(from_range)
        left, right = from_range.remove(slot_id)
        if left:
            slot_ranges.append(left)
        if right:
            slot_ranges.append(right)

    def add_slot(slot, to_shard):
        slot_range = SlotRange(slot, slot)
        for existing_range in to_shard["slot_ranges"]:
            if existing_range.merge(slot_range):
                return
        to_shard["slot_ranges"].append(slot_range)

    def find_slot(slot, config):
        for shard in config:
            for slot_range in shard["slot_ranges"]:
                if slot_range.contains(slot):
                    return shard, slot_range
        return None, None

    def pack(slot_ranges):
        slot_objects = sorted(slot_ranges, key=lambda x: x.start)
        packed = []
        for slot_range in slot_objects:
            if packed and packed[-1].merge(slot_range):
                continue
            packed.append(slot_range)
        return packed

    for slot in range(args.slot_start, args.slot_end + 1):
        shard, slot_range = find_slot(slot, config)
        if shard == None or shard == new_owner:
            continue
        remove_slot(slot, slot_range, shard["slot_ranges"])
        add_slot(slot, new_owner)

    for shard in config:
        shard["slot_ranges"] = pack(shard["slot_ranges"])

    # Use the custom encoder for printing the JSON
    print(f"Pushing new config:\n{json.dumps(config, indent=2, cls=ClusterConfigEncoder)}\n")
    push_config(config)


def migrate(args):
    config = build_config_from_existing(args)
    target = find_master(config, args.target_host, args.target_port)
    target_node = target["master"]
    target_node.update_id()

    # Find source node
    source = None
    for node in config:
        slots: Iterable[SlotRange] = node["slot_ranges"]
        for slot in slots:
            if slot.start <= args.slot_start and slot.end >= args.slot_end:
                source = node
                break
    if source == None:
        die_with_err("Unsupported slot range migration (currently only 1-node migration supported)")

    source["migrations"] = [
        {
            "slot_ranges": [{"start": args.slot_start, "end": args.slot_end}],
            "node_id": target_node.id,
            "ip": target_node.host,
            "port": target_node.port,
        }
    ]
    push_config(config)

    # wait for migration finish
    sync_status = []
    while True:
        sync_status = send_command(target_node, ["DFLYCLUSTER", "SLOT-MIGRATION-STATUS"])
        if len(sync_status) == 0:
            # Migration didn't start yet
            continue
        if len(sync_status) != 1:
            die_with_err(f"Unexpected number of migrations {len(sync_status)}: {sync_status}")
        if "FINISHED" in sync_status[0]:
            print(f"Migration finished: {sync_status[0]}")
            break

    # Push new config to all nodes
    print("Updating all nodes with new slots state")
    move(args)


def populate(args):
    config = build_config_from_existing(args)
    for shard in config:
        master = shard["master"]
        slot_ranges = shard["slot_ranges"]
        for slot_range in slot_ranges:
            cmd = [
                "debug",
                "populate",
                str(args.size),
                "key",
                str(args.valsize),
                "SLOTS",
                str(slot_range.start),
                str(slot_range.end),
            ]
            send_command(master, cmd)


def print_config(args):
    config = build_config_from_existing(args)
    print(json.dumps(config, indent=2, cls=ClusterConfigEncoder))


def shutdown(args):
    config = build_config_from_existing(args)
    for node in get_nodes_from_config(config):
        send_command(node, ["shutdown"])


def main():
    parser = argparse.ArgumentParser(
        description="""
Dragonfly Manual Cluster Manager

This tool helps managing a Dragonfly cluster manually.
Cluster can either be local or remote:
- Starting Dragonfly instances must be done locally, binary path can be set with `--dragonfly_bin` (default: ../build-opt/dragonfly)
- Remote Dragonflies must already be started, and initialized with `--cluster_mode=yes`

Example usage:

Create a 3 node cluster locally:
  ./cluster_mgr.py --action=create_locally --num_masters=3
This will create 3 Dragonfly processes with ports 7001-7003.
Ports can be overridden with `--first_port`.

Create a 6 node cluster locally, 3 of them masters with 1 replica each:
  ./cluster_mgr.py --action=create_locally --num_masters=3 --replicas_per_master=1

Connect to existing cluster and print current config:
  ./cluster_mgr.py --action=print_config
This will connect to 127.0.0.1:6379 by default. Override with `--target_host` and `--target_port`

Configure an existing Dragonfly server to be a standalone cluster (owning all slots):
  ./cluster_mgr.py --action=config_single_remote
This connects to an *existing* Dragonfly server, and pushes a config telling it to own all slots.
This will connect to 127.0.0.1:6379 by default. Override with `--target_host` and `--target_port`

Attach an existing Dragonfly server to an existing cluster (owning no slots):
  ./cluster_mgr.py --action=attach --attach_host=HOST --attach_port=PORT
This will connect to existing cluster present at 127.0.0.1:6379 by default. Override with
`--target_host` and `--target_port`.
To attach node as a replica - use --attach_as_replica=True. In such case, the node will be a
replica of --target_host/--target_port.

To set up a new cluster - start the servers and then use
  ./cluster_mgr.py --action=config_single_remote ...
  ./cluster_mgr.py --action=attach ...
And repeat `--action=attach` for all servers.
Afterwards, distribute the slots between the servers as desired with `--action=move` or
`--action=migrate`.

To detach (remove) a node from the cluster:
  ./cluster_mgr.py --action=detach --target_host=X --target_port=X
Notes:
- If the node is a master, it must not have any slots assigned to it.
- The node will not be notified that it's no longer in a cluster. It's a good idea to shut it down
  after detaching it from the cluster.

To take over (turn replica to master):
  ./cluster_mgr.py --action=takeover --target_host=X --target_port=X
Notes:
- You'll need to run REPLICAOF NO ONE on the new master
- If previous master had other replicas, you'll need to update them with REPLICAOF as well
- Previous master will be detached from cluster. It's a good idea to shut it down.

Connect to cluster and move slots 10-20 to target:
  ./cluster_mgr.py --action=move --slot_start=10 --slot_end=20 --target_host=X --target_port=X
WARNING: This will NOT migrate existing data, i.e. data in slots 10-20 will be erased.

Migrate slots 10-20 to target:
  ./cluster_mgr.py --action=migrate --slot_start=10 --slot_end=20 --target_host=X --target_port=X
Unlike --action=move above, this will migrate the data to the new owner.

Connect to cluster and shutdown all nodes:
  ./cluster_mgr.py --action=shutdown --target_port=X
WARNING: Be careful! This will close all Dragonfly servers connected to the cluster.
""",
        formatter_class=RawTextHelpFormatter,
    )
    parser.add_argument(
        "--action",
        default="",
        help="Which action to take? See `--help`",
    )
    parser.add_argument(
        "--num_masters", type=int, default=3, help="Number of master nodes in cluster"
    )
    parser.add_argument(
        "--replicas_per_master", type=int, default=0, help="How many replicas for each master"
    )
    parser.add_argument("--first_port", type=int, default=7001, help="First master's port")
    parser.add_argument("--threads", type=int, default=2, help="Threads per node")
    parser.add_argument(
        "--slot_start", type=int, default=0, help="First slot to move / migrate (inclusive)"
    )
    parser.add_argument(
        "--slot_end", type=int, default=100, help="Last slot to move / migrate (inclusive)"
    )
    parser.add_argument("--target_host", default="127.0.0.1", help="Master host/ip")
    parser.add_argument("--target_port", type=int, default=6379, help="Master port")
    parser.add_argument(
        "--attach_host", default="127.0.0.1", help="New cluster node master host/ip"
    )
    parser.add_argument(
        "--attach_port", type=int, default=6379, help="New cluster node master port"
    )
    parser.add_argument(
        "--attach_as_replica", type=bool, default=False, help="Is the attached node a replica?"
    )
    parser.add_argument(
        "--dragonfly_bin", default="../build-opt/dragonfly", help="Dragonfly binary path"
    )
    parser.add_argument(
        "--size", type=int, default=1000000, help="Number of keys to populate in each slotrange"
    )
    parser.add_argument(
        "--valsize", type=int, default=16, help="Value size for each key during population"
    )

    args = parser.parse_args()

    actions = dict(
        [
            (f.__name__, f)
            for f in [
                create_locally,
                shutdown,
                config_single_remote,
                attach,
                detach,
                takeover,
                move,
                print_config,
                migrate,
                populate,
            ]
        ]
    )
    action = actions.get(args.action.lower())
    if action:
        action(args)
    else:
        die_with_err(f'Error - unknown action "{args.action}". See --help')


if __name__ == "__main__":
    main()
