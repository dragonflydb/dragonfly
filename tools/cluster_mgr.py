#!/usr/bin/env python3
import argparse
from argparse import RawTextHelpFormatter
import json
import math
import redis
import subprocess
import time

"""
To install: pip install -r requirements.txt
"""


class Node:
    def __init__(self, host, port):
        self.id = ""
        self.host = host
        self.port = port


class Master:
    def __init__(self, host, port):
        self.node = Node(host, port)
        self.replicas = []


def start_node(node, threads):
    f = open(f"/tmp/dfly.cluster.node.{node.port}.log", "w")
    print(f"- Log file for node {node.port}: {f.name}")
    subprocess.Popen(
        [
            "../build-opt/dragonfly",
            f"--port={node.port}",
            "--cluster_mode=yes",
            f"--proactor_threads={threads}",
            "--dbfilename=",
            f"--logtostderr",
            "--proactor_affinity_mode=off",
        ],
        stderr=f,
    )


def send_command(node, command, print_errors=True):
    client = redis.Redis(decode_responses=True, host=node.host, port=node.port)

    for i in range(0, 5):
        try:
            result = client.execute_command(*command)
            client.close()
            return result
        except Exception as e:
            if print_errors:
                print(e)
            time.sleep(0.1 * i)

    if print_errors:
        print(f"Unable to run command {command} against {node.host}:{node.port} after 5 attempts!")

    return Exception()


def update_id(node):
    node.id = send_command(node, ["dflycluster", "myid"])
    print(f"- ID {node.id}")


def build_config_from_list(masters):
    total_slots = 16384
    slots_per_node = math.floor(total_slots / len(masters))

    def build_node(node):
        return {"id": node.id, "ip": node.host, "port": node.port}

    config = []
    for i, master in enumerate(masters):
        c = {
            "slot_ranges": [{"start": i * slots_per_node, "end": (i + 1) * slots_per_node - 1}],
            "master": build_node(master.node),
            "replicas": [build_node(replica) for replica in master.replicas],
        }

        config.append(c)

    config[-1]["slot_ranges"][-1]["end"] += total_slots % len(masters)
    return config


def get_nodes_from_config(config):
    nodes = []
    for shard in config:
        nodes.append(Node(shard["master"]["ip"], shard["master"]["port"]))
        for replica in shard["replicas"]:
            nodes.append(Node(replica["ip"], replica["port"]))
    for node in nodes:
        update_id(node)
    return nodes


def push_config(config):
    def push_to_node(node, config):
        config_str = json.dumps(config, indent=2)
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
        master = Master("localhost", next_port)
        next_port += 1
        for j in range(args.replicas_per_master):
            replica = Node("localhost", next_port)
            master.replicas.append(replica)
            next_port += 1
        masters.append(master)

    nodes = []
    for master in masters:
        nodes.append(master.node)
        for replica in master.replicas:
            nodes.append(replica)

    print("Starting nodes...")
    for node in nodes:
        start_node(node, args.threads)
    print()

    if args.replicas_per_master > 0:
        print("Configuring replication...")
        for master in masters:
            for replica in master.replicas:
                response = send_command(replica, ["replicaof", master.node.host, master.node.port])
                print(f"- {replica.port} replicating {master.node.port}: {response}")
        print()

    print(f"Getting IDs...")
    for n in nodes:
        update_id(n)
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
    update_id(master.node)

    test = send_command(master.node, ["get", "x"], print_errors=False)
    if type(test) is not Exception:
        print("Node either not found or already configured")
        exit(-1)

    config = build_config_from_list([master])
    print(f"Pushing config:\n{config}\n")
    push_config(config)
    print()


def build_config_from_existing(args):
    def list_to_dict(l):
        return {l[i]: l[i + 1] for i in range(0, len(l), 2)}

    def build_node(node_list):
        d = list_to_dict(node_list)
        return {"id": d["id"], "ip": d["endpoint"], "port": d["port"]}

    def build_slots(slot_list):
        slots = []
        for i in range(0, len(slot_list), 2):
            slots.append({"start": slot_list[i], "end": slot_list[i + 1]})
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
    return config


def find_node(config, port):
    new_owner = None
    for shard in config:
        if shard["master"]["port"] == port:
            new_owner = shard
            break
    else:
        print(f"Can't find master with port {port} (hint: use flag --target_port).")
        exit(-1)
    return new_owner


def attach(args):
    print(f"Attaching remote Dragonfly {args.attach_host}:{args.attach_port} to cluster")
    newcomer = Master(args.attach_host, args.attach_port)
    update_id(newcomer.node)

    newcomer_config = build_config_from_list([newcomer])
    newcomer_config[0]["slot_ranges"] = []
    config = build_config_from_existing(args)
    print(f"Pushing config:\n{config}\n")
    push_config([*config, newcomer_config[0]])
    print()


def move(args):
    config = build_config_from_existing(args)
    new_owner = find_node(config, args.target_port)

    def remove_slot(slot, from_range, from_shard):
        if from_range["start"] == slot:
            from_range["start"] += 1
            if from_range["start"] > from_range["end"]:
                from_shard["slot_ranges"].remove(from_range)
        elif from_range["end"] == slot:
            from_range["end"] -= 1
            if from_range["start"] > from_range["end"]:
                from_shard["slot_ranges"].remove(from_range)
        else:
            assert (
                slot > from_range["start"] and slot < from_range["end"]
            ), f'{slot} {from_range["start"]} {from_range["end"]}'
            from_shard["slot_ranges"].append({"start": slot + 1, "end": from_range["end"]})
            from_range["end"] = slot - 1

    def add_slot(slot, to_shard):
        for slot_range in to_shard["slot_ranges"]:
            if slot == slot_range["start"] - 1:
                slot_range["start"] -= 1
                return
            if slot == slot_range["end"] + 1:
                slot_range["end"] += 1
                return
        to_shard["slot_ranges"].append({"start": slot, "end": slot})

    def find_slot(slot, config):
        for shard in config:
            if shard == new_owner:
                continue
            for slot_range in shard["slot_ranges"]:
                if slot >= slot_range["start"] and slot <= slot_range["end"]:
                    return shard, slot_range
        return None, None

    def pack(slot_ranges):
        new_range = []
        while True:
            changed = False
            new_range = []
            slot_ranges.sort(key=lambda x: x["start"])
            for i, slot_range in enumerate(slot_ranges):
                added = False
                for j in range(i):
                    prev_slot_range = slot_ranges[j]
                    if prev_slot_range["end"] + 1 == slot_range["start"]:
                        prev_slot_range["end"] = slot_range["end"]
                        changed = True
                        added = True
                        break
                if not added:
                    new_range.append(slot_range)
            slot_ranges = new_range
            if not changed:
                break
        return new_range

    for slot in range(args.slot_start, args.slot_end + 1):
        shard, slot_range = find_slot(slot, config)
        if shard == None:
            continue
        if shard == new_owner:
            continue
        remove_slot(slot, slot_range, shard)
        add_slot(slot, new_owner)

    for shard in config:
        shard["slot_ranges"] = pack(shard["slot_ranges"])

    print(f"Pushing new config:\n{json.dumps(config, indent=2)}\n")
    push_config(config)


def migrate(args):
    config = build_config_from_existing(args)
    target = find_node(config, args.target_port)
    target_node = Node(target["master"]["ip"], target["master"]["port"])
    update_id(target_node)

    # Find source node
    source = None
    for node in config:
        slots = node["slot_ranges"]
        for slot in slots:
            if slot["start"] >= args.slot_start and slot["end"] <= args.slot_end:
                source = node
                break
    if source == None:
        print("Unsupported slot range migration (currently only 1-node migration supported)")
        exit(-1)
    source_node = Node(source["master"]["ip"], source["master"]["port"])
    update_id(source_node)

    # do migration
    sync_id = send_command(
        target_node,
        [
            "DFLYCLUSTER",
            "START-SLOT-MIGRATION",
            source_node.host,
            source_node.port,
            args.slot_start,
            args.slot_end,
        ],
    )

    # wait for migration finish
    sync_status = []
    while True:
        sync_status = send_command(target_node, ["DFLYCLUSTER", "SLOT-MIGRATION-STATUS"])
        assert len(sync_status) == 1
        if sync_status[0].endswith("STABLE_SYNC"):
            break

    print("Reached stable sync: ", sync_status)
    res = send_command(source_node, ["DFLYCLUSTER", "SLOT-MIGRATION-FINALIZE", sync_id])
    assert res == "OK"

    # Push new config to all nodes
    move(args)


def print_config(args):
    config = build_config_from_existing(args)
    print(json.dumps(config, indent=2))


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
- Starting Dragonfly instances must be done locally, binary is assumed to be under ../build-opt
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
This will connect to localhost:6379 by default. Override with `--target_host` and `--target_port`

Configure an existing Dragonfly server to be a standalone cluster (owning all slots):
  ./cluster_mgr.py --action=config_single_remote
This connects to an *existing* Dragonfly server, and pushes a config telling it to own all slots.
This will connect to localhost:6379 by default. Override with `--target_host` and `--target_port`

Attach an existing Dragonfly server to an existing cluster (owning no slots):
  ./cluster_mgr.py --action=attach --attach_host=HOST --attach_port=PORT
This will connect to existing cluster present at localhost:6379 by default. Override with
`--target_host` and `--target_port`

To set up a new cluster - start the servers and then use
  ./cluster_mgr.py --action=create_single_remote ...
  ./cluster_mgr.py --action=attach ...
And repeat `--action=attach` for all servers.
Afterwards, distribute the slots between the servers as desired with `--action=move` or
`--action=migrate`

Connect to cluster and move slots 10-20 to master with port 7002:
  ./cluster_mgr.py --action=move --slot_start=10 --slot_end=20 --new_owner=7002

Migrate slots 10-20 to master with port 7002
  ./cluster_mgr.py --action=migrate --slot_start=10 --slot_end=20 --new_owner=7002

Connect to cluster and shutdown all nodes:
  ./cluster_mgr.py --action=shutdown
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
    parser.add_argument("--target_host", default="localhost", help="Master host/ip")
    parser.add_argument("--target_port", type=int, default=6379, help="Master port")
    parser.add_argument(
        "--attach_host", default="localhost", help="New cluster node master host/ip"
    )
    parser.add_argument(
        "--attach_port", type=int, default=6379, help="New cluster node master port"
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
                move,
                print_config,
                migrate,
            ]
        ]
    )
    action = actions.get(args.action.lower())
    if action:
        action(args)
    else:
        print(f'Error - unknown action "{args.action}". See --help')
        exit(-1)


if __name__ == "__main__":
    main()
