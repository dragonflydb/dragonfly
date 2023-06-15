#!/usr/bin/env python3
import argparse
import json
import math
import redis
import subprocess
import time

'''
To install: pip install -r requirements.txt
'''


class Node:
    def __init__(self, port, admin_port):
        self.id = ''
        self.port = port
        self.admin_port = admin_port


class Master:
    def __init__(self, port, admin_port):
        self.node = Node(port, admin_port)
        self.replicas = []


def start_node(node, threads):
    f = open(f'/tmp/dfly.cluster.node.{node.port}.log', 'w')
    print(f'- Log file for node {node.port}: {f.name}')
    subprocess.Popen(['../build-dbg/dragonfly', f'--port={node.port}',
                      f'--admin_port={node.admin_port}', '--cluster_mode=yes', f'--proactor_threads={threads}',
                      '--dbfilename=', f'--logtostderr'], stderr=f)


def send_command(node, command):
    client = redis.Redis(decode_responses=True,
                         host="localhost", port=node.admin_port)

    for i in range(0, 5):
        try:
            result = client.execute_command(*command)
            client.close()
            return result
        except:
            time.sleep(0.1)

    print(
        f'Unable to connect to localhost:{node.admin_port} after 5 attempts!')


def update_id(node):
    id = send_command(node, ['dflycluster', 'myid'])
    node.id = id
    print(f'- ID for {node.port}: {id}')


def build_config(masters):
    total_slots = 16384
    slots_per_node = math.floor(total_slots / len(masters))

    def build_node(node):
        return {
            "id": node.id,
            "ip": "localhost",
            "port": node.port
        }

    config = []
    for i, master in enumerate(masters):
        c = {
            "slot_ranges": [
                {
                    "start": i * slots_per_node,
                    "end": (i+1) * slots_per_node - 1
                }
            ],
            "master": build_node(master.node),
            "replicas": [build_node(replica) for replica in master.replicas]
        }

        config.append(c)

    config[-1]["slot_ranges"][-1]["end"] += total_slots % len(masters)
    return json.dumps(config, indent=2)


def push_config(nodes, config):
    for node in nodes:
        response = send_command(node, ['dflycluster', 'config', config])
        print(f'- Push into {node.port}: {response}')


def main():
    parser = argparse.ArgumentParser(description='Local Cluster Manager')
    parser.add_argument('--num_masters', type=int, default=3,
                        help='Number of master nodes in cluster')
    parser.add_argument('--replicas_per_master', type=int, default=0,
                        help='How many replicas for each master')
    parser.add_argument('--first_port', type=int,
                        default=7001, help="First master's port")
    parser.add_argument('--first_admin_port', type=int,
                        default=17_001, help="First master's admin port")
    parser.add_argument('--threads', type=int, default=2,
                        help="Threads per node")
    args = parser.parse_args()

    print(f'Setting up a Dragonfly cluster:')
    print(f'- Master nodes: {args.num_masters}')
    print(
        f'- Ports: {args.first_port}...{args.first_port + args.num_masters - 1}')
    print(
        f'- Admin ports: {args.first_admin_port}...{args.first_admin_port + args.num_masters - 1}')
    print(f'- Replicas for each master: {args.replicas_per_master}')
    print()

    next_port = args.first_port
    next_admin_port = args.first_admin_port
    masters = []
    for i in range(args.num_masters):
        master = Master(next_port, next_admin_port)
        next_port += 1
        next_admin_port += 1
        for j in range(args.replicas_per_master):
            replica = Node(next_port, next_admin_port)
            master.replicas.append(replica)
            next_port += 1
            next_admin_port += 1
        masters.append(master)

    nodes = []
    for master in masters:
        nodes.append(master.node)
        for replica in master.replicas:
            nodes.append(replica)

    print('Starting nodes...')
    for node in nodes:
        start_node(node, args.threads)
    print()

    if args.replicas_per_master > 0:
        print('Configuring replication...')
        for master in masters:
            for replica in master.replicas:
                response = send_command(
                    replica, ['replicaof', 'localhost', master.node.port])
                print(
                    f'- {replica.port} replicating {master.node.port}: {response}')
        print()

    print(f'Getting IDs...')
    for n in nodes:
        update_id(n)
    print()

    config = build_config(masters)
    print(f'Pushing config...')
    push_config(nodes, config)
    print()


if __name__ == "__main__":
    main()
