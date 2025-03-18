import json
from collections import defaultdict
from pprint import pprint

import docker


def get_host_port(ports: dict, container_port: str, match_ip: str = "0.0.0.0") -> int:
    for cport, hports in ports.items():
        if cport == container_port:
            for hp in hports:
                if hp.get("HostIp", "-") == match_ip:
                    return int(hp["HostPort"])
    return -1


client = docker.from_env()

cluster_config = {"nodes": []}
nodes = cluster_config["nodes"]

port_maps = defaultdict(dict)
containers = client.containers.list(filters={"name": "dragonfly-dragonfly-"})
for container in containers:
    node_info = {
        "externally_routable_ip": "127.0.0.1",
        "ssh_config": {
            "host": container.name,
            "hostname": "127.0.0.1",
            "password": "passwd",
            "user": "root",
        },
    }
    ssh_config = node_info["ssh_config"]
    ssh_port = get_host_port(container.ports, "22/tcp")
    if ssh_port != -1:
        ssh_config["port"] = ssh_port
    app_port = get_host_port(container.ports, "6379/tcp")
    if app_port != -1:
        node_info["app_port"] = app_port
    if app_port != -1 and ssh_port != -1:
        node_info["hostname"] = container.name
        nodes.append(node_info)
pprint(cluster_config)

with open("duck_cluster.json", "w") as f:
    json.dump(cluster_config, f)
