#!/usr/bin/env python3
"""Cluster config monitor for AFL++ cluster mode fuzzing.

Run alongside AFL++ when using `./run_fuzzer.sh cluster`.

Architecture:
  - Node 0 (peer):  regular (non-AFL) Dragonfly, port 6380, stable across the session
  - Node 1 (fuzz):  AFL-instrumented Dragonfly, port 6379, may restart on crash/loop-limit

This script watches node 1 and re-pushes the cluster config whenever it comes back up,
so that AFL++ always fuzzes node 1 in a configured cluster context.

Node IDs are fixed via --cluster_node_id so the JSON config can be built without querying.
"""

import argparse
import asyncio
import json
import logging
import sys
import time

import redis.asyncio as aioredis

MAX_SLOT = 16383

# Fixed node IDs (must match --cluster_node_id flags passed to each Dragonfly instance)
DEFAULT_NODE0_ID = "0" * 40
DEFAULT_NODE1_ID = "1" * 40


def _build_config(
    node0_id: str, node0_port: int, node1_id: str, node1_port: int, ip: str = "127.0.0.1"
) -> str:
    split = MAX_SLOT // 2
    return json.dumps(
        [
            {
                "slot_ranges": [{"start": 0, "end": split}],
                "master": {"id": node0_id, "ip": ip, "port": node0_port},
                "replicas": [],
            },
            {
                "slot_ranges": [{"start": split + 1, "end": MAX_SLOT}],
                "master": {"id": node1_id, "ip": ip, "port": node1_port},
                "replicas": [],
            },
        ]
    )


async def _ping(port: int, timeout: float = 2.0) -> bool:
    try:
        r = aioredis.Redis(
            host="127.0.0.1", port=port, socket_timeout=timeout, socket_connect_timeout=timeout
        )
        await r.ping()
        await r.aclose()
        return True
    except Exception:
        return False


async def _push_config(port: int, config_json: str, timeout: float = 5.0) -> bool:
    try:
        r = aioredis.Redis(
            host="127.0.0.1", port=port, socket_timeout=timeout, socket_connect_timeout=timeout
        )
        await r.execute_command("DFLYCLUSTER", "CONFIG", config_json)
        await r.aclose()
        return True
    except Exception as e:
        logging.debug(f"push_config port={port}: {e}")
        return False


async def _wait_up(port: int, timeout: float = 120.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await _ping(port):
            return True
        await asyncio.sleep(0.3)
    return False


async def _wait_down(port: int) -> None:
    """Block until the node stops responding."""
    while await _ping(port, timeout=1.0):
        await asyncio.sleep(0.3)


async def run(node0_port: int, node0_id: str, node1_port: int, node1_id: str) -> None:
    config = _build_config(node0_id, node0_port, node1_id, node1_port)

    # Skip the initial startup: seeds already embed DFLYCLUSTER CONFIG so node1 is
    # configured from the very first AFL++ iteration without any external push.
    # Pushing CONFIG during AFL++ calibration makes all seeds appear "variable"
    # (different coverage between calibration runs) which triggers an AFL++ 4.34c bug.
    # Instead, wait for node1 to go down first, then handle restarts.
    logging.info(f"Waiting for node1 (port {node1_port}) initial startup to complete...")
    if not await _wait_up(node1_port):
        logging.error("Node1 never came up — exiting monitor")
        sys.exit(1)
    logging.info("Node1 is up — skipping initial config push (handled by seeds)")
    await _wait_down(node1_port)
    logging.info("Node1 went down for the first time — entering restart loop")

    while True:
        logging.info(f"Waiting for node1 (port {node1_port}) to restart...")
        if not await _wait_up(node1_port):
            logging.error("Node1 never came back up — exiting monitor")
            sys.exit(1)

        logging.info("Node1 restarted — pushing cluster config to both nodes")
        ok0 = await _push_config(node0_port, config)
        ok1 = await _push_config(node1_port, config)
        if ok0 and ok1:
            logging.info("Cluster re-configured after restart")
        else:
            logging.warning(f"Config push partial: node0={ok0} node1={ok1}")

        # Wait for node1 to go down again (crash or afl_loop_limit restart)
        await _wait_down(node1_port)
        logging.info("Node1 went down — will re-configure on next startup")
        await asyncio.sleep(0.2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Cluster config monitor for AFL++ cluster fuzzing")
    parser.add_argument("--node0-port", type=int, default=6380)
    parser.add_argument("--node0-id", default=DEFAULT_NODE0_ID)
    parser.add_argument("--node1-port", type=int, default=6379)
    parser.add_argument("--node1-id", default=DEFAULT_NODE1_ID)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    asyncio.run(run(args.node0_port, args.node0_id, args.node1_port, args.node1_id))


if __name__ == "__main__":
    main()
