#!/usr/bin/env python3
"""Count hardware events while Dragonfly saves a set.

For OAHSet and StringSet, one Dragonfly with one proactor thread on --cpu builds the set that
server_commands.py builds: --members random 32-byte ASCII members, added with SADD. After one SAVE
to warm up, `perf stat` counts each group of events over --saves SAVE commands. perf starts with
its events disabled and gets them enabled through its control FIFO only while the SAVE commands
run. Counts are divided by the number of members saved. Every repeat starts new servers. The
events are those of AMD Zen 5, as in walk_counters.py, and perf runs through `sudo -n`.

Writes save_counters.csv (repeat, set, group, event, per_member) and save_counters.json.
"""

import argparse
import datetime
import json
import os
from pathlib import Path
import re
import select
import statistics
import subprocess
import tempfile

import server_commands

GROUPS = (
    "cycles,instructions,ex_ret_brn_misp,ls_dmnd_fills_from_sys.all,"
    "ls_any_fills_from_sys.dram_io_all",
    "cycles,ls_alloc_mab_count,ls_hw_pf_dc_fills.all,ls_l1_d_tlb_miss.all",
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dragonfly", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cpu", type=int, default=min(os.sched_getaffinity(0)))
    parser.add_argument("--port", type=int, default=16379)
    parser.add_argument("--members", type=int, default=1_000_000)
    parser.add_argument("--saves", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)

    members = server_commands.random_members(args.members)
    saved = args.saves * len(members)
    counters = []
    for repeat in range(args.repeats):
        for container, oah in (("OAHSet", True), ("StringSet", False)):
            with tempfile.TemporaryDirectory() as scratch:
                scratch = Path(scratch)
                server = server_commands.start_server(args, args.dragonfly, oah, scratch)
                try:
                    client = server_commands.Client(args.port)
                    server_commands.add_members(client, b"r", members)
                    save(client, 1)
                    for group, events in enumerate(GROUPS, 1):
                        for event, value in measure(events, server.pid, client, args, scratch):
                            counters.append(
                                {
                                    "repeat": repeat,
                                    "set": container,
                                    "group": group,
                                    "event": event,
                                    "per_member": round(value / saved, 3),
                                }
                            )
                finally:
                    server_commands.stop_server(args, server)

    (args.output / "save_counters.csv").write_text(server_commands.as_csv(counters))
    metadata = {
        "finished_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "binary": server_commands.describe("default", args.dragonfly),
        "server_cpu": args.cpu,
        "members": args.members,
        "saves_per_measurement": args.saves,
        "repeats": args.repeats,
        "event_groups": GROUPS,
        "perf": subprocess.run(
            ["perf", "--version"], capture_output=True, text=True
        ).stdout.strip(),
    }
    (args.output / "save_counters.json").write_text(json.dumps(metadata, indent=2) + "\n")
    summarize(counters)


def summarize(counters):
    # ls_alloc_mab_count adds up the L1 misses in flight in every cycle, so dividing it by the
    # cycles of its group gives the average number of misses the core overlaps.
    for container in ("OAHSet", "StringSet"):
        samples = {}
        for row in counters:
            if row["set"] == container:
                samples.setdefault((row["group"], row["event"]), []).append(row["per_member"])
        median = {key: statistics.median(values) for key, values in samples.items()}
        print(
            f"{container:9}",
            f"cycles={median[1, 'cycles']:.1f} instructions={median[1, 'instructions']:.1f}",
            f"IPC={median[1, 'instructions'] / median[1, 'cycles']:.2f}",
            f"MLP={median[2, 'ls_alloc_mab_count'] / median[2, 'cycles']:.2f}",
            f"dram={median[1, 'ls_any_fills_from_sys.dram_io_all']:.3f}",
            f"demand_sys={median[1, 'ls_dmnd_fills_from_sys.all']:.3f}",
            f"hwpf={median[2, 'ls_hw_pf_dc_fills.all']:.3f}",
            f"brmisp={median[1, 'ex_ret_brn_misp']:.3f}",
            f"tlb={median[2, 'ls_l1_d_tlb_miss.all']:.3f}",
        )


def save(client, count):
    for _ in range(count):
        if client.call("SAVE") != "OK":
            raise RuntimeError("SAVE failed")


def measure(events, pid, client, args, scratch):
    """Counts the events in the server only during the SAVE commands; yields (event, count)."""
    control, ack = scratch / "perf.control", scratch / "perf.ack"
    os.mkfifo(control)
    os.mkfifo(ack)
    perf = subprocess.Popen(
        ["sudo", "-n", "perf", "stat", "-x,", "-e", events, "-p", str(pid), "--delay=-1"]
        + [f"--control=fifo:{control},{ack}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Opening a FIFO for both reading and writing doesn't wait for perf to open the other end.
    control_fd, ack_fd = os.open(control, os.O_RDWR), os.open(ack, os.O_RDWR)
    try:
        tell(perf, control_fd, ack_fd, "enable")
        save(client, args.saves)
        tell(perf, control_fd, ack_fd, "disable")
    finally:
        os.close(control_fd)
        os.close(ack_fd)
        control.unlink()
        ack.unlink()

        # perf stat prints its counts when interrupted.
        subprocess.run(["sudo", "-n", "kill", "-INT", str(perf.pid)], check=False)
    output = perf.communicate(timeout=60)[1]
    if perf.returncode not in (0, -2, 130):
        raise RuntimeError(f"perf failed: {output}")
    for line in output.splitlines():
        fields = line.split(",")
        if len(fields) > 4 and re.fullmatch(r"[\d.]+", fields[0]):
            if fields[4] not in ("100.00", ""):
                raise RuntimeError(f"multiplexed counter: {line}")
            yield fields[2], float(fields[0])


def tell(perf, control_fd, ack_fd, word):
    os.write(control_fd, f"{word}\n".encode())

    # perf answers with "ack\n" and the string's terminating NUL.
    if not select.select([ack_fd], [], [], 30)[0] or os.read(ack_fd, 16) != b"ack\n\0":
        raise RuntimeError(f"perf did not acknowledge {word!r} (exit code {perf.poll()})")


if __name__ == "__main__":
    main()
