#!/usr/bin/env python3
"""Count hardware events per walked member with the walk.cc driver and perf stat.

Each case runs the driver with 20 walks and with none over the same 1,000,000-member set; the
difference divided by 20 million is the cost of one member. The events form two groups, each small
enough to count without multiplexing, and their names are specific to AMD Zen processors.
--binary takes the walk.cc builds as walk.py does: default=PATH and w4o1=PATH. perf runs through
`sudo -n`.
"""

import argparse
import csv
import statistics
import subprocess
from pathlib import Path

GROUPS = (
    "cycles,instructions,ex_ret_brn_misp,ls_dmnd_fills_from_sys.all,"
    "ls_any_fills_from_sys.dram_io_all",
    "cycles,ls_alloc_mab_count,ls_hw_pf_dc_fills.all,ls_l1_d_tlb_miss.all,"
    "ls_l1_d_tlb_miss.all_l2_miss",
)
# Set, walk, keys, build, driver container, and driver flags.
CASES = (
    ("OAHSet 16/2", "iterator", "ascii", "default", "oah", []),
    ("OAHSet 16/2", "iterator", "binary", "default", "oah", ["--binary"]),
    ("OAHSet 4/1", "iterator", "ascii", "w4o1", "oah", []),
    ("StringSet", "iterator", "ascii", "default", "ss", []),
    ("StringSet", "iterator", "binary", "default", "ss", ["--binary"]),
    ("OAHSet 16/2", "scan", "ascii", "default", "oah", ["--scan"]),
    ("OAHSet 4/1", "scan", "ascii", "w4o1", "oah", ["--scan"]),
    ("StringSet", "scan", "ascii", "default", "ss", ["--scan"]),
)
PASSES = 20
MEMBERS = 1_000_000


def count(cpu, binary, container, passes, extra):
    totals = {}
    for index, group in enumerate(GROUPS):
        command = ["sudo", "-n", "perf", "stat", "-x,", "-e", group, "--", "taskset", "-c"]
        command += [str(cpu), binary, f"--set={container}", f"--count={MEMBERS}"]
        command += [f"--passes={passes}", "--logtostderr", *extra]
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        for line in result.stderr.splitlines():
            fields = line.split(",")
            if len(fields) > 4 and fields[0].replace(".", "").isdigit():
                if fields[4] not in ("100.00", ""):
                    raise RuntimeError(f"multiplexed counter: {line}")
                # Both groups count cycles; the second group's events get a "#1" suffix on clashes.
                event = fields[2] if fields[2] not in totals else f"{fields[2]}#{index}"
                totals[event] = float(fields[0])
    return totals


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", action="append", required=True, metavar="LABEL=PATH")
    parser.add_argument("--cpu", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    binaries = dict(value.split("=", 1) for value in args.binary)
    if set(binaries) != {"default", "w4o1"}:
        parser.error("pass --binary default=PATH and --binary w4o1=PATH")
    args.output.mkdir(parents=True, exist_ok=True)
    rows = []
    for repeat in range(args.repeats):
        for name, walk, keys, build, container, extra in CASES:
            base = count(args.cpu, binaries[build], container, 0, extra)
            full = count(args.cpu, binaries[build], container, PASSES, extra)
            for event in full:
                per_member = (full[event] - base[event]) / (PASSES * MEMBERS)
                rows.append([name, walk, keys, repeat, event, round(per_member, 4)])
            print(name, walk, keys, repeat, flush=True)
    with open(args.output / "walk_counters.csv", "w", newline="") as file:
        writer = csv.writer(file, lineterminator="\n")
        writer.writerow(["set", "walk", "keys", "repeat", "event", "per_member"])
        writer.writerows(rows)

    # ls_alloc_mab_count adds up the L1 misses in flight in every cycle, so dividing it by the
    # cycles of its group gives the average number of misses the core overlaps.
    for name, walk, keys, *_ in CASES:
        median = {}
        for event in {row[4] for row in rows}:
            values = [r[5] for r in rows if r[:3] == [name, walk, keys] and r[4] == event]
            median[event] = statistics.median(values)
        print(
            f"{name:12} {walk:8} {keys:6}",
            f"cycles={median['cycles']:.1f} instructions={median['instructions']:.1f}",
            f"IPC={median['instructions'] / median['cycles']:.2f}",
            f"MLP={median['ls_alloc_mab_count'] / median['cycles#1']:.2f}",
            f"dram={median['ls_any_fills_from_sys.dram_io_all']:.3f}",
            f"demand_sys={median['ls_dmnd_fills_from_sys.all']:.3f}",
            f"hwpf={median['ls_hw_pf_dc_fills.all']:.3f}",
            f"brmisp={median['ex_ret_brn_misp']:.3f}",
            f"tlb={median['ls_l1_d_tlb_miss.all']:.3f}",
            f"tlb_l2={median['ls_l1_d_tlb_miss.all_l2_miss']:.4f}",
        )


if __name__ == "__main__":
    main()
