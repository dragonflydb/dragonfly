#!/usr/bin/env python3
"""Drive MEMORY DEFRAGMENT in a loop, recording fragmentation and per-shard
phase timings to JSONL for plotting.

Connects to an already-running dragonfly. Each cycle:
  1. Capture stderr-log byte offset
  2. MEMORY ARENA SUMMARY -> before
  3. MEMORY DEFRAGMENT    -> blocks until the slice finishes; capture reply
  4. MEMORY ARENA SUMMARY -> after
  5. Read log delta, parse defrag[*] lines, attribute per shard
  6. Emit one JSONL record
  7. Sleep

Run dragonfly with:
    ./build-dbg/dragonfly --alsologtostderr ...
This writes glog files into /tmp/ with /tmp/dragonfly.INFO as a stable symlink
to the current INFO log; the script reads from that symlink.

Plot in pandas with:
    pd.read_json("run.jsonl", lines=True)
"""

import argparse
import asyncio
import json
import os
import re
import time

import redis.asyncio as aioredis


ARENA_TOTAL_RE = re.compile(r"Total:\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)%")


def parse_arena_total(report: str) -> dict | None:
    """Pull the machine-wide Total: row from MEMORY ARENA SUMMARY."""
    last_total = None
    for line in report.splitlines():
        match = ARENA_TOTAL_RE.search(line)
        if match:
            last_total = match
    if not last_total:
        return None
    reserved, committed, used, wasted, waste_pct = last_total.groups()
    return {
        "reserved": int(reserved),
        "committed": int(committed),
        "used": int(used),
        "wasted": int(wasted),
        "waste_pct": float(waste_pct),
    }


# defrag[CYCLE_DONE] shard=0 cycle=7 targets_done=12/15 (80.0%) bytes_freed=...
CYCLE_DONE_RE = re.compile(
    r"defrag\[CYCLE_DONE\]\s+shard=(\d+)\s+cycle=(\d+)\s+"
    r"targets_done=(\d+)/(\d+)\s+\(([\d.]+)%\)\s+"
    r"bytes_freed=([\d.]+)(KiB|MiB|GiB|B)/[^\s]+\s+\(([\d.]+)%\)\s+"
    r"bytes_moved=([\d.]+)(KiB|MiB|GiB|B)\s+"
    r"cycle_took=([\d.]+)ms\s+freed_rate=([\d.]+)MiB/s"
)

PHASE_RE = re.compile(
    r"defrag\[(CENSUS|PLAN|EVACUATE|VERIFY)\]\s+shard=(\d+)\s+cycle=(\d+)\s+"
    r".*?\stook=([\d.]+)ms(?:\s+cpu=([\d.]+)ms)?"
)

UNIT = {"B": 1, "KiB": 1024, "MiB": 1024 * 1024, "GiB": 1024 * 1024 * 1024}


def parse_log_delta(text: str) -> dict[int, dict]:
    """Extract per-shard summary from a slice of dragonfly stderr."""
    by_shard: dict[int, dict] = {}

    for match in PHASE_RE.finditer(text):
        phase, shard_str, cycle_str, took_ms, cpu_ms = match.groups()
        shard = int(shard_str)
        rec = by_shard.setdefault(
            shard,
            {
                "shard": shard,
                "cycle_id": int(cycle_str),
                "phase_ms": {},
                "phase_cpu_ms": {},
            },
        )
        rec.setdefault("phase_cpu_ms", {})
        rec["phase_ms"][phase.lower()] = float(took_ms)
        if cpu_ms is not None:
            rec["phase_cpu_ms"][phase.lower()] = float(cpu_ms)

    for match in CYCLE_DONE_RE.finditer(text):
        (
            shard_str,
            cycle_str,
            done,
            total,
            _done_pct,
            freed_v,
            freed_u,
            _freed_pct,
            moved_v,
            moved_u,
            cycle_took,
            freed_rate,
        ) = match.groups()
        shard = int(shard_str)
        rec = by_shard.setdefault(
            shard, {"shard": shard, "cycle_id": int(cycle_str), "phase_ms": {}}
        )
        rec.update(
            {
                "cycle_id": int(cycle_str),
                "targets_complete": int(done),
                "targets_total": int(total),
                "bytes_freed": int(float(freed_v) * UNIT[freed_u]),
                "bytes_moved": int(float(moved_v) * UNIT[moved_u]),
                "cycle_took_ms": float(cycle_took),
                "freed_rate_mibs": float(freed_rate),
            }
        )

    return by_shard


async def get_arena(client: aioredis.Redis) -> dict | None:
    report = await client.execute_command("MEMORY", "ARENA", "SUMMARY")
    if isinstance(report, bytes):
        report = report.decode()
    return parse_arena_total(report)


async def run_defragment(client: aioredis.Redis) -> str:
    reply = await client.execute_command("MEMORY", "DEFRAGMENT")
    if isinstance(reply, bytes):
        reply = reply.decode()
    return reply


def read_log_delta(path: str, start_offset: int) -> tuple[str, int]:
    """Return text written to `path` since `start_offset`, plus new EOF."""
    with open(path, "rb") as fh:
        fh.seek(0, os.SEEK_END)
        end = fh.tell()
        if end < start_offset:
            # log was truncated/rotated under us; restart from 0
            start_offset = 0
        fh.seek(start_offset)
        data = fh.read(end - start_offset)
    return data.decode(errors="replace"), end


def log_size(path: str) -> int:
    try:
        return os.path.getsize(path)
    except FileNotFoundError:
        return 0


def print_summary(records: list[dict]) -> None:
    if not records:
        return
    print("\n=== summary ===")
    for rec in records:
        before = rec.get("before") or {}
        after = rec.get("after") or {}
        waste_before = before.get("waste_pct")
        waste_after = after.get("waste_pct")
        waste_str = (
            f"{waste_before:.2f}% -> {waste_after:.2f}%"
            if waste_before is not None and waste_after is not None
            else "waste n/a"
        )
        call_ms = rec.get("call_ms", 0.0)

        shards = rec.get("shards") or []
        committed_drop = (
            (before.get("committed", 0) - after.get("committed", 0)) if before and after else 0
        )
        freed = sum(s.get("bytes_freed", 0) for s in shards)

        # Aggregate per-phase CPU=ms across shards (max), preferring the new
        # cpu= field over the wall-clock took= field. "-" for unparsed.
        phase_keys = ("census", "plan", "evacuate", "verify")
        phase_max: dict[str, float | None] = {k: None for k in phase_keys}
        any_cpu = False
        any_wall = False
        for s in shards:
            cpu_map = s.get("phase_cpu_ms") or {}
            wall_map = s.get("phase_ms") or {}
            for k in phase_keys:
                v = cpu_map.get(k, wall_map.get(k))
                if k in cpu_map:
                    any_cpu = True
                elif k in wall_map:
                    any_wall = True
                if v is None:
                    continue
                phase_max[k] = v if phase_max.get(k) is None else max(phase_max[k], v)

        if any_cpu or any_wall:
            phase_str = " ".join(
                f"{k[:4]}={'-' if phase_max[k] is None else f'{phase_max[k]:.1f}ms'}"
                for k in phase_keys
            )
            label = "cpu" if any_cpu else "wall"
            phases_part = f"phases.{label}[{phase_str}]"
        else:
            phases_part = "phases[no transitions]"

        print(
            f"cycle {rec['cycle']:>3}: waste {waste_str}  call={call_ms:.1f}ms  "
            f"{phases_part}  "
            f"committed_drop={committed_drop:>+13,}B  freed={freed:>10,}B"
        )

    total_cpu_ms = sum(r.get("call_ms", 0.0) for r in records)
    first_ts = records[0].get("ts_ns")
    last_ts = records[-1].get("ts_ns")
    wall_ms = (last_ts - first_ts) / 1_000_000.0 if first_ts and last_ts else 0.0
    first_waste = (records[0].get("before") or {}).get("waste_pct")
    last_waste = (records[-1].get("after") or {}).get("waste_pct")
    waste_summary = (
        f"{first_waste:.2f}% -> {last_waste:.2f}%"
        if first_waste is not None and last_waste is not None
        else "n/a"
    )
    print(
        f"\ntotals: {len(records)} cycles  "
        f"defrag_cpu={total_cpu_ms:.1f}ms  wall={wall_ms / 1000.0:.2f}s  "
        f"waste {waste_summary}"
    )
    print("(phases.cpu = server CPU per phase; phases.wall = older log without cpu= field)")


async def main(args: argparse.Namespace) -> None:
    client = aioredis.Redis(host=args.host, port=args.port, db=0)

    out_dir = os.path.dirname(args.output)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    records: list[dict] = []
    last_waste: float | None = None
    stop_reason = f"reached --cycles={args.cycles}"
    with open(args.output, "w") as out_fh:
        for cycle in range(args.cycles):
            log_offset = log_size(args.log_path)
            ts_ns = time.time_ns()

            before = await get_arena(client)
            call_start_ns = time.monotonic_ns()
            reply = await run_defragment(client)
            call_ms = (time.monotonic_ns() - call_start_ns) / 1_000_000.0
            after = await get_arena(client)

            # Tiny pause so any tail-end log line lands before we read.
            await asyncio.sleep(0.05)
            log_text, _new_offset = read_log_delta(args.log_path, log_offset)
            shards = parse_log_delta(log_text)

            record = {
                "cycle": cycle,
                "ts_ns": ts_ns,
                "call_ms": call_ms,
                "before": before,
                "after": after,
                "defrag_reply": reply,
                "shards": [shards[k] for k in sorted(shards)],
            }
            out_fh.write(json.dumps(record) + "\n")
            out_fh.flush()
            records.append(record)

            committed_drop = (before["committed"] - after["committed"]) if before and after else 0
            n_shards = len(shards)
            total_freed = sum(s.get("bytes_freed", 0) for s in shards.values())
            print(
                f"cycle={cycle} shards_logged={n_shards} "
                f"freed={total_freed:,}B committed_drop={committed_drop:,}B"
            )

            current_waste = (after or {}).get("waste_pct")
            last_waste = current_waste if current_waste is not None else last_waste
            if (
                args.target_waste is not None
                and current_waste is not None
                and current_waste <= args.target_waste
            ):
                stop_reason = (
                    f"reached target waste {args.target_waste:.2f}% at "
                    f"{current_waste:.2f}% (cycle {cycle})"
                )
                break

            if cycle + 1 < args.cycles:
                await asyncio.sleep(args.sleep_ms / 1000.0)

    await client.aclose()
    if (
        stop_reason.startswith("reached --cycles=")
        and args.target_waste is not None
        and last_waste is not None
    ):
        stop_reason = (
            f"reached --cycles={args.cycles} (target {args.target_waste:.2f}% "
            f"not reached, final {last_waste:.2f}%)"
        )
    print(f"\nstopped: {stop_reason}")
    print_summary(records)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument(
        "--cycles", type=int, default=15, help="max cycles; loop also stops on --target-waste"
    )
    parser.add_argument(
        "--target-waste",
        type=float,
        default=None,
        help="stop when arena waste_pct drops to this value or below; unset = run until --cycles",
    )
    parser.add_argument("--sleep-ms", type=int, default=500)
    parser.add_argument("--log-path", default="/tmp/dragonfly.INFO")
    parser.add_argument("--output", default="runs/defrag_run.jsonl")
    asyncio.run(main(parser.parse_args()))
