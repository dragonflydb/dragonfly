#!/usr/bin/env python3
"""Create an uneven memory-fragmentation baseline for defrag experiments.

This script only creates and deletes keys. It does not run MEMORY DEFRAGMENT and
does not wait for background defrag counters.
"""

import argparse
import asyncio
import random
from collections import Counter, defaultdict

import aioredis


def parse_profile(profile: str) -> list[tuple[float, int]]:
    """Parse "live_ratio:weight,..." into weighted live-ratio entries."""
    entries = []
    for raw_part in profile.split(","):
        part = raw_part.strip()
        if not part:
            continue
        ratio_text, weight_text = part.split(":", 1)
        ratio = float(ratio_text)
        weight = int(weight_text)
        if not 0.0 <= ratio <= 1.0:
            raise ValueError(f"live ratio must be in [0, 1]: {ratio}")
        if weight <= 0:
            raise ValueError(f"profile weight must be positive: {weight}")
        entries.append((ratio, weight))

    if not entries:
        raise ValueError("profile must contain at least one live_ratio:weight entry")
    return entries


def make_ratio_deck(profile: list[tuple[float, int]], rng: random.Random) -> list[float]:
    deck = []
    for live_ratio, weight in profile:
        deck.extend([live_ratio] * weight)
    rng.shuffle(deck)
    return deck


def key_name(prefix: str, key_id: int) -> str:
    return f"{prefix}:{key_id}"


def parse_arena_summary(report: str) -> tuple[str | None, list[tuple[int, int, int, int, float]]]:
    """Return the machine-wide Total line and top-level bin rows from MEMORY ARENA SUMMARY."""
    lines = report.splitlines()
    machine_start = 0
    for index, line in enumerate(lines):
        if "Arena statistics for machine" in line:
            machine_start = index

    machine_lines = lines[machine_start:]

    total_line = None
    rows = []
    for line in machine_lines:
        parts = line.replace("%", "").split()
        if not parts:
            continue
        if parts[0] == "Total:" and len(parts) >= 6:
            total_line = line
            continue
        if len(parts) != 6 or not parts[0].isdigit():
            continue

        block_size, _reserved, committed, used, wasted, waste_pct = parts
        rows.append((int(wasted), int(block_size), int(committed), int(used), float(waste_pct)))

    rows.sort(reverse=True)
    return total_line, rows


def print_arena_summary(report: str, top_bins: int) -> None:
    total_line, rows = parse_arena_summary(report)

    if total_line:
        parts = total_line.replace("%", "").split()
        print(
            "arena_total "
            f"reserved={int(parts[1]):,} "
            f"committed={int(parts[2]):,} "
            f"used={int(parts[3]):,} "
            f"wasted={int(parts[4]):,} "
            f"waste_pct={float(parts[5]):.2f}%"
        )
    else:
        print("arena_total unavailable")

    if top_bins <= 0:
        return

    print("top_waste_bins:")
    for wasted, block_size, committed, used, waste_pct in rows[:top_bins]:
        print(
            f"  block={block_size} committed={committed:,} used={used:,} "
            f"wasted={wasted:,} waste_pct={waste_pct:.2f}%"
        )


async def snapshot(
    connection: aioredis.Redis, label: str, include_arena: bool, top_bins: int
) -> None:
    print(f"\n=== {label} ===")

    info = await connection.execute_command("INFO", "memory")
    print(f"used_memory={int(info.get('used_memory', 0)):,}")
    print(f"used_memory_rss={int(info.get('used_memory_rss', 0)):,}")
    print(f"object_used_memory={int(info.get('object_used_memory', 0)):,}")
    print(f"table_used_memory={int(info.get('table_used_memory', 0)):,}")

    if include_arena:
        report = await connection.execute_command("MEMORY", "ARENA", "SUMMARY")
        print_arena_summary(report, top_bins)


async def populate(
    connection: aioredis.Redis, keys_count: int, prefix: str, value_size: int
) -> None:
    print(f"creating {keys_count:,} keys prefix={prefix!r} value_size={value_size:,}")
    await connection.execute_command("DEBUG", "POPULATE", keys_count, prefix, value_size)


async def flushdb(connection: aioredis.Redis) -> None:
    print("flushing current database")
    await connection.execute_command("FLUSHDB")


async def delete_batches(connection: aioredis.Redis, keys: list[str], batch_size: int) -> int:
    deleted = 0
    for start in range(0, len(keys), batch_size):
        deleted += await connection.delete(*keys[start : start + batch_size])
    return deleted


async def create_fragmentation(connection: aioredis.Redis, args: argparse.Namespace) -> None:
    rng = random.Random(args.seed)
    profile = parse_profile(args.profile)
    deck = make_ratio_deck(profile, rng)

    await flushdb(connection)
    await snapshot(connection, "before_populate", args.arena, args.top_arena_bins)
    await populate(connection, args.keys, args.key_name, args.value_size)
    await snapshot(connection, "after_populate", args.arena, args.top_arena_bins)

    chunks_by_ratio = Counter()
    deleted_by_ratio = defaultdict(int)
    live_by_ratio = defaultdict(int)
    total_deleted = 0
    chunks = 0

    for chunk_start in range(0, args.keys, args.chunk_keys):
        chunk_end = min(args.keys, chunk_start + args.chunk_keys)
        chunk_size = chunk_end - chunk_start

        if chunks and chunks % len(deck) == 0:
            deck = make_ratio_deck(profile, rng)

        live_ratio = deck[chunks % len(deck)]
        live_count = round(chunk_size * live_ratio)
        delete_count = chunk_size - live_count

        ids = list(range(chunk_start, chunk_end))
        delete_ids = rng.sample(ids, delete_count)
        delete_keys = [key_name(args.key_name, key_id) for key_id in delete_ids]

        deleted = await delete_batches(connection, delete_keys, args.delete_batch)
        live = chunk_size - deleted

        chunks_by_ratio[live_ratio] += 1
        deleted_by_ratio[live_ratio] += deleted
        live_by_ratio[live_ratio] += live
        total_deleted += deleted
        chunks += 1

        if args.snapshot_every_chunks and chunks % args.snapshot_every_chunks == 0:
            await snapshot(
                connection, f"after_delete_chunk_{chunks}", args.arena, args.top_arena_bins
            )

    print("\n=== planned_fragmentation ===")
    print(f"chunks={chunks:,}")
    print(f"chunk_keys={args.chunk_keys:,}")
    print(f"seed={args.seed}")
    print(f"profile={args.profile}")
    print(f"created_keys={args.keys:,}")
    print(f"deleted_keys={total_deleted:,}")
    print(f"live_keys={args.keys - total_deleted:,}")
    print(f"estimated_deleted_value_bytes={total_deleted * args.value_size:,}")
    print(f"estimated_live_value_bytes={(args.keys - total_deleted) * args.value_size:,}")

    print("\nby_live_ratio:")
    for ratio in sorted(chunks_by_ratio.keys(), reverse=True):
        print(
            f"  live_ratio={ratio:.2f} chunks={chunks_by_ratio[ratio]:,} "
            f"live_keys={live_by_ratio[ratio]:,} deleted_keys={deleted_by_ratio[ratio]:,}"
        )

    await snapshot(connection, "after_delete", args.arena, args.top_arena_bins)


async def main(args: argparse.Namespace) -> None:
    pool = aioredis.ConnectionPool(
        host=args.server,
        port=args.port,
        db=0,
        decode_responses=True,
        max_connections=args.max_connections,
    )
    connection = aioredis.Redis(connection_pool=pool)
    await create_fragmentation(connection, args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create an uneven fragmentation baseline.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-k", "--keys", type=int, default=800_000, help="number of keys to create")
    parser.add_argument(
        "--mul",
        type=float,
        default=1.0,
        help="scale --keys by this multiplier (e.g. --mul 5 -> 5x keys)",
    )
    parser.add_argument(
        "-v", "--value-size", type=int, default=645, help="value size for DEBUG POPULATE"
    )
    parser.add_argument("-n", "--key-name", default="key-for-testing", help="base key name")
    parser.add_argument("-s", "--server", default="localhost", help="server host")
    parser.add_argument("-p", "--port", type=int, default=6379, help="server port")

    parser.add_argument("--seed", type=int, default=12345, help="deterministic deletion seed")
    parser.add_argument(
        "--chunk-keys",
        type=int,
        default=512,
        help="contiguous key-id region size assigned one live ratio",
    )
    parser.add_argument("--delete-batch", type=int, default=1000, help="DEL batch size")
    parser.add_argument(
        "--max-connections", type=int, default=16, help="redis connection pool size"
    )
    parser.add_argument(
        "--profile",
        default="0.95:2,0.80:2,0.60:3,0.30:2,0.10:1",
        help="comma-separated live_ratio:weight entries",
    )
    parser.add_argument(
        "--arena", action="store_true", help="include MEMORY ARENA SUMMARY snapshots"
    )
    parser.add_argument(
        "--top-arena-bins",
        type=int,
        default=5,
        help="number of machine-wide arena waste bins to print",
    )
    parser.add_argument(
        "--snapshot-every-chunks",
        type=int,
        default=0,
        help="print snapshots every N deleted chunks; 0 disables intermediate snapshots",
    )

    args = parser.parse_args()
    if args.mul != 1.0:
        args.keys = int(args.keys * args.mul)
    asyncio.run(main(args))
