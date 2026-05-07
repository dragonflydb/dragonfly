#!/usr/bin/env python3
"""Create an uneven memory-fragmentation baseline for defrag experiments.

This script only creates and deletes keys. It does not run MEMORY DEFRAGMENT and
does not wait for background defrag counters.
"""

import argparse
import asyncio
import random
from collections import Counter, defaultdict
from dataclasses import dataclass

import aioredis


@dataclass(frozen=True)
class WideBand:
    name: str
    value_size: int
    byte_share: float
    profile: str


WIDE_BANDS = [
    WideBand("tiny", 64, 0.02, "0.99:6,0.90:3,0.60:1"),
    WideBand("small", 128, 0.04, "0.98:4,0.80:3,0.35:2,0.08:1"),
    WideBand("med1", 256, 0.10, "0.98:3,0.65:2,0.25:3,0.04:2"),
    WideBand("med2", 512, 0.22, "0.95:3,0.55:3,0.15:3,0.03:1"),
    WideBand("large1", 1024, 0.28, "0.95:3,0.60:3,0.20:3,0.05:1"),
    WideBand("large2", 2048, 0.24, "0.90:4,0.45:3,0.12:2,0.03:1"),
    WideBand("huge", 4096, 0.10, "0.85:5,0.35:3,0.08:2"),
]


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


def format_bytes(size: int) -> str:
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024.0 or unit == "TiB":
            if unit == "B":
                return f"{int(value)}B"
            return f"{value:.1f}{unit}"
        value /= 1024.0

    raise AssertionError("unreachable")


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


async def delete_fragmented_chunks(
    connection: aioredis.Redis,
    *,
    rng: random.Random,
    prefix: str,
    keys_count: int,
    value_size: int,
    chunk_keys: int,
    profile_text: str,
    delete_batch: int,
    snapshot_every_chunks: int,
    snapshot_prefix: str,
    include_arena: bool,
    top_arena_bins: int,
) -> dict:
    profile = parse_profile(profile_text)
    deck = make_ratio_deck(profile, rng)

    chunks_by_ratio = Counter()
    deleted_by_ratio = defaultdict(int)
    live_by_ratio = defaultdict(int)
    total_deleted = 0
    chunks = 0

    for chunk_start in range(0, keys_count, chunk_keys):
        chunk_end = min(keys_count, chunk_start + chunk_keys)
        chunk_size = chunk_end - chunk_start

        if chunks and chunks % len(deck) == 0:
            deck = make_ratio_deck(profile, rng)

        live_ratio = deck[chunks % len(deck)]
        live_count = round(chunk_size * live_ratio)
        delete_count = chunk_size - live_count

        ids = list(range(chunk_start, chunk_end))
        delete_ids = rng.sample(ids, delete_count)
        delete_keys = [key_name(prefix, key_id) for key_id in delete_ids]

        deleted = await delete_batches(connection, delete_keys, delete_batch)
        live = chunk_size - deleted

        chunks_by_ratio[live_ratio] += 1
        deleted_by_ratio[live_ratio] += deleted
        live_by_ratio[live_ratio] += live
        total_deleted += deleted
        chunks += 1

        if snapshot_every_chunks and chunks % snapshot_every_chunks == 0:
            await snapshot(
                connection,
                f"{snapshot_prefix}_after_delete_chunk_{chunks}",
                include_arena,
                top_arena_bins,
            )

    return {
        "chunks": chunks,
        "chunk_keys": chunk_keys,
        "profile": profile_text,
        "created_keys": keys_count,
        "deleted_keys": total_deleted,
        "live_keys": keys_count - total_deleted,
        "value_size": value_size,
        "chunks_by_ratio": chunks_by_ratio,
        "deleted_by_ratio": deleted_by_ratio,
        "live_by_ratio": live_by_ratio,
    }


def print_fragmentation_summary(summary: dict, *, label: str, seed: int) -> None:
    print("\n=== planned_fragmentation ===")
    print(f"label={label}")
    print(f"chunks={summary['chunks']:,}")
    print(f"chunk_keys={summary['chunk_keys']:,}")
    print(f"seed={seed}")
    print(f"profile={summary['profile']}")
    print(f"value_size={summary['value_size']:,}")
    print(f"created_keys={summary['created_keys']:,}")
    print(f"deleted_keys={summary['deleted_keys']:,}")
    print(f"live_keys={summary['live_keys']:,}")
    print(f"estimated_created_value_bytes={summary['created_keys'] * summary['value_size']:,}")
    print(f"estimated_deleted_value_bytes={summary['deleted_keys'] * summary['value_size']:,}")
    print(f"estimated_live_value_bytes={summary['live_keys'] * summary['value_size']:,}")

    print("\nby_live_ratio:")
    for ratio in sorted(summary["chunks_by_ratio"].keys(), reverse=True):
        print(
            f"  live_ratio={ratio:.2f} chunks={summary['chunks_by_ratio'][ratio]:,} "
            f"live_keys={summary['live_by_ratio'][ratio]:,} "
            f"deleted_keys={summary['deleted_by_ratio'][ratio]:,}"
        )


def print_wide_distribution(
    summaries: list[tuple[WideBand, dict]], target_value_bytes: int
) -> None:
    total_created_keys = sum(summary["created_keys"] for _, summary in summaries)
    total_live_keys = sum(summary["live_keys"] for _, summary in summaries)
    total_deleted_keys = sum(summary["deleted_keys"] for _, summary in summaries)
    total_created_bytes = sum(
        summary["created_keys"] * summary["value_size"] for _, summary in summaries
    )
    total_live_bytes = sum(summary["live_keys"] * summary["value_size"] for _, summary in summaries)
    total_deleted_bytes = sum(
        summary["deleted_keys"] * summary["value_size"] for _, summary in summaries
    )

    print("\n=== wide_object_distribution ===")
    print(f"target_value_bytes={target_value_bytes:,} ({format_bytes(target_value_bytes)})")
    print(f"created_value_bytes={total_created_bytes:,} ({format_bytes(total_created_bytes)})")
    print(f"live_value_bytes={total_live_bytes:,} ({format_bytes(total_live_bytes)})")
    print(f"deleted_value_bytes={total_deleted_bytes:,} ({format_bytes(total_deleted_bytes)})")
    print()

    print(
        f"{'band':<8} {'size':>6} {'share':>7} {'keys_created':>13} "
        f"{'keys_live':>12} {'keys_deleted':>13} {'bytes_created':>13} "
        f"{'bytes_live':>10} {'bytes_deleted':>13} {'live%':>7} {'chunks':>7}"
    )

    for band, summary in summaries:
        created_keys = summary["created_keys"]
        live_keys = summary["live_keys"]
        deleted_keys = summary["deleted_keys"]
        value_size = summary["value_size"]
        created_bytes = created_keys * value_size
        live_bytes = live_keys * value_size
        deleted_bytes = deleted_keys * value_size
        live_ratio = live_keys / created_keys if created_keys else 0.0

        print(
            f"{band.name:<8} {value_size:>6,} {band.byte_share:>6.1%} "
            f"{created_keys:>13,} {live_keys:>12,} {deleted_keys:>13,} "
            f"{format_bytes(created_bytes):>13} {format_bytes(live_bytes):>10} "
            f"{format_bytes(deleted_bytes):>13} {live_ratio:>6.1%} "
            f"{summary['chunks']:>7,}"
        )

    total_live_ratio = total_live_keys / total_created_keys if total_created_keys else 0.0
    print(
        f"{'total':<8} {'-':>6} {'100.0%':>7} {total_created_keys:>12,} "
        f"{total_live_keys:>12,} {total_deleted_keys:>13,} "
        f"{format_bytes(total_created_bytes):>13} {format_bytes(total_live_bytes):>10} "
        f"{format_bytes(total_deleted_bytes):>13} {total_live_ratio:>6.1%} "
        f"{sum(summary['chunks'] for _, summary in summaries):>7,}"
    )


def print_wide_live_ratio_distribution(summaries: list[tuple[WideBand, dict]]) -> None:
    print("\n=== wide_live_ratio_distribution ===")
    print(
        f"{'band':<8} {'ratio':>7} {'chunks':>8} {'keys_total':>12} "
        f"{'keys_live':>12} {'keys_deleted':>13} {'bytes_deleted':>14}"
    )

    for band, summary in summaries:
        value_size = summary["value_size"]
        for ratio in sorted(summary["chunks_by_ratio"].keys(), reverse=True):
            live_keys = summary["live_by_ratio"][ratio]
            deleted_keys = summary["deleted_by_ratio"][ratio]
            keys = live_keys + deleted_keys
            deleted_bytes = deleted_keys * value_size

            print(
                f"{band.name:<8} {ratio:>6.1%} {summary['chunks_by_ratio'][ratio]:>8,} "
                f"{keys:>12,} {live_keys:>12,} {deleted_keys:>12,} "
                f"{format_bytes(deleted_bytes):>14}"
            )


async def create_uniform_fragmentation(
    connection: aioredis.Redis, args: argparse.Namespace
) -> None:
    rng = random.Random(args.seed)

    await flushdb(connection)
    await snapshot(connection, "before_populate", args.arena, args.top_arena_bins)
    await populate(connection, args.keys, args.key_name, args.value_size)
    await snapshot(connection, "after_populate", args.arena, args.top_arena_bins)

    summary = await delete_fragmented_chunks(
        connection,
        rng=rng,
        prefix=args.key_name,
        keys_count=args.keys,
        value_size=args.value_size,
        chunk_keys=args.chunk_keys,
        profile_text=args.profile,
        delete_batch=args.delete_batch,
        snapshot_every_chunks=args.snapshot_every_chunks,
        snapshot_prefix="uniform",
        include_arena=args.arena,
        top_arena_bins=args.top_arena_bins,
    )

    print_fragmentation_summary(summary, label="uniform", seed=args.seed)
    await snapshot(connection, "after_delete", args.arena, args.top_arena_bins)


def wide_chunk_keys(value_size: int) -> int:
    return max(32, round((256 * 1024) / value_size))


async def create_wide_fragmentation(connection: aioredis.Redis, args: argparse.Namespace) -> None:
    rng = random.Random(args.seed)
    target_value_bytes = args.keys * args.value_size

    await flushdb(connection)
    await snapshot(connection, "before_populate", args.arena, args.top_arena_bins)

    band_specs = []
    for band in WIDE_BANDS:
        keys_count = max(1, round((target_value_bytes * band.byte_share) / band.value_size))
        prefix = f"{args.key_name}:{band.name}"
        chunk_keys = wide_chunk_keys(band.value_size)
        band_specs.append((band, prefix, keys_count, chunk_keys))
        await populate(connection, keys_count, prefix, band.value_size)

    await snapshot(connection, "after_populate", args.arena, args.top_arena_bins)

    summaries = []
    for band, prefix, keys_count, chunk_keys in band_specs:
        summary = await delete_fragmented_chunks(
            connection,
            rng=rng,
            prefix=prefix,
            keys_count=keys_count,
            value_size=band.value_size,
            chunk_keys=chunk_keys,
            profile_text=band.profile,
            delete_batch=args.delete_batch,
            snapshot_every_chunks=args.snapshot_every_chunks,
            snapshot_prefix=band.name,
            include_arena=args.arena,
            top_arena_bins=args.top_arena_bins,
        )
        summaries.append((band, summary))

    print("\n=== wide_workload ===")
    print(f"seed={args.seed}")
    print(f"target_value_bytes={target_value_bytes:,}")
    print(f"bands={len(WIDE_BANDS)}")

    created_keys = sum(summary["created_keys"] for _, summary in summaries)
    deleted_keys = sum(summary["deleted_keys"] for _, summary in summaries)
    live_keys = sum(summary["live_keys"] for _, summary in summaries)
    deleted_value_bytes = sum(
        summary["deleted_keys"] * summary["value_size"] for _, summary in summaries
    )
    live_value_bytes = sum(summary["live_keys"] * summary["value_size"] for _, summary in summaries)

    print(f"created_keys={created_keys:,}")
    print(f"deleted_keys={deleted_keys:,}")
    print(f"live_keys={live_keys:,}")
    print(f"estimated_deleted_value_bytes={deleted_value_bytes:,}")
    print(f"estimated_live_value_bytes={live_value_bytes:,}")

    print_wide_distribution(summaries, target_value_bytes)
    print_wide_live_ratio_distribution(summaries)

    print("\nby_band:")
    for band, summary in summaries:
        print(
            f"  band={band.name} value_size={band.value_size:,} "
            f"byte_share={band.byte_share:.2f} chunk_keys={summary['chunk_keys']:,} "
            f"profile={band.profile} created={summary['created_keys']:,} "
            f"deleted={summary['deleted_keys']:,} live={summary['live_keys']:,}"
        )

    for band, summary in summaries:
        print_fragmentation_summary(summary, label=f"wide:{band.name}", seed=args.seed)

    await snapshot(connection, "after_delete", args.arena, args.top_arena_bins)


async def create_fragmentation(connection: aioredis.Redis, args: argparse.Namespace) -> None:
    if args.workload == "wide":
        await create_wide_fragmentation(connection, args)
    else:
        await create_uniform_fragmentation(connection, args)


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
    parser.add_argument(
        "--workload",
        choices=["uniform", "wide"],
        default="uniform",
        help="fragmentation workload preset",
    )

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
