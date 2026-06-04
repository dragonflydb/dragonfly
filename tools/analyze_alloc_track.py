#!/usr/bin/env python3
"""Analyze Dragonfly `MEMORY TRACK` output to find large-allocation hot spots.

Dragonfly's allocation tracker (enabled by the DF_ENABLE_MEMORY_TRACKING build
option, ON by default) logs a record per sampled allocation/deallocation into the
server INFO log:

    I0602 12:34:56.789012 4711 allocation_tracker.cc:88] Allocating 32768 bytes (0x7f...). Stack: 0x...  symbolA
    0x...  symbolB
    0x...  symbolC

    I0602 12:34:56.789020 4711 allocation_tracker.cc:108] Deallocating 32768 bytes (0x7f...)
    0x...  symbolA
    0x...  symbolB

This script groups those records by call stack to answer:
  * which call sites produce large allocations, and how many bytes/objects;
  * how long those allocations live (when dealloc records are present, i.e. the
    tracker was configured with a single band at sample_odds=1.0);
  * which allocations are still outstanding at the end of the capture
    ("live at end") -- the ones that actually pin RSS.

Usage:
    # Find the spots (one or more bands added via MEMORY TRACK ADD ...):
    python3 tools/analyze_alloc_track.py dragonfly.INFO

    # Restrict to a size window and show more groups:
    python3 tools/analyze_alloc_track.py dragonfly.INFO --min 30000 --max 40000 --top 20

    # Emit machine-readable output to store/diff later:
    python3 tools/analyze_alloc_track.py dragonfly.INFO --json report.json

The log can be passed as a path or on stdin ("-").
"""

import argparse
import json
import re
import sys
from collections import defaultdict

# Matches the first physical line of a tracker record. Handles both glog
# (Iyyyymmdd) and absl (Immdd) prefixes. Captures timestamp, op, size, ptr.
RECORD_RE = re.compile(
    r"^[IWEF](?P<date>\d{4,8})\s+(?P<time>\d{2}:\d{2}:\d{2}\.\d{6})\s+\S+\s+\S+\]\s+"
    r"(?P<op>Allocating|Deallocating)\s+(?P<size>\d+)\s+bytes\s+\(0x(?P<ptr>[0-9a-fA-F]+)\)"
    r"(?:\.\s+Stack:\s+(?P<frame0>.*))?$"
)

# A bare stack frame line emitted by helio's GetStacktrace(): "0xADDR  symbol".
FRAME_RE = re.compile(r"^0x[0-9a-fA-F]+\s+(?P<symbol>.+?)\s*$")

# Strip "+0x1234" offsets so the same function groups together.
OFFSET_RE = re.compile(r"\+0x[0-9a-fA-F]+$")


def to_seconds(date_str, time_str):
    """Build a comparable float timestamp. Exact for deltas within one day."""
    h, m, s = time_str.split(":")
    secs = int(h) * 3600 + int(m) * 60 + float(s)
    # date is mmdd (absl) or yyyymmdd (glog); fold the day in so ordering across
    # a midnight boundary still increases monotonically.
    mmdd = date_str[-4:]
    day = int(mmdd[:2]) * 31 + int(mmdd[2:])
    return day * 86400 + secs


def clean_symbol(sym):
    return OFFSET_RE.sub("", sym).strip()


def parse(stream):
    """Yield dicts: {op, size, ptr, ts, frames:[symbol,...]}."""
    cur = None
    for raw in stream:
        line = raw.rstrip("\n")
        m = RECORD_RE.match(line)
        if m:
            if cur:
                yield cur
            cur = {
                "op": m.group("op"),
                "size": int(m.group("size")),
                "ptr": m.group("ptr"),
                "ts": to_seconds(m.group("date"), m.group("time")),
                "frames": [],
            }
            f0 = m.group("frame0")
            if f0:
                fm = FRAME_RE.match(f0.strip())
                cur["frames"].append(clean_symbol(fm.group("symbol")) if fm else f0.strip())
            continue
        if cur is not None:
            fm = FRAME_RE.match(line)
            if fm:
                cur["frames"].append(clean_symbol(fm.group("symbol")))
            elif line.strip() == "":
                # blank line ends a record's stack but keep collecting until next record
                pass
            else:
                # a non-frame, non-record line: a new (untracked) log entry -> close record
                yield cur
                cur = None
    if cur:
        yield cur


def site_key(frames, depth):
    return tuple(frames[:depth])


def analyze(records, depth, min_size, max_size):
    sites = defaultdict(lambda: {"allocs": 0, "alloc_bytes": 0, "frames": None})
    # ptr -> list of (ts, size, site_key) for outstanding allocations (FIFO per ptr).
    live = defaultdict(list)
    lifetimes = []  # (lifetime_seconds, size, site_key)
    dealloc_unmatched = 0

    for r in records:
        if r["size"] < min_size or r["size"] > max_size:
            continue
        key = site_key(r["frames"], depth)
        if r["op"] == "Allocating":
            s = sites[key]
            s["allocs"] += 1
            s["alloc_bytes"] += r["size"]
            if s["frames"] is None:
                s["frames"] = r["frames"]
            live[r["ptr"]].append((r["ts"], r["size"], key))
        else:  # Deallocating
            q = live.get(r["ptr"])
            if q:
                ts0, size0, key0 = q.pop(0)
                lifetimes.append((max(0.0, r["ts"] - ts0), size0, key0))
            else:
                dealloc_unmatched += 1

    # Whatever is still in `live` was never freed within the capture window.
    live_at_end = defaultdict(lambda: {"count": 0, "bytes": 0})
    for q in live.values():
        for _ts, size, key in q:
            live_at_end[key]["count"] += 1
            live_at_end[key]["bytes"] += size

    return sites, lifetimes, live_at_end, dealloc_unmatched


def pct(values, p):
    if not values:
        return 0.0
    values = sorted(values)
    idx = min(len(values) - 1, int(round((p / 100.0) * (len(values) - 1))))
    return values[idx]


def human(n):
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(n) < 1024 or unit == "GiB":
            return f"{n:.1f}{unit}" if unit != "B" else f"{int(n)}B"
        n /= 1024
    return f"{n:.1f}GiB"


def load_sites(path, depth, min_size, max_size):
    """Parse one log and return ({site_key: {allocs, alloc_bytes, frames}}, record_count)."""
    stream = sys.stdin if path == "-" else open(path, "r", errors="replace")
    records = list(parse(stream))
    if path != "-":
        stream.close()
    sites, _lt, _live, _unm = analyze(records, depth, min_size, max_size)
    return sites, len(records)


def run_diff(path_a, path_b, args):
    """Show allocation sites present in A (e.g. absl) that are absent or much smaller in B (glog)."""
    la, lb = args.label_a, args.label_b
    sites_a, na = load_sites(path_a, args.depth, args.min_size, args.max_size)
    sites_b, nb = load_sites(path_b, args.depth, args.min_size, args.max_size)
    print(
        f"A = {la}: {path_a}  ({na} records, {sum(s['allocs'] for s in sites_a.values())} allocs in window)"
    )
    print(
        f"B = {lb}: {path_b}  ({nb} records, {sum(s['allocs'] for s in sites_b.values())} allocs in window)"
    )
    print()

    a_only, heavier = [], []
    for key, s in sites_a.items():
        b = sites_b.get(key)
        if b is None or b["allocs"] == 0:
            a_only.append((key, s))
        elif s["alloc_bytes"] >= 2 * b["alloc_bytes"]:
            heavier.append((key, s, b))
    a_only.sort(key=lambda kv: kv[1]["alloc_bytes"], reverse=True)
    heavier.sort(key=lambda t: t[1]["alloc_bytes"] - t[2]["alloc_bytes"], reverse=True)

    a_only_bytes = sum(s["alloc_bytes"] for _k, s in a_only)
    print(
        f"== Sites present in {la} but ABSENT in {lb}: {len(a_only)} sites, {human(a_only_bytes)} =="
    )
    print(f"   (the allocations {la} makes that {lb} does not -- the differential cost)")
    for i, (key, s) in enumerate(a_only[: args.top], 1):
        avg = s["alloc_bytes"] / s["allocs"] if s["allocs"] else 0
        print(f"\n#{i}  {s['allocs']} allocs  {human(s['alloc_bytes'])}  (avg {human(avg)})")
        for f in (s["frames"] or [])[: args.depth]:
            print(f"      {f}")

    if heavier:
        print(f"\n== Sites >=2x heavier in {la} than {lb} ==")
        for key, s, b in heavier[: args.top]:
            top = (s["frames"] or ["(no frames)"])[0]
            print(
                f"   {la}: {human(s['alloc_bytes'])} ({s['allocs']})  vs  "
                f"{lb}: {human(b['alloc_bytes'])} ({b['allocs']})  <- {top}"
            )

    if args.json:
        out = {
            "a": {"label": la, "path": path_a, "records": na},
            "b": {"label": lb, "path": path_b, "records": nb},
            "a_only": [
                {"allocs": s["allocs"], "alloc_bytes": s["alloc_bytes"], "frames": s["frames"]}
                for _k, s in a_only
            ],
        }
        with open(args.json, "w") as fh:
            json.dump(out, fh, indent=2)
        print(f"\nWrote {args.json}")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "logs",
        nargs="+",
        help="one INFO log for a report, or TWO logs (A B) for a diff (A=absl, B=glog). '-' = stdin",
    )
    ap.add_argument(
        "--depth", type=int, default=6, help="stack frames used to group call sites (default 6)"
    )
    ap.add_argument(
        "--min",
        type=int,
        default=0,
        dest="min_size",
        help="ignore allocations smaller than this (bytes)",
    )
    ap.add_argument(
        "--max",
        type=int,
        default=1 << 62,
        dest="max_size",
        help="ignore allocations larger than this (bytes)",
    )
    ap.add_argument("--top", type=int, default=15, help="how many call sites to print (default 15)")
    ap.add_argument(
        "--label-a", default="A", help="label for the first log in diff mode (e.g. absl)"
    )
    ap.add_argument(
        "--label-b", default="B", help="label for the second log in diff mode (e.g. glog)"
    )
    ap.add_argument("--json", metavar="FILE", help="also write a machine-readable report here")
    args = ap.parse_args()

    if len(args.logs) == 2:
        run_diff(args.logs[0], args.logs[1], args)
        return
    if len(args.logs) != 1:
        ap.error("provide 1 log (single report) or 2 logs (A B, for a diff)")

    stream = sys.stdin if args.logs[0] == "-" else open(args.logs[0], "r", errors="replace")
    records = list(parse(stream))
    if args.logs[0] != "-":
        stream.close()

    sites, lifetimes, live_at_end, dealloc_unmatched = analyze(
        records, args.depth, args.min_size, args.max_size
    )

    n_alloc = sum(s["allocs"] for s in sites.values())
    n_bytes = sum(s["alloc_bytes"] for s in sites.values())
    print(f"Parsed {len(records)} tracker records.")
    print(
        f"Allocations in [{args.min_size}, {args.max_size}] bytes: {n_alloc} totalling {human(n_bytes)}"
    )
    if dealloc_unmatched:
        print(f"(note: {dealloc_unmatched} deallocations had no matching alloc in the window)")
    print()

    print(f"== Top {args.top} allocation sites by total bytes (grouped by {args.depth} frames) ==")
    ranked = sorted(sites.values(), key=lambda s: s["alloc_bytes"], reverse=True)[: args.top]
    for i, s in enumerate(ranked, 1):
        avg = s["alloc_bytes"] / s["allocs"] if s["allocs"] else 0
        print(f"\n#{i}  {s['allocs']} allocs  {human(s['alloc_bytes'])}  (avg {human(avg)})")
        for f in (s["frames"] or [])[: args.depth]:
            print(f"      {f}")

    n_dealloc = sum(
        1
        for r in records
        if r["op"] == "Deallocating" and args.min_size <= r["size"] <= args.max_size
    )

    if n_dealloc == 0:
        print("\n== Liveness & lifetimes: not available in this capture ==")
        print("   No deallocation records were logged -- expected for sampled and/or multi-band")
        print("   tracking (deallocs are only logged with a SINGLE band at sample_odds=1.0).")
        print("   The sites above are allocations *seen over time*, NOT necessarily still live,")
        print("   so do not read them as 'pins RSS'. For true lifetimes + live-at-end, re-run with")
        print("   e.g.  --df allocation_tracker=30000:40000:1.0")
    else:
        secs = [lt for lt, _sz, _k in lifetimes]
        print("\n== Lifetimes ==")
        print(f"   matched alloc/dealloc pairs: {len(lifetimes)}  (deallocs seen: {n_dealloc})")
        for p in (50, 90, 99, 100):
            print(f"   p{p}: {pct(secs, p) * 1e6:.1f} us")
        if live_at_end:
            print("\n== Still live at end of capture (these pin RSS) ==")
            ranked_live = sorted(live_at_end.items(), key=lambda kv: kv[1]["bytes"], reverse=True)[
                : args.top
            ]
            for key, v in ranked_live:
                top = key[0] if key else "(no frames)"
                print(f"   {v['count']} live  {human(v['bytes'])}  <- {top}")
        else:
            print(
                "\n== Nothing live at end: every tracked allocation was freed (transient, not a leak). =="
            )

    if args.json:
        out = {
            "summary": {
                "records": len(records),
                "allocs": n_alloc,
                "alloc_bytes": n_bytes,
                "deallocs": n_dealloc,
            },
            "sites": [
                {"allocs": s["allocs"], "alloc_bytes": s["alloc_bytes"], "frames": s["frames"]}
                for s in sorted(sites.values(), key=lambda s: s["alloc_bytes"], reverse=True)
            ],
            "lifetimes_us": sorted(lt * 1e6 for lt, _s, _k in lifetimes),
            # Only meaningful when deallocations were logged (single band at odds 1.0).
            "live_at_end": (
                [
                    {"count": v["count"], "bytes": v["bytes"], "top_frame": (k[0] if k else None)}
                    for k, v in sorted(
                        live_at_end.items(), key=lambda kv: kv[1]["bytes"], reverse=True
                    )
                ]
                if n_dealloc
                else []
            ),
        }
        with open(args.json, "w") as fh:
            json.dump(out, fh, indent=2)
        print(f"\nWrote {args.json}")


if __name__ == "__main__":
    main()
