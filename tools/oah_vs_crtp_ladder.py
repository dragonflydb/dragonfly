#!/usr/bin/env python3
"""Builds the consolidated OAHSet vs CrtpDenseSet ladder comparison table.

Reads the two google-benchmark JSON files produced by oah_vs_crtp_ladder.sh
(string_set_test's ladder benchmarks, oah_set_test's OAHSet benchmarks) and
prints/saves a single report:
  - per op (Add/Get/Erase), per cell (elements x key size): every ladder
    rung's mean time, side by side with OAHSet's.
  - "OAH vs <rung>" columns: explicit "OAH faster by X%" / "OAH SLOWER by X%"
    for every rung, so the diminishing-returns progression (StringSet -> NoTag
    -> Crtp -> Blob -> Fused) is visible at a glance.
  - cv (coefficient of variation) flagged wherever any of the six values
    that feed a row exceeds a noise threshold.
  - a per-op summary averaging "OAH vs <rung>" across all 9 cells.
"""
import argparse
import json
import re
import sys
from collections import defaultdict

# Ladder rungs in increasing-optimization order. Maps rung label -> the
# BM_<Op> name suffix used in string_set_test.cc (None == no suffix, the
# StringSet baseline).
RUNGS = [
    ("StringSet (virtual)", None),
    ("NoTag (devirt only)", "_NoTag"),
    ("Crtp (devirt+tag)", "_Crtp"),
    ("Blob (devirt+tag+SSO)", "_Blob"),
    ("Fused (devirt+tag+SSO+fusion)", "_Fused"),
]

OPS = ["Add", "Get", "Erase"]
CV_NOISE_THRESHOLD = 3.0  # percent; flag any cell where a feeding value's cv exceeds this.

STRING_SET_NAME_RE = re.compile(
    r"^BM_(Add|Get|Erase)(_NoTag|_Crtp|_Blob|_Fused)?/elements:(\d+)/Key Size:(\d+)$"
)
OAH_NAME_RE = re.compile(r"^BM_(Add|Get|Erase)/elements:(\d+)/KeySize:(\d+)$")


def load_benchmarks(path):
    with open(path) as f:
        data = json.load(f)
    return data.get("benchmarks", [])


def parse_string_set(benchmarks):
    """Returns (means, cvs, mem_means, mem_cvs), each keyed by
    (op, suffix, elements, keysize). Memory_Used only exists on Add cells
    (Get/Erase don't track it) - mem dicts simply have no entries for those.
    """
    means, cvs, mem_means, mem_cvs = {}, {}, {}, {}
    for b in benchmarks:
        run_name = b.get("run_name", b.get("name", ""))
        m = STRING_SET_NAME_RE.match(run_name)
        if not m:
            continue
        # group(2) is None for the StringSet baseline (no suffix) - keep it as
        # None, matching RUNGS' baseline entry, instead of collapsing to "".
        op, suffix, elements, keysize = m.group(1), m.group(2), int(m.group(3)), int(m.group(4))
        key = (op, suffix, elements, keysize)
        agg = b.get("aggregate_name")
        if agg == "mean":
            means[key] = b["real_time"]
            if "Memory_Used" in b:
                mem_means[key] = b["Memory_Used"]
        elif agg == "cv":
            # google benchmark reports cv as a fraction (e.g. 0.0269), not pct.
            cvs[key] = b["real_time"] * 100.0
            if "Memory_Used" in b:
                mem_cvs[key] = b["Memory_Used"] * 100.0
    return means, cvs, mem_means, mem_cvs


def parse_oah(benchmarks):
    """Returns (means, cvs, mem_means, mem_cvs), each keyed by
    (op, elements, keysize)."""
    means, cvs, mem_means, mem_cvs = {}, {}, {}, {}
    for b in benchmarks:
        run_name = b.get("run_name", b.get("name", ""))
        m = OAH_NAME_RE.match(run_name)
        if not m:
            continue
        op, elements, keysize = m.group(1), int(m.group(2)), int(m.group(3))
        key = (op, elements, keysize)
        agg = b.get("aggregate_name")
        if agg == "mean":
            means[key] = b["real_time"]
            if "Memory_Used" in b:
                mem_means[key] = b["Memory_Used"]
        elif agg == "cv":
            cvs[key] = b["real_time"] * 100.0
            if "Memory_Used" in b:
                mem_cvs[key] = b["Memory_Used"] * 100.0
    return means, cvs, mem_means, mem_cvs


def pct_diff_label(oah_ns, other_ns):
    """OAH vs other: positive => OAH faster, negative => OAH SLOWER."""
    if other_ns is None or oah_ns is None or other_ns == 0:
        return "n/a"
    pct = (other_ns - oah_ns) / other_ns * 100.0
    if pct > 0:
        return f"OAH faster by {pct:.1f}%"
    elif pct < 0:
        return f"OAH SLOWER by {-pct:.1f}%"
    return "tie (0.0%)"


def fmt_ns(v):
    return f"{v:,.0f} ns" if v is not None else "MISSING"


def build_report(string_set_means, string_set_cvs, oah_means, oah_cvs):
    lines = []
    lines.append("=" * 100)
    lines.append("OAHSet vs CrtpDenseSet ladder comparison")
    lines.append("Ladder: StringSet (virtual dispatch, baseline) -> NoTag (devirt only) ->")
    lines.append("        Crtp (devirt + hash-tag cache) -> Blob (+ lean blob/SSO, no sds) ->")
    lines.append(
        "        Fused (+ single-pass FindOrEmptyAround Add(), mirrors OAHSet's ProbeWindow)"
    )
    lines.append("Compared against: OAHSet, tuned config (kShiftLog=2, load factor 1)")
    lines.append("=" * 100)

    cells = sorted({(e, k) for (_, _, e, k) in string_set_means.keys()})
    summary_pct = defaultdict(list)  # (op, rung_label) -> [pct, ...]

    for op in OPS:
        lines.append("")
        lines.append(f"--- {op} " + "-" * (96 - len(op)))
        header = ["elements", "keySize"] + [r[0] for r in RUNGS] + ["OAHSet"]
        lines.append(" | ".join(header))
        for elements, keysize in cells:
            row_vals = []
            noisy = False
            for _, suffix in RUNGS:
                key = (op, suffix, elements, keysize)
                mean = string_set_means.get(key)
                cv = string_set_cvs.get(key)
                if cv is not None and cv > CV_NOISE_THRESHOLD:
                    noisy = True
                row_vals.append(mean)
            oah_key = (op, elements, keysize)
            oah_mean = oah_means.get(oah_key)
            oah_cv = oah_cvs.get(oah_key)
            if oah_cv is not None and oah_cv > CV_NOISE_THRESHOLD:
                noisy = True
            row_vals.append(oah_mean)

            row_str = [str(elements), str(keysize)] + [fmt_ns(v) for v in row_vals]
            if noisy:
                row_str[-1] += " [NOISY]"
            lines.append(" | ".join(row_str))

            for (label, _), mean in zip(RUNGS, row_vals[:-1]):
                pct_label = pct_diff_label(oah_mean, mean)
                lines.append(f"    OAH vs {label}: {pct_label}")
                if mean is not None and oah_mean is not None and mean != 0:
                    # Positive => OAH faster, negative => OAH SLOWER (same sign
                    # convention as pct_diff_label above).
                    summary_pct[(op, label)].append((mean - oah_mean) / mean * 100.0)

        lines.append("")
        lines.append(f"  {op} summary (average across all {len(cells)} cells):")
        for label, _ in RUNGS:
            vals = summary_pct.get((op, label), [])
            if not vals:
                continue
            avg = sum(vals) / len(vals)
            verdict = (
                f"OAH faster by {avg:.1f}% on average"
                if avg > 0
                else (f"OAH SLOWER by {-avg:.1f}% on average" if avg < 0 else "tie on average")
            )
            lines.append(f"    OAH vs {label}: {verdict}")

    lines.append("")
    lines.append("=" * 100)
    lines.append("Diminishing-returns narrative: read each op's summary top to bottom.")
    lines.append("OAH's margin over StringSet (top rung) should be the largest; each")
    lines.append("subsequent rung removes one structural confounder (virtual dispatch,")
    lines.append("then missing hash-tag cache, then sds overhead, then the redundant")
    lines.append("double-scan in Add()) - the margin should shrink monotonically toward")
    lines.append("the bottom rung (Fused) if the original OAH-vs-StringSet gap was mostly")
    lines.append("implementation artifacts rather than open-addressing vs chaining itself.")
    lines.append("=" * 100)
    return "\n".join(lines)


def fmt_bytes(v):
    return f"{v:,.0f} B" if v is not None else "MISSING"


def build_memory_report(string_set_mem, string_set_mem_cvs, oah_mem, oah_mem_cvs):
    """Second array: same ladder/OAH comparison, but for Memory_Used instead
    of latency. Only Add tracks Memory_Used (see context.txt bug #4 - Get/
    Erase never did), so this table has a single op section, not three.
    """
    lines = []
    lines.append("=" * 100)
    lines.append("OAHSet vs CrtpDenseSet ladder comparison - MEMORY (Add only; Get/Erase")
    lines.append("never tracked Memory_Used in this benchmark suite, see context.txt bug #4)")
    lines.append("=" * 100)

    cells = sorted({(e, k) for (_, _, e, k) in string_set_mem.keys()})
    summary_pct = defaultdict(list)
    op = "Add"

    header = ["elements", "keySize"] + [r[0] for r in RUNGS] + ["OAHSet"]
    lines.append(" | ".join(header))
    for elements, keysize in cells:
        row_vals = []
        noisy = False
        for _, suffix in RUNGS:
            key = (op, suffix, elements, keysize)
            mem = string_set_mem.get(key)
            cv = string_set_mem_cvs.get(key)
            if cv is not None and cv > CV_NOISE_THRESHOLD:
                noisy = True
            row_vals.append(mem)
        oah_key = (op, elements, keysize)
        oah_mem_val = oah_mem.get(oah_key)
        row_vals.append(oah_mem_val)

        row_str = [str(elements), str(keysize)] + [fmt_bytes(v) for v in row_vals]
        if noisy:
            row_str[-1] += " [NOISY]"
        lines.append(" | ".join(row_str))

        for (label, _), mem in zip(RUNGS, row_vals[:-1]):
            pct_label = pct_diff_label(oah_mem_val, mem)
            # For memory, "OAH faster" reads oddly - relabel as smaller/larger.
            pct_label = pct_label.replace("faster", "smaller").replace("SLOWER", "LARGER")
            lines.append(f"    OAH vs {label}: {pct_label}")
            if mem is not None and oah_mem_val is not None and mem != 0:
                summary_pct[label].append((mem - oah_mem_val) / mem * 100.0)

    lines.append("")
    lines.append(f"  Memory summary (average across all {len(cells)} cells):")
    for label, _ in RUNGS:
        vals = summary_pct.get(label, [])
        if not vals:
            continue
        avg = sum(vals) / len(vals)
        if avg > 0:
            verdict = f"OAH smaller by {avg:.1f}% on average"
        elif avg < 0:
            verdict = f"OAH LARGER by {-avg:.1f}% on average"
        else:
            verdict = "tie on average"
        lines.append(f"    OAH vs {label}: {verdict}")
    lines.append("=" * 100)
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--string-set-json", required=True)
    ap.add_argument("--oah-json", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    string_set_means, string_set_cvs, string_set_mem, string_set_mem_cvs = parse_string_set(
        load_benchmarks(args.string_set_json)
    )
    oah_means, oah_cvs, oah_mem, oah_mem_cvs = parse_oah(load_benchmarks(args.oah_json))

    if not string_set_means:
        print(f"ERROR: no matching benchmarks parsed from {args.string_set_json}", file=sys.stderr)
        sys.exit(1)
    if not oah_means:
        print(f"ERROR: no matching benchmarks parsed from {args.oah_json}", file=sys.stderr)
        sys.exit(1)

    report = build_report(string_set_means, string_set_cvs, oah_means, oah_cvs)
    mem_report = ""
    if string_set_mem and oah_mem:
        mem_report = "\n\n" + build_memory_report(
            string_set_mem, string_set_mem_cvs, oah_mem, oah_mem_cvs
        )
    else:
        print(
            "WARNING: no Memory_Used counters found in one or both JSON files - "
            "skipping memory table",
            file=sys.stderr,
        )

    full_report = report + mem_report
    print(full_report)
    with open(args.out, "w") as f:
        f.write(full_report + "\n")


if __name__ == "__main__":
    main()
