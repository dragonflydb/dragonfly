**OAHSet article: measurements and reproduction**

The [article](../../oah_set_article.md) measures four string sets: OAHSet with its default
constants (16-slot probe windows, growth at two members per slot: OAHSet 16/2), OAHSet with
4-slot windows and growth at one member per slot (OAHSet 4/1), StringSet, and
`std::unordered_set<std::string>`. All code was measured at commit
`534e38340c3319fef9028d20bb00f97be65df0ab`. Five tools produced the data:

| Tool | Measures | Output |
| --- | --- | --- |
| [oah_set_bench](../../../src/core/oah_set_bench.cc), run by [run.py](run.py) | memory; single-member operations; building, loading, and walking whole sets | `raw.csv`, `summary.csv`, `environment.json` |
| unit-test benchmarks in `oah_set_test` and `string_set_test`, run by [run_unit_tests.py](run_unit_tests.py) | Add, Get, Erase, Grow, Shrink, Scan, Clear | `unit_tests.csv`, `unit_tests.json` |
| [walk.cc](walk.cc), run by [walk.py](walk.py) and [walk_counters.py](walk_counters.py) | iterator and Scan walks over 1,000,000 members, with hardware counters | `walk.csv`, `walk.json`, `walk_counters.csv` |
| Dragonfly and memtier_benchmark, run by [server_commands.py](server_commands.py) | whole-set commands and their stalls; server CPU time per `SISMEMBER` | `whole_set_commands.csv`, `command_overhead.csv`, `server_commands.json` |
| Dragonfly and perf, run by [save_counters.py](save_counters.py) | hardware counters while Dragonfly saves a set | `save_counters.csv`, `save_counters.json` |

[plot.py](plot.py) draws the figures, and [export_docx.py](export_docx.py) converts the article
into `docs/oah_set_article.docx`.

**Builds**

All binaries are Release builds with blaze.sh's default `-march=haswell -mtune=skylake`, so
OAHSet scans its probe windows and packs ASCII keys with AVX2 code. Packaged releases, built by
`make release`, target `-march=core2 -msse4.1 -mpopcnt` and use 128-bit SSE code for both, which
these measurements do not cover.

At this commit, the harness and the walk driver have no CMake targets. Add them to
`src/core/CMakeLists.txt`:

```cmake
add_executable(oah_set_bench EXCLUDE_FROM_ALL oah_set_bench.cc)
cxx_link(oah_set_bench dfly_core)
add_executable(oah_set_walk EXCLUDE_FROM_ALL ${CMAKE_SOURCE_DIR}/docs/benchmarks/oah_set/walk.cc)
cxx_link(oah_set_walk dfly_core)
```

The stock `BM_Grow` cases always use 262,144 members and also time the destruction of the grown
set; with an overload factor of 1, the OAHSet case stops after one insertion, before the table
grows. [bm_grow.patch](bm_grow.patch) makes both cases time exactly one doubling of a set of about
32,768, 262,144, or 2,097,152 members, for any constants:

```sh
git apply docs/benchmarks/oah_set/bm_grow.patch
```

OAHSet 4/1 comes from a copy of the tree, made after these changes, with two constants changed in
`src/core/oah_table.h`. Compile both trees with `-falign-functions=64`: code placement alone can
move nanosecond-scale results by 10–20% between binaries.

```sh
mkdir /tmp/oah-w4o1
tar -c --exclude='./build-*' --exclude=./.git . | tar -x -C /tmp/oah-w4o1
sed -i -e 's/kShiftLog = 4;/kShiftLog = 2;/' -e 's/kOverloadFactor = 2;/kOverloadFactor = 1;/' \
  /tmp/oah-w4o1/src/core/oah_table.h
OPTS="-DWITH_GCP=OFF -DWITH_AWS=OFF -DWITH_SEARCH=OFF -DWITH_GPERF=OFF -DWITH_SIMSIMD=OFF"
for tree in . /tmp/oah-w4o1; do
  (cd $tree && ./helio/blaze.sh -release -build-dir build-oah $OPTS \
    -DCMAKE_CXX_FLAGS=-falign-functions=64 &&
    ninja -C build-oah oah_set_bench oah_set_walk oah_set_test string_set_test &&
    ./helio/blaze.sh -release -build-dir build-server $OPTS && ninja -C build-server dragonfly)
done
```

The recorded set binaries took a shortcut: the set sources (OAHSet, StringSet, and the ASCII codec
they share with CompactObject), the two unit tests, the harness, and the driver were compiled with
an existing Release build's compile commands plus `-falign-functions=64`, then linked with that
build's other objects, none of which were out of date. The OAHSet 4/1 build put the modified
`oah_table.h` first on the include path. The two servers were built as above, from an export of
the commit; their code placement is not aligned, which does not matter at the scale of commands.

**Run**

From the repository root:

```sh
B=build-oah W=/tmp/oah-w4o1/build-oah OUT=/tmp/oah-set
python3 docs/benchmarks/oah_set/run.py --cpu 9 --output $OUT \
  --binary default=$B/oah_set_bench --binary w4o1=$W/oah_set_bench
python3 docs/benchmarks/oah_set/run_unit_tests.py --cpu 9 --output $OUT \
  --build $B --variant OAHSet_w4o1=$W
python3 docs/benchmarks/oah_set/walk.py --cpu 9 --output $OUT \
  --binary default=$B/oah_set_walk --binary w4o1=$W/oah_set_walk
python3 docs/benchmarks/oah_set/walk_counters.py --cpu 9 --output $OUT \
  --binary default=$B/oah_set_walk --binary w4o1=$W/oah_set_walk
python3 docs/benchmarks/oah_set/server_commands.py --cpu 9 --client-cpus 1-3 --repeats 9 \
  --output $OUT --dragonfly default=build-server/dragonfly \
  --dragonfly w4o1=/tmp/oah-w4o1/build-server/dragonfly
python3 docs/benchmarks/oah_set/save_counters.py --cpu 9 --output $OUT \
  --dragonfly build-server/dragonfly
```

`--cpu` pins every measured process to one CPU; `--client-cpus` should be other cores, ideally
in another core complex.

The six commands took about 32 minutes, 2 hours, 1 minute, 1.5 minutes, 13 minutes, and
half a minute. `run.py` reads `compile_commands.json` next to each binary to record how the
harness was compiled. `server_commands.py` needs memtier_benchmark; it and `save_counters.py`
start Dragonfly on port 16379. `walk_counters.py` and `save_counters.py` run `perf` through
`sudo -n` and count AMD Zen events. Figures and the Word document need a scratch environment:

```sh
python3 -m venv /tmp/oah-set-venv
/tmp/oah-set-venv/bin/pip install matplotlib==3.11.2 numpy==2.5.5 python-docx==1.2.0 \
  markdown-it-py==4.3.0
/tmp/oah-set-venv/bin/python docs/benchmarks/oah_set/plot.py --data $OUT/summary.csv \
  --unit $OUT/unit_tests.csv --server $OUT/whole_set_commands.csv --output $OUT
/tmp/oah-set-venv/bin/python docs/benchmarks/oah_set/export_docx.py
```

**Recorded environment**

The host had an AMD Ryzen AI Max+ 395 (Zen 5; two 8-core complexes with 32 MB of L3 each and 1 MB
of L2 per core), Linux 7.0, GCC 13.3, mimalloc 2.5.2, and memtier_benchmark 2.5.2. Every measured
process ran on CPU 9, with the `performance` governor and energy preference of the amd-pstate-epp
driver; boost stayed enabled. The scripts, the probe client, and memtier_benchmark ran on CPUs 1–3,
in the other core complex.

The host was shared. The runners record wall time, CPU time, and involuntary context switches for
every process they start; the median harness process ran on its CPU for 99.8% of its wall time.
The StringSet and `std::unordered_set` code is identical in both harness builds, so their two sets
of medians show how far a result moves without a code change. They agree within 7% for 1,000 and
1,000,000 members and for 8- and 256-byte keys. With 16- and 32-byte keys, insertion and iteration
agree within 4%, lookups within 12%, and erasure within 16%, while duplicate insertion differs by
up to 58%. With 100-byte keys, insertion, lookups, and erasure differ by up to 36%. A difference
between the two OAHSet builds means little unless it clearly exceeds these. Most of the spread
comes from separate processes, not rounds: each group pools seven rounds from each of only three
processes, and the heap layout differs between processes.

**Files**

- [environment.json](environment.json), [raw.csv](raw.csv), [summary.csv](summary.csv): the
  harness run. `environment.json` records the source revision and file hashes, the compile
  commands, the CPU, and every invocation with its resource usage. `raw.csv` holds 1,890 samples:
  15 scenarios × 2 builds × 3 containers × 7 rounds × 3 processes. `summary.csv` has the median,
  quartiles, and extremes of each of the 90 groups.
- [unit_tests.csv](unit_tests.csv), [unit_tests.json](unit_tests.json): every unit-test benchmark
  sample, with the binaries' hashes and Google Benchmark's context.
- [walk.csv](walk.csv), [walk.json](walk.json): every walk sample and invocation;
  [walk_counters.csv](walk_counters.csv): every hardware-counter sample, per walked member.
- [whole_set_commands.csv](whole_set_commands.csv), [command_overhead.csv](command_overhead.csv),
  [server_commands.json](server_commands.json): the server run, with the binaries' hashes, the
  server flags, and the memtier_benchmark invocations.
- [save_counters.csv](save_counters.csv), [save_counters.json](save_counters.json): every `SAVE`
  counter sample, per saved member, with the binary's hash and the event groups.
- [prefetch_walk.csv](prefetch_walk.csv), [prefetch_harness.csv](prefetch_harness.csv): every
  sample of the prefetch A/B. `prefetch` names the variant, and `run` numbers the pairs.
- [bm_grow.patch](bm_grow.patch): the `BM_Grow` change described under Builds.
- [memory.png](memory.png), [performance.png](performance.png), and
  [operations.png](operations.png), each with an SVG version: the article's figures. In the
  performance figure, whiskers show the interquartile range of the 21 rounds.
- [layout.mmd](layout.mmd) and [layout.png](layout.png): the memory-layout diagram, rendered by
  mermaid-cli as `mmdc -i layout.mmd -o layout.png -s 2 --size 1400 -b white`.

**Harness workload and timing**

The corpus uses seed `20261116`. Its first eight characters encode an invertible 48-bit ID
permutation, which guarantees distinct members and disjoint missing keys. The remaining characters
come from a seeded PRNG. ASCII keys use a 64-character alphabet; binary keys have the high bit set
in their first byte and arbitrary bytes after it, so OAHSet stores them raw.

The 15 scenarios cover 1,000 members with 32-byte ASCII keys; 100,000 members with ASCII and
binary keys of 8, 16, 32, 100, and 256 bytes; 1,000,000 members with 32-byte ASCII and binary
keys; and 100,000 and 1,000,000 members with 32-byte ASCII keys inserted into reserved sets.
`run.py` runs the whole matrix three times and alternates the two builds within every scenario.

Each process runs one unreported warm-up round and seven measured rounds, rotating the order of
the three containers. All containers receive the same strings in the same order; the strings live
outside the containers, and every insertion copies its key. A separate shuffled order drives
lookups, duplicate insertion, and erasure. A timing is the elapsed time of a whole batch divided by
its operations, so the results describe throughput, not single-operation latency.

- Insertion starts from an empty set and includes every resize. The reserved scenarios call
  `Reserve(count)` or `reserve(count)` first, as RDB loading does for a saved set.
- Lookup hits and misses each run five passes over their own query stream and check every result.
- Duplicate insertion tries to insert every member once more; nothing may be added.
- Iteration runs five full walks and decodes OAHSet's packed keys, as RDB saving and `SMEMBERS` do.
  Each visit adds the key's length and first byte to a checked checksum.
- Erasure removes every member and includes OAHSet's automatic shrinking.

The harness also checks final cardinality, operation totals, and that destroying a container
returns every tracked allocation. Expiry and batched insertion are not part of the workload.

**Memory accounting**

Memory per member is `(heap_bytes + object_bytes) / count`, sampled after unique insertion. For
OAHSet and StringSet, the harness adds `MiMemoryResource::used()` to the change in
`zmalloc_used_memory_tl`, which together cover the bucket array, links or overflow arrays, and key
allocations with their size-class rounding. `std::unordered_set` allocates its nodes and buckets
through the same resource; its heap-backed string buffers are counted with `mi_usable_size`, while
short strings live inside the nodes. `structure_bytes` and `key_bytes` split the heap total into
index and keys. The container objects take 56 bytes for OAHSet, 64 for StringSet, and 56 for
`std::unordered_set`.

The totals exclude input and query storage, allocator metadata, retained free pages, and the
transient peak while a set resizes; they are live allocations, not RSS.

**Unit-test benchmarks**

`run_unit_tests.py` runs the `BM_` cases of `oah_set_test` and `string_set_test` that match
`BM_(Add|Get|Erase|Grow|Shrink|Scan|Clear)/`. Each case reports one iteration's real time; the
CSV also divides it by the member count. Three passes over the binaries, with three repetitions
each, give nine samples per case. Keys are random strings over 36 lowercase letters and digits, so
OAHSet packs keys of 10 to 128 bytes and stores 1,000-byte keys raw.

- `BM_Add` inserts all members into a set reserved for them, as RDB loading does.
- `BM_Get` looks up every member; `BM_Erase` removes every member, including OAHSet's shrinking.
- `BM_Grow`, patched, fills a set to its growth threshold and times the insertions up to one
  doubling, which rehashes every member. It uses 16-byte keys.
- `BM_Shrink` reserves four times the needed capacity and times the shrink to the member count
  rounded up to a power of two. OAHSet 16/2 starts from half as many buckets as OAHSet 4/1 and
  StringSet, so it halves its table while they quarter theirs.
- `BM_Scan` walks the whole set with Scan cursors and reads each key's size.
- `BM_Clear` times `Clear()` of a full set, which frees every member.

**Walks**

`walk.cc` builds one set of 1,000,000 harness keys without reservation and times full walks over
it, either with iterators or with Scan cursors; `walk.py` runs five walks per process and five
processes per case. [walk_counters.py](walk_counters.py) explains the results with hardware
counters: it runs the driver under `perf stat` with 20 walks and with none, and divides the
difference by the 20 million members walked. Medians of three runs per case, with ASCII keys
unless noted:

| Per walked member | Cycles | Instructions | IPC | Misses in flight | DRAM fills | Mispredictions |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Iterator, OAHSet 16/2 | 79.7 | 113.6 | 1.43 | 2.44 | 0.264 | 0.067 |
| Iterator, OAHSet 16/2, binary keys | 75.8 | 66.7 | 0.88 | 4.64 | 0.598 | 0.034 |
| Iterator, OAHSet 4/1 | 93.0 | 117.7 | 1.27 | 1.74 | 0.344 | 0.385 |
| Iterator, StringSet | 63.3 | 54.0 | 0.85 | 4.63 | 0.583 | 0.538 |
| Iterator, StringSet, binary keys | 62.1 | 54.0 | 0.87 | 5.79 | 0.561 | 0.537 |
| Scan, OAHSet 16/2 | 130.5 | 275.8 | 2.11 | 1.45 | 0.122 | 1.279 |
| Scan, OAHSet 4/1 | 129.3 | 198.9 | 1.54 | 1.06 | 0.366 | 0.985 |
| Scan, StringSet | 276.8 | 311.8 | 1.13 | 1.25 | 0.780 | 0.690 |

`ls_alloc_mab_count` adds up the L1 data-cache misses in flight in every cycle, so dividing it by
the cycles gives the average number of misses the core overlaps. DRAM fills count
`ls_any_fills_from_sys.dram_io_all`, and mispredictions `ex_ret_brn_misp`. Unpacking ASCII keys
adds about 47 instructions per member to OAHSet's iterator walk, and the core then overlaps about
2.4 misses instead of 4.6. StringSet's loop stays short, the core overlaps 4.6 of its misses, and
StringSet walks faster. `SAVE` does far more work per member, which changes the outcome; see
SAVE counters.

**Prefetch A/B**

OAHSet's `Add` and `Scan` request memory with `oah::PrefetchRead` before they need it. To measure
what that buys, a third tree compiles the prefetches out, and its harness and walk driver run
against the default tree's:

```sh
mkdir /tmp/oah-nopf
tar -c --exclude='./build-*' --exclude=./.git . | tar -x -C /tmp/oah-nopf
sed -i 's/__builtin_prefetch(ptr, 0, 1);/(void)ptr;/' /tmp/oah-nopf/src/core/oah_base.h
(cd /tmp/oah-nopf && ./helio/blaze.sh -release -build-dir build-oah $OPTS \
  -DCMAKE_CXX_FLAGS=-falign-functions=64 && ninja -C build-oah oah_set_bench oah_set_walk)
```

The two variants ran in pairs of processes, back to back on CPU 9, alternating which went first,
with the arguments of `walk.py` and `run.py`. The walk driver ran 12 pairs for each of four walks
over 1,000,000 members: OAHSet's Scan walk with ASCII and with binary keys, and two controls whose
code the change leaves alone, OAHSet's iterator walk and StringSet's Scan walk. The harness ran
three pairs for each of three scenarios with 32-byte ASCII keys: 1,000,000 members, growing and
reserved, and 100,000 growing members.

Without prefetches, OAHSet's Scan walk took 17.5% longer with ASCII keys and 11% longer with
binary keys, as medians of the paired ratios, and it was slower in all 12 pairs of both. The
controls moved by 1.5% and 3%, and each was slower without prefetches in only 7 of 12 pairs.
OAHSet's insertion took 0.4–2.4% longer, within the −5.2% to +6.3% that the medians of the
unchanged StringSet and `std::unordered_set` code moved.

The recorded variant took a shortcut like the other set binaries: both variants linked one set of
set objects, and only the harness and the driver were compiled twice, once with the modified
header first on the include path. `Add` and `Scan` are inline, so they lost their prefetches; the
only OAHSet code that kept them is `OAHSet::Fill`, which copies a set and which neither binary
calls.

The A/B ran a day after the other measurements, on the same CPU with the same settings. Its
insertion times stayed within 2% of the main run's, but its walks took 8–33% longer: OAHSet's
ASCII Scan walk took 63.0 ns per member instead of 47.4. Only paired ratios are reported.

**Server commands**

`server_commands.py` starts every binary with one proactor thread on the measured CPU and
`--use_oah_set=true`, then the first binary again with `--use_oah_set=false` for StringSet. Each
of the nine repeats runs the three servers in turn, and every server runs two phases. The article
reports medians of the nine repeats.

The whole-set phase builds a set of 1,000,000 random 32-byte ASCII members with pipelined `SADD`
batches of 1,000; memory per member is the growth of `used_memory`. With
`--slowlog_log_slower_than=1000`, the slow log keeps every batch that ran for 1 ms or longer: the
batches that grew the table. A client walks the set with `SSCAN ... COUNT 1000`, and `SAVE` runs
three times. `DEL` and `DFLY LOAD` of the snapshot then alternate three times, so the first `DEL`
frees the set that `SADD` built and the other two free loaded sets. The client walks the last
loaded set with `SSCAN`, and `UNLINK` frees it in steps between other work, its row lasting until
`used_memory` returns to the empty level, polled every 2 ms. Throughout, a probe connection sends
`PING` about every 0.15 ms, and `max_wait_ms` is the slowest reply during the row: how long any
other client would have waited. `wall_ms` is the time the client waited for the command, and
`server_cpu_ms` is the CPU time of the server's threads, read from `/proc/PID/task/*/schedstat`.
INFO's `used_cpu_*` fields do not work for this: at this commit they print microseconds without
leading zeros, so 1 s and 5 µs reads as 1.5 s.

The CSV calls both walks `sscan` and every deletion `del`; in each server, the first of each works
on the set that `SADD` built. Built and loaded sets differ in where their keys live. `SADD`
allocates keys in the order the members arrive, which is random with respect to the table.
Loading allocates them in the order the snapshot lists them, which is the table's order, so a walk
or a `DEL` over a loaded set reads memory almost sequentially. In a separate check, re-adding a
built set's members in `SSCAN` order, without saving or loading, made `SSCAN` as cheap as on a
loaded set, so the order is what matters. A long-running server holds sets like the built one; a
restarted server or a replica after a full sync holds loaded sets.

The single-member phase builds a new set of 1,000,000 32-byte members with `SADD`, and
memtier_benchmark sends `SISMEMBER` for random members from three threads on `--client-cpus`: 60
connections at pipeline depth 1, then 6 at depth 32, for 10 seconds each. CPU time per command is
the server's CPU time over the commands it processed, from `INFO STATS`. Both loads keep the server
thread busy all the time, which matters because Helio's proactor spins for up to 20 µs before it
sleeps: at lower load, polling would be charged to the commands. `server_busy` in the CSV is
server CPU time over wall time.

**SAVE counters**

`save_counters.py` explains the `SAVE` results. For OAHSet 16/2 and StringSet, it starts the
default server on the measured CPU, builds the same set as `server_commands.py`, and saves it once
to warm up. Then `perf stat` counts each of two event groups over three more `SAVE` commands,
enabled through perf's control FIFO only while they run, and the counts are divided by the
3,000,000 members saved. Each of the five repeats starts new servers. Medians, with IPC and misses
in flight computed from the medians as for the walks:

| Per saved member | OAHSet 16/2 | StringSet |
| --- | ---: | ---: |
| Cycles | 344.4 | 412.8 |
| Instructions | 589.2 | 513.9 |
| IPC | 1.71 | 1.25 |
| Misses in flight | 1.33 | 1.72 |
| L1 demand fills | 6.61 | 7.25 |
| DRAM fills | 0.573 | 1.234 |
| Hardware prefetch fills | 2.23 | 2.17 |
| Mispredictions | 0.512 | 0.923 |
| L1 DTLB misses | 1.03 | 0.72 |

The cycles come from the first group; those of the second group, which the misses-in-flight ratio
uses, agree within 1.4%. L1 demand fills count `ls_dmnd_fills_from_sys.all`: demand misses of the
L1 data cache, wherever they were served. Hardware prefetch fills count `ls_hw_pf_dc_fills.all`,
and DTLB misses `ls_l1_d_tlb_miss.all`. Instructions, mispredictions, L1 demand fills, prefetch
fills, and DTLB misses varied by at most 5% between repeats, largest over smallest; cycles, misses
in flight, and DRAM fills, which depend on what the rest of the shared host did, by up to 34%. In
every repeat, OAHSet 16/2 needed 10–24% fewer cycles than StringSet.

`SAVE` executes about 470 more instructions per member than an iterator walk, so only one or two
misses overlap in either set, and the number of stalls decides: OAHSet 16/2 mispredicts 45% fewer
branches and takes 54% fewer fills from DRAM.

**Validation**

With the default constants, all 52 OAHSet and 28 StringSet unit tests pass. The OAHSet 4/1 build
fails three tests that expect the default constants' bucket counts:
`OAHSetTest.ShrinkToMinimumBuckets`, `OAHSetTest.OAHSetAddFindTest`, and
`ShrinkSizes/ShrinkTest.BasicShrink/buckets_524288`. The harness and the walk driver check every
result and checksum, and every harness sample passed.

The [OAHSet design](../../oah_set.md) describes the implementation independently of these
measurements.
