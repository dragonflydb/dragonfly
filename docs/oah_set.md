# OAHSet design

`OAHSet` is an open-addressing hash set of strings with an optional expiry per member. Dragonfly
stores every SET value that is not an intset in it; `--use_oah_set=false` selects `StringSet`
instead.

## Memory layout

The set is a flat array of 64-bit slots that point to separately allocated keys. Example with
`B = 32` home buckets:

```text
              |<--- probe window of home bucket 5 --->|
slot     0..4     5       6     7..14     15    16..20  21..31  32..46
      +-------+-------+-------+-------+-------+-------+-------+-------+
      |  ...  |   A   |   0   |  ...  |   V   |  ...  |  ...  |  ...  |
      +-------+---+---+-------+-------+---+---+-------+-------+-------+
                  |                       |                    15 extra slots
                  v                       v
         +--------+-------+             +---+---+---+---+  overflow array shared
         | header | key A |             | B | 0 | C | 0 |  by home buckets 0..15
         +--------+-------+             +-+-+---+-+-+---+
                                          |       |
                                          v       v
                    +--------+--------+-------+  +--------+-------+
                    | expiry | header | key B |  | header | key C |
                    +--------+--------+-------+  +--------+-------+
```

A slot is zero (empty), points to a key allocation (an *entry*, like `A`), or points to an overflow
array (a *vector*, like `V`). A member with home bucket `b` lives in its probe window, slots `b` to
`b + 15`, or in the overflow array at slot `b | 15`: home buckets 0–15 share slot 15, buckets
16–31 share slot 31, and so on. `B` is a power of two; the 15 extra slots let the last windows
extend past `B` without wrapping. Overflow arrays can contain empty slots.

A key allocation holds an optional 4-byte expiry, a 1-, 2-, or 4-byte header with the key length
and a packing flag, and the key bytes. 7-bit ASCII keys of 8–128 bytes are packed to 7 bits per
character; other keys are stored raw.

## Slot encoding

Allocations are 8-byte aligned and user-space addresses fit in 52 bits, which leaves 15 bits of
each slot for metadata:

```text
entry word
 63           52 51                                     3  2   1   0
+---------------+----------------------------------------+---+---+---+
|  fingerprint  |         key allocation address         | 0 | T | 0 |
+---------------+----------------------------------------+---+---+---+

vector word
 63 62        52 51                                     3  2   1   0
+--+------------+----------------------------------------+---+---+---+
|L | slot count |         overflow array address         | 0 | 0 | 1 |
+--+------------+----------------------------------------+---+---+---+
```

- Bit 0 tells entries and vectors apart. `T` marks a key allocation that starts with an expiry.
- The fingerprint caches 12 hash bits, so lookups skip most non-matching keys without reading
  them. Its top bits repeat the low bits of the home bucket number, so an entry's home bucket
  follows from its slot and fingerprint.
- From 128 slots on, `L` is set and the slot count is stored as its log2.

## Lookup

1. Encode and hash the query. The top `log2(B)` hash bits are its home bucket `b`.
2. Use SIMD to compare its fingerprint with the entries in slots `b` to `b + 15`, and confirm each
   match by comparing the packing flag, length, and key bytes.
3. If nothing matched and slot `b | 15` holds a vector, search that overflow array the same way.

## Insertion and deletion

`Add` puts a new member in the first empty slot of its window. If the window is full, the member
goes to the overflow array at slot `b | 15`; if that slot holds an entry, the entry and the new
member move into a new 2-slot array. The moved entry stays reachable because every home bucket
whose window covers slot `b | 15` uses it for overflow. Overflow arrays reuse empty slots, then
grow by 2 slots up to 128 and double after that.

`Add` returns false for an existing member and keeps its expiry; `AddMany` with a TTL and
`keepttl=false` updates it. An expired duplicate is replaced. The new key is allocated before the
duplicate check, so adding an existing member costs an allocation and a free.

`Erase` frees the key allocation and clears its slot, freeing the overflow array if it becomes
empty. Lookups do not stop at empty slots, so erasing needs no tombstones or backward shifting.

## Resizing

The bucket array is allocated on first insertion. The set grows before an insertion once it holds
`2 * (B + 15)` members, an average of two per slot, so at least half of them are in overflow arrays.
Growing at least doubles `B`. After `Erase`, if fewer than `B / 2` members remain and `B > 2`, `B`
shrinks to the member count rounded up to a power of two. `Reserve(n)` sizes the set for `n`
members.

Resizing moves slot words and rebuilds overflow arrays; key allocations stay in place. Growing
rehashes every key, which reads its allocation; shrinking computes new positions from fingerprints
and old slots alone. Resizing invalidates iterators, but reads never resize the set, even when
they delete expired members.

`Clear` frees all keys and overflow arrays but keeps the bucket array's memory for reuse.

## Expiry

The caller sets the current time in seconds with `set_time`. A TTL is stored as the absolute time
`now + ttl`; `UINT32_MAX` means no expiry. Reads and `Add` delete expired members lazily, so
`UpperBoundSize()` may still count them; `SizeSlow()` deletes them first and returns the exact
size.

## Key views

`DecodeKey` returns an `oah::key::Decoded`. Packed keys are decoded into a buffer inside it, so
keep it alive while using `view()`. Raw keys are not copied: `view()` points into the key
allocation and becomes invalid when the member is erased or expires, or when defragmentation or a
newly added expiry reallocates it.

## Scan

Each `Scan` call reports the members of the next non-empty home bucket, recognized by their
fingerprints. The cursor is a hash prefix, so it survives resizes and keeps the Redis `SCAN`
guarantees: members present for the whole scan are reported at least once, possibly more.

## Threading

`OAHSet` has no locks; only the thread that owns its shard may access it. This includes lookups,
which can delete expired members.

## Code

- [oah_set.h](../src/core/oah_set.h): `OAHSet` insertion and copying (`Fill`).
- [oah_table.h](../src/core/oah_table.h): `OAHTable`, the lookup, erase, scan, expiry, and resizing
  logic shared with `OAHMap`.
- [oah_entry.h](../src/core/oah_entry.h): entries and key allocations.
- [oah_ptr.h](../src/core/oah_ptr.h), [ptr_vector.h](../src/core/ptr_vector.h): slots and overflow
  arrays.
- [oah_base.h](../src/core/oah_base.h): tag bits, the length header, and ASCII packing.
- [oah_set_test.cc](../src/core/oah_set_test.cc): tests.
