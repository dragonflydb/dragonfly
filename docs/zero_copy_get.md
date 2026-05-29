# Zero-Copy GET for Large Strings

A `GET` on a large string value in Dragonfly transports the user-visible
bytes from the shard's `CompactObj` storage to the client socket without
materializing the string anywhere in between. The shard does not allocate
a `std::string`; the reply builder does not buffer the full payload.

The borrowed pointer survives concurrent mutations of the same key via a
Copy-on-Write mechanism: a writer that races a reader installs a fresh
allocation on the `CompactObj` and leaves the old buffer owned by a
refcount-bearing pin, which is freed on the buffer's owning shard once
the last reader is done with it.

## What's covered

- `LARGE_STR_TAG` (or `detail::LargeString`) values (heap-allocated, >> 1024 bytes).
- Raw and ASCII-packed encodings. Huffman-encoded strings can be added later
  if we prove their value.
- `EXTERNAL_TAG` (tiered) values require an asynchronous disk fetch and are out of scope.

Currently, `GET` is the only command that uses this path. Mutating reads
(`GETDEL`, `GETEX`, `GETSET`) and multi-key reads (`MGET`) keep the
materializing `pv.ToString()` path. Can be fixed later.

## Threading model

Dragonfly's shared-nothing design pins each shard to a single proactor
thread with a thread-local mimalloc heap. A connection is bound to one
proactor; `GET` for a foreign key hops to the owning shard via
`SingleHopT(cb)`, then resumes on the connection's proactor for reply
construction. The socket write itself is fiber-synchronous but may
yield the fiber while waiting on the kernel — other commands on the
same shard can run during that window. While mimalloc supports cross-thread
frees, the design routes all deallocations back to the buffer's owning
shard so we can track memory usage consistently.

## Building blocks

### `detail::LargeString`

The 16-byte storage for non-inline raw strings in `CompactObj`:

```cpp
struct LargeString {
  void* ptr;                   // mimalloc allocation on owning shard
  uint64_t sz : 56;            // current length in bytes
  uint64_t read_pending : 1;   // at least one outstanding read pin
  uint64_t reserved : 7;
};
```

`read_pending` is set when one or more readers hold a borrowed view
into `ptr`. While the bit is set, `LargeString::SetString` and
`LargeString::Free` do not deallocate `ptr` directly; they look it up
in the thread-local pin registry and hand the buffer off to the matching
`PendingRead` entry (which becomes the sole owner of the buffer until the
last reader unpins).

`LargeString::DefragIfNeeded` returns false (no defrag) while
`read_pending` is set — a reallocation would invalidate outstanding
borrowed views.

`LargeString::AppendString` mutates in place and would corrupt pinned
readers; it `CHECK`s `!IsReadPending()`. The only caller is
`rdb_load`, which never produces values that have been pinned.

### `cmn::BorrowedString`

Move-only owning handle for a borrowed bulk string (`src/common/borrowed_string.h`).
Carries the encoded view, an encoding tag (0 = raw, non-zero = packed), and an
opaque pin. The user-visible decoded size is computed on demand by
`BorrowedStringOps::DecodedSize` (the encoding plus the packed view determine
it, so no need to store it in the handle). The destructor releases the pin via
the registered `BorrowedStringOps`.

`CompactObj::TryBorrow()` is the sole producer; from that point the object is
moved (never copied) through the reply path until destruction triggers the unpin.

### `cmn::BorrowedStringOps`

Abstract interface set once by `CompactObj::InitThreadLocal`. The destructor of
`BorrowedString` detaches the pin and calls `Release(void*)` to decrement its
refcount; `DecodeChunk(...)` unpacks up to `max_count` bytes of an ASCII-packed
source into a destination buffer and returns the number of bytes written
(non-final chunks may be smaller than `max_count` due to alignment).

This is the only seam between `common/` (or `facade/`) and the core internals:
the concrete impl (`CompactObjBorrowOps`, file-local in `compact_object.cc`)
casts the pin to `PendingRead*` and calls `detail::ascii_unpack` — callers
outside `core/` never see either type.

### `CompactObj::TryBorrow`

Returns a `cmn::BorrowedString` iff this `CompactObj` holds a large raw
or packed string; otherwise `std::nullopt` (caller falls back to
`GetSlice`/`ToString`).

```cpp
std::optional<cmn::BorrowedString> CompactObj::TryBorrow() const;
```

On success: stamps `read_pending` on the `LargeString` (via a controlled
`const_cast` — the bit is bookkeeping, not part of the logical value) and
registers a `PendingRead` in the thread-local pin map. The returned
`BorrowedString` owns the pin; no further action is required from the caller
beyond letting it go out of scope (or calling `Unpin()` explicitly).

### Pin registry

Lives in `compact_object.cc`'s thread-local scope alongside `local_mr`
and the huffman tables. Not exposed beyond a handful of `CompactObj`
static methods; `EngineShard` only invokes the periodic drain.

```cpp
// compact_object.cc (internal)
struct PendingRead {
  const void* ptr;
  std::atomic<uint32_t> refcnt;
  bool orphaned;

  void UnpinRead();  // release-store fetch_sub; caller must not touch pin after
};
```

The map is single-threaded — only the owning thread mutates it. Other
threads touch only individual `PendingRead::refcnt` via `UnpinRead`
(which is called indirectly through `BorrowedStringOps::Release`).
There is no queue: `UnpinRead` is a single release-store `fetch_sub`,
and the owning thread reaps refcnt==0 entries by iterating the map.

Why no cross-thread queue: an earlier design pushed pins onto an MPSC
free list when refcnt hit zero, but that allowed the same pin to be
pushed twice if a new reader pinned the same buffer between the
unpin-to-zero and the drain. Map iteration avoids the race entirely
and costs O(N) per drain where N is the number of in-flight reads on
this thread — small in practice (one pin per active GET reply).

Public surface:

- `CompactObj::TryBorrow()` — owning thread. Entry point for callers.
  Stamps `read_pending`, registers pin, returns the owning `BorrowedString`.
- `CompactObj::DrainPendingReads()` — owning thread, static. Iterates
  the map, reaps zero-refcnt entries: frees orphaned buffers, erases the
  slot, deletes the entry. Called periodically from `EngineShard::Heartbeat`.

The orphan transition is a private detail of `LargeString::SetString`
and `LargeString::Free`: they look the ptr up in `tl.pin_map`, set
`entry->orphaned = true`, and erase from the map.

### Reply builder integration

`SinkReplyBuilder` accumulates iovecs via `WritePieces` (copy into scratch) or
`WriteRef` (pointer-only), then `Send()` does a synchronous `writev`. The
borrowed bytes must outlive that write.

`RedisReplyBuilderBase::SendBulkStringBorrowed(cmn::BorrowedString)` is the
additional method that serializes borrowed strings. It's either writes raw string by reference,
or iteratively decodes chunks into temporary buffer and writes them into a sink.
Finally it calls `Flush()` so all references reach the kernel before the function
returns — at which point `~BorrowedString` (on the parameter) releases the
pin via `BorrowedStringOps::Release`. No deferred-release machinery is
needed; pin lifetime is plain C++ scope-based.

### Capture / replay

`payload::Payload` gains a `cmn::BorrowedString` alternative directly
(`sizeof(Payload)` is preserved — `BorrowedString` fits in the variant's
existing space). The `CapturingReplyBuilder` override of
`SendBulkStringBorrowed` moves the borrow into the payload — the captured
payload owns the pin for the capture's lifetime. On replay,
`CaptureVisitor` moves it back into the real sink's
`SendBulkStringBorrowed`, where the same Flush-then-release flow takes over.

## End-to-end path

GET callback on the owning shard calls `pv.TryBorrow()`; on success the
returned `BorrowedString` is moved through `SingleHopT` back to the
connection's proactor, which calls `rb->SendBulkStringBorrowed(std::move(bs))`
to emit the iovecs. The trailing `Flush()` drains everything via `writev`;
`~BorrowedString` then releases the pin. `EngineShard::Heartbeat` periodically
calls `CompactObj::DrainPendingReads()` to reap entries with `refcnt == 0`
(freeing the buffer if orphaned).

## Concurrency picture

A write racing a read on the same key. Shard A is the buffer's owning
thread; Connection X's proactor is the IO thread constructing the GET reply.

```mermaid
sequenceDiagram
    autonumber
    participant A as Shard A (owning thread)
    participant X as Connection X (proactor)

    Note over A: GET k callback
    A->>A: bs = pv.TryBorrow()<br/>(stamps read_pending, registers pin in tl.pin_map)
    A-->>X: BorrowedString{view, pin} (via SingleHopT return value)

    X->>X: rb->SendBulkStringBorrowed(std::move(bs))<br/>(raw → WriteRef; packed → WriteDecodedAscii)<br/>Flush() drains iovecs via writev

    Note over A: concurrent SET k newval (other connection hops to A)
    A->>A: LargeString::SetString sees read_pending=1
    A->>A: lookup old_ptr in tl.pin_map,<br/>mark orphaned, erase
    A->>A: allocate fresh buffer, clear read_pending

    X->>X: ~BorrowedString at function exit<br/>→ BorrowedStringOps::Release → UnpinRead<br/>(release fetch_sub, no further access)

    Note over A: Heartbeat
    A->>A: DrainPendingReads()<br/>refcnt==0 + orphaned → deallocate old buffer
```

If no mutation occurs between borrow and unpin, the drain finds the
entry with `orphaned=false` and just removes the map slot; the
`CompactObj` continues to own the buffer through its normal lifecycle.

## Race conditions and safety

### Drain vs re-pin

If `UnpinRead` decrements an entry's `refcnt` to zero and another
reader's `TryBorrow` bumps it back to 1 before the next drain, the
drain's iteration observes `refcnt > 0` under acquire ordering and
skips the entry. The map slot continues to point at the same entry;
the new reader's eventual unpin brings it back to zero and the
following drain cycle reaps it. No double-free.

### Post-drain mutation

The `LargeString::read_pending` bit can outlive the corresponding map
entry — after the last reader unpins and the drain has removed the
non-orphaned entry, the bit remains set on the `CompactObj`'s
`LargeString`. A subsequent mutation looks `ptr` up in `tl.pin_map`,
doesn't find it, and falls through to the normal deallocate.

This is safe because the "not found" outcome only happens when no
reader pinned the same address between the unpin-to-zero and the
mutation. If `tl.pin_map` doesn't contain `ptr` at mutation time,
every prior pin's `refcnt` is zero AND no new pin has executed —
nothing is reading `ptr`, and the deallocate is safe. The bit being
stale-set is harmless.

### Defrag

The defrag task would reallocate a `LargeString` buffer in place and
invalidate any outstanding borrowed views. `DefragIfNeeded` returns
false (no defrag) when `read_pending=1`. Once the pins clear, the
next defrag pass picks up the same value.

### Cross-thread free

A pin allocated on shard A but unpinned by a different IO thread is
reaped from A's `tl.pin_map` on A's next drain. The IO thread's only
access to the pin is the `fetch_sub` inside `UnpinRead`; once that
returns, the pin pointer is no longer referenced from the IO thread, so A is free to delete
the entry whenever the drain observes `refcnt == 0`.
The buffer is freed on A's thread via its mimalloc heap, matching where the buffer was allocated.

### Capture / replay window

Captured payloads hold a `cmn::BorrowedString` that owns the pin. The pin
is released only when the real sink's `SendBulkStringBorrowed` runs at
replay time (its trailing `Flush()` drains the iovecs, then
`~BorrowedString` releases). The borrowed source is therefore valid for
the entire squashing / `MULTI`-`EXEC` pipeline.

## What's out of scope

- **MGET.** `CollectKeys` today allocates a per-shard storage buffer
  and packs values into it. Zero-copy MGET would carry borrowed views
  per result instead.
- **Huffman-encoded large strings.** `TryBorrow` returns `nullopt` for
  `HUFFMAN_ENC`. Huffman codes are variable-length so decoding without
  materialisation would require a stateful streaming decoder.
- **`EXTERNAL_TAG` (tiered) values.** Asynchronous disk fetch and
  materialization; outside the in-memory zero-copy story.
- **`SMALL_TAG` and inline values.** Already cheap to copy.

## Tag / flag / counter reference

| Symbol | Where | Meaning |
|---|---|---|
| `LARGE_STR_TAG` | `CompactObj::TagEnum` | Heap-allocated raw large string |
| `read_pending` | `detail::LargeString` | One or more readers may be borrowing `ptr` |

## File map

| Concern | File |
|---|---|
| `BorrowedString`, `BorrowedStringOps` interface | `src/common/borrowed_string.h` |
| `read_pending` bit, `TryBorrow`, pin registry, `DrainPendingReads`, `CompactObjBorrowOps` | `src/core/compact_object.{h,cc}` |
| `SendBulkStringBorrowed`, `WriteDecodedAscii` | `src/facade/reply_builder.{h,cc}` |
| Capture/replay override, `Payload` variant alternative | `src/facade/reply_capture.{h,cc}`, `src/facade/reply_payload.h` |
| HTTP-API visitor materialization | `src/server/http_api.cc` |
| Tests | `src/core/compact_object_test.cc` |
