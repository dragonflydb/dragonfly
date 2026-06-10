## V2 Connection Fiber — All Blocking, Syscalls, and CPU-Intensive Points (Corrected)

### FIBER BLOCKING (fiber suspends, yields CPU to proactor/other fibers)

**1. Idle Await** — dragonfly_connection.cc ~line 3153
```cpp
io_event_.await(should_wake);
```
Fiber parks here when: no data in `io_buf_`, no pending commands, no replies ready. This is the "sleeping" state. Wakes when: multishot CQE delivers data (callback sets `io_buf_`), async command completes, control message arrives, or migration requested.

**2. Flush → Send → io_uring SEND** — reply_builder.cc → uring_socket.cc
```cpp
fc.Get();  // ← FIBER SUSPENDS
```
Fiber suspends after queuing an SQE for `IORING_OP_SENDMSG`/`IORING_OP_SEND`. **Important**: `fc.Get()` does NOT trigger a syscall. It just suspends the fiber. The proactor batches all pending SQEs from all fibers and submits them in a **single** `io_uring_submit_and_get_events()` call per event-loop iteration. So 50 fibers flushing = 50 SQEs = 1 syscall.

**3. Backpressure Await** — dragonfly_connection.cc ~line 3223
```cpp
io_event_.await([this, &is_ready_to_migrate]() { ... });
```
Parks when pipeline queue exceeds memory limit. Wakes when commands drain below threshold.

**4. Transaction Execute barrier (CONDITIONAL)** — transaction.cc line 968
```cpp
run_barrier_.Wait();  // ← FIBER SUSPENDS until all shards complete the hop
```
**When it fires**: Only for commands that target a shard OTHER than the connection's local shard, or multi-shard commands. For single-shard commands where the key is on the local shard AND keys are uncontended, `OPTIMISTIC_EXECUTION` runs the command inline during scheduling — `run_barrier_.Wait()` returns immediately (run_cnt == 0).

**5. Transaction scheduling barrier (multi-shard only)** — transaction.cc line 802
```cpp
run_barrier_.Wait();  // ← FIBER SUSPENDS until all shards schedule
```
**When it fires**: Only for multi-key commands spanning 2+ shards (MSET, ZUNIONSTORE, etc.). Single-key commands skip this entirely.

**6. CLIENT PAUSE (rare, admin-triggered)** — server_state.cc line 211
```cpp
client_pause_ec_.await([is_write, this]() { ... });
```
**When it fires**: Only when an admin has issued `CLIENT PAUSE <ms> [WRITE|ALL]` — used for controlled failover or live migration. Under normal operation, the check (`etl.IsPaused()`) is a cheap atomic read that returns `false` immediately. **Never fires during benchmarks.**

**7. ParseRedis yield (max_busy_cycles)** — dragonfly_connection.cc ~line 1472
```cpp
ThisFiber::Yield();
```
**When it fires**: Only when parsing a massive buffer takes longer than `max_busy_read_cycles` (default: 70μs worth of TSC cycles). Prevents a single connection's parse from starving all other fibers on the thread. Rare for normal-sized commands.

**8. Blocking commands (BLPOP, BRPOP, WAIT, XREAD BLOCK)** — transaction.cc line 1453
```cpp
blocking_barrier_.Wait(tp);  // ← FIBER PARKS until key gets data or timeout
```
The fiber parks **inside the command handler** (deep in the call stack, not at the IoLoopV2 idle await). It stays blocked until another connection pushes data to the watched key (triggering `blocking_barrier_.Close()`), or the timeout expires. The connection cannot process ANY other commands while blocked here.

---

### SYSTEM CALLS (kernel transitions, but non-blocking to the fiber)

**9. Data arrival path — TWO MODES:**

**9a. Multishot recv (HOT PATH on io_uring + buffer ring + non-TLS):**
```cpp
// Kernel delivers data via IORING_OP_RECV_MULTISHOT → CQE → proactor callback
cb(RecvNotification{io::MutableBytes(buf_ptr, segment_len)});  // uring_socket.cc:478
```
**ZERO `recv()` syscalls.** The kernel fills pre-provided buffer ring memory and fires a CQE. The proactor callback delivers `io::MutableBytes` directly to `NotifyOnRecv`, which copies into `io_buf_`. This is V2's main architectural win — data arrives without any fiber involvement or syscall from userspace.

**9b. TryRecv fallback (epoll, TLS, or multishot re-arm):**
```cpp
ssize_t res = recv(fd, buf.data(), buf.size(), 0);  // O_NONBLOCK
```
Called in a loop inside `ReadPendingInput()`. Each call is a non-blocking syscall (~100-200ns kernel transition). Returns `EAGAIN` when kernel buffer is empty. **Does NOT block the fiber** — it returns immediately. Used only when multishot is unavailable.

---

### CPU-INTENSIVE (no blocking, but significant cycle consumption)

**10. RESP Parsing** — dragonfly_connection.cc ParseRedis loop
```cpp
do {
    result = redis_parser_->Parse(read_buffer, &consumed, parsed_cmd_);
    ...
} while (OK == result && read_buffer.size() > 0);
```
Per-byte state machine + per-command `CommandContext` allocation + arg slice setup. For pipelined workloads (p=10+), this is a major CPU consumer.

**11. Command execution (inline single-shard)** — main_service.cc
```cpp
cid->Invoke(tail_args, cmd_cntx);
```
When `OPTIMISTIC_EXECUTION` succeeds (local shard, uncontended), the entire command handler runs synchronously on the connection fiber: hash table lookups, sorted set operations, string manipulation, memory alloc/dealloc.

**12. Reply serialization** — inside `ReplyBatch()` / `SendReply()`
Formats RESP protocol bytes into `reply_builder_`'s iovec buffer. For bulk responses (LRANGE of 1000 elements), involves many `memcpy` operations building the output.

**13. Command lookup / validation** — main_service.cc
```cpp
const auto [cid, args_no_cmd] = registry_.FindExtended(args);
```
Plus `VerifyCommandState()` — ACL checks, command state validation. Lightweight per-command but adds up at high throughput.

**14. Cross-shard coordination setup (CPU before the barrier block):**
For multi-key commands (MSET, ZUNIONSTORE), before `run_barrier_.Wait()` fires, the fiber burns CPU on:
- Shard placement calculation (CRC16 per key)
- Intent lock acquisition/queuing
- Serializing dispatch messages to remote proactor threads
This is pure CPU overhead that precedes the fiber block at point 5.

**15. Control path processing (ProcessControlMessages)** — dragonfly_connection.cc line 3175
```cpp
bool quota_reached = ProcessControlMessages(async_dispatch_quota);
```
Drains `dispatch_q_`: PubSub messages, migration signals, checkpoint hooks. Under PubSub floods, this can become a major CPU sink — bounded by `async_dispatch_quota` to prevent data-path starvation.

**16. Memory pool management:**
`GetFromPoolOrCreate()` / `ReleaseParsedCommand()` traverse the `pipeline_req_pool_` linked list. At extreme pipeline depths (p=500), cache-line misses from pointer chasing become measurable.

---

### NOT IN THE CONNECTION CODE BUT REAL CPU TAX

**17. Proactor event loop overhead:**
CQE reaping (`io_uring_peek_batch_cqe`), mapping completions back to suspended fibers, run-queue management. Invisible from dragonfly_connection.cc but consumes a real percentage of the thread's budget, especially under massive pipelining (thousands of CQEs/sec).

---

### Summary: Where time goes for SET/GET at p=1 (single shard, local)

| Phase | What happens | Blocking? |
|-------|-------------|-----------|
| Idle | `io_event_.await()` — waiting for data | FIBER PARKS |
| Data arrives | Multishot CQE → callback copies to `io_buf_` → `io_event_.notify()` | CPU (callback) |
| Parse | `ParseRedis()` — state machine + alloc | CPU |
| Execute | `cid->Invoke()` inline (OPTIMISTIC_EXECUTION) | CPU |
| Reply build | `SendReply()` → format RESP into vecs | CPU |
| Flush | `reply_builder_->Flush()` → `fc.Get()` | FIBER PARKS |
| Kernel sends | Proactor batches SQE → `io_uring_submit_and_get_events()` | 1 syscall (batched) |
| Loop back | Check for more data, goto idle if none | CPU |
