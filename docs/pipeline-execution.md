# Pipeline Execution Architecture

> From TCP accept to command execution on shards.

## High-Level Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                        CLIENT                                        │
│   redis-cli / app driver sends: SET foo bar \r\n GET foo \r\n ...    │
└───────────────────────────┬──────────────────────────────────────────┘
                            │ TCP
                            ▼
┌──────────────────────────────────────────────────────────────────────┐
│  1. LISTENER  (Listener::OnConnectionStart)                          │
│     • Accepts TCP connection                                         │
│     • Configures socket (TCP_NODELAY, keepalive, TCP_DEFER_ACCEPT)   │
│     • Picks a proactor thread via PickConnectionProactor()           │
│     • Creates Connection object, hands off to proactor               │
└───────────────────────────┬──────────────────────────────────────────┘
                            │ dispatched to proactor fiber
                            ▼
┌──────────────────────────────────────────────────────────────────────┐
│  2. CONNECTION I/O LOOP  (Connection::IoLoop)                        │
│     • Reads bytes from socket into io_buf_                           │
│     • Calls ParseRedis() → redis_parser_ decodes RESP frames         │
│     • Single cmd → dispatch_sync (inline on IO fiber)                │
│     • Pipelined cmds → enqueue to parsed_cmd linked list             │
│     • Backpressure: pauses reads when queue exceeds limits           │
└───────────────────────────┬──────────────────────────────────────────┘
                            │
              ┌─────────────┴──────────────┐
              │ single / last command       │ pipelined commands
              ▼                             ▼
┌─────────────────────────┐   ┌────────────────────────────────────────┐
│  3a. SYNC DISPATCH      │   │  3b. ASYNC FIBER  (Connection::       │
│  Inline on IO fiber:    │   │      AsyncFiber)                       │
│  DispatchCommand(       │   │  • Woken via cnd_.notify_one()         │
│    ..., ONLY_SYNC)      │   │  • Dequeues from parsed_head_          │
│                         │   │  • Pipeline squashing: batches N cmds  │
│                         │   │    into one DispatchManyCommands call   │
│                         │   │  • Also processes admin messages       │
│                         │   │    (PubSub, Monitor, etc.)             │
└────────────┬────────────┘   └──────────────┬─────────────────────────┘
             │                               │
             └───────────┬───────────────────┘
                         ▼
┌──────────────────────────────────────────────────────────────────────┐
│  4. COMMAND DISPATCH  (Service::DispatchCommand)                      │
│     a. FindExtended() → look up CommandId in registry                │
│     b. VerifyCommandState() → ACL, cluster, replica checks           │
│     c. PrepareTransaction() → create Transaction, determine shards   │
│        └─ Transaction::InitByArgs() hashes keys → shard IDs          │
│     d. InvokeCmd() → cid->Invoke(args, cmd_cntx)                    │
│        └─ Command handler (e.g. SET, GET) calls                      │
│           tx->ScheduleSingleHop(callback)                            │
└───────────────────────────┬──────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────────┐
│  5. TRANSACTION SCHEDULING  (Transaction::ScheduleInternal)          │
│     • Assigns global txid for ordering                               │
│     • Single-shard: batch-queued via per-thread schedule_q_          │
│     • Multi-shard: shard_set->Add() to each participating shard      │
│     • ScheduleInShard() per shard:                                   │
│       ├─ Acquire key fingerprint locks                               │
│       ├─ If keys free + single hop → optimistic inline execution     │
│       └─ Otherwise → insert into shard TxQueue                       │
│     • run_barrier_.Wait() → coordinator waits for all shards         │
│     • On conflict → cancel, reassign txid, retry                    │
└───────────────────────────┬──────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────────┐
│  6. SHARD EXECUTION  (EngineShard::PollExecution)                    │
│     • Disarm transaction (atomic is_armed flag)                      │
│     • Execute head of TxQueue in order                               │
│     • RunInShard() → RunCallback() invokes command lambda            │
│     • Release key locks, remove from TxQueue                         │
│     • FinishHop() decrements barrier → coordinator unblocks          │
│     • Notify any blocked transactions waiting on keys                │
└───────────────────────────┬──────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────────┐
│  7. RESPONSE                                                         │
│     • Command writes to RedisReplyBuilder (RESP-encoded buffer)      │
│     • Batch mode delays flush until pipeline batch completes         │
│     • SinkReplyBuilder::Flush() → writev() to socket                │
│     • Connection recycles state, loops back to step 2                │
└──────────────────────────────────────────────────────────────────────┘
```

## Detailed Diagrams

### Connection Lifecycle & Two-Fiber Model

Each connection uses up to two fibers on the same proactor thread:

```
Proactor Thread N
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  ┌──────────────────────────────┐                               │
│  │  IO Fiber (per connection)   │                               │
│  │                              │                               │
│  │  socket.recv() ──►io_buf_    │                               │
│  │         │                    │                               │
│  │  redis_parser_.Parse()       │                               │
│  │         │                    │                               │
│  │  ┌──────┴──────┐            │                               │
│  │  │ Single cmd? │            │                               │
│  │  └──────┬──────┘            │                               │
│  │    YES  │   NO (pipelined)  │                               │
│  │    │    │    │               │                               │
│  │    │    │    ▼               │                               │
│  │    │    │  EnqueueParsedCmd  │    ┌──────────────────────┐   │
│  │    │    │  ─────────────────────► │  Async Fiber         │   │
│  │    │    │  cnd_.notify_one() │    │  (per connection)    │   │
│  │    │    │                    │    │                      │   │
│  │    ▼    │                    │    │  cnd_.wait()         │   │
│  │  DispatchCommand()           │    │       │              │   │
│  │  (inline, sync)              │    │       ▼              │   │
│  │                              │    │  SquashPipeline()    │   │
│  │  ◄── reply ──────────────────│────│── OR ProcessCmd()    │   │
│  │                              │    │       │              │   │
│  │  reply_builder_.Flush()      │    │  DispatchCommand()   │   │
│  │  ─────► socket.send()        │    │       │              │   │
│  │                              │    │  reply_builder_      │   │
│  └──────────────────────────────┘    │  .Flush()            │   │
│                                      └──────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Parsing & Pipelining Decision

```
          io_buf_ contains: "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
                             *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
                                         │
                                         ▼
                              ┌─────────────────────┐
                              │  redis_parser_       │
                              │  .Parse(io_buf_)     │
                              └────────┬────────────┘
                                       │  CMD 1: SET foo bar
                                       ▼
                              ┌─────────────────────┐
                              │  DispatchSingle()    │
                              │  has_more = true     │──── more data in buffer?
                              └────────┬────────────┘     YES → async path
                                       │
                                       ▼
                         ┌─────────────────────────────┐
                         │  EnqueueParsedCommand(cmd1)  │
                         │  parsed_cmd_q_len_ = 1       │
                         └─────────────┬───────────────┘
                                       │
                                       ▼  (loop continues)
                              ┌─────────────────────┐
                              │  redis_parser_       │
                              │  .Parse(io_buf_)     │
                              └────────┬────────────┘
                                       │  CMD 2: GET foo
                                       ▼
                              ┌─────────────────────┐
                              │  DispatchSingle()    │
                              │  has_more = false    │──── buffer exhausted
                              └────────┬────────────┘
                                       │
                                       ▼
                         ┌─────────────────────────────┐
                         │  EnqueueParsedCommand(cmd2)  │
                         │  parsed_cmd_q_len_ = 2       │
                         │  cnd_.notify_one()           │──► wakes AsyncFiber
                         └─────────────────────────────┘


  Backpressure thresholds:
  ┌─────────────────────────────────────────────────────┐
  │  pipeline_buffer_limit  = 128 MB  (bytes in queue)  │
  │  pipeline_queue_limit   = 10000   (commands queued) │
  │  When exceeded → IoLoop pauses socket reads          │
  └─────────────────────────────────────────────────────┘
```

### Pipeline Squashing

When multiple commands are queued, the Async Fiber can squash them for efficiency:

```
  AsyncFiber sees parsed_cmd_q_len_ > pipeline_squash threshold
                         │
                         ▼
              ┌─────────────────────┐
              │   SquashPipeline()  │
              │                     │
              │   Collect up to N   │
              │   commands from     │
              │   parsed_head_      │
              └────────┬────────────┘
                       │
                       ▼
              ┌─────────────────────────┐
              │  DispatchManyCommands()  │
              │                         │
              │  Groups commands by     │
              │  target shard, executes │
              │  as batch transaction   │
              └────────┬────────────────┘
                       │
                       ▼
              Responses collected per-cmd
              and flushed together
```

### Command Dispatch → Transaction → Shard

```
  Service::DispatchCommand(args)
       │
       ├─1─► FindExtended(args) ──► CommandId* (SET, GET, LPUSH, ...)
       │
       ├─2─► VerifyCommandState()
       │       • ACL permission check
       │       • Cluster slot validation
       │       • Replica write rejection
       │
       ├─3─► PrepareTransaction()
       │       │
       │       ▼
       │     Transaction::InitByArgs(keys)
       │       │
       │       ├─► hash(key) % num_shards ──► shard_id(s)
       │       │
       │       ├─► unique_shard_cnt_ = 1?
       │       │     YES → single-shard fast path
       │       │     NO  → multi-shard coordination
       │       │
       │       └─► Populate shard_data_[] with key ranges per shard
       │
       └─4─► InvokeCmd() ──► cid->Invoke(args, cmd_cntx)
                                    │
                      ┌─────────────┴─────────────┐
                      │    Command Handler         │
                      │    e.g. SetFamily::Set()   │
                      │                            │
                      │    tx->ScheduleSingleHop(  │
                      │      [](Transaction* t,    │
                      │       EngineShard* shard) { │
                      │        // access db_slice  │
                      │        // modify data      │
                      │        return OpStatus::OK;│
                      │      });                   │
                      └────────────────────────────┘
```

### Transaction Scheduling: Single-Shard Fast Path

```
  Coordinator Fiber (proactor thread of the connection)
       │
       │  tx->ScheduleSingleHop(cb)
       │       │
       │       ▼
       │  ScheduleInternal()
       │       │
       │       ├─► Is single shard + concluding? → try OPTIMISTIC execution
       │       │
       │       ▼
       │  schedule_queues[shard_id].Push(&ctx)
       │       │
       │       ├─► If same proactor as shard:
       │       │     ScheduleBatchInShard() runs INLINE
       │       │       │
       │       │       ▼
       │       │     ScheduleInShard(shard, optimistic=true)
       │       │       │
       │       │       ├─► Acquire key locks → granted?
       │       │       │     YES + optimistic → RunCallback(shard) INLINE
       │       │       │                         │
       │       │       │                         ▼
       │       │       │                       cb(tx, shard)  ◄── command runs HERE
       │       │       │                         │
       │       │       │                       Release locks
       │       │       │                       FinishHop()
       │       │       │                         │
       │       │       │                       ◄─┘ returns to coordinator
       │       │       │
       │       │       │     NO → insert into TxQueue, wait for PollExecution
       │       │       │
       │       ├─► If different proactor:
       │       │     shard_set->Add(shard_id, ScheduleBatchInShard)
       │       │     run_barrier_.Wait()  ◄── fiber yields until shard completes
       │       │
       │       ▼
       │  Return to caller with result
       │
       ▼
  Reply to client
```

### Transaction Scheduling: Multi-Shard Coordination

```
  Coordinator Fiber
       │
       │  tx->Execute(cb, conclude=true)
       │       │
       │       ▼
       │  ScheduleInternal()
       │       │
       │       ├──── For each participating shard:
       │       │       shard_set->Add(shard_i, ScheduleInShard)
       │       │
       │       │     ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
       │       │     │  Shard 0    │  │  Shard 1    │  │  Shard 2    │
       │       │     │             │  │             │  │             │
       │       │     │ Schedule    │  │ Schedule    │  │ Schedule    │
       │       │     │ InShard()   │  │ InShard()   │  │ InShard()   │
       │       │     │  │          │  │  │          │  │  │          │
       │       │     │  ├ Acquire  │  │  ├ Acquire  │  │  ├ Acquire  │
       │       │     │  │ locks    │  │  │ locks    │  │  │ locks    │
       │       │     │  │          │  │  │          │  │  │          │
       │       │     │  ├ Insert   │  │  ├ Insert   │  │  ├ Insert   │
       │       │     │  │ in TxQ   │  │  │ in TxQ   │  │  │ in TxQ   │
       │       │     │  │          │  │  │          │  │  │          │
       │       │     │  ▼          │  │  ▼          │  │  ▼          │
       │       │     │ FinishHop() │  │ FinishHop() │  │ FinishHop() │
       │       │     └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │       │            │                │                │
       │       │            └────────────────┼────────────────┘
       │       │                             │
       │       ◄── run_barrier_.Wait() ──────┘  (all shards scheduled)
       │       │
       │       ├── Conflict? → CancelShardCb(), retry with new txid
       │       │
       │       ▼
       │  DispatchHop()
       │       │
       │       ├─── Set is_armed = true on all shards
       │       │
       │       ├─── For each shard:
       │       │      shard_set->Add(shard_i, PollExecution)
       │       │
       │       │    ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
       │       │    │  Shard 0    │  │  Shard 1    │  │  Shard 2    │
       │       │    │             │  │             │  │             │
       │       │    │ PollExec()  │  │ PollExec()  │  │ PollExec()  │
       │       │    │  │          │  │  │          │  │  │          │
       │       │    │  ├ Disarm   │  │  ├ Disarm   │  │  ├ Disarm   │
       │       │    │  ├ RunIn    │  │  ├ RunIn    │  │  ├ RunIn    │
       │       │    │  │ Shard()  │  │  │ Shard()  │  │  │ Shard()  │
       │       │    │  │  │       │  │  │  │       │  │  │  │       │
       │       │    │  │  ├ cb()  │  │  │  ├ cb()  │  │  │  ├ cb()  │
       │       │    │  │  ├ unlock│  │  │  ├ unlock│  │  │  ├ unlock│
       │       │    │  │  ▼       │  │  │  ▼       │  │  │  ▼       │
       │       │    │  FinishHop  │  │  FinishHop  │  │  FinishHop  │
       │       │    └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │       │           │                │                │
       │       │           └────────────────┼────────────────┘
       │       │                            │
       │       ◄── run_barrier_.Wait() ─────┘  (all shards executed)
       │
       ▼
  Return to caller, reply to client
```

### Shard-Side Execution Detail (PollExecution)

```
  EngineShard::PollExecution(context, trans)
       │
       ├─1─► trans->DisarmInShard(sid)
       │       atomic is_armed: true → false
       │
       ├─2─► Check for awakened blocking transactions (BLPOP, etc.)
       │       if (AWAKED_Q) → RunInShard() immediately, return
       │
       ├─3─► Process continuation_trans_ (multi-hop transaction)
       │       if armed → RunInShard()
       │       if concludes → continuation_trans_ = nullptr
       │
       ├─4─► Process TxQueue head (in-order execution)
       │       │
       │       ▼
       │     ┌──────────────────────────────────────────┐
       │     │              TxQueue (ordered by txid)    │
       │     │  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐ │
       │     │  │tx 42 │→ │tx 45 │→ │tx 47 │→ │tx 51 │ │
       │     │  │ARMED │  │wait  │  │wait  │  │ARMED │ │
       │     │  └──┬───┘  └──────┘  └──────┘  └──────┘ │
       │     └─────┼────────────────────────────────────┘
       │           │
       │           ▼
       │     tx42->DisarmInShard() → true
       │     committed_txid_ = 42
       │     tx42->RunInShard(shard)
       │       ├─ RunCallback()     ◄── command lambda executes
       │       ├─ Remove from TxQueue
       │       ├─ Release key locks
       │       ├─ NotifyPending()   ◄── wake blocked waiters
       │       └─ FinishHop()       ◄── decrement coordinator barrier
       │
       └─5─► Out-of-Order (OOO) execution
               If caller's transaction has no key conflicts with
               TxQueue head → execute immediately, skip queue ordering
               (Optimization for uncontended keys)
```

### Threading Model

```
  ┌─────────────────────────────────────────────────────────────────┐
  │                    Proactor Pool (N threads)                     │
  │                                                                 │
  │  ┌─────────────────────┐  ┌─────────────────────┐              │
  │  │  Proactor Thread 0  │  │  Proactor Thread 1  │   ...        │
  │  │                     │  │                     │              │
  │  │  ┌───────────────┐  │  │  ┌───────────────┐  │              │
  │  │  │ EngineShard 0 │  │  │  │ EngineShard 1 │  │              │
  │  │  │  • TxQueue    │  │  │  │  • TxQueue    │  │              │
  │  │  │  • DbSlice    │  │  │  │  • DbSlice    │  │              │
  │  │  │  • TaskQueue  │  │  │  │  • TaskQueue  │  │              │
  │  │  └───────────────┘  │  │  └───────────────┘  │              │
  │  │                     │  │                     │              │
  │  │  ┌─ Connection A ─┐ │  │  ┌─ Connection C ─┐ │              │
  │  │  │ IO fiber       │ │  │  │ IO fiber       │ │              │
  │  │  │ Async fiber    │ │  │  │ Async fiber    │ │              │
  │  │  └────────────────┘ │  │  └────────────────┘ │              │
  │  │  ┌─ Connection B ─┐ │  │  ┌─ Connection D ─┐ │              │
  │  │  │ IO fiber       │ │  │  │ IO fiber       │ │              │
  │  │  │ Async fiber    │ │  │  │ Async fiber    │ │              │
  │  │  └────────────────┘ │  │  └────────────────┘ │              │
  │  │                     │  │                     │              │
  │  └─────────────────────┘  └─────────────────────┘              │
  │                                                                 │
  │  Note: A connection's fibers run on one proactor, but its       │
  │  transactions execute callbacks on ANY shard's proactor via     │
  │  shard_set->Add() cross-thread dispatch.                        │
  └─────────────────────────────────────────────────────────────────┘
```

### Cross-Thread Dispatch via shard_set->Add()

```
  Connection on Proactor 0          Proactor 2 (owns Shard 2)
  ┌──────────────────────┐          ┌──────────────────────┐
  │                      │          │                      │
  │  tx->Schedule...()   │          │  EngineShard 2       │
  │    │                 │          │    │                  │
  │    ├─ shard_set      │          │    │  TaskQueue       │
  │    │  ->Add(2, cb) ──┼────►─────┼──► │  ┌────┐         │
  │    │                 │  (MPSC   │    │  │ cb │ enqueued │
  │    │                 │   queue) │    │  └──┬─┘         │
  │    │                 │          │    │     │            │
  │    │  barrier.Wait() │          │    │     ▼            │
  │    │  (fiber sleeps) │          │    │  cb() executes   │
  │    │       .         │          │    │  on shard fiber  │
  │    │       .         │          │    │     │            │
  │    │       .         │          │    │  FinishHop()     │
  │    │       ◄─────────┼────◄─────┼────│  barrier--      │
  │    │  (fiber wakes)  │          │    │                  │
  │    ▼                 │          │                      │
  │  continue...         │          └──────────────────────┘
  └──────────────────────┘
```

## Key Source Files

| Component | File | Key Entry Points |
|-----------|------|-----------------|
| Listener | `src/facade/dragonfly_listener.cc` | `OnConnectionStart()`, `PickConnectionProactor()` |
| Connection | `src/facade/dragonfly_connection.cc` | `IoLoop()`, `ParseRedis()`, `AsyncFiber()` |
| RESP Parser | `src/facade/redis_parser.cc` | `Parse()` |
| Service | `src/server/main_service.cc` | `DispatchCommand()`, `InvokeCmd()`, `PrepareTransaction()` |
| Transaction | `src/server/transaction.cc` | `ScheduleSingleHop()`, `ScheduleInternal()`, `DispatchHop()`, `RunInShard()` |
| Shard | `src/server/engine_shard_set.cc` | `PollExecution()`, `Add()` (TaskQueue) |
| Reply | `src/facade/reply_builder.h` | `SinkReplyBuilder::Flush()`, `RedisReplyBuilder` |

## Key Optimizations

| Optimization | Description |
|---|---|
| **Optimistic Execution** | Single-shard transactions with uncontended keys execute inline during scheduling — no queue insertion needed |
| **Pipeline Squashing** | Multiple pipelined commands batched into a single `DispatchManyCommands` call, reducing per-command overhead |
| **Out-of-Order (OOO)** | Transactions whose keys don't conflict with the TxQueue head can skip ahead and execute immediately |
| **Inline Single-Shard** | When the connection's proactor owns the target shard, the entire schedule→execute path runs without cross-thread dispatch |
| **Batch Scheduling** | Single-shard transactions from multiple connections are batched into `ScheduleBatchInShard` to amortize dispatch cost |
| **Reply Batching** | Pipelined responses accumulate in `SinkReplyBuilder` buffer and flush together via `writev()` |
