# Connection Disk Offload

---

## 1. Watermark Flags

Three thresholds form a three-level band:

| Flag                                   | Default      | Role |
|----------------------------------------|--------------|------|
| `pipeline_disk_offload_threshold`      | 0 (disabled) | **Offload trigger** – `parsed_cmd_q_bytes_ >= threshold` → incoming bytes go to disk instead of `io_buf_`. |
| `disk_backpressure_hysteresis_arm`     | x KB         | **High-water mark** – once `total_backing + queued >= arm`, `hysteresis_armed` is set, enabling the drain phase. |
| `disk_backpressure_hysteresis_trigger` | y KB         | **Low-water mark** – while armed and `total_backing + queued < trigger`, `IsDraining()=true`. New socket reads are blocked until the queue fully drains to memory. |


**Why hysteresis?**

We want to keep the disk queue active as long as it's busy and we also want to avoid the disk tax for pipelines that are being drained. Think of DrainDiskQueue reads to io_buf_, RecvNotification fires and we are forced to write
to disk. With hysterisis, we allow backpressure to fall naturally to tcp buffers while we drain the last chunks from the queue. Also note, we can use a staging buffer within the disk queue but I don't think it's worth it right now.

---

## 2. `DiskBackedQueue` State Machine

```mermaid
stateDiagram-v2
    [*] --> IDLE
    IDLE --> ACTIVE : PushAsync called
    ACTIVE --> IDLE : backing empty, both tracks idle, hysteresis_armed reset
    ACTIVE --> CANCELLED : Cancel or I/O error

    state ACTIVE {
        state WriteTrack {
            [*] --> WriteIdle
            WriteIdle --> Writing : PushAsync - MaybeFlushQueue
            Writing --> WriteIdle : write CQE fires, queue empty
            Writing --> Writing   : write CQE fires, more queued - chain next write
        }
        --
        state ReadTrack {
            [*] --> ReadIdle
            ReadIdle --> Draining : hysteresis armed AND total+queued below trigger
            ReadIdle --> Popping  : DrainDiskQueue calls PopAsync
            Draining --> Popping  : DrainDiskQueue calls PopAsync - CanPush false during drain
            Draining --> ReadIdle : backing empty - hysteresis_armed reset
            Popping --> ReadIdle  : read CQE fires, bytes to io_buf
        }
    }

    CANCELLED --> [*] : ConnectionFlow teardown waits IsActive = false
```

> **Note**: The two tracks run concurrently. A write CQE and a read CQE can be in-flight
> simultaneously — they always target different file offsets (writes append at `write_offset`,
> reads consume from `next_read_offset`). `IsActive() = false` only when **both** tracks
> are idle and `total_backing_bytes = 0`.

### Key predicates

```
IsActive()   = (!cancelled && total_backing_bytes > 0)
             || !write_queue.empty()
             || write_in_flight
             || pop_in_flight

IsDraining() = hysteresis_armed
             && (total_backing_bytes + queued_bytes) < hysteresis_trigger

CanPush(n)   = !cancelled
             && !IsDraining()
             && (total_backing_bytes + queued_bytes + n) < max_backing_size
```

---

## 3. `DrainDiskQueue` – per-loop drain step

Called once per `IoLoopV2` iteration, before `ReadPendingInput`.

```mermaid
flowchart TD
    A(["DrainDiskQueue called"]) --> B{"threshold=0 or disk empty?"}
    B -- yes --> Z(["return – no-op"])
    B -- no  --> C{"pop already in-flight?"}
    C -- yes --> Z
    C -- no  --> D{"io_buf_ append space = 0?"}
    D -- yes --> Z
    D -- no  --> E{"parsed_cmd_q_bytes_ >= threshold?"}
    E -- yes --> Z2(["return – pipeline full, wait for cmd drain"])
    E -- no  --> F["PopAsync(io_buf_.AppendBuffer)"]
    F --> G(["CQE fires: io_buf_.CommitWrite, io_event_.notify"])
```

The guard on `parsed_cmd_q_bytes_ >= threshold` is the back-pressure gate: the fiber
must wait for shard threads to execute commands and free memory before draining more
disk data into the parser.

---

## 4. `NotifyOnRecv` – recv routing

Called from the io_uring recv callback (edge-triggered).

```mermaid
flowchart TD
    A(["NotifyOnRecv"]) --> B{"socket error?"}
    B -- yes --> Z(["set io_ec_, return"])
    B -- no  --> C{"should_offload?\ndisk active OR q_bytes >= threshold"}
    C -- no  --> D["TryRecv into io_buf_, CommitWrite"]
    C -- yes --> E{"CanPush(kMaxChunkSize)?"}
    E -- no  --> Z2(["return – TCP backpressure builds naturally"])
    E -- yes --> F["TryRecv into Chunk buffer"]
    F --> G{"recv error?"}
    G -- yes --> Z3(["set io_ec_ or drop EAGAIN, return"])
    G -- no  --> H["PushAsync chunk to disk"]
    H --> I{"pop_in_flight?"}
    I -- yes --> Z4(["return – pop CQE callback will notify"])
    I -- no  --> J(["io_event_.notify"])
```

---

## 5. `IoLoopV2` – main loop

```mermaid
flowchart TD
    START(["IoLoopV2 start"]) --> INIT["read offload_threshold\npre-init disk queue\nregister RecvOnNotify CB"]
    INIT --> LOOP

    LOOP(["loop iteration"]) --> MIG["HandleMigrateRequest"]
    MIG --> WAITER["subscribe cmd_completion_waiter if in-flight commands"]
    WAITER --> DRAIN["DrainDiskQueue"]

    DRAIN --> PIP{"pending_input_ AND NOT pop_in_flight?"}
    PIP -- yes --> READ["ReadPendingInput: TryRecv into io_buf_"]
    PIP -- no  --> BUFCHECK
    READ --> BUFCHECK

    BUFCHECK{"io_buf_ empty OR pop_in_flight?"} -- no --> PARSE_GATE
    BUFCHECK -- yes --> POLL["if empty AND NOT pop_in_flight: NotifyOnRecv poll"]
    POLL --> FLUSH["reply_builder_.Flush"]
    FLUSH --> AWAIT["io_event_.await — wakes on any of:\n• can_parse: InputLen gt 0 AND NOT pop_in_flight\n• cmd_ready: head can reply\n• cmd_exec:  head ready to execute\n• can_drain: disk non-empty AND q_bytes lt thr AND NOT pop_in_flight\n• can_poll:  pending_input_ AND NOT pop_in_flight\n• dispatch_q non-empty\n• migration requested\n• io_ec_ set"]
    AWAIT --> LOOP

    PARSE_GATE --> DISPATCH{"dispatch_q non-empty?"}
    DISPATCH -- yes --> CTRL["drain control messages, continue"]
    CTRL --> LOOP

    DISPATCH -- no --> FA{"force_await?\nq_bytes >= thr AND no cmd to execute"}
    FA -- no  --> OVERLIMIT{"pre_over_limit OR pop_in_flight OR io_buf_ empty?"}
    OVERLIMIT -- no  --> PARSE["ParseLoop: parse → execute → reply"]
    PARSE --> LOOP

    OVERLIMIT -- yes --> EXEC["ExecuteBatch / ReplyBatch"]
    FA -- yes --> EXEC
    EXEC --> FA2{"force_await?"}
    FA2 -- yes --> AWAIT2["io_event_.await:\ncmd_ready OR q_bytes &lt; thr OR io_ec_ set"]
    AWAIT2 --> BPL
    FA2 -- no  --> BPL
    BPL{"post_over_limit?"} -- yes --> AWAIT3["io_event_.await pipeline backpressure relief"]
    AWAIT3 --> LOOP
    BPL -- no --> LOOP
```

### `force_await` dual role

`force_await = threshold > 0 AND parsed_cmd_q_bytes_ >= threshold AND parsed_to_execute_ = null`

1. **Spin guard** – prevents the fiber from busy-looping when `io_buf_` has data but the
   pipeline is full. Forces a yield so shard threads can run and drain commands.
2. **Offload bypass guard** – `ReadPendingInput` calls `TryRecv` directly into `io_buf_`
   with no `should_offload()` check. With `force_await` set, the parse guard skips
   `ReadPendingInput`, ensuring bytes continue going to disk rather than leaking into
   the in-memory buffer.

### The pop-in-flight `io_event_` notify guards

Two sites suppress `io_event_.notify()` while a pop CQE owns `io_buf_.AppendBuffer()`:

| Site | Guard |
|------|-------|
| `RegisterOnRecv` lambda | `if (!disk_queue_ \|\| !IsPopInFlight()) notify()` |
| `PushAsync` write-CQE callback | `if (!IsPopInFlight()) notify()` |

Without both guards the fiber wakes, sees `can_parse=false` (buffer half-written),
loops again, and starves the proactor that needs to fire the pop CQE.

The await predicate adds the matching guard on `can_poll`:
```cpp
bool can_poll = pending_input_ && !pop_in_flight;
```
This prevents spinning on `ReadPendingInput` (which is guarded by `!pop_in_flight`
at its call site) while `pending_input_` stays true.

---

## 6. End-of-connection teardown

```mermaid
sequenceDiagram
    participant CF as ConnectionFlow
    participant DQ as DiskBackedQueue
    participant IO as io_event

    CF->>DQ: Cancel()
    Note over DQ: cancelled=true, flush pending write CBs with operation_canceled
    CF->>DQ: IsActive()?
    alt in-flight CQEs still pending
        CF->>IO: await NOT IsActive()
        DQ-->>IO: write/pop CQE lands, notify
        IO-->>CF: wakes
    end
    CF->>DQ: reset() / Close()
    Note over DQ: unlink backing file
```
