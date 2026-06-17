// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
// A minimal demo that implements SET/GET commands with Dragonfly's
// shard-per-thread architecture: each proactor thread owns one shard (its entry in the global
// g_shards vector, holding that shard's hash map), and commands are dispatched to the correct shard
// through a per-shard fb2::FiberQueue (MPSC) using the SuspendedCommand async
// mechanism. Each proactor runs a consumer fiber draining its own queue,
// while connection fibers on any thread enqueue callbacks via Add().
// The synchronous fallback uses FiberQueue::Await when async is not
// supported by the caller.

#include <mimalloc-new-delete.h>  // Routes global operator new/delete through mimalloc.
#include <xxhash.h>

#include <coroutine>

#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "base/cycle_clock.h"
#include "base/init.h"
#include "base/proc_util.h"
#include "facade/conn_context.h"
#include "facade/dragonfly_connection.h"
#include "facade/dragonfly_listener.h"
#include "facade/facade_stats.h"
#include "facade/facade_types.h"
#include "facade/reply_builder.h"
#include "facade/reply_payload.h"
#include "facade/service_interface.h"
#include "util/accept_server.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/fibers/pool.h"
#include "util/fibers/synchronization.h"
#ifdef __linux__
#include "util/fibers/uring_proactor.h"
#endif
#include "util/http/http_common.h"
#include "util/http/http_handler.h"
#include "util/http/http_server_utils.h"

ABSL_FLAG(uint32_t, port, 6379, "server port");
ABSL_FLAG(uint32_t, fq_size, 256, "per-shard FiberQueue capacity");
ABSL_FLAG(uint16_t, uring_recv_buffer_cnt, 0,
          "How many buffer ring entries to allocate per thread for io_uring receive operations. "
          "Relevant only for modern kernels with io_uring enabled");
// No-op flags accepted for compatibility with Dragonfly invocation patterns.
// ok_backend always binds on all interfaces (AcceptServer default) and has no persistence.
ABSL_FLAG(std::string, bind, "", "Bind address (no-op in ok_backend)");
ABSL_FLAG(std::string, dbfilename, "", "DB filename (no-op in ok_backend)");

using namespace util;
using namespace std;
using absl::GetFlag;

namespace facade {

namespace {

// All state for one shard. g_shards[sid] is owned by shard sid: in the shared-nothing model each
// proactor thread owns exactly one shard and initializes its own slot (see RunEngine).
//  - `queue`/`consumer`: the MPSC FiberQueue and the fiber draining it. Both live on the owning
//    proactor thread, but callbacks are enqueued from connection fibers on ANY thread via
//    Add()/Await() -- which is why g_shards is global, not thread-local.
//  - `db`: this shard's slice of the keyspace (touched only by the owning thread).
struct Shard {
  void Set(string_view key, string_view value) {
    db.insert_or_assign(string(key), string(value));
  }

  optional<string> Get(string_view key) const {
    auto it = db.find(key);
    if (it == db.end())
      return nullopt;
    return it->second;
  }

  unique_ptr<fb2::FiberQueue> queue;
  fb2::Fiber consumer;
  absl::flat_hash_map<string, string> db;
};

// One entry per shard, indexed by proactor index. Resized once in RunEngine; each proactor thread
// then initializes its own entry.
vector<Shard> g_shards;

// The shard owned by the calling proactor thread. Queue callbacks run on their shard's owning
// thread, so inside a callback MyShard() is that shard.
Shard& MyShard() {
  return g_shards[fb2::ProactorBase::me()->GetPoolIndex()];
}

// Determine the owning shard for a given key.
constexpr uint64_t kShardHashSeed = 120577240643ULL;
unsigned KeyShard(string_view key, unsigned num_shards) {
  XXH64_hash_t hash = XXH64(key.data(), key.size(), kShardHashSeed);
  return hash % num_shards;
}

// Minimal coroutine type for async command dispatch.
// The coroutine starts immediately (suspend_never initial_suspend), dispatches
// work to a shard, suspends at co_await, and when resumed by SendReply()
// writes the reply directly to the connection's reply builder.
struct AsyncCmd {
  // C++20 coroutine contract: every coroutine return type must contain a nested
  // "promise_type" that the compiler uses to control the coroutine lifecycle.
  struct promise_type {
    AsyncCmd get_return_object() {
      return AsyncCmd{};
    }

    // Called before the coroutine body runs. suspend_never means "start executing
    // immediately" (eager start), so code before the first co_await runs inline
    // within the caller's context.
    std::suspend_never initial_suspend() noexcept {
      return {};
    }

    // Called after the coroutine body finishes (after co_return or falling off the end).
    // suspend_never means the coroutine frame is destroyed automatically — we don't
    // need to call handle.destroy() manually.
    std::suspend_never final_suspend() noexcept {
      return {};
    }

    // The coroutine has no meaningful return value (replies go to the reply builder).
    void return_void() {
    }

    // Required by the standard; we don't expect exceptions in this code.
    void unhandled_exception() {
      LOG(FATAL) << "Unhandled exception in AsyncCmd coroutine";
    }
  };

  // Allow `return SetAsync(...)` in DispatchCommand.
  operator DispatchResult() const noexcept {
    return DispatchResult::OK;
  }
};

struct CmdContext : public facade::ParsedCommand {
  void ReuseInternal() final {
    get_result.reset();
    batch_is_get = false;
  }

  fb2::EmbeddedBlockingCounter blocker{0};
  optional<string> get_result;

  // Set by DispatchSquashedBatch so the reply payload can be built after the shard work completes.
  bool batch_is_get = false;
};

// Custom awaiter that registers the coroutine with the connection at the exact
// suspension point. When a coroutine hits `co_await ResolveAwaiter{ctx}`:
//
//  1. await_ready() returns false → the coroutine will suspend.
//  2. await_suspend(h) is called with the coroutine's own handle. This is the
//     only place inside a coroutine where you can obtain the handle that will
//     be used to resume it. We call Resolve() here to register (blocker, handle)
//     with the ParsedCommand so the connection knows what to resume later.
//  3. The coroutine is now suspended. The shard thread eventually calls
//     blocker.Dec(), the connection detects CanReply()==true, calls SendReply()
//     which does handle.resume().
//  4. await_resume() runs — the coroutine continues after co_await and writes
//     the reply to the reply builder.
struct ResolveAwaiter {
  CmdContext* ctx;

  bool await_ready() const noexcept {
    return false;
  }

  void await_suspend(std::coroutine_handle<> h) const noexcept {
    ctx->Resolve(&ctx->blocker, h);
  }

  void await_resume() const noexcept {
  }
};

// Async SET: dispatches write to shard, suspends, then sends OK on resume.
AsyncCmd SetAsync(CmdContext* ctx, ProactorPool* pool) {
  string_view key = ctx->at(1);
  string_view value = ctx->at(2);
  unsigned shard_id = KeyShard(key, pool->size());

  ctx->blocker.Start(1);
  g_shards[shard_id].queue->Add([key, value, ctx] {
    MyShard().Set(key, value);
    ctx->blocker.Dec();
  });

  co_await ResolveAwaiter{ctx};

  // Resumed by CmdContext::SendReply() — write directly to reply builder.
  ctx->rb()->SendOk();
}

// Async GET: dispatches read to shard, suspends, then sends result on resume.
AsyncCmd GetAsync(CmdContext* ctx, ProactorPool* pool) {
  string_view key = ctx->at(1);
  unsigned shard_id = KeyShard(key, pool->size());

  ctx->blocker.Start(1);
  g_shards[shard_id].queue->Add([key, ctx] {
    ctx->get_result = MyShard().Get(key);
    ctx->blocker.Dec();
  });

  co_await ResolveAwaiter{ctx};

  DCHECK(!ctx->mc_command());  // We do not support MC protocol.

  // Resumed by CmdContext::SendReply() — write directly to reply builder.
  auto* rb = static_cast<RedisReplyBuilder*>(ctx->rb());
  if (ctx->get_result) {
    rb->SendBulkString(*ctx->get_result);
  } else {
    rb->SendNull();
  }
}

// Maximum number of commands a single destination shard may take in one squashed batch; the run
// stops at the first command whose target shard is already full. Bounds the work one shard callback
// does (and that shard's result residency), independent of pipeline depth.
constexpr unsigned kMaxSquashSize = 80;

// A leading run of squashable commands grouped by destination shard. per_shard[sid] holds the
// commands targeting shard sid, in parse order.
using ShardBuckets = vector<absl::InlinedVector<CmdContext*, 4>>;

// Result of the (shared) grouping pass: the run length and the number of distinct shards it spans.
struct GroupResult {
  unsigned processed = 0;   // number of commands folded into the batch
  unsigned num_active = 0;  // number of shards with at least one command
};

// Shared pass 1 for both squashers: classify a leading run of single-key SET/GET commands and group
// them by destination shard, setting batch_is_get on each accepted command. Stops at the first
// command that can't join: non-squashable (wrong arity, PING, COMMAND, unknown), or one whose
// destination shard has already taken kMaxSquashSize commands.
GroupResult GroupSquashableRun(ParsedCommand* first, unsigned count, unsigned num_shards,
                               ShardBuckets* per_shard) {
  GroupResult res;
  auto* cmd = first;
  for (unsigned i = 0; i < count; i++, cmd = cmd->next) {
    DCHECK(cmd && !cmd->empty());

    string_view name = cmd->Front();
    bool is_get = absl::EqualsIgnoreCase(name, "GET");
    bool is_set = absl::EqualsIgnoreCase(name, "SET");
    if (!is_get && !is_set)
      break;
    if ((is_get && cmd->size() < 2) || (is_set && cmd->size() < 3))
      break;

    auto* ctx = static_cast<CmdContext*>(cmd);
    unsigned sid = KeyShard(ctx->at(1), num_shards);
    DCHECK_LT(sid, num_shards);
    auto& bucket = (*per_shard)[sid];

    if (bucket.size() == kMaxSquashSize)  // this shard is full -> end the run here
      break;
    ctx->batch_is_get = is_get;

    // first command for this shard -> one more active shard
    res.num_active += unsigned(bucket.empty());
    bucket.push_back(ctx);
    res.processed++;
  }
  return res;
}

// Runs one shard's batched ops on the owning proactor thread (so MyShard() is this shard's slice):
// GET reads into the command's get_result, SET writes the value. Shared by both squashers' shard
// callbacks.
void RunShardOps(absl::Span<CmdContext* const> cmds) {
  auto& shard = MyShard();
  for (auto* ctx : cmds) {
    if (ctx->batch_is_get) {
      ctx->get_result = shard.Get(ctx->at(1));
    } else {
      shard.Set(ctx->at(1), ctx->at(2));
    }
  }
}

class OkService : public ServiceInterface {
 public:
  explicit OkService(ProactorPool* pool) : pool_(pool) {
  }

  DispatchResult DispatchCommand(ParsedArgs args, ParsedCommand* cmd, AsyncPreference mode) final;
  uint32_t DispatchSquashedBatch(ParsedCommand* first, unsigned count,
                                 ConnectionContext* cntx) final;
  void ConfigureHttpHandlers(util::HttpListenerBase* base, bool is_privileged) final;

  ConnectionContext* CreateContext(Connection* owner) final {
    return new ConnectionContext{owner};
  }

  ParsedCommand* AllocateParsedCommand() final {
    return new CmdContext{};
  }

 private:
  // Dispatches the run already grouped into per_shard (active = number of non-empty buckets): one
  // task per active shard, blocks the connection fiber until they finish, then stores a captured
  // payload per command for the connection to emit in parse order.
  void EmitBlockingBatch(unsigned num_active_shards, ShardBuckets* per_shard);

  // Synchronous fallback handlers (used when async is not available).
  DispatchResult HandleSetSync(ParsedCommand* cmd);
  DispatchResult HandleGetSync(ParsedCommand* cmd);

  ProactorPool* pool_;
};

DispatchResult OkService::DispatchCommand([[maybe_unused]] ParsedArgs args, ParsedCommand* cmd,
                                          AsyncPreference mode) {
  if (cmd->empty()) {
    cmd->rb()->SendError("ERR empty command");
    return DispatchResult::OK;
  }

  string_view cmd_name = cmd->Front();

  auto* cmd_ctx = static_cast<CmdContext*>(cmd);

  // Mark the command as deferred so the pipeline can dispatch subsequent commands
  // without waiting for this one to complete. Required ONLY_ASYNC,
  // and beneficial for PREFER_ASYNC for write batching.
  if (mode != AsyncPreference::ONLY_SYNC)
    cmd_ctx->SetDeferredReply();

  if (absl::EqualsIgnoreCase(cmd_name, "SET")) {
    if (cmd->size() < 3) {
      cmd->SendError("ERR wrong number of arguments for 'SET' command");
      return DispatchResult::OK;
    }
    if (mode == AsyncPreference::ONLY_SYNC)
      return HandleSetSync(cmd);

    return SetAsync(cmd_ctx, pool_);
  }

  if (absl::EqualsIgnoreCase(cmd_name, "GET")) {
    if (cmd->size() < 2) {
      cmd->SendError("ERR wrong number of arguments for 'GET' command");
      return DispatchResult::OK;
    }
    if (mode == AsyncPreference::ONLY_SYNC)
      return HandleGetSync(cmd);

    return GetAsync(cmd_ctx, pool_);
  }

  if (absl::EqualsIgnoreCase(cmd_name, "PING")) {
    cmd->SendSimpleString("PONG");
  } else if (absl::EqualsIgnoreCase(cmd_name, "COMMAND")) {
    cmd->SendSimpleString("OK");
  } else {
    cmd->SendError("ERR unknown command");
  }
  return DispatchResult::OK;
}

// Squash a leading run of single-key SET/GET commands. The grouping pass (GroupSquashableRun) folds
// the run into per-shard buckets; EmitBlockingBatch then dispatches one task per active shard, so
// cross-thread cost drops from one Add + one Dec per command to one Add + one Dec per active shard.
// Returns the run length consumed.
uint32_t OkService::DispatchSquashedBatch(ParsedCommand* first, unsigned count,
                                          [[maybe_unused]] ConnectionContext* cntx) {
  const unsigned num_shards = pool_->size();
  VLOG(1) << "DispatchSquashedBatch: " << count << " commands";

  // Grouping scratch, private to this call: the squasher can preempt in FiberQueue::Add() when a
  // shard queue is full, so a shared buffer could be corrupted by a re-entrant call on this thread.
  ShardBuckets per_shard(num_shards);
  GroupResult group = GroupSquashableRun(first, count, num_shards, &per_shard);
  if (group.processed == 0)
    return 0;

  EmitBlockingBatch(group.num_active, &per_shard);
  return group.processed;
}

// Blocking squasher: dispatch one task per active shard, block the connection fiber until every
// shard finishes (like dragonfly's squasher blocking on its transaction hops), then store each
// reply as a captured payload. Resolve() also marks the command deferred; the connection emits the
// payloads in parse order afterwards, so bucket order here does not matter. per_shard stays valid
// on this parked fiber's stack while the tasks run, so the shard callbacks reference it by pointer.
void OkService::EmitBlockingBatch(unsigned num_active_shards, ShardBuckets* per_shard) {
  fb2::BlockingCounter bc(num_active_shards);
  for (unsigned sid = 0; sid < per_shard->size(); sid++) {
    if (per_shard->at(sid).empty())
      continue;
    g_shards[sid].queue->Add([cmds = per_shard->at(sid), bc]() mutable {
      RunShardOps(cmds);
      bc->Dec();
    });
  }
  bc->Wait();

  for (auto& bucket : *per_shard) {
    for (auto* ctx : bucket) {
      if (!ctx->batch_is_get) {
        ctx->Resolve(payload::SimpleString{"OK"});
      } else if (ctx->get_result) {
        ctx->Resolve(payload::BulkString{std::move(*ctx->get_result)});
      } else {
        ctx->Resolve(payload::Null{});
      }
    }
  }
}

DispatchResult OkService::HandleSetSync(ParsedCommand* cmd) {
  string_view key = cmd->at(1);
  string_view value = cmd->at(2);
  unsigned shard_id = KeyShard(key, pool_->size());

  g_shards[shard_id].queue->Await([key, value] { MyShard().Set(key, value); });

  cmd->rb()->SendOk();
  return DispatchResult::OK;
}

DispatchResult OkService::HandleGetSync(ParsedCommand* cmd) {
  string_view key = cmd->at(1);
  unsigned shard_id = KeyShard(key, pool_->size());

  optional<string> result =
      g_shards[shard_id].queue->Await([key]() -> optional<string> { return MyShard().Get(key); });

  auto* rb = static_cast<RedisReplyBuilder*>(cmd->rb());
  if (result) {
    rb->SendBulkString(*result);
  } else {
    rb->SendNull();
  }
  return DispatchResult::OK;
}

void HandleMetrics(ProactorPool* pool, const util::http::QueryArgs&, util::HttpContext* send) {
  namespace h2 = boost::beast::http;

  // Aggregate facade stats from all proactor threads.
  FacadeStats total;
  fb2::Mutex mu;
  pool->AwaitFiberOnAll([&](auto*) {
    std::lock_guard lk(mu);
    total += *tl_facade_stats;
  });

  const auto& conn = total.conn_stats;
  const auto& reply = total.reply_stats;

  string body;

#define APPEND_BODY(...) absl::StrAppend(&body, __VA_ARGS__)

  // Connection metrics
  APPEND_BODY("# TYPE dragonfly_connections_received_total counter\n");
  APPEND_BODY("dragonfly_connections_received_total ", conn.conn_received_cnt, "\n");

  APPEND_BODY("# TYPE dragonfly_connected_clients gauge\n");
  APPEND_BODY("dragonfly_connected_clients ", conn.num_conns_main, "\n");

  APPEND_BODY("# TYPE dragonfly_blocked_clients gauge\n");
  APPEND_BODY("dragonfly_blocked_clients ", conn.num_blocked_clients, "\n");

  APPEND_BODY("# TYPE dragonfly_num_migrations counter\n");
  APPEND_BODY("dragonfly_num_migrations ", conn.num_migrations, "\n");

  // Command metrics
  APPEND_BODY("# TYPE dragonfly_commands_processed_total counter\n");
  APPEND_BODY("dragonfly_commands_processed_total ", conn.command_cnt_main, "\n");

  // Pipeline metrics
  APPEND_BODY("# TYPE dragonfly_pipelined_commands_total counter\n");
  APPEND_BODY("dragonfly_pipelined_commands_total ", conn.pipelined_cmd_cnt, "\n");

  APPEND_BODY("# TYPE dragonfly_pipelined_commands_duration_seconds counter\n");
  APPEND_BODY("dragonfly_pipelined_commands_duration_seconds ", conn.pipelined_cmd_latency * 1e-6,
              "\n");

  APPEND_BODY("# TYPE dragonfly_pipelined_wait_duration_seconds counter\n");
  APPEND_BODY("dragonfly_pipelined_wait_duration_seconds ", conn.pipelined_wait_latency * 1e-6,
              "\n");

  APPEND_BODY("# TYPE dragonfly_pipeline_queue_length gauge\n");
  APPEND_BODY("dragonfly_pipeline_queue_length ", conn.pipeline_queue_entries, "\n");

  APPEND_BODY("# TYPE dragonfly_pipeline_throttle_total counter\n");
  APPEND_BODY("dragonfly_pipeline_throttle_total ", conn.pipeline_throttle_count, "\n");

  APPEND_BODY("# TYPE dragonfly_pipeline_dispatch_calls_total counter\n");
  APPEND_BODY("dragonfly_pipeline_dispatch_calls_total ", conn.pipeline_dispatch_calls, "\n");

  APPEND_BODY("# TYPE dragonfly_pipeline_dispatch_commands_total counter\n");
  APPEND_BODY("dragonfly_pipeline_dispatch_commands_total ", conn.pipeline_dispatch_commands, "\n");

  APPEND_BODY("# TYPE dragonfly_pipeline_dispatch_flush_seconds counter\n");
  APPEND_BODY("dragonfly_pipeline_dispatch_flush_seconds ",
              conn.pipeline_dispatch_flush_usec * 1e-6, "\n");

  APPEND_BODY("# TYPE dragonfly_pipeline_dispatch_flush_total counter\n");
  APPEND_BODY("dragonfly_pipeline_dispatch_flush_total ", conn.pipeline_dispatch_flush_count, "\n");

  // Network I/O metrics
  APPEND_BODY("# TYPE dragonfly_net_input_bytes_total counter\n");
  APPEND_BODY("dragonfly_net_input_bytes_total ", conn.io_read_bytes, "\n");

  APPEND_BODY("# TYPE dragonfly_net_output_bytes_total counter\n");
  APPEND_BODY("dragonfly_net_output_bytes_total ", reply.io_write_bytes, "\n");

  APPEND_BODY("# TYPE dragonfly_net_input_recv_total counter\n");
  APPEND_BODY("dragonfly_net_input_recv_total ", conn.io_read_cnt, "\n");

  APPEND_BODY("# TYPE dragonfly_net_output_send_total counter\n");
  APPEND_BODY("dragonfly_net_output_send_total ", reply.io_write_cnt, "\n");

  APPEND_BODY("# TYPE dragonfly_net_read_yields_total counter\n");
  APPEND_BODY("dragonfly_net_read_yields_total ", conn.num_read_yields, "\n");

  // Reply metrics
  APPEND_BODY("# TYPE dragonfly_reply_total counter\n");
  APPEND_BODY("dragonfly_reply_total ", reply.send_stats.count, "\n");

  APPEND_BODY("# TYPE dragonfly_reply_duration_seconds counter\n");
  APPEND_BODY("dragonfly_reply_duration_seconds ",
              base::CycleClock::ToUsec(reply.send_stats.total_duration) * 1e-6, "\n");

  util::http::StringResponse resp = util::http::MakeStringResponse(h2::status::ok);
  util::http::SetMime(util::http::kTextMime, &resp);
  resp.body() = std::move(body);
  send->Invoke(std::move(resp));
}

void OkService::ConfigureHttpHandlers(util::HttpListenerBase* base, bool is_privileged) {
  base->RegisterCb("/metrics", [this](const auto& args, util::HttpContext* send) {
    HandleMetrics(pool_, args, send);
  });
}

void RunEngine(ProactorPool* pool, AcceptServer* acceptor) {
  OkService service(pool);

  Connection::Init(pool->size());
  pool->Await([](auto*) { tl_facade_stats = new FacadeStats; });

  // Create a FiberQueue per shard and start a consumer fiber draining it on the owning
  // proactor thread. The queue and its consumer fiber both live on that thread, so cross-thread
  // producers (connection fibers) only touch the lock-free queue + EventCount.
  const unsigned num_shards = pool->size();
  const uint32_t fq_size = GetFlag(FLAGS_fq_size);
  g_shards.resize(num_shards);
  pool->AwaitFiberOnAll([fq_size](unsigned index, ProactorBase*) {
    g_shards[index].queue = make_unique<fb2::FiberQueue>(fq_size);
    g_shards[index].consumer =
        fb2::Fiber(absl::StrCat("shard_q", index), [index] { g_shards[index].queue->Run(); });
  });

  acceptor->AddListener(GetFlag(FLAGS_port),
                        new Listener{Protocol::REDIS, &service, Listener::Role::MAIN});

  acceptor->Run();
  acceptor->Wait();

  // Stop each consumer fiber on its owning proactor thread and join it there.
  pool->AwaitFiberOnAll([](unsigned index, ProactorBase*) {
    g_shards[index].queue->Shutdown();
    g_shards[index].consumer.Join();
    g_shards[index].db.clear();
  });
  g_shards.clear();
}

}  // namespace

}  // namespace facade

#ifdef __linux__
#define USE_URING 1
#else
#define USE_URING 0
#endif

void RegisterBufRings(util::ProactorPool* pool) {
#ifdef __linux__
  auto bufcnt = GetFlag(FLAGS_uring_recv_buffer_cnt);
  if (bufcnt == 0) {
    return;
  }

  if (pool->at(0)->GetKind() != util::ProactorBase::IOURING) {
    LOG(WARNING) << "uring_recv_buffer_cnt requires io_uring proactor";
    return;
  }

  base::sys::KernelVersion kver;
  base::sys::GetKernelVersion(&kver);
  unsigned ver = kver.kernel * 100 + kver.major;
  if (ver < 602) {
    LOG(WARNING) << "uring_recv_buffer_cnt requires kernel >= 6.2";
    return;
  }

  CHECK_LE(bufcnt, 16384u);

  bufcnt = absl::bit_ceil(bufcnt);
  pool->AwaitBrief([&](unsigned, util::ProactorBase* pb) {
    auto* up = static_cast<fb2::UringProactor*>(pb);
    int res = up->RegisterBufferRing(facade::kRecvSockGid, bufcnt, facade::kRecvBufSize);
    if (res != 0) {
      LOG(ERROR) << "Failed to register buf ring: " << util::detail::SafeErrorMessage(res);
      exit(1);
    }
  });
  LOG(INFO) << "Registered a bufring with " << bufcnt << " buffers of size " << facade::kRecvBufSize
            << " per thread";
#endif
}

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  CHECK_GT(GetFlag(FLAGS_port), 0u);

#if USE_URING
  unique_ptr<util::ProactorPool> pp(fb2::Pool::IOUring(1024));
#else
  unique_ptr<util::ProactorPool> pp(fb2::Pool::Epoll());
#endif
  pp->Run();

  RegisterBufRings(pp.get());

  AcceptServer acceptor(pp.get());
  facade::RunEngine(pp.get(), &acceptor);

  pp->Stop();

  return 0;
}
