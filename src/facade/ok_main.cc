// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
// A minimal demo that implements SET/GET commands with Dragonfly's
// shard-per-thread architecture: each proactor thread owns a thread-local
// hash map, and commands are dispatched to the correct shard via
// ProactorBase::DispatchBrief (non-blocking) using the SuspendedCommand
// async mechanism. Falls back to synchronous AwaitBrief when async is
// not supported by the caller.

#include <xxhash.h>

#include <coroutine>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "base/cycle_clock.h"
#include "base/init.h"
#include "facade/conn_context.h"
#include "facade/dragonfly_connection.h"
#include "facade/dragonfly_listener.h"
#include "facade/facade_stats.h"
#include "facade/reply_builder.h"
#include "facade/service_interface.h"
#include "util/accept_server.h"
#include "util/fibers/pool.h"
#include "util/fibers/synchronization.h"
#include "util/http/http_common.h"
#include "util/http/http_handler.h"
#include "util/http/http_server_utils.h"

ABSL_FLAG(uint32_t, port, 6379, "server port");

using namespace util;
using namespace std;
using absl::GetFlag;

namespace facade {

namespace {

// Thread-local shard storage — each proactor thread has its own hash map.
thread_local absl::flat_hash_map<string, string> shard_db;

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
  }

  fb2::EmbeddedBlockingCounter blocker{0};
  optional<string> get_result;
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
  string key(ctx->at(1));
  string value(ctx->at(2));
  unsigned shard_id = KeyShard(key, pool->size());

  ctx->blocker.Start(1);
  pool->at(shard_id)->DispatchBrief([k = std::move(key), v = std::move(value), ctx]() mutable {
    shard_db.insert_or_assign(std::move(k), std::move(v));
    ctx->blocker.Dec();
  });

  co_await ResolveAwaiter{ctx};

  // Resumed by Connection::SendReply() — write directly to reply builder.
  ctx->rb()->SendOk();
}

// Async GET: dispatches read to shard, suspends, then sends result on resume.
AsyncCmd GetAsync(CmdContext* ctx, ProactorPool* pool) {
  string key(ctx->at(1));
  unsigned shard_id = KeyShard(key, pool->size());

  ctx->blocker.Start(1);
  pool->at(shard_id)->DispatchBrief([k = std::move(key), ctx] {
    auto it = shard_db.find(k);
    if (it != shard_db.end())
      ctx->get_result = it->second;
    ctx->blocker.Dec();
  });

  co_await ResolveAwaiter{ctx};

  DCHECK(!ctx->mc_command());  // We do not support MC protocol.
  // Resumed by Connection::SendReply() — write directly to reply builder.
  auto* rb = static_cast<RedisReplyBuilder*>(ctx->rb());
  if (ctx->get_result) {
    rb->SendBulkString(*ctx->get_result);
  } else {
    rb->SendNull();
  }
}

class OkService : public ServiceInterface {
 public:
  explicit OkService(ProactorPool* pool) : pool_(pool) {
  }

  DispatchResult DispatchCommand(ParsedArgs args, ParsedCommand* cmd, AsyncPreference mode) final;
  DispatchManyResult DispatchManyCommands(ParsedCommand* head, unsigned count,
                                          SinkReplyBuilder* builder, ConnectionContext* cntx) final;
  void ConfigureHttpHandlers(util::HttpListenerBase* base, bool is_privileged) final;

  ConnectionContext* CreateContext(Connection* owner) final {
    return new ConnectionContext{owner};
  }

  ParsedCommand* AllocateParsedCommand() final {
    return new CmdContext{};
  }

 private:
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

// Relevant only for V1 pipelining flow.
// Currently does not implement squashing and is very naive.
// Use --enable_resp_io_loop_v2=true to go through the more optimized V2 flow
// that doesn't call DispatchManyCommands at all.
DispatchManyResult OkService::DispatchManyCommands(ParsedCommand* head, unsigned count,
                                                   SinkReplyBuilder* builder,
                                                   ConnectionContext* cntx) {
  for (unsigned i = 0; i < count; i++) {
    ParsedCommand* cmd = head;
    head = head->next;
    cmd->Init(builder, cntx);
    DispatchCommand(ParsedArgs{*cmd}, cmd, AsyncPreference::ONLY_SYNC);
  }
  return DispatchManyResult{
      .processed = static_cast<uint32_t>(count),
      .account_in_stats = true,
  };
}

DispatchResult OkService::HandleSetSync(ParsedCommand* cmd) {
  string_view key = cmd->at(1);
  string_view value = cmd->at(2);
  unsigned shard_id = KeyShard(key, pool_->size());

  pool_->at(shard_id)->AwaitBrief([k = string(key), v = string(value)]() mutable {
    shard_db.insert_or_assign(std::move(k), std::move(v));
  });

  cmd->rb()->SendOk();
  return DispatchResult::OK;
}

DispatchResult OkService::HandleGetSync(ParsedCommand* cmd) {
  string_view key = cmd->at(1);
  unsigned shard_id = KeyShard(key, pool_->size());

  optional<string> result = pool_->at(shard_id)->AwaitBrief([key]() -> optional<string> {
    auto it = shard_db.find(key);
    if (it == shard_db.end())
      return nullopt;
    return it->second;
  });

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

  // Connection metrics
  absl::StrAppend(&body, "# HELP connections_received_total Total connections received\n");
  absl::StrAppend(&body, "# TYPE connections_received_total counter\n");
  absl::StrAppend(&body, "connections_received_total ", conn.conn_received_cnt, "\n");

  absl::StrAppend(&body, "# HELP connected_clients Number of connected clients\n");
  absl::StrAppend(&body, "# TYPE connected_clients gauge\n");
  absl::StrAppend(&body, "connected_clients ", conn.num_conns_main, "\n");

  absl::StrAppend(&body, "# HELP blocked_clients Number of blocked clients\n");
  absl::StrAppend(&body, "# TYPE blocked_clients gauge\n");
  absl::StrAppend(&body, "blocked_clients ", conn.num_blocked_clients, "\n");

  absl::StrAppend(&body, "# HELP num_migrations Connection migrations between threads\n");
  absl::StrAppend(&body, "# TYPE num_migrations counter\n");
  absl::StrAppend(&body, "num_migrations ", conn.num_migrations, "\n");

  // Command metrics
  absl::StrAppend(&body, "# HELP commands_processed_total Total commands processed\n");
  absl::StrAppend(&body, "# TYPE commands_processed_total counter\n");
  absl::StrAppend(&body, "commands_processed_total ", conn.command_cnt_main, "\n");

  // Pipeline metrics
  absl::StrAppend(&body, "# HELP pipelined_commands_total Total pipelined commands\n");
  absl::StrAppend(&body, "# TYPE pipelined_commands_total counter\n");
  absl::StrAppend(&body, "pipelined_commands_total ", conn.pipelined_cmd_cnt, "\n");

  absl::StrAppend(&body,
                  "# HELP pipelined_commands_duration_seconds Total pipelined cmd latency\n");
  absl::StrAppend(&body, "# TYPE pipelined_commands_duration_seconds counter\n");
  absl::StrAppend(&body, "pipelined_commands_duration_seconds ", conn.pipelined_cmd_latency * 1e-6,
                  "\n");

  absl::StrAppend(&body, "# HELP pipelined_wait_duration_seconds Pipeline queue wait latency\n");
  absl::StrAppend(&body, "# TYPE pipelined_wait_duration_seconds counter\n");
  absl::StrAppend(&body, "pipelined_wait_duration_seconds ", conn.pipelined_wait_latency * 1e-6,
                  "\n");

  absl::StrAppend(&body, "# HELP pipeline_queue_length Pending commands in pipeline queue\n");
  absl::StrAppend(&body, "# TYPE pipeline_queue_length gauge\n");
  absl::StrAppend(&body, "pipeline_queue_length ", conn.pipeline_queue_entries, "\n");

  absl::StrAppend(&body, "# HELP pipeline_throttle_total Pipeline throttle events\n");
  absl::StrAppend(&body, "# TYPE pipeline_throttle_total counter\n");
  absl::StrAppend(&body, "pipeline_throttle_total ", conn.pipeline_throttle_count, "\n");

  absl::StrAppend(&body, "# HELP pipeline_dispatch_calls_total Pipeline batch dispatch calls\n");
  absl::StrAppend(&body, "# TYPE pipeline_dispatch_calls_total counter\n");
  absl::StrAppend(&body, "pipeline_dispatch_calls_total ", conn.pipeline_dispatch_calls, "\n");

  absl::StrAppend(&body,
                  "# HELP pipeline_dispatch_commands_total Commands via pipeline dispatch\n");
  absl::StrAppend(&body, "# TYPE pipeline_dispatch_commands_total counter\n");
  absl::StrAppend(&body, "pipeline_dispatch_commands_total ", conn.pipeline_dispatch_commands,
                  "\n");

  absl::StrAppend(&body,
                  "# HELP pipeline_dispatch_flush_seconds Pipeline dispatch flush duration\n");
  absl::StrAppend(&body, "# TYPE pipeline_dispatch_flush_seconds counter\n");
  absl::StrAppend(&body, "pipeline_dispatch_flush_seconds ",
                  conn.pipeline_dispatch_flush_usec * 1e-6, "\n");

  absl::StrAppend(&body, "# HELP pipeline_skip_flush_total Times pipeline flush was skipped\n");
  absl::StrAppend(&body, "# TYPE pipeline_skip_flush_total counter\n");
  absl::StrAppend(&body, "pipeline_skip_flush_total ", conn.skip_pipeline_flushing, "\n");

  // Network I/O metrics
  absl::StrAppend(&body, "# HELP net_input_bytes_total Total bytes read from network\n");
  absl::StrAppend(&body, "# TYPE net_input_bytes_total counter\n");
  absl::StrAppend(&body, "net_input_bytes_total ", conn.io_read_bytes, "\n");

  absl::StrAppend(&body, "# HELP net_output_bytes_total Total bytes written to network\n");
  absl::StrAppend(&body, "# TYPE net_output_bytes_total counter\n");
  absl::StrAppend(&body, "net_output_bytes_total ", reply.io_write_bytes, "\n");

  absl::StrAppend(&body, "# HELP net_input_recv_total Total read syscalls\n");
  absl::StrAppend(&body, "# TYPE net_input_recv_total counter\n");
  absl::StrAppend(&body, "net_input_recv_total ", conn.io_read_cnt, "\n");

  absl::StrAppend(&body, "# HELP net_output_send_total Total write syscalls\n");
  absl::StrAppend(&body, "# TYPE net_output_send_total counter\n");
  absl::StrAppend(&body, "net_output_send_total ", reply.io_write_cnt, "\n");

  absl::StrAppend(&body, "# HELP net_read_yields_total Read yields due to busy limit\n");
  absl::StrAppend(&body, "# TYPE net_read_yields_total counter\n");
  absl::StrAppend(&body, "net_read_yields_total ", conn.num_read_yields, "\n");

  // Reply metrics
  absl::StrAppend(&body, "# HELP reply_total Total reply send calls\n");
  absl::StrAppend(&body, "# TYPE reply_total counter\n");
  absl::StrAppend(&body, "reply_total ", reply.send_stats.count, "\n");

  absl::StrAppend(&body, "# HELP reply_duration_seconds Total reply send duration\n");
  absl::StrAppend(&body, "# TYPE reply_duration_seconds counter\n");
  absl::StrAppend(&body, "reply_duration_seconds ",
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

  acceptor->AddListener(GetFlag(FLAGS_port),
                        new Listener{Protocol::REDIS, &service, Listener::Role::MAIN});

  acceptor->Run();
  acceptor->Wait();
}

}  // namespace

}  // namespace facade

#ifdef __linux__
#define USE_URING 1
#else
#define USE_URING 0
#endif

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  CHECK_GT(GetFlag(FLAGS_port), 0u);

#if USE_URING
  unique_ptr<util::ProactorPool> pp(fb2::Pool::IOUring(1024));
#else
  unique_ptr<util::ProactorPool> pp(fb2::Pool::Epoll());
#endif
  pp->Run();

  AcceptServer acceptor(pp.get());
  facade::RunEngine(pp.get(), &acceptor);

  pp->Stop();

  return 0;
}
