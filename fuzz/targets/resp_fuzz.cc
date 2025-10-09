// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <fcntl.h>   // for open()
#include <unistd.h>  // for write()

#include <cstring>  // for strlen()
#include <memory>
#include <string_view>
#include <vector>

extern "C" {
#include "redis/zmalloc.h"
}

#include <mimalloc.h>

#include "base/init.h"
#include "base/logging.h"
#include "facade/dragonfly_connection.h"
#include "facade/redis_parser.h"
#include "facade/reply_builder.h"
#include "io/io.h"
#include "server/conn_context.h"
#include "server/main_service.h"
#include "server/test_utils.h"
#include "util/fibers/pool.h"

ABSL_DECLARE_FLAG(std::string, dbfilename);
ABSL_DECLARE_FLAG(std::string, dir);

// Simplified constants (no runtime flags)
static constexpr const char* kMonitorFile = "crash_commands.log";  // in current dir
static constexpr size_t kNumVirtualConnections = 1;                // single connection

using namespace dfly;
using namespace facade;
using namespace util;

namespace {

// Single virtual connection (deterministic)
// File descriptor for monitor output (opened once)
int g_monitor_fd = -1;

// Per-connection state
struct VirtualConnection {
  std::unique_ptr<TestConnection> conn;
  io::StringSink sink;  // In-memory sink for replies (no real socket needed)
  std::unique_ptr<RedisReplyBuilder> builder;
  std::unique_ptr<RedisParser> parser;
  dfly::ConnectionContext* context = nullptr;

  void Initialize(ProactorBase* proactor) {
    conn = std::make_unique<TestConnection>(Protocol::REDIS);
    context = static_cast<dfly::ConnectionContext*>(conn->cntx());
    context->ns = &namespaces->GetDefaultNamespace();
    context->skip_acl_validation = true;
    // Use StringSink instead of socket - replies go to memory, not network
    builder = std::make_unique<RedisReplyBuilder>(&sink);
    parser = std::make_unique<RedisParser>(RedisParser::Mode::SERVER);
  }

  void Reset() {
    sink.Clear();  // Clear previous replies
    builder = std::make_unique<RedisReplyBuilder>(&sink);
    parser = std::make_unique<RedisParser>(RedisParser::Mode::SERVER);
    context->transaction = nullptr;
  }
};

// Global state for fuzzer - initialized once
struct FuzzerState {
  std::unique_ptr<fb2::Pool> pool;
  std::unique_ptr<Service> service;
  std::vector<VirtualConnection> connections;
  bool initialized = false;

  void Initialize() {
    if (initialized)
      return;

    // Initialize mimalloc
    init_zmalloc_threadlocal(mi_heap_get_backing());

    // Deterministic single-threaded pool: IOUring on Linux, Epoll elsewhere
#ifdef __linux__
    pool.reset(fb2::Pool::IOUring(16, 1));
#else
    pool.reset(fb2::Pool::Epoll(1));
#endif
    pool->Run();

    // Disable snapshot saving for fuzzing (no need to save to disk)
    absl::SetFlag(&FLAGS_dbfilename, "");
    absl::SetFlag(&FLAGS_dir, "/tmp");

    // Initialize service (this is the core Dragonfly engine)
    service = std::make_unique<Service>(pool.get());
    service->Init(nullptr, {});  // No network, in-process only

    // Single virtual connection for determinism
    const size_t num_conns = kNumVirtualConnections;
    connections.resize(num_conns);
    for (size_t i = 0; i < num_conns; ++i) {
      pool->at(i % pool->size())->Await([this, i] {
        connections[i].Initialize(pool->at(i % pool->size()));
      });
    }

    initialized = true;

    // Configure logging into files in current directory
    std::string log_prefix = std::string("./") + "dragonfly";
    google::SetLogDestination(google::INFO, (log_prefix + ".INFO.").c_str());
    google::SetLogDestination(google::WARNING, (log_prefix + ".WARNING.").c_str());
    google::SetLogDestination(google::ERROR, (log_prefix + ".ERROR.").c_str());

    // Open monitor file for commands (always on)
    g_monitor_fd = open(kMonitorFile, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (g_monitor_fd >= 0) {
      const char* header = "=== Fuzzer Monitor Started (Persistent Mode) ===\n";
      write(g_monitor_fd, header, strlen(header));
    }
  }

  void ProcessInput(const uint8_t* data, size_t size) {
    if (!initialized || size == 0)
      return;

    // Single connection: process as a single chunk
    const size_t num_conns = connections.size();
    size_t chunk_size = size / num_conns;
    if (chunk_size == 0)
      chunk_size = size;

    std::vector<fb2::Fiber> fibers;
    fibers.reserve(num_conns);

    // Dispatch commands to different connections in parallel
    for (size_t conn_id = 0; conn_id < num_conns; ++conn_id) {
      size_t start = conn_id * chunk_size;
      size_t end = (conn_id == num_conns - 1) ? size : (conn_id + 1) * chunk_size;

      if (start >= size)
        break;

      size_t proactor_idx = conn_id % pool->size();
      fibers.push_back(pool->at(proactor_idx)->LaunchFiber([this, conn_id, data, start, end] {
        ProcessConnectionInput(conn_id, data + start, end - start);
      }));
    }

    // Wait for all connections to finish processing
    for (auto& fiber : fibers) {
      fiber.Join();
    }
  }

 private:
  void ProcessConnectionInput(size_t conn_id, const uint8_t* data, size_t size) {
    if (size == 0)
      return;

    auto& vconn = connections[conn_id];
    vconn.Reset();

    // Parse and execute all commands in this chunk
    uint32_t consumed = 0;
    while (consumed < size) {
      RespExpr::Vec args;
      RedisParser::Buffer buffer{data + consumed, size - consumed};
      uint32_t chunk_consumed = 0;

      auto result = vconn.parser->Parse(buffer, &chunk_consumed, &args);
      consumed += chunk_consumed;

      if (result == RedisParser::OK && !args.empty()) {
        // Convert RespExpr to CmdArgVec
        CmdArgVec cmd_args;
        for (const auto& arg : args) {
          if (arg.type == RespExpr::STRING) {
            auto buf = arg.GetBuf();
            cmd_args.emplace_back(reinterpret_cast<const char*>(buf.data()), buf.size());
          }
        }

        if (!cmd_args.empty()) {
          // Monitor mode - log commands to file (AFL++ intercepts stdout/stderr)
          if (g_monitor_fd >= 0) {
            std::string cmd_str = "[conn";
            cmd_str += std::to_string(conn_id + 1);  // Show conn1-conn4 instead of conn0-conn3
            cmd_str += "] ";
            for (size_t i = 0; i < cmd_args.size(); ++i) {
              if (i > 0)
                cmd_str += " ";
              cmd_str += "\"";
              cmd_str += std::string(cmd_args[i]);
              cmd_str += "\"";
            }
            cmd_str += "\n";
            // Write directly to file descriptor (bypasses AFL++ interception)
            write(g_monitor_fd, cmd_str.c_str(), cmd_str.length());
          }

          // Dispatch command through full service stack
          // This tests: parser → registry → transaction → shards → execution
          // with concurrent load from multiple "clients"
          service->DispatchCommand(CmdArgList{cmd_args}, vconn.builder.get(), vconn.context);
          vconn.context->transaction = nullptr;
        }

        // Reset parser for next command
        vconn.parser = std::make_unique<RedisParser>(RedisParser::Mode::SERVER);
      } else {
        // Invalid or incomplete command, stop processing this connection
        break;
      }
    }
  }
};

FuzzerState* g_state = nullptr;

}  // namespace

// AFL++ persistent mode entry point
// This is called repeatedly by AFL++ for each test case
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  // Initialize once (persistent mode)
  if (!g_state) {
    g_state = new FuzzerState();
    g_state->Initialize();
  }

  // Process fuzzer input
  g_state->ProcessInput(data, size);

  return 0;  // Always return 0 to continue fuzzing
}

// Main function for AFL++ persistent mode
int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  // AFL++ persistent mode with DEFERRED initialization
  // Initialize forkserver BEFORE heavy Dragonfly init
  unsigned char input_buf[64 * 1024];

#ifdef __AFL_HAVE_MANUAL_CONTROL
  __AFL_INIT();  // Initialize AFL++ forkserver FIRST
#endif

  // NOW initialize Dragonfly (after forkserver, so not measured as timeout)
  g_state = new FuzzerState();
  g_state->Initialize();

  // Persistent mode loop - process stays alive for multiple test cases
  while (__AFL_LOOP(10000)) {  // 10000 iterations before restart
    // Read ALL data from stdin until EOF (AFL++ closes stdin when done)
    ssize_t len = 0;
    ssize_t n;
    do {
      n = read(0, input_buf + len, sizeof(input_buf) - len);
      if (n > 0)
        len += n;
    } while (n > 0 && len < (ssize_t)sizeof(input_buf));

    if (len > 0) {
      LLVMFuzzerTestOneInput(input_buf, len);
    }
  }

  return 0;
}
