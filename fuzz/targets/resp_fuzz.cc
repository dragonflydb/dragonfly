// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

// AFL++ Fuzzing harness for RESP protocol with multiple virtual connections
// This harness initializes a full Dragonfly service in-process with IoUring
// and simulates multiple concurrent client connections.
//
// Input format: Fuzzer input is split into chunks, each chunk represents
// commands from a virtual connection. This tests concurrent command execution.

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

ABSL_DECLARE_FLAG(bool, force_epoll);
ABSL_FLAG(bool, fuzzer_monitor, false, "Log commands like redis-cli MONITOR");
ABSL_FLAG(std::string, fuzzer_monitor_file, "/tmp/dragonfly_fuzzer_commands.log",
          "File to write monitored commands");

using namespace dfly;
using namespace facade;
using namespace util;

namespace {

// Number of virtual connections to simulate
constexpr size_t kNumVirtualConnections = 4;
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

    // Create fiber pool - use IoUring on Linux (default), Epoll elsewhere
    // This matches production behavior
#ifdef __linux__
    if (absl::GetFlag(FLAGS_force_epoll)) {
      pool.reset(fb2::Pool::Epoll(4));  // 4 threads for realistic load
    } else {
      pool.reset(fb2::Pool::IOUring(16, 4));  // IoUring with 4 threads
    }
#else
    pool.reset(fb2::Pool::Epoll(4));
#endif
    pool->Run();

    // Initialize service (this is the core Dragonfly engine)
    service = std::make_unique<Service>(pool.get());
    service->Init(nullptr, {});  // No network, in-process only

    // Create multiple virtual connections to simulate concurrent clients
    connections.resize(kNumVirtualConnections);
    for (size_t i = 0; i < kNumVirtualConnections; ++i) {
      pool->at(i % pool->size())->Await([this, i] {
        connections[i].Initialize(pool->at(i % pool->size()));
      });
    }

    initialized = true;

    // Suppress logging to avoid performance overhead
    absl::SetFlag(&FLAGS_minloglevel, 2);  // ERROR level only

    // Open monitor file if requested
    if (absl::GetFlag(FLAGS_fuzzer_monitor)) {
      std::string log_file = absl::GetFlag(FLAGS_fuzzer_monitor_file);
      // O_APPEND - add to end of file, not overwrite
      g_monitor_fd = open(log_file.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
      if (g_monitor_fd >= 0) {
        const char* header = "\n=== Fuzzer Monitor Session ===\n";
        write(g_monitor_fd, header, strlen(header));
      }
    }
  }

  void ProcessInput(const uint8_t* data, size_t size) {
    if (!initialized || size == 0)
      return;

    // Split fuzzer input across multiple virtual connections
    // This simulates concurrent client activity
    size_t chunk_size = size / kNumVirtualConnections;
    if (chunk_size == 0)
      chunk_size = size;

    std::vector<fb2::Fiber> fibers;
    fibers.reserve(kNumVirtualConnections);

    // Dispatch commands to different connections in parallel
    for (size_t conn_id = 0; conn_id < kNumVirtualConnections; ++conn_id) {
      size_t start = conn_id * chunk_size;
      size_t end = (conn_id == kNumVirtualConnections - 1) ? size : (conn_id + 1) * chunk_size;

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
            cmd_str += std::to_string(conn_id);
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

// Main function for AFL++ (always compiled, not just for non-libFuzzer)
int main(int argc, char** argv) {
  // AFL++ will replace stdin with fuzzer input
  MainInitGuard guard(&argc, &argv);

  // Initialize fuzzer state
  g_state = new FuzzerState();
  g_state->Initialize();

  // Read from stdin (AFL++ provides input here)
  constexpr size_t kMaxInputSize = 64 * 1024;  // 64KB max
  std::vector<uint8_t> input;
  input.reserve(kMaxInputSize);

  uint8_t byte;
  while (std::cin.read(reinterpret_cast<char*>(&byte), 1) && input.size() < kMaxInputSize) {
    input.push_back(byte);
  }

  // Process input
  if (!input.empty()) {
    LLVMFuzzerTestOneInput(input.data(), input.size());
  }

  return 0;
}
