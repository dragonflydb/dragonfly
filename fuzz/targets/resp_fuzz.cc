// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

// AFL++ Fuzzing harness for RESP protocol with multiple virtual connections
// This harness initializes a full Dragonfly service in-process with IoUring
// and simulates multiple concurrent client connections.
//
// Input format: Fuzzer input is split into chunks, each chunk represents
// commands from a virtual connection. This tests concurrent command execution.

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
#include "server/conn_context.h"
#include "server/main_service.h"
#include "server/test_utils.h"
#include "util/fibers/pool.h"

ABSL_DECLARE_FLAG(bool, force_epoll);

using namespace dfly;
using namespace facade;
using namespace util;

namespace {

// Number of virtual connections to simulate
constexpr size_t kNumVirtualConnections = 4;

// Per-connection state
struct VirtualConnection {
  std::unique_ptr<TestConnection> conn;
  std::unique_ptr<SinkReplyBuilder> builder;
  std::unique_ptr<RedisParser> parser;
  ConnectionContext* context = nullptr;

  void Initialize(ProactorBase* proactor) {
    conn = std::make_unique<TestConnection>(Protocol::REDIS);
    context = static_cast<ConnectionContext*>(conn->cntx());
    context->ns = &namespaces->GetDefaultNamespace();
    context->skip_acl_validation = true;
    builder = std::make_unique<SinkReplyBuilder>(conn->socket());
    parser = std::make_unique<RedisParser>(false);  // server mode
  }

  void Reset() {
    builder = std::make_unique<SinkReplyBuilder>(conn->socket());
    parser = std::make_unique<RedisParser>(false);
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
    FLAGS_minloglevel = 2;  // ERROR level only
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
            cmd_args.emplace_back(arg.GetBuf());
          }
        }

        if (!cmd_args.empty()) {
          // Dispatch command through full service stack
          // This tests: parser → registry → transaction → shards → execution
          // with concurrent load from multiple "clients"
          service->DispatchCommand(CmdArgList{cmd_args}, vconn.builder.get(), vconn.context);
          vconn.context->transaction = nullptr;
        }

        // Reset parser for next command
        vconn.parser = std::make_unique<RedisParser>(false);
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

// For non-libFuzzer AFL++ mode, provide a main function
#ifndef FUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION
extern "C" int main(int argc, char** argv) {
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
#endif
