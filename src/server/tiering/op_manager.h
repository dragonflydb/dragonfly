// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "server/tiering/common.h"
#include "util/fibers/fiber2.h"
#include "util/fibers/synchronization.h"

namespace dfly::tiering {

// Single-thread MPSC queue for asynchronous storage operations handled by a worker fiber.
// Reorders operations interally to mimize storage access, guarantees order stability only relative
// to single key.
class OpQueue {
 public:
  explicit OpQueue(Storage* storage) : storage_{storage} {
  }

  void Start();       // Start worker fiber
  void Stop();        // Request stop, doesn't unblock storage operation
  void TEST_Drain();  // Wait for queue to drain before stopping

  // Enqueue operation asynchronously. Callback is invoked once the operation was processed,
  // it does not guarantee the value was flushed to storage (or ever will if skipped).
  void Enqueue(std::string_view key, Op op);

 private:
  void Loop();  // worker fiber loop

 private:
  Storage* storage_;

  bool cancelled_ = false;
  util::fb2::EventCount waker_;
  util::fb2::Fiber worker_;

  std::deque<std::string> key_queue_;
  absl::flat_hash_map<std::string, std::vector<Op>> key_ops_;
};

// Manages a pool of queues for asynchronously dispatching storage operations.
// Operations can be viewed as "instantaneous" - their effects will be observed by all following
// accesses to the same key, even if they are still pending. Guarantees stable order
// only relative to single key, otherwise free to reorder arbitrarily.
class OpManager {
 public:
  OpManager(Storage* storage, unsigned num_queues);

  void Start();
  void Stop();

  Future<std::string> Read(std::string_view key);

  // Resolving does NOT guarantee that the write/delete operation actually was executed by the
  // storage. It might happen later or be skipped at all. Use it only for backpressure.
  using Backpressure = Future<void>;
  Backpressure Write(std::string_view key, std::string value);
  Backpressure Delete(std::string_view key);

 private:
  OpQueue* GetQueue(std::string_view key);

 private:
  std::vector<std::unique_ptr<OpQueue>> queues_;  // immovable, so wrap in pointer
};

};  // namespace dfly::tiering
