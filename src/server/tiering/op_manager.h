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
  explicit OpQueue(Storage* storage, KeyStorage* key_storage)
      : storage_{storage}, key_storage_{key_storage} {
  }

  void Start();       // Start worker fiber
  void Stop();        // Request stop, doesn't unblock storage operation
  void TEST_Drain();  // Wait for queue to drain before stopping

  // Enqueue operation, assumes valid order relative to key storage events.
  void Enqueue(std::string_view key, Op op);

  // Dismiss STORE operation that is enqueued (or in progress).
  void DismissStore(std::string_view key);

 private:
  void Loop();  // worker fiber loop

 private:
  Storage* storage_;
  KeyStorage* key_storage_;

  bool worker_stopped_ = false;
  util::fb2::EventCount waker_;
  util::fb2::Fiber worker_;

  bool operation_dismissed_ = false;
  std::deque<std::string> key_queue_;
  absl::flat_hash_map<std::string, std::vector<Op>> key_ops_;
};

// Manages a pool of queues for asynchronously dispatching storage operations.
// Operations can be viewed as "instantaneous" - their effects will be observed by all following
// accesses to the same key, even if they are still pending. Guarantees stable order
// only relative to single key, otherwise free to reorder arbitrarily.
class OpManager {
 public:
  OpManager(Storage* storage, KeyStorage* key_storage, unsigned num_queues);

  void Start();
  void Stop();

  // Enqueue read operation that resolves to stored value. Locator must be valid.
  Future<std::string> Read(std::string_view key, BlobLocator locator);

  // Equeue store operation, resolves when stored or cancelled.
  // Can be dismissed until stored event is emitted to the key storage.
  Future<void> Store(std::string_view key, std::string value);

  // Enqueue delete operaton, resolved when deleted.
  Future<void> Delete(std::string_view key, BlobLocator locator);

  // Dismiss enqueued store.
  void DismissStore(std::string_view key);

 private:
  OpQueue* GetQueue(std::string_view key);

 private:
  std::vector<std::unique_ptr<OpQueue>> queues_;  // immovable, so wrap in pointer
};

};  // namespace dfly::tiering
