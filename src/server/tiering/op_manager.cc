// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/op_manager.h"

#include <absl/cleanup/cleanup.h>

#include <optional>

#include "base/logging.h"

namespace dfly::tiering {

void OpQueue::Start() {
  DCHECK(storage_);
  worker_ = util::MakeFiber(&OpQueue::Loop, this);
}

void OpQueue::Stop() {
  worker_stopped_ = true;
  waker_.notify();
  worker_.JoinIfNeeded();
}

void OpQueue::TEST_Drain() {
  while (!key_queue_.empty())
    util::ThisFiber::Yield();
  Stop();
}

void OpQueue::Enqueue(std::string_view key, Op op) {
  auto [it, inserted] = key_ops_.try_emplace(key);
  it->second.push_back(std::move(op));
  if (inserted) {
    key_queue_.emplace_back(key);
    waker_.notify();
  }
}

void OpQueue::DismissStore(std::string_view key) {
  // If the STORE operation is still enqueued, remove it from queue,
  // otherwise it must be handled currently, so dismiss it.
  if (auto it = key_ops_.find(key); it != key_ops_.end()) {
    auto& back = it->second.back();
    DCHECK_EQ(back.type, Op::STORE);
    if (back.callback)
      back.callback("");
    it->second.pop_back();
  } else {
    DCHECK_EQ(key_queue_.front(), key);
    operation_dismissed_ = true;
  }
}

void OpQueue::Loop() {
  while (true) {
    waker_.await([this] { return !key_queue_.empty() || worker_stopped_; });
    if (worker_stopped_)
      return;

    const auto& key = key_queue_.front();
    absl::Cleanup cleanup = [this] {
      key_queue_.pop_front();
      operation_dismissed_ = false;
    };

    VLOG(0) << "Handling " << key;

    std::optional<BlobLocator> locator;
    std::optional<std::string> value;

    // Perform read while allowing for more entries to be enqueued
    {
      const auto& operations = key_ops_[key];
      if (!operations.empty() && operations.front().type == Op::READ) {
        locator = std::get<BlobLocator>(operations.front().value);
        value = storage_->Read(*locator);
      }
    }

    std::vector<Op> operations = std::move(key_ops_.extract(key).mapped());
    if (operations.empty())
      continue;

    // Resolve all reads
    auto it = operations.begin();
    for (; it != operations.end() && it->type == Op::READ; ++it) {
      DCHECK(value);
      it->callback(*value);
    }

    // If only reads are present, report as fetched and continue
    if (it == operations.end()) {
      DCHECK(locator && value);
      if (key_storage_->ReportFetched(key, *value))
        storage_->Delete(*locator);
      continue;
    }

    DCHECK_LE(std::distance(it, operations.end()), 2);  // No more than DEL - STORE

    // Resolve delete
    if (it != operations.end() && it->type == Op::DEL) {
      storage_->Delete(std::get<BlobLocator>(it->value));
      if (it->callback)
        it->callback("");
      ++it;
    }

    if (it == operations.end())
      continue;

    DCHECK_EQ(std::distance(it, operations.end()), 1);  // STORE can't follow STORE
    DCHECK_EQ(it->type, Op::STORE);

    *locator = storage_->Store(std::move(std::get<std::string>(it->value)));
    if (it->callback)
      it->callback("");

    if (operation_dismissed_) {
      storage_->Delete(*locator);  // Dismissed while suspended
    } else {
      VLOG(0) << "Reporing stored " << key << " at " << locator->offset;
      key_storage_->ReportStored(key, *locator);
    }
  }
}

OpManager::OpManager(Storage* storage, KeyStorage* key_storage, unsigned num_queues) {
  queues_.reserve(num_queues);
  for (size_t i = 0; i < num_queues; i++)
    queues_.emplace_back(std::make_unique<OpQueue>(storage, key_storage));
}

void OpManager::Start() {
  for (auto& queue : queues_)
    queue->Start();
}

void OpManager::Stop() {
  for (auto& queue : queues_)
    queue->Stop();
}

Future<std::string> OpManager::Read(std::string_view key, BlobLocator locator) {
  Future<std::string> future;
  auto cb = [future](std::string_view value) mutable { future.Resolve(std::string{value}); };
  GetQueue(key)->Enqueue(key, {Op::READ, locator, std::move(cb)});
  return future;
}

Future<void> OpManager::Store(std::string_view key, std::string value) {
  Future<void> future;
  auto cb = [future](std::string_view) mutable { future.Resolve(); };
  GetQueue(key)->Enqueue(key, {Op::STORE, std::move(value), std::move(cb)});
  return future;
}

Future<void> OpManager::Delete(std::string_view key, BlobLocator locator) {
  Future<void> future;
  auto cb = [future](std::string_view) mutable { future.Resolve(); };
  GetQueue(key)->Enqueue(key, {Op::DEL, locator, std::move(cb)});
  return future;
}

void OpManager::DismissStore(std::string_view key) {
  GetQueue(key)->DismissStore(key);
}

OpQueue* OpManager::GetQueue(std::string_view key) {
  return queues_[std::hash<std::string_view>{}(key) % queues_.size()].get();
}

}  // namespace dfly::tiering
