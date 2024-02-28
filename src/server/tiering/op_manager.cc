// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/op_manager.h"

#include <optional>

#include "base/logging.h"

namespace dfly::tiering {

void OpQueue::Start() {
  DCHECK(storage_);
  worker_ = util::MakeFiber(&OpQueue::Loop, this);
}

void OpQueue::Stop() {
  cancelled_ = true;
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

  // DEL can only be followed by SET because we expect the keystore to track item validity
  DCHECK(it->second.empty() || it->second.back().type != Op::DEL || op.type == Op::SET);

  it->second.push_back(std::move(op));

  if (inserted) {
    key_queue_.emplace_back(key);
    waker_.notify();
  }
}

void OpQueue::Loop() {
  while (true) {
    waker_.await([this] { return !key_queue_.empty() || cancelled_; });
    if (cancelled_)
      return;

    const auto& key = key_queue_.front();
    std::optional<std::string> value;

    // Any write invalidates the stored value, so read only if necessary.
    // Note: this is a suspension point, key_ops_ can grow!
    if (key_ops_[key].front().type == Op::GET)
      value = storage_->Read(key);

    // Atomically fulfill all pending operations
    bool updated = false;
    {
      util::FiberAtomicGuard guard;
      for (auto& op : key_ops_[key]) {
        // We rely on the keystore to provide us operations in valid order
        switch (op.type) {
          case Op::GET:
            DCHECK(value);  // No GET after DEL
            break;
          case Op::DEL:
            DCHECK(value);  // No DEL after DEL
            value.reset();
            break;
          case Op::SET:
            value = std::move(op.value);
            break;
        }

        updated |= op.type != Op::GET;
        if (op.callback)
          op.callback(value ? std::string_view(*value) : "");
      }
    }

    // Remove all executed operations
    key_ops_.erase(key);

    // If value was updated, issue operation to storage
    if (updated) {
      if (value)
        storage_->Write(key, *value);
      else
        storage_->Delete(key);
    }

    key_queue_.pop_front();
  }
}

OpManager::OpManager(Storage* storage, unsigned num_queues) {
  queues_.reserve(num_queues);
  for (size_t i = 0; i < num_queues; i++)
    queues_.emplace_back(std::make_unique<OpQueue>(storage));
}

void OpManager::Start() {
  for (auto& queue : queues_)
    queue->Start();
}

void OpManager::Stop() {
  for (auto& queue : queues_)
    queue->Stop();
}

Future<std::string> OpManager::Read(std::string_view key) {
  Future<std::string> future;
  auto cb = [future](std::string_view value) mutable { future.Resolve(std::string{value}); };
  GetQueue(key)->Enqueue(key, {Op::GET, "", std::move(cb)});
  return future;
}

Future<void> OpManager::Write(std::string_view key, std::string value) {
  Future<void> future;
  auto cb = [future](std::string_view) mutable { future.Resolve(); };
  GetQueue(key)->Enqueue(key, {Op::SET, std::move(value), std::move(cb)});
  return future;
}

Future<void> OpManager::Delete(std::string_view key) {
  Future<void> future;
  auto cb = [future](std::string_view) mutable { future.Resolve(); };
  GetQueue(key)->Enqueue(key, {Op::DEL, "", std::move(cb)});
  return future;
}

OpQueue* OpManager::GetQueue(std::string_view key) {
  return queues_[std::hash<std::string_view>{}(key) % queues_.size()].get();
}

}  // namespace dfly::tiering
