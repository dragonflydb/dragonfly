// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <variant>

#include "util/fibers/synchronization.h"

namespace dfly::tiering {

// Points to location of stored blob, usually managed by Storage
struct BlobLocator {
  size_t offset, len;
};

// Interface for simple fiber-blocking disk storage.
class Storage {
 public:
  virtual std::string Read(BlobLocator) = 0;
  virtual BlobLocator Store(std::string_view value) = 0;
  virtual void Delete(BlobLocator) = 0;
};

// Interface for reporting back events back to overseeing key storage.
class KeyStorage {
 public:
  // Report an item as successfully stored under specified locator.
  // The locator is valid until:
  // - a DEL operation is enqueued
  // - ReportFetched was called and returned true
  virtual void ReportStored(std::string_view key, BlobLocator locator) = 0;

  // Report an item as fetched, only called while holding valid blob locator.
  // Retruns true if item was cached in memory and should be deleted.
  virtual bool ReportFetched(std::string_view key, std::string_view value) = 0;
};

// Descriptor of pending storage operation. Based on communication with key store, the
// following statements are valid:
// - a READ cannot be enqueued after any STORE or DEL (its locator will be invalid)
// - at most two write operations can be enqueued - STORE after DEL
struct Op {
  enum {
    READ,   // Read from valid blob locator
    STORE,  // Request string to be stored, no valid locator shall exist
    DEL,    // Delete valid blob locator
  } type;

  // BlobLocator for READ/DEL, string for STORE
  std::variant<BlobLocator, std::string> value{};

  // Called once executed with current value for reads and empty content for writes.
  std::function<void(std::string_view)> callback{};
};

// Simple thread-safe fiber-blocking future for waiting for value. Pass by value.
template <typename T> struct Future {
  Future() : block{std::make_shared<Block>()} {
  }

  T Get() {
    block->waker.await([this] { return block->value.has_value(); });
    return std::move(*block->value);
  }

  void Resolve(T result) {
    block->value = std::move(result);
    block->waker.notify();
  }

 private:
  struct Block {
    std::optional<T> value;
    util::fb2::EventCount waker;
  };
  std::shared_ptr<Block> block;
};

template <> struct Future<void> : private Future<std::monostate> {
  Future() : Future<std::monostate>{} {
  }

  void Wait() {
    Future<std::monostate>::Get();
  }

  void Resolve() {
    Future<std::monostate>::Resolve({});
  }
};

}  // namespace dfly::tiering
