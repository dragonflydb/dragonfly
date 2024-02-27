// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <memory>
#include <string>

#include "util/fibers/synchronization.h"

namespace dfly::tiering {

// Interface for simple fiber-blocking disk storage.
class Storage {
 public:
  virtual std::string Read(std::string_view key) = 0;
  virtual void Write(std::string_view key, std::string value) = 0;
  virtual void Delete(std::string_view key) = 0;
};

// Descriptor of pending storage operation.
struct Op {
  enum { GET, SET, DEL } type;
  std::string value{};
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
