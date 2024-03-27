// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <optional>

#include "util/fibers/synchronization.h"

namespace dfly::tiering {

struct DiskSegment {
  DiskSegment() = default;
  DiskSegment(size_t offset, size_t length) : offset{offset}, length{length} {
  }
  DiskSegment(std::pair<size_t, size_t> p) : offset{p.first}, length(p.second) {
  }

  // Mesured in bytes, offset should be aligned to page boundaries (4kb)
  size_t offset = 0, length = 0;
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
};  // namespace dfly::tiering
