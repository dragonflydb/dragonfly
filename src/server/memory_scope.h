// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>

namespace dfly {

class MemoryScope {
 public:
  explicit MemoryScope(int obj_type);

  MemoryScope(const MemoryScope&) = delete;
  MemoryScope& operator=(const MemoryScope&) = delete;

  void Suspend();
  void Resume();

  ~MemoryScope();

 private:
  void Checkpoint(int64_t used_memory);

  int obj_type_;
  int64_t mem_baseline_ = 0;

  int64_t delta_ = 0;

  bool suspended_ = false;
};

}  // namespace dfly
