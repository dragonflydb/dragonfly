// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>

namespace dfly {

class CmdMemoryScope {
 public:
  explicit CmdMemoryScope(int obj_type = -1);

  CmdMemoryScope(const CmdMemoryScope&) = delete;
  CmdMemoryScope& operator=(const CmdMemoryScope&) = delete;

  ~CmdMemoryScope();

 private:
  int obj_type_;
  int64_t mem_baseline_ = 0;

  // The sum of movements in all child scopes below this scope
  int64_t child_delta_ = 0;

  CmdMemoryScope* parent_ = nullptr;
};

template <typename F> auto WithMemTrack(int obj_type, F f) {
  CmdMemoryScope scope(obj_type);
  return f();
}

}  // namespace dfly
