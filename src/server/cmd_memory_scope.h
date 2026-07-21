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

  // An escape hatch. It tells the scope not to count this many bytes when it computes delta. For
  // example when we want to exclude key size from calculation, or more generally some object was
  // allocated using the same memory resource we are tracking, but we do not want to count it.
  void MarkDeducted(int64_t bytes);

  ~CmdMemoryScope();

 private:
  int obj_type_;
  int64_t mem_baseline_ = 0;

  // The sum of movements in all child scopes below this scope
  int64_t child_delta_ = 0;
  // The part of the delta that we do not want to count, eg key allocations
  int64_t deductions_ = 0;

  CmdMemoryScope* parent_ = nullptr;
};

void MarkDeductedFromCurrentScope(int64_t bytes);

template <typename F> auto WithMemTrack(int obj_type, F f) {
  CmdMemoryScope scope(obj_type);
  return f();
}

}  // namespace dfly
