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

  // Used when the running operation yields. Suspend checkpoints the completed segment, and Resume
  // starts a fresh segment from current memory.
  void Suspend();
  void Resume();

  ~CmdMemoryScope();

 private:
  // Updates the delta using current memory vs baseline and then resets baseline. To be used before
  // suspend as well as at final accounting.
  void Checkpoint(int64_t used_memory);

  int obj_type_;
  int64_t mem_baseline_ = 0;

  // Raw memory movement accumulated by completed running segments of this scope.
  int64_t delta_ = 0;

  // The sum of movements in all child scopes below this scope
  int64_t child_delta_ = 0;
  // The part of the delta that we do not want to count, eg key allocations
  int64_t deductions_ = 0;

  bool suspended_ = false;
  CmdMemoryScope* parent_ = nullptr;
};

void MarkDeductedFromCurrentScope(int64_t bytes);
void SuspendCurrentCmdMemoryScope();
void ResumeCurrentCmdMemoryScope();

template <typename F> auto WithMemTrack(int obj_type, F f) {
  CmdMemoryScope scope(obj_type);
  return f();
}

}  // namespace dfly
