// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>

namespace dfly {

class CommandRegistry;
struct CompactValue;

class StreamMemTracker {
 public:
  StreamMemTracker();

  void UpdateStreamSize(CompactValue& pv) const;

 private:
  size_t start_size_{0};
};

class StreamFamily {
 public:
  static void Register(CommandRegistry* registry);
};

}  // namespace dfly
