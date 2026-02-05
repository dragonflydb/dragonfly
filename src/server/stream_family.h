// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common.h"

namespace dfly {

class CommandRegistry;

class CompactObj;
using PrimeValue = CompactValue;

class StreamMemTracker {
 public:
  StreamMemTracker();

  void UpdateStreamSize(PrimeValue& pv) const;

 private:
  size_t start_size_{0};
};

class StreamFamily {
 public:
  static void Register(CommandRegistry* registry);
};

}  // namespace dfly
