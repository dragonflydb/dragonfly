// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

namespace dfly {
class CommandRegistry;

class HllFamily {
 public:
  static void Register(CommandRegistry* registry);

  static const char kInvalidHllErr[];
};

}  // namespace dfly
