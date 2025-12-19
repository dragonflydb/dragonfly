// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

namespace dfly {
class CommandRegistry;

class JsonFamily {
 public:
  static void Register(CommandRegistry* registry);
};

}  // namespace dfly
