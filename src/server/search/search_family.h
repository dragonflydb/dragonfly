// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

namespace dfly {
class CommandRegistry;

class SearchFamily {
 public:
  static void Register(CommandRegistry* registry);
  static void Shutdown();
};

}  // namespace dfly
