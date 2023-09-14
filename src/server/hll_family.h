// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

/// @brief This would implement HLL (HyperLogLog, aka PF) related commands: PFADD, PFCOUNT, PFMERGE
/// For more details about these command see:
///     PFADD: https://redis.io/commands/pfadd/
///     PFCOUNT: https://redis.io/commands/pfcount/
///     PFMERGE: https://redis.io/commands/pfmerge/
namespace dfly {
class CommandRegistry;

class HllFamily {
 public:
  /// @brief Register the function that would be called to operate on user commands.
  /// @param registry The location to which the handling functions would be registered.
  ///
  /// We are assuming that this would have a valid registry to work on (i.e this do not point to
  /// null!).
  static void Register(CommandRegistry* registry);

  static const char kInvalidHllErr[];
};

}  // namespace dfly
