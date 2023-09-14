// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

/// @brief This would implement bits related string commands: GETBIT, SETBIT, BITCOUNT, BITOP.
/// Note: The name of this derive from the same file name in Redis source code.
/// For more details about these command see:
///     BITPOS: https://redis.io/commands/bitpos/
///     BITCOUNT: https://redis.io/commands/bitcount/
///     BITFIELD: https://redis.io/commands/bitfield/
///     BITFIELD_RO: https://redis.io/commands/bitfield_ro/
///     BITOP: https://redis.io/commands/bitop/
///     GETBIT: https://redis.io/commands/getbit/
///     SETBIT: https://redis.io/commands/setbit/
namespace dfly {
class CommandRegistry;

class BitOpsFamily {
 public:
  /// @brief Register the function that would be called to operate on user commands.
  /// @param registry The location to which the handling functions would be registered.
  ///
  /// We are assuming that this would have a valid registry to work on (i.e this do not point to
  /// null!).
  static void Register(CommandRegistry* registry);
};

}  // end of namespace dfly
