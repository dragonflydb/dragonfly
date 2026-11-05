// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/lua_internals.h"

// Internal Lua headers from the Lua source tree. They define many generic macros, so only this file
// includes them.
extern "C" {
#include "ltable.h"
}

namespace dfly {

bool LuaTableHasHashPart(lua_State* lua, int idx) {
  const Table* table = static_cast<const Table*>(lua_topointer(lua, idx));
  return !isdummy(table);
}

}  // namespace dfly
