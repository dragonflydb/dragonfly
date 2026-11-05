// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

typedef struct lua_State lua_State;

namespace dfly {

// Returns whether the table at idx has a hash part. A table without one has only the integer keys
// of its array part.
bool LuaTableHasHashPart(lua_State* lua, int idx);

}  // namespace dfly
