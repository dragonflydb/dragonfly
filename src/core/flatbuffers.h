// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#ifndef __USE_GNU  // needed to flatbuffers to compile with musl libc.
#define FLATBUFFERS_LOCALE_INDEPENDENT 0
#endif

#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/flexbuffers.h>
#include <flatbuffers/idl.h>

namespace dfly {
using FlatJson = flexbuffers::Reference;
}  // namespace dfly
