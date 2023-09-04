// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/search/base.h"

namespace dfly::search {

FtVector BytesToFtVector(std::string_view value);

float VectorDistance(const FtVector& v1, const FtVector& v2);

}  // namespace dfly::search
