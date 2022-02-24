// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/types/span.h>

namespace dfly {
using MutableSlice = absl::Span<char>;
using MutSliceSpan = absl::Span<MutableSlice>;

}  // namespace dfly
