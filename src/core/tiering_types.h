// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <boost/intrusive/list_hook.hpp>

#include "core/compact_object.h"

namespace dfly::tiering {

struct TieredColdRecord : public ::boost::intrusive::list_base_hook<
                              boost::intrusive::link_mode<boost::intrusive::normal_link>> {
  uint64_t key_hash;  // Allows searching the entry in the dbslice.
  CompactValue value;
  uint16_t db_index;
  uint32_t page_index;
};
static_assert(sizeof(TieredColdRecord) == 48);

}  // namespace dfly::tiering
