// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/query_driver.h"

namespace dfly {
namespace search {

QueryDriver::QueryDriver() : scanner_(std::make_unique<Scanner>()) {
}

QueryDriver::~QueryDriver() {
}

}  // namespace search

}  // namespace dfly
