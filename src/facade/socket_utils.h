// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>

namespace dfly {

// Returns information about the TCP socket state by its descriptor
std::string GetSocketInfo(int socket_fd);

}  // namespace dfly
