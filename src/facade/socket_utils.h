// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>

namespace dfly {

// Returns information about the TCP socket state by its descriptor
std::string GetSocketInfo(int socket_fd);

// Returns kernel queue sizes, readiness and TCP_INFO highlights of the socket by its descriptor.
// Used to diagnose stalled streams: tells apart data stuck in the sender's kernel queue,
// in the receiver's kernel queue, or not handed to the kernel at all.
std::string GetSocketQueuesInfo(int socket_fd);

}  // namespace dfly
