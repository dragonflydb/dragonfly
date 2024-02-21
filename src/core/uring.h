// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#ifdef __linux__
#include "util/fibers/uring_file.h"
#include "util/fibers/uring_proactor.h"
namespace dfly {

using util::fb2::FiberCall;
using util::fb2::LinuxFile;
using util::fb2::OpenLinux;
using util::fb2::OpenRead;

}  // namespace dfly

#endif
