// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#ifdef USE_FB2

#include "util/fibers/uring_proactor.h"
#include "util/uring/uring_file.h"
namespace dfly {

using util::fb2::FiberCall;
using util::fb2::LinuxFile;
using util::fb2::OpenLinux;
using util::fb2::OpenRead;

}  // namespace dfly

#else
#include "util/uring/proactor.h"
#include "util/uring/uring_file.h"

namespace dfly {

using util::uring::FiberCall;
using util::uring::LinuxFile;
using util::uring::OpenLinux;
using util::uring::OpenRead;

}  // namespace dfly
#endif
