// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>

#include "base/gtest.h"
#include "base/logging.h"
#include "util/fibers/fibers.h"
#include "util/fibers/pool.h"

namespace dfly::tiering {

class PoolTestBase : public testing::Test {
 protected:
  void SetUp() override {
    pp_.reset(util::fb2::Pool::IOUring(16, 2));
    pp_->Run();
  }

  void TearDown() override {
    pp_->Stop();
    pp_.reset();
  }

  std::unique_ptr<util::ProactorPool> pp_;
};

}  // namespace dfly::tiering
