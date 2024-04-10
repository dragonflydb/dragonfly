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
  static void SetUpTestSuite();
  static void TearDownTestSuite();

  static std::unique_ptr<util::ProactorPool> pp_;
};

std::unique_ptr<util::ProactorPool> PoolTestBase::pp_ = nullptr;

void PoolTestBase::SetUpTestSuite() {
  pp_.reset(util::fb2::Pool::IOUring(16, 2));
  pp_->Run();
}

void PoolTestBase::TearDownTestSuite() {
  pp_->Stop();
  pp_.reset();
}

}  // namespace dfly::tiering
