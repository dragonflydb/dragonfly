// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/disk_backed_queue.h"

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <string>
#include <thread>
#include <vector>

#include "base/flags.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "io/io.h"
#include "util/fibers/uring_proactor.h"

namespace dfly {
namespace {

using namespace facade;

TEST(DiskBackedQueueTest, ReadWrite) {
  auto proactor = std::make_unique<util::fb2::UringProactor>();

  auto pthread = std::thread{[ptr = proactor.get()] {
    static_cast<util::fb2::UringProactor*>(ptr)->Init(0, 8);
    ptr->Run();
  }};

  proactor->Await([]() {
    DiskBackedQueue backing(1 /* id */);
    EXPECT_FALSE(backing.Init());

    std::vector<std::string> commands;
    for (size_t i = 0; i < 100; ++i) {
      commands.push_back(absl::StrCat("SET FOO", i, " BAR"));
      auto ec = backing.Push(commands.back());
      EXPECT_FALSE(ec);
    }

    std::vector<std::string> results;
    for (size_t i = 0; i < 4; ++i) {
      auto ec = backing.PopN([&](io::MutableBytes read) {
        std::string str(reinterpret_cast<const char*>(read.data()), read.size());
        results.push_back(std::move(str));
      });
      EXPECT_FALSE(ec);
    }

    EXPECT_EQ(results.size(), commands.size());

    for (size_t i = 0; i < results.size(); ++i) {
      EXPECT_EQ(results[i], commands[i]);
    }
  });

  proactor->Stop();
  pthread.join();
}

}  // namespace
}  // namespace dfly
