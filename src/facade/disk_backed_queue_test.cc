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

    std::string commands;
    for (size_t i = 0; i < 100; ++i) {
      auto cmd = absl::StrCat("SET FOO", i, " BAR");
      auto bytes = io::MutableBytes(reinterpret_cast<uint8_t*>(cmd.data()), cmd.size());
      EXPECT_FALSE(backing.Write(bytes));
      absl::StrAppend(&commands, cmd);
    }

    std::string results;
    while (!backing.Empty()) {
      LOG(INFO) << "ping";
      std::string buf(1024, 'c');
      auto bytes = io::MutableBytes(reinterpret_cast<uint8_t*>(buf.data()), buf.size());
      auto res = backing.ReadTo(bytes);
      EXPECT_TRUE(res);
      absl::StrAppend(&results, buf.substr(0, *res));
    }

    EXPECT_EQ(results.size(), commands.size());
    EXPECT_EQ(results, commands);

    EXPECT_FALSE(backing.Close());
  });

  proactor->Stop();
  pthread.join();
}

}  // namespace
}  // namespace dfly
