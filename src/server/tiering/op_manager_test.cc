// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/op_manager.h"

#include <gtest/gtest.h>

#include <memory>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_cat.h"
#include "server/tiering/common.h"
#include "server/tiering/test_common.h"
#include "util/fibers/fibers.h"
#include "util/fibers/future.h"

namespace dfly::tiering {

using namespace std;
using namespace std::string_literals;

ostream& operator<<(ostream& os, const OpManager::Stats& stats) {
  return os << "pending_read_cnt: " << stats.pending_read_cnt
            << ", pending_stash_cnt: " << stats.pending_stash_cnt
            << ", alloc_bytes: " << stats.disk_stats.allocated_bytes
            << ", capacity_bytes: " << stats.disk_stats.capacity_bytes
            << ", heap_buf_allocs: " << stats.disk_stats.heap_buf_alloc_count
            << ", registered_buf_allocs: " << stats.disk_stats.registered_buf_alloc_count
            << ", max_file_size: " << stats.disk_stats.max_file_size
            << ", pending_ops: " << stats.disk_stats.pending_ops;
}

struct OpManagerTest : PoolTestBase, OpManager {
  OpManagerTest() : OpManager(256_MB) {
  }

  void Open() {
    EXPECT_FALSE(OpManager::Open("op_manager_test_backing"));
  }

  void Close() {
    OpManager::Close();
    EXPECT_EQ(unlink("op_manager_test_backing"), 0);
  }

  util::fb2::Future<std::string> Read(EntryId id, DiskSegment segment) {
    util::fb2::Future<std::string> future;
    Enqueue(id, segment, [future](bool, std::string* value) mutable {
      future.Resolve(*value);
      return false;
    });
    return future;
  }

  void NotifyStashed(EntryId id, const io::Result<DiskSegment>& segment) override {
    ASSERT_TRUE(segment);
    auto [it, inserted] = stashed_.emplace(id, *segment);
    ASSERT_TRUE(inserted);
  }

  bool NotifyFetched(EntryId id, std::string_view value, DiskSegment segment,
                     bool modified) override {
    fetched_[id] = value;
    return false;
  }

  bool NotifyDelete(DiskSegment segment) override {
    return true;
  }

  absl::flat_hash_map<EntryId, std::string> fetched_;
  absl::flat_hash_map<EntryId, DiskSegment> stashed_;
};

TEST_F(OpManagerTest, SimpleStashesWithReads) {
  pp_->at(0)->Await([this] {
    Open();

    for (unsigned i = 0; i < 100; i++) {
      EXPECT_FALSE(Stash(i, absl::StrCat("VALUE", i, "cancelled"), {}));
      EXPECT_FALSE(Stash(i, absl::StrCat("VALUE", i, "cancelled"), {}));
      EXPECT_FALSE(Stash(i, absl::StrCat("VALUE", i, "real"), {}));
    }

    EXPECT_EQ(GetStats().pending_stash_cnt, 100);

    while (stashed_.size() < 100)
      util::ThisFiber::SleepFor(1ms);

    EXPECT_EQ(GetStats().disk_stats.allocated_bytes, 100 * kPageSize) << GetStats();

    for (unsigned i = 0; i < 100; i++) {
      EXPECT_GE(stashed_[i].offset, i > 0);
      EXPECT_EQ(stashed_[i].length, 10 + (i > 9));
      EXPECT_EQ(Read(i, stashed_[i]).Get(), absl::StrCat("VALUE", i, "real"));
      EXPECT_EQ(fetched_.extract(i).mapped(), absl::StrCat("VALUE", i, "real"));
    }

    Close();
  });
}

TEST_F(OpManagerTest, DeleteAfterReads) {
  pp_->at(0)->Await([this] {
    Open();

    EXPECT_FALSE(Stash(0u, absl::StrCat("DATA"), {}));
    while (stashed_.empty())
      util::ThisFiber::SleepFor(1ms);

    std::vector<util::fb2::Future<std::string>> reads;
    for (unsigned i = 0; i < 100; i++)
      reads.emplace_back(Read(0u, stashed_[0u]));
    DeleteOffloaded(stashed_[0u]);

    for (auto& fut : reads)
      EXPECT_EQ(fut.Get(), "DATA");

    Close();
  });
}

TEST_F(OpManagerTest, ReadSamePageDifferentOffsets) {
  pp_->at(0)->Await([this] {
    Open();

    // Build single numbers blob
    std::string numbers = "H";  // single padding byte to recognize it as small keys
    std::vector<DiskSegment> number_segments;
    for (size_t i = 0; i < 100; i++) {
      std::string number = std::to_string(i);
      number_segments.emplace_back(numbers.size(), number.size());
      numbers += number;
    }

    EXPECT_FALSE(Stash(0u, numbers, {}));
    while (stashed_.empty())
      util::ThisFiber::SleepFor(1ms);

    EXPECT_EQ(stashed_[0u].offset, 0u);

    // Issue lots of concurrent reads
    std::vector<util::fb2::Future<std::string>> futures;
    for (size_t i = 0; i < 100; i++)
      futures.emplace_back(Read(std::make_pair(0, absl::StrCat("k", i)), number_segments[i]));

    for (size_t i = 0; i < 100; i++)
      EXPECT_EQ(futures[i].Get(), std::to_string(i));

    Close();
  });
}

TEST_F(OpManagerTest, Modify) {
  pp_->at(0)->Await([this] {
    Open();

    Stash(0u, "D", {});
    while (stashed_.empty())
      util::ThisFiber::SleepFor(1ms);

    // Atomically issue sequence of modify-read operations
    std::vector<util::fb2::Future<std::string>> futures;
    for (size_t i = 0; i < 10; i++) {
      Enqueue(0u, stashed_[0u], [i](bool, std::string* v) {
        absl::StrAppend(v, i);
        return true;
      });
      futures.emplace_back(Read(0u, stashed_[0u]));
    }

    // Expect futures to resolve with correct values
    std::string expected = "D";
    for (size_t i = 0; i < futures.size(); i++) {
      absl::StrAppend(&expected, i);
      EXPECT_EQ(futures[i].Get(), expected);
    }

    Close();
  });
}

}  // namespace dfly::tiering
