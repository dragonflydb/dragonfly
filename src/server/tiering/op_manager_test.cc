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

struct TestDecoder : tiering::BareDecoder {
  std::unique_ptr<tiering::Decoder> Clone() const override {
    return std::make_unique<TestDecoder>();
  }

  void Initialize(std::string_view slice) override {
    tiering::BareDecoder::Initialize(slice);
    value = slice;
  }

  string value;
};

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
  }

  util::fb2::Future<std::string> Read(PendingId id, DiskSegment segment) {
    util::fb2::Future<std::string> future;
    Enqueue(id, segment, TestDecoder{}, [future](io::Result<tiering::Decoder*> res) mutable {
      auto* decoder = static_cast<TestDecoder*>(*res);
      future.Resolve(decoder->value);
    });
    return future;
  }

  void NotifyStashed(const OwnedEntryId& id, const io::Result<DiskSegment>& segment) override {
    VLOG(1) << std::get<0>(id) << " stashed";
    ASSERT_TRUE(segment);
    auto [it, inserted] = stashed_.emplace(id, *segment);
    ASSERT_TRUE(inserted);
  }

  bool NotifyFetched(const OwnedEntryId& id, DiskSegment segment, Decoder* decoder) override {
    auto* tdecoder = static_cast<TestDecoder*>(decoder);
    fetched_[id] = std::move(tdecoder->value);
    return false;
  }

  bool NotifyDelete(DiskSegment segment) override {
    return true;
  }

  std::error_code Stash(PendingId id, std::string_view value) {
    return PrepareAndStash(id, value.size(), [=](io::MutableBytes bytes) {
      memcpy(bytes.data(), value.data(), value.size());
      return value.size();
    });
  }

  void WaitForPendingStashes() {
    // Wait for both: pending_stash_cnt tracks entries awaiting version-matching IO completion,
    // but cancelled stash IOs (version-mismatched, superseded by newer stashes for the same id)
    // may still be in flight. Their callbacks free the allocated segments via MarkAsFree,
    // so we must also wait for pending_ops to drain to ensure allocated_bytes is accurate.
    while (GetStats().pending_stash_cnt > 0 || GetStats().disk_stats.pending_ops > 0)
      util::ThisFiber::SleepFor(1ms);
  }

  absl::flat_hash_map<OwnedEntryId, std::string> fetched_;
  absl::flat_hash_map<OwnedEntryId, DiskSegment> stashed_;
};

TEST_F(OpManagerTest, SimpleStashesWithReads) {
  pp_->at(0)->Await([this] {
    Open();

    for (unsigned i = 0; i < 100; i++) {
      EXPECT_FALSE(Stash(i, absl::StrCat("VALUE", i, "cancelled")));
      EXPECT_FALSE(Stash(i, absl::StrCat("VALUE", i, "cancelled")));
      EXPECT_FALSE(Stash(i, absl::StrCat("VALUE", i, "real")));
    }

    EXPECT_EQ(GetStats().pending_stash_cnt, 100);
    WaitForPendingStashes();

    EXPECT_EQ(stashed_.size(), 100u);
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

    EXPECT_FALSE(Stash(0u, absl::StrCat("DATA")));
    WaitForPendingStashes();

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

    EXPECT_FALSE(Stash(0u, numbers));
    WaitForPendingStashes();

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

// Test ABA scenario: stash an entry, issue an async read, delete it and re-stash a new value
// under the same id - all without yielding so the read I/O stays in flight. When I/O completes,
// version tracking in pending_stash_ver_ must ensure only the new stash triggers NotifyStashed
// while the old one is silently discarded (its segment freed).
//
// NOTE: We cannot guarantee that the first read completes after the second stash because we have
// no control over io_uring completion ordering. In practice, the read submitted first likely
// completes before or around the same time as the stash. To fully test the interleaving where
// the new entry's read is issued while the original read is still in flight, we would need a
// mock DiskStorage that allows explicit control over when I/O completions are delivered.
// TODO: Add a DiskStorage mock to enable deterministic I/O completion ordering in tests.
TEST_F(OpManagerTest, StashDeleteRestashWhileReading) {
  pp_->at(0)->Await([this] {
    Open();

    // Stash initial value under id 0
    EXPECT_FALSE(Stash(0u, "ORIGINAL"));
    WaitForPendingStashes();

    DiskSegment original_segment = stashed_.at(0u);

    // Issue an async read - don't wait on it yet so it stays in flight.
    auto read_fut = Read(0u, original_segment);

    // Without yielding: delete the entry, clear tracking, re-stash under the same id.
    // At this point the read for ORIGINAL is still pending in io_uring, and we're issuing
    // a new stash for id 0 with a bumped version.
    DeleteOffloaded(original_segment);
    stashed_.clear();
    EXPECT_FALSE(Stash(0u, "REPLACEMENT"));

    // Both the read and the new stash are now in flight. Let them complete.
    WaitForPendingStashes();
    EXPECT_EQ(read_fut.Get(), "ORIGINAL");

    // Verify only the replacement was notified (single entry in stashed_).
    ASSERT_EQ(stashed_.size(), 1u);
    ASSERT_EQ(1, stashed_.count(0u));
    DiskSegment new_segment = stashed_.at(0u);

    // Read the replacement and verify correctness
    EXPECT_EQ(Read(0u, new_segment).Get(), "REPLACEMENT");

    Close();
  });
}

TEST_F(OpManagerTest, Modify) {
  pp_->at(0)->Await([this] {
    Open();

    std::ignore = Stash(0u, "D");
    WaitForPendingStashes();

    // Atomically issue sequence of modify-read operations
    std::vector<util::fb2::Future<std::string>> futures;
    for (size_t i = 0; i < 10; i++) {
      Enqueue(0u, stashed_[0u], TestDecoder{}, [i](io::Result<tiering::Decoder*> res) {
        auto* decoder = static_cast<TestDecoder*>(*res);
        absl::StrAppend(&decoder->value, i);
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
