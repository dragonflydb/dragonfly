// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/disk_storage.h"

#include <memory>

#include "base/gtest.h"
#include "base/logging.h"
#include "server/tiering/common.h"
#include "server/tiering/test_common.h"
#include "util/fibers/fibers.h"
#include "util/fibers/pool.h"

namespace dfly::tiering {

using namespace std;
using namespace std::string_literals;

struct DiskStorageTest : public PoolTestBase {
  ~DiskStorageTest() {
    EXPECT_EQ(pending_ops_, 0);
  }

  void Open() {
    storage_ = make_unique<DiskStorage>(256_MB);
    storage_->Open("disk_storage_test_backing");
  }

  void Close() {
    storage_->Close();
    storage_.reset();
    unlink("disk_storage_test_backing");
  }

  void Stash(size_t index, string value) {
    pending_ops_++;
    auto buf = make_shared<string>(value);
    storage_->Stash(io::Buffer(*buf), {}, [this, index, buf](io::Result<DiskSegment> segment) {
      EXPECT_TRUE(segment);
      EXPECT_GT(segment->length, 0u);
      segments_[index] = *segment;
      pending_ops_--;
    });
  }

  void Read(size_t index) {
    pending_ops_++;
    storage_->Read(segments_[index], [this, index](io::Result<string_view> value) {
      EXPECT_TRUE(value);
      last_reads_[index] = *value;
      pending_ops_--;
    });
  }

  void Delete(size_t index) {
    storage_->MarkAsFree(segments_[index]);
    segments_.erase(index);
    last_reads_.erase(index);
  }

  void Wait() const {
    while (pending_ops_ > 0) {
      ::util::ThisFiber::SleepFor(1ms);
    }
  }

  DiskStorage::Stats GetStats() const {
    return storage_->GetStats();
  }

 protected:
  int pending_ops_ = 0;

  std::unordered_map<size_t, string> last_reads_;
  std::unordered_map<size_t, DiskSegment> segments_;
  std::unique_ptr<DiskStorage> storage_;
};

TEST_F(DiskStorageTest, Basic) {
  pp_->at(0)->Await([this] {
    // Write 100 values
    Open();
    for (size_t i = 0; i < 100; i++)
      Stash(i, absl::StrCat("value", i));
    Wait();
    EXPECT_EQ(segments_.size(), 100);

    EXPECT_EQ(GetStats().allocated_bytes, 100 * kPageSize);

    // Read all 100 values
    for (size_t i = 0; i < 100; i++)
      Read(i);
    Wait();

    // Expect them to be equal to written
    for (size_t i = 0; i < 100; i++)
      EXPECT_EQ(last_reads_[i], absl::StrCat("value", i));

    // Delete all values
    for (size_t i = 0; i < 100; i++)
      Delete(i);
    EXPECT_EQ(GetStats().allocated_bytes, 0);

    Close();
  });
}

TEST_F(DiskStorageTest, ReUse) {
  pp_->at(0)->Await([this] {
    Open();

    Stash(0, "value1");
    Wait();
    EXPECT_EQ(segments_[0].offset, 0u);

    Delete(0);

    Stash(1, "value2");
    Wait();
    EXPECT_EQ(segments_[1].offset, 0u);

    Close();
  });
}

}  // namespace dfly::tiering
