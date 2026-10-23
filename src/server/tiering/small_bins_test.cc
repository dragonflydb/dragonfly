// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/small_bins.h"

#include <absl/strings/str_cat.h>

#include <algorithm>

#include "base/gtest.h"
#include "base/logging.h"
#include "server/tiering/disk_storage.h"

namespace dfly::tiering {

using namespace std;
using namespace std::string_literals;

string SmallString(size_t len) {
  return string(len, 'a');
}

struct SmallBinsTest : public ::testing::Test {
  std::pair<tiering::SmallBins::BinId, std::string> Serialize(SmallBins::FilledBin& bin) {
    std::string out(4_KB, 'c');
    size_t written = bins_.SerializeBin(&bin, {reinterpret_cast<uint8_t*>(out.data()), out.size()});
    out.resize(written);
    return {bin.id, out};
  }

 protected:
  SmallBins bins_;
};

TEST_F(SmallBinsTest, SimpleStashRead) {
  // Fill single bin
  std::optional<SmallBins::FilledBin> bin;
  for (unsigned i = 0; !bin; i++)
    bin = bins_.Stash(0, absl::StrCat("k", i), absl::StrCat("v", i));
  auto [id, data] = Serialize(*bin);

  // Verify cut locations point to correct values
  auto segments = bins_.ReportStashed(id, DiskSegment{0, 4_KB});
  for (auto [dbid, key, location] : segments) {
    auto value = "v"s + key.substr(1);
    EXPECT_EQ(value, data.substr(location.offset, location.length));
  }
}

TEST_F(SmallBinsTest, SimpleDeleteAbort) {
  SmallBins bins;

  // Fill single bin
  std::optional<SmallBins::FilledBin> bin;
  unsigned i = 0;
  for (; !bin; i++)
    bin = bins_.Stash(0, absl::StrCat("k", i), absl::StrCat("v", i));
  auto [id, data] = Serialize(*bin);

  // Delete all even values
  for (unsigned j = 0; j <= i; j += 2)
    bins_.Delete(0, absl::StrCat("k", j));

  auto remaining = bins_.ReportStashAborted(id);
  sort(remaining.begin(), remaining.end());

  // Expect all odd keys still to exist
  EXPECT_EQ(remaining.size(), i / 2);
  for (unsigned j = 1; j < i; j += 2) {
    std::pair<DbIndex, std::string> needle{0, absl::StrCat("k", j)};
    EXPECT_TRUE(binary_search(remaining.begin(), remaining.end(), needle)) << j;
  }
}

TEST_F(SmallBinsTest, PartialStashDelete) {
  // Fill single bin
  std::optional<SmallBins::FilledBin> bin;
  unsigned i = 0;
  for (; !bin; i++)
    bin = bins_.Stash(0, absl::StrCat("k", i), absl::StrCat("v", i));
  auto [id, data] = Serialize(*bin);

  // Delete all even values
  for (unsigned j = 0; j <= i; j += 2)
    bins_.Delete(0, absl::StrCat("k", j));

  auto segments = bins_.ReportStashed(id, DiskSegment{0, 4_KB});

  // Expect all odd keys still to exist
  EXPECT_EQ(segments.size(), i / 2);
  for (auto& [dbid, key, segment] : segments) {
    EXPECT_EQ(key, "k"s + data.substr(segment.offset, segment.length).substr(1));
  }

  // Delete all stashed values
  while (!segments.empty()) {
    auto segment = std::get<2>(segments.back());
    segments.pop_back();
    auto bin = bins_.Delete(segment);

    EXPECT_EQ(bin.segment.offset, 0u);
    EXPECT_EQ(bin.segment.length, 4_KB);

    if (segments.empty()) {
      EXPECT_TRUE(bin.empty);
    } else {
      EXPECT_TRUE(bin.fragmented);  // half of the values were deleted
    }
  }
}

TEST_F(SmallBinsTest, UpdateStatsAfterDelete) {
  // caused https://github.com/dragonflydb/dragonfly/issues/3240
  for (unsigned i = 0; i < 10; i++) {
    auto spilled_bin = bins_.Stash(0, absl::StrCat("k", i), SmallString(128));
    ASSERT_FALSE(spilled_bin);
  }

  EXPECT_GT(bins_.GetStats().current_bin_bytes, 128 * 10);
  for (unsigned i = 0; i < 10; i++) {
    auto res = bins_.Delete(0, absl::StrCat("k", i));
    ASSERT_FALSE(res);
  }
  EXPECT_EQ(0u, bins_.GetStats().current_bin_bytes);
}

TEST_F(SmallBinsTest, StashedEntriesBytes) {
  // Fill single bin.
  std::optional<SmallBins::FilledBin> bin;
  unsigned i = 0;
  for (; !bin; i++)
    bin = bins_.Stash(0, absl::StrCat("k", i), absl::StrCat("v", i));
  auto [id, data] = Serialize(*bin);

  // Delete all even values from the pending bin (mirrors PartialStashDelete which drains cleanly).
  for (unsigned j = 0; j <= i; j += 2)
    bins_.Delete(0, absl::StrCat("k", j));

  EXPECT_EQ(bins_.GetStats().stashed_entries_bytes, 0u);

  auto segments = bins_.ReportStashed(id, DiskSegment{0, 4_KB});
  ASSERT_GT(segments.size(), 0u);

  // After stash, bytes counter equals the on-disk size of the entries (value + header each).
  size_t total_bytes = 0;
  for (auto& [dbid, key, segment] : segments)
    total_bytes += segment.length + SmallBins::kEntryHeaderSize;
  EXPECT_EQ(bins_.GetStats().stashed_entries_bytes, total_bytes);
  EXPECT_EQ(bins_.GetStats().stashed_entries_cnt, segments.size());

  // Deleting entries one by one drains the byte counter back to zero.
  while (!segments.empty()) {
    auto segment = std::get<2>(segments.back());
    segments.pop_back();
    bins_.Delete(segment);
  }
  EXPECT_EQ(bins_.GetStats().stashed_entries_bytes, 0u);
  EXPECT_EQ(bins_.GetStats().stashed_entries_cnt, 0u);
}

TEST_F(SmallBinsTest, DeleteAllPendingKeysReleasesBin) {
  // Fill a single bin and move it to the pending (in-flight) state via SerializeBin.
  std::optional<SmallBins::FilledBin> bin;
  unsigned i = 0;
  for (; !bin; i++)
    bin = bins_.Stash(0, absl::StrCat("k", i), absl::StrCat("v", i));
  auto [id, data] = Serialize(*bin);

  EXPECT_EQ(bins_.GetStats().pending_bins_cnt, 1u);

  // Delete every key of the in-flight bin. Only the final delete should report the bin id.
  std::optional<SmallBins::BinId> reported;
  for (unsigned j = 0; j < i; j++) {
    auto res = bins_.Delete(0, absl::StrCat("k", j));
    if (res) {
      EXPECT_FALSE(reported) << "bin id reported more than once";
      reported = res;
    }
  }

  ASSERT_TRUE(reported);
  EXPECT_EQ(*reported, id);
  // The now-empty pending entry must be released, otherwise empty maps pile up forever.
  EXPECT_EQ(bins_.GetStats().pending_bins_cnt, 0u);
}

TEST_F(SmallBinsTest, ReportStashFailedReturnsAllKeys) {
  // A bin that filled up but failed to stash before serialization is never tracked as pending.
  std::optional<SmallBins::FilledBin> bin;
  unsigned i = 0;
  for (; !bin; i++)
    bin = bins_.Stash(0, absl::StrCat("k", i), absl::StrCat("v", i));

  EXPECT_EQ(bins_.GetStats().pending_bins_cnt, 0u);  // not serialized yet

  auto keys = bins_.ReportStashFailed(std::move(*bin));
  sort(keys.begin(), keys.end());

  // The key that triggers the fill opens a new current bin, so the filled bin holds k0..k(i-2).
  EXPECT_EQ(keys.size(), i - 1);
  for (unsigned j = 0; j + 1 < i; j++) {
    std::pair<DbIndex, std::string> needle{0, absl::StrCat("k", j)};
    EXPECT_TRUE(binary_search(keys.begin(), keys.end(), needle)) << j;
  }
  // Recovering a failed bin must not register it as pending.
  EXPECT_EQ(bins_.GetStats().pending_bins_cnt, 0u);
}

}  // namespace dfly::tiering
