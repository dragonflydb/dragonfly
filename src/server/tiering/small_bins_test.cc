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

TEST(SmallBins, SimpleStashRead) {
  SmallBins bins;

  // Fill single bin
  std::optional<SmallBins::FilledBin> bin;
  for (unsigned i = 0; !bin; i++)
    bin = bins.Stash(absl::StrCat("k", i), absl::StrCat("v", i));

  // Verify cut locations point to correct values
  auto segments = bins.ReportStashed(bin->first, DiskSegment{0, 4_KB});
  for (auto [key, location] : segments) {
    auto value = "v"s + key.substr(1);
    EXPECT_EQ(value, bin->second.substr(location.offset, location.length));
  }
}

TEST(SmallBins, SimpleDeleteAbort) {
  SmallBins bins;

  // Fill single bin
  std::optional<SmallBins::FilledBin> bin;
  unsigned i = 0;
  for (; !bin; i++)
    bin = bins.Stash(absl::StrCat("k", i), absl::StrCat("v", i));

  // Delete all even values
  for (unsigned j = 0; j <= i; j += 2)
    bins.Delete(absl::StrCat("k", j));

  auto remaining = bins.ReportStashAborted(bin->first);
  sort(remaining.begin(), remaining.end());

  // Expect all odd keys still to exist
  EXPECT_EQ(remaining.size(), i / 2);
  for (unsigned j = 1; j < i; j += 2)
    EXPECT_TRUE(binary_search(remaining.begin(), remaining.end(), absl::StrCat("k", j))) << j;
}

TEST(SmallBins, PartialStash) {
  SmallBins bins;

  // Fill single bin
  std::optional<SmallBins::FilledBin> bin;
  unsigned i = 0;
  for (; !bin; i++)
    bin = bins.Stash(absl::StrCat("k", i), absl::StrCat("v", i));

  // Delete all even values
  for (unsigned j = 0; j <= i; j += 2)
    bins.Delete(absl::StrCat("k", j));

  auto segments = bins.ReportStashed(bin->first, DiskSegment{0, 4_KB});

  // Expect all odd keys still to exist
  EXPECT_EQ(segments.size(), i / 2);
  for (auto& [key, segment] : segments) {
    EXPECT_EQ(key, "k"s + bin->second.substr(segment.offset, segment.length).substr(1));
  }
}

}  // namespace dfly::tiering
