// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/page_usage_stats.h"

#include <absl/container/flat_hash_set.h>
#include <cmake-build-debug/third_party/libs/hdr_histogram/include/hdr/hdr_histogram.h>
#include <glog/logging.h>
#include <hdr/hdr_histogram.h>

#include <string>

#include "core/bloom.h"

extern "C" {
#include <unistd.h>

#include "redis/zmalloc.h"
mi_page_usage_stats_t mi_heap_page_is_underutilized(mi_heap_t* heap, void* p, float ratio,
                                                    bool collect_stats);
}

namespace dfly {

namespace {
constexpr auto kUsageHistPoints = std::array{50, 90, 99};
constexpr auto kInitialSBFCap = 1000;
constexpr auto kFProb = 0.001;
constexpr auto kGrowthFactor = 2;
constexpr auto kHistSignificantFigures = 3;

FilterWithSize MakeSBF() {
  return {.sbf = SBF{kInitialSBFCap, kFProb, kGrowthFactor, PMR_NS::get_default_resource()},
          .size = 0};
}

}  // namespace

void FilterWithSize::Add(uintptr_t address) {
  const auto s = std::to_string(address);
  if (sbf.Add(s)) {
    size += 1;
  }
}

void CollectedPageStats::Merge(CollectedPageStats&& other, ShardId shard_id) {
  this->pages_scanned += other.pages_scanned;
  this->pages_marked_for_realloc += other.pages_marked_for_realloc;
  this->pages_full += other.pages_full;
  this->pages_reserved_for_malloc += other.pages_reserved_for_malloc;
  this->pages_with_heap_mismatch += other.pages_with_heap_mismatch;
  this->pages_above_threshold += other.pages_above_threshold;
  shard_wide_summary.emplace(std::make_pair(shard_id, std::move(other.page_usage_hist)));
}

UniquePages::UniquePages(float threshold)
    : pages_scanned{MakeBloomFilter()},
      pages_marked_for_realloc{MakeBloomFilter()},
      pages_full{MakeBloomFilter()},
      pages_reserved_for_malloc{MakeBloomFilter()},
      pages_with_heap_mismatch{MakeBloomFilter()},
      pages_above_threshold{MakeBloomFilter()} {
  hdr_histogram* h = nullptr;
  const auto init_result = hdr_init(1, 100, kHistSignificantFigures, &h);
  CHECK_EQ(0, init_result) << "failed to initialize histogram";
  page_usage_hist = h;
}

UniquePages::~UniquePages() {
  hdr_close(page_usage_hist);
}

void UniquePages::AddStat(mi_page_usage_stats_t stat) {
  const auto address = stat.page_address;
  pages_scanned.Add(address);
  if (stat.flags == MI_DFLY_PAGE_BELOW_THRESHOLD) {
    pages_marked_for_realloc.Add(address);
  } else {
    if (stat.flags & MI_DFLY_PAGE_FULL) {
      pages_full.Add(address);
    } else if (stat.flags & MI_DFLY_HEAP_MISMATCH) {
      pages_with_heap_mismatch.Add(address);
    } else if (stat.flags & MI_DFLY_PAGE_USED_FOR_MALLOC) {
      pages_reserved_for_malloc.Add(address);
    } else {
      // We record usage only for pages which have usage above the given threshold but which are not
      // full. This allows tuning the threshold for future commands. This also excludes full pages,
      // so the only pages here have: threshold < usage% < 100
      pages_above_threshold.Add(address);
      const double perc = static_cast<double>(stat.used) / static_cast<double>(stat.capacity);
      hdr_record_value(page_usage_hist, perc * 100);
    }
  }
}

CollectedPageStats UniquePages::CollectedStats() const {
  CollectedPageStats::ShardUsageSummary usage;
  for (const auto p : kUsageHistPoints) {
    usage[p] = hdr_value_at_percentile(page_usage_hist, p);
  }
  return CollectedPageStats{.pages_scanned = pages_scanned.size,
                            .pages_marked_for_realloc = pages_marked_for_realloc.size,
                            .pages_full = pages_full.size,
                            .pages_reserved_for_malloc = pages_reserved_for_malloc.size,
                            .pages_with_heap_mismatch = pages_with_heap_mismatch.size,
                            .pages_above_threshold = pages_above_threshold.size,
                            .page_usage_hist = std::move(usage),
                            .shard_wide_summary = {}};
}

PageUsage::PageUsage(CollectPageStats collect_stats, float threshold)
    : collect_stats_{collect_stats}, threshold_{threshold} {
}

bool PageUsage::IsPageForObjectUnderUtilized(void* object) {
  mi_page_usage_stats_t stat;
  zmalloc_page_is_underutilized(object, threshold_, collect_stats_ == CollectPageStats::YES, &stat);
  return ConsumePageStats(stat);
}

bool PageUsage::IsPageForObjectUnderUtilized(mi_heap_t* heap, void* object) {
  return ConsumePageStats(mi_heap_page_is_underutilized(heap, object, threshold_,
                                                        collect_stats_ == CollectPageStats::YES));
}

bool PageUsage::ConsumePageStats(mi_page_usage_stats_t stat) {
  const bool should_reallocate = stat.flags == MI_DFLY_PAGE_BELOW_THRESHOLD;
  if (collect_stats_ == CollectPageStats::YES) {
    unique_pages_.AddStat(stat);
  }
  return should_reallocate;
}

}  // namespace dfly
