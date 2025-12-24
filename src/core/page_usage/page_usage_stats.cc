// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/page_usage/page_usage_stats.h"

#include <absl/container/flat_hash_set.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_join.h>
#include <glog/logging.h>
#include <hdr/hdr_histogram.h>

#include <string>

#include "base/cycle_clock.h"

extern "C" {
#include <unistd.h>

#include "redis/zmalloc.h"
mi_page_usage_stats_t mi_heap_page_is_underutilized(mi_heap_t* heap, void* p, float ratio,
                                                    bool collect_stats);
}

namespace dfly {

using absl::StrAppend;
using absl::StrFormat;
using absl::StripTrailingAsciiWhitespace;

namespace {
constexpr auto kUsageHistPoints = std::array{50, 90, 99};
constexpr auto kHistSignificantFigures = 3;

HllBufferPtr InitHllPtr() {
  HllBufferPtr p;
  p.size = getDenseHllSize();
  p.hll = new uint8_t[p.size];
  CHECK_EQ(0, createDenseHll(p));
  return p;
}

}  // namespace

CycleQuota::CycleQuota(const uint64_t quota_usec)
    : quota_cycles_{quota_usec == kMaxQuota ? kMaxQuota : base::CycleClock::FromUsec(quota_usec)} {
  Arm();
}

void CycleQuota::Arm() {
  start_cycles_ = base::CycleClock::Now();
}

bool CycleQuota::Depleted() const {
  if (quota_cycles_ == kMaxQuota)
    return false;
  return UsedCycles() >= quota_cycles_;
}

uint64_t CycleQuota::UsedCycles() const {
  return base::CycleClock::Now() - start_cycles_;
}

void CollectedPageStats::Merge(CollectedPageStats&& other, uint16_t shard_id) {
  this->pages_scanned += other.pages_scanned;
  this->pages_marked_for_realloc += other.pages_marked_for_realloc;
  this->pages_full += other.pages_full;
  this->pages_reserved_for_malloc += other.pages_reserved_for_malloc;
  this->pages_with_heap_mismatch += other.pages_with_heap_mismatch;
  this->pages_above_threshold += other.pages_above_threshold;
  this->objects_skipped_not_required += other.objects_skipped_not_required;
  this->objects_skipped_not_supported += other.objects_skipped_not_supported;
  shard_wide_summary.emplace(std::make_pair(shard_id, std::move(other.page_usage_hist)));
}

CollectedPageStats CollectedPageStats::Merge(std::vector<CollectedPageStats>&& stats,
                                             const float threshold) {
  CollectedPageStats result;
  result.threshold = threshold;

  size_t shard_index = 0;
  for (CollectedPageStats& stat : stats) {
    result.Merge(std::move(stat), shard_index++);
  }
  return result;
}

std::string CollectedPageStats::ToString() const {
  std::string response;
  StrAppend(&response, "Page usage threshold: ", threshold * 100, "\n");
  StrAppend(&response, "Pages scanned: ", pages_scanned, "\n");
  StrAppend(&response, "Pages marked for reallocation: ", pages_marked_for_realloc, "\n");
  StrAppend(&response, "Pages full: ", pages_full, "\n");
  StrAppend(&response, "Pages reserved for malloc: ", pages_reserved_for_malloc, "\n");
  StrAppend(&response, "Pages skipped due to heap mismatch: ", pages_with_heap_mismatch, "\n");
  StrAppend(&response, "Pages with usage above threshold: ", pages_above_threshold, "\n");
  StrAppend(&response,
            "Objects skipped (do not require defragmentation): ", objects_skipped_not_required,
            "\n");
  StrAppend(&response,
            "Objects skipped (do not support defragmentation): ", objects_skipped_not_supported,
            "\n");
  for (const auto& [shard_id, usage] : shard_wide_summary) {
    StrAppend(&response, "[Shard ", shard_id, "]\n");
    for (const auto& [percentage, count] : usage) {
      StrAppend(&response,
                StrFormat(" %d%% pages are below %d%% block usage\n", percentage, count));
    }
  }
  StripTrailingAsciiWhitespace(&response);
  return response;
}

PageUsage::UniquePages::UniquePages()
    : pages_scanned{InitHllPtr()},
      pages_marked_for_realloc{InitHllPtr()},
      pages_full{InitHllPtr()},
      pages_reserved_for_malloc{InitHllPtr()},
      pages_with_heap_mismatch{InitHllPtr()},
      pages_above_threshold{InitHllPtr()} {
  hdr_histogram* h = nullptr;
  const auto init_result = hdr_init(1, 100, kHistSignificantFigures, &h);
  CHECK_EQ(0, init_result) << "failed to initialize histogram";
  page_usage_hist = h;
}

PageUsage::UniquePages::~UniquePages() {
  delete[] pages_scanned.hll;
  delete[] pages_marked_for_realloc.hll;
  delete[] pages_full.hll;
  delete[] pages_reserved_for_malloc.hll;
  delete[] pages_with_heap_mismatch.hll;
  delete[] pages_above_threshold.hll;
  hdr_close(page_usage_hist);
}

void PageUsage::UniquePages::AddStat(mi_page_usage_stats_t stat) {
  const auto data = reinterpret_cast<const unsigned char*>(&stat.page_address);
  constexpr size_t size = sizeof(stat.page_address);
  pfadd_dense(pages_scanned, data, size);
  if (stat.flags == MI_DFLY_PAGE_BELOW_THRESHOLD) {
    pfadd_dense(pages_marked_for_realloc, data, size);
  } else {
    if (stat.flags & MI_DFLY_PAGE_FULL) {
      pfadd_dense(pages_full, data, size);
    } else if (stat.flags & MI_DFLY_HEAP_MISMATCH) {
      pfadd_dense(pages_with_heap_mismatch, data, size);
    } else if (stat.flags & MI_DFLY_PAGE_USED_FOR_MALLOC) {
      pfadd_dense(pages_reserved_for_malloc, data, size);
    } else {
      // We record usage only for pages which have usage above the given threshold but which are not
      // full. This allows tuning the threshold for future commands. This also excludes full pages,
      // so the only pages here have: threshold < usage% < 100
      pfadd_dense(pages_above_threshold, data, size);
      const double perc = static_cast<double>(stat.used) / static_cast<double>(stat.capacity);
      hdr_record_value(page_usage_hist, perc * 100);
    }
  }
}

CollectedPageStats PageUsage::UniquePages::CollectedStats() const {
  CollectedPageStats::ShardUsageSummary usage;
  for (const auto p : kUsageHistPoints) {
    usage[p] = hdr_value_at_percentile(page_usage_hist, p);
  }

  return CollectedPageStats{
      .pages_scanned = static_cast<uint64_t>(pfcountSingle(pages_scanned)),
      .pages_marked_for_realloc = static_cast<uint64_t>(pfcountSingle(pages_marked_for_realloc)),
      .pages_full = static_cast<uint64_t>(pfcountSingle(pages_full)),
      .pages_reserved_for_malloc = static_cast<uint64_t>(pfcountSingle(pages_reserved_for_malloc)),
      .pages_with_heap_mismatch = static_cast<uint64_t>(pfcountSingle(pages_with_heap_mismatch)),
      .pages_above_threshold = static_cast<uint64_t>(pfcountSingle(pages_above_threshold)),
      .objects_skipped_not_required = objects_skipped_not_required,
      .objects_skipped_not_supported = objects_skipped_not_supported,
      .page_usage_hist = std::move(usage),
      .shard_wide_summary = {}};
}

PageUsage::PageUsage(CollectPageStats collect_stats, float threshold, CycleQuota quota)
    : collect_stats_{collect_stats}, threshold_{threshold}, quota_{quota} {
}

void PageUsage::ArmQuotaTimer() {
  quota_.Arm();
}

uint64_t PageUsage::UsedQuotaCycles() const {
  return quota_.UsedCycles();
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
  return force_reallocate_ || should_reallocate;
}

bool PageUsage::QuotaDepleted() const {
  return quota_.Depleted();
}

}  // namespace dfly
