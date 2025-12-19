// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/page_usage/page_usage_stats.h"

#include <absl/flags/reflection.h>
#include <gmock/gmock-matchers.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/compact_object.h"
#include "core/qlist.h"
#include "core/score_map.h"
#include "core/search/block_list.h"
#include "core/search/search.h"
#include "core/small_string.h"
#include "core/sorted_map.h"
#include "core/string_map.h"
#include "core/string_set.h"
#include "redis/redis_aux.h"
#include "util/fibers/fibers.h"

extern "C" {
#include "redis/zmalloc.h"
}

ABSL_DECLARE_FLAG(bool, experimental_flat_json);

using namespace dfly;
using namespace std::chrono_literals;

class PageUsageStatsTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    init_zmalloc_threadlocal(mi_heap_get_backing());
  }

  static void TearDownTestSuite() {
    mi_heap_collect(mi_heap_get_backing(), true);
    mi_heap_visit_blocks(
        mi_heap_get_backing(), false,
        [](auto*, auto* a, void*, size_t block_sz, void*) {
          LOG(ERROR) << "Unfreed allocations: block_size " << block_sz
                     << ", allocated: " << a->used * block_sz;
          return true;
        },
        nullptr);
  }

  PageUsageStatsTest() : m_(mi_heap_get_backing()) {
    InitTLStatelessAllocMR(&m_);
  }

  void SetUp() override {
    CompactObj::InitThreadLocal(&m_);

    score_map_ = std::make_unique<ScoreMap>();
    sorted_map_ = std::make_unique<detail::SortedMap>();
    string_set_ = std::make_unique<StringSet>();
    string_map_ = std::make_unique<StringMap>();
    SmallString::InitThreadLocal(m_.heap());
    qlist_ = std::make_unique<QList>(2, 2);
  }

  void TearDown() override {
    score_map_.reset();
    sorted_map_.reset();
    string_set_.reset();
    string_map_.reset();
    small_string_.Free();
    qlist_->Clear();
    EXPECT_EQ(zmalloc_used_memory_tl, 0);
  }

  MiMemoryResource m_;
  std::unique_ptr<ScoreMap> score_map_;
  std::unique_ptr<detail::SortedMap> sorted_map_;
  std::unique_ptr<StringSet> string_set_;
  std::unique_ptr<StringMap> string_map_;
  SmallString small_string_{};
  std::unique_ptr<QList> qlist_;
  CompactValue c_obj_{};
};

TEST_F(PageUsageStatsTest, Defrag) {
  score_map_->AddOrUpdate("test", 0.1);
  sorted_map_->InsertNew(0.1, "x");
  string_set_->Add("a");
  string_map_->AddOrUpdate("key", "value");
  small_string_.Assign("small-string");

  // INT_TAG, defrag will be skipped
  c_obj_.SetString("1");

  qlist_->Push("xxxx", QList::HEAD);

  {
    PageUsage p{CollectPageStats::YES, 0.1};
    score_map_->begin().ReallocIfNeeded(&p);
    sorted_map_->DefragIfNeeded(&p);
    string_set_->begin().ReallocIfNeeded(&p);
    string_map_->begin().ReallocIfNeeded(&p);
    small_string_.DefragIfNeeded(&p);
    c_obj_.DefragIfNeeded(&p);
    qlist_->DefragIfNeeded(&p);

    const auto stats = p.CollectedStats();
    EXPECT_GT(stats.pages_scanned, 0);
    EXPECT_EQ(stats.objects_skipped_not_required, 1);
  }

  {
    PageUsage p{CollectPageStats::NO, 0.1};
    score_map_->begin().ReallocIfNeeded(&p);
    sorted_map_->DefragIfNeeded(&p);
    string_set_->begin().ReallocIfNeeded(&p);
    string_map_->begin().ReallocIfNeeded(&p);
    small_string_.DefragIfNeeded(&p);
    qlist_->DefragIfNeeded(&p);
    EXPECT_EQ(p.CollectedStats().pages_scanned, 0);
  }
}

TEST_F(PageUsageStatsTest, StatCollection) {
  constexpr auto threshold = 0.5;
  PageUsage p{CollectPageStats::YES, threshold};
  for (size_t i = 0; i < 10000; ++i) {
    p.ConsumePageStats({.page_address = uintptr_t{100000 + i},
                        .block_size = 1,
                        .capacity = 100,
                        .reserved = 100,
                        .used = 65,
                        .flags = 0});
  }

  for (size_t i = 0; i < 2000; ++i) {
    p.ConsumePageStats({.page_address = uintptr_t{200000 + i},
                        .block_size = 1,
                        .capacity = 100,
                        .reserved = 100,
                        .used = 85,
                        .flags = 0});
  }

  for (size_t i = 0; i < 1000; ++i) {
    p.ConsumePageStats({.page_address = uintptr_t{300000 + i},
                        .block_size = 1,
                        .capacity = 100,
                        .reserved = 100,
                        .used = 89,
                        .flags = 0});
  }

  constexpr auto page_count_per_flag = 150;

  auto start = 0;
  for (const uint8_t flag : {MI_DFLY_PAGE_FULL, MI_DFLY_PAGE_USED_FOR_MALLOC, MI_DFLY_HEAP_MISMATCH,
                             MI_DFLY_PAGE_BELOW_THRESHOLD}) {
    for (size_t i = 0; i < page_count_per_flag; ++i) {
      p.ConsumePageStats({.page_address = uintptr_t{start + i},
                          .block_size = 1,
                          .capacity = 100,
                          .reserved = 100,
                          .used = 100,
                          .flags = flag});
    }
    start += page_count_per_flag;
  }

  CollectedPageStats st;
  st.Merge(p.CollectedStats(), 1);

  EXPECT_GT(st.pages_scanned, 12000);

  // Expect a small error margin due to HLL
  EXPECT_NEAR(st.pages_full, page_count_per_flag, 5);
  EXPECT_NEAR(st.pages_reserved_for_malloc, page_count_per_flag, 5);
  EXPECT_NEAR(st.pages_marked_for_realloc, page_count_per_flag, 5);

  const auto usage = st.shard_wide_summary;

  EXPECT_EQ(usage.size(), 1);
  EXPECT_TRUE(usage.contains(1));

  const CollectedPageStats::ShardUsageSummary expected{{50, 65}, {90, 85}, {99, 89}};
  EXPECT_EQ(usage.at(1), expected);
}

TEST_F(PageUsageStatsTest, JSONCons) {
  // Because of the static encoding it is not possible to easily test the flat encoding. Once the
  // encoding flag is set, it is not re-read. If friend class is used to access the compact object
  // inner fields and call `DefragIfNeeded` directly on the flat variant of the union, the test will
  // still fail. This is because freeing the compact object code path takes the wrong branch based
  // on encoding. The flat encoding was tested manually adjusting this same test with changed
  // encoding.
  std::string data = R"({"contents":[)";
  for (size_t i = 0; i < 1000; ++i) {
    const auto si = std::to_string(i);
    data += R"({"id":)" + si + R"(,"class":"v___)" + si + R"("})";
    if (i < 999) {
      data += ",";
    }
  }
  data += R"(], "data": "some", "count": 1, "checked": false})";

  auto* mr = static_cast<MiMemoryResource*>(CompactObj::memory_resource());
  size_t before = mr->used();

  auto parsed = ParseJsonUsingShardHeap(data);
  EXPECT_TRUE(parsed.has_value());

  c_obj_.SetJson(std::move(parsed.value()));
  c_obj_.SetJsonSize(mr->used() - before);
  EXPECT_GT(c_obj_.MallocUsed(), 0);

  PageUsage p{CollectPageStats::YES, 0.1};
  p.SetForceReallocate(true);

  c_obj_.DefragIfNeeded(&p);
  EXPECT_GT(c_obj_.MallocUsed(), 0);

  const auto stats = p.CollectedStats();
  EXPECT_GT(stats.pages_scanned, 0);
  EXPECT_EQ(stats.objects_skipped_not_required, 0);

  EXPECT_EQ(c_obj_.ObjType(), OBJ_JSON);

  auto json_obj = c_obj_.GetJson();
  EXPECT_EQ(json_obj->at("data").as_string_view(), "some");
  EXPECT_EQ(json_obj->at("count").as_integer<uint8_t>(), 1);
  EXPECT_EQ(json_obj->at("checked").as_bool(), false);
}

TEST_F(PageUsageStatsTest, QuotaChecks) {
  {
    PageUsage p{CollectPageStats::NO, 0};
    EXPECT_FALSE(p.QuotaDepleted());
  }
  {
    PageUsage p{CollectPageStats::NO, 0, 4};
    util::ThisFiber::SleepFor(5us);
    EXPECT_TRUE(p.QuotaDepleted());
  }
}

TEST_F(PageUsageStatsTest, BlockList) {
  search::BlockList<search::SortedVector<search::DocId>> bl{&m_, 20};
  PageUsage p{CollectPageStats::NO, 0.1};
  p.SetForceReallocate(true);

  // empty list
  auto result = bl.Defragment(&p);
  EXPECT_FALSE(result.quota_depleted);
  EXPECT_EQ(result.objects_moved, 0);

  // single item will move twice, once for the blocklist and once for the sorted vector
  bl.Insert(1);
  result = bl.Defragment(&p);
  EXPECT_FALSE(result.quota_depleted);
  EXPECT_EQ(result.objects_moved, 2);

  // quota depleted without defragmentation
  PageUsage p_zero{CollectPageStats::NO, 0.1, 0};
  p_zero.SetForceReallocate(true);
  result = bl.Defragment(&p_zero);
  EXPECT_TRUE(result.quota_depleted);
  EXPECT_EQ(result.objects_moved, 0);
}

TEST_F(PageUsageStatsTest, BlockListDefragmentResumes) {
  search::BlockList<search::SortedVector<search::DocId>> bl{&m_, 20};
  PageUsage p{CollectPageStats::NO, 0.1};
  p.SetForceReallocate(true);

  for (size_t i = 0; i < 1000; ++i) {
    bl.Insert(i);
  }

  PageUsage p_small_quota{CollectPageStats::NO, 0.1, 10};
  p_small_quota.SetForceReallocate(true);
  util::ThisFiber::SleepFor(10us);
  auto result = bl.Defragment(&p_small_quota);
  EXPECT_TRUE(result.quota_depleted);
  EXPECT_GE(result.objects_moved, 0);

  result = bl.Defragment(&p);
  EXPECT_FALSE(result.quota_depleted);
  EXPECT_GT(result.objects_moved, 0);
}

TEST_F(PageUsageStatsTest, BlockListWithPairs) {
  search::BlockList<search::SortedVector<std::pair<search::DocId, double>>> bl{&m_, 20};
  PageUsage p{CollectPageStats::NO, 0.1};
  p.SetForceReallocate(true);

  for (size_t i = 0; i < 100; ++i) {
    bl.Insert({i, i * 1.1});
  }

  const auto result = bl.Defragment(&p);
  EXPECT_FALSE(result.quota_depleted);
  EXPECT_GT(result.objects_moved, 0);
}

TEST_F(PageUsageStatsTest, BlockListWithNonDefragmentableContainer) {
  search::BlockList<search::CompressedSortedSet> bl{&m_, 20};
  PageUsage p{CollectPageStats::NO, 0.1};
  p.SetForceReallocate(true);

  // empty list
  auto result = bl.Defragment(&p);
  EXPECT_FALSE(result.quota_depleted);
  EXPECT_EQ(result.objects_moved, 0);

  // will reallocate once for the blocklist, the inner sorted set will be skipped
  bl.Insert(1);
  result = bl.Defragment(&p);
  EXPECT_FALSE(result.quota_depleted);
  EXPECT_EQ(result.objects_moved, 1);
}

class MockDocument final : public search::DocumentAccessor {
 public:
  MockDocument() {
    words.reserve(1000);
    for (size_t i = 0; i < 1000; ++i) {
      words.push_back(absl::StrFormat("word-%d", i));
    }
  }

  std::optional<StringList> GetStrings(std::string_view active_field) const override {
    return {{words[absl::GetCurrentTimeNanos() % words.size()]}};
  }
  std::optional<VectorInfo> GetVector(std::string_view active_field) const override {
    return std::nullopt;
  }
  std::optional<NumsList> GetNumbers(std::string_view active_field) const override {
    return {{1, 2, 3, 4}};
  }
  std::optional<StringList> GetTags(std::string_view active_field) const override {
    return {{words[absl::GetCurrentTimeNanos() % words.size()]}};
  }

  std::vector<std::string> words;
};

TEST_F(PageUsageStatsTest, DefragmentTagIndex) {
  search::Schema schema;
  schema.fields["field_name"] =
      search::SchemaField{search::SchemaField::TAG, 0, "fn", search::SchemaField::TagParams{}};
  search::FieldIndices index{schema, {}, &m_, nullptr};

  PageUsage p{CollectPageStats::NO, 0.1};
  p.SetForceReallocate(true);

  // Empty index
  search::DefragmentResult result = index.Defragment(&p);
  EXPECT_FALSE(result.quota_depleted);
  EXPECT_EQ(result.objects_moved, 0);

  const MockDocument md;
  index.Add(1, md);

  result = index.Defragment(&p);
  EXPECT_FALSE(result.quota_depleted);
  // single doc with single term returned by `GetTags` should result in two reallocations.
  EXPECT_EQ(result.objects_moved, 2);

  PageUsage p_zero{CollectPageStats::NO, 0.1, 0};
  p_zero.SetForceReallocate(true);
  result = index.Defragment(&p_zero);
  EXPECT_TRUE(result.quota_depleted);
  EXPECT_EQ(result.objects_moved, 0);
}

TEST_F(PageUsageStatsTest, TagIndexDefragResumeWithChanges) {
  search::Schema schema;
  schema.fields["field_name"] =
      search::SchemaField{search::SchemaField::TAG, 0, "fn", search::SchemaField::TagParams{}};
  search::FieldIndices index{schema, {}, &m_, nullptr};

  PageUsage p{CollectPageStats::NO, 0.1};
  p.SetForceReallocate(true);

  const MockDocument md;
  for (size_t i = 0; i < 100; ++i) {
    index.Add(i, md);
  }

  PageUsage p_small_quota{CollectPageStats::NO, 0.1, 10};
  p_small_quota.SetForceReallocate(true);
  util::ThisFiber::SleepFor(10us);
  search::DefragmentResult result = index.Defragment(&p_small_quota);
  EXPECT_TRUE(result.quota_depleted);
  EXPECT_GE(result.objects_moved, 0);

  index.Remove(99, md);

  for (size_t i = 200; i < 300; ++i) {
    index.Add(i, md);
  }

  result = index.Defragment(&p);
  EXPECT_FALSE(result.quota_depleted);
  EXPECT_GT(result.objects_moved, 0);
}

TEST_F(PageUsageStatsTest, DefragmentIndexWithNonDefragmentableFields) {
  search::Schema schema;
  schema.fields["text"] =
      search::SchemaField{search::SchemaField::TEXT, 0, "fn", search::SchemaField::TextParams{}};
  schema.fields["num"] = search::SchemaField{search::SchemaField::NUMERIC, 0, "fn",
                                             search::SchemaField::NumericParams{}};
  search::IndicesOptions options{{}};
  search::FieldIndices index{schema, options, &m_, nullptr};

  PageUsage p{CollectPageStats::NO, 0.1};
  p.SetForceReallocate(true);

  const MockDocument md;
  index.Add(1, md);

  // Unsupported index types will skip defragmenting themselves
  const search::DefragmentResult result = index.Defragment(&p);
  EXPECT_FALSE(result.quota_depleted);
  EXPECT_EQ(result.objects_moved, 0);
}
