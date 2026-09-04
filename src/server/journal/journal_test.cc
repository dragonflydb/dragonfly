#include <absl/cleanup/cleanup.h>
#include <absl/flags/reflection.h>
#include <absl/strings/str_join.h>

#include <array>
#include <random>
#include <string>

#include "base/flags.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "core/detail/gen_utils.h"
#include "server/common.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal_slice.h"
#include "server/journal/pending_buf.h"
#include "server/journal/serializer.h"
#include "server/journal/types.h"
#include "server/serializer_commons.h"
#include "strings/human_readable.h"
#include "util/fibers/fibers.h"

ABSL_DECLARE_FLAG(uint32_t, shard_repl_backlog_time_ms);
ABSL_DECLARE_FLAG(strings::MemoryBytesFlag, shard_repl_backlog_max_bytes);
ABSL_DECLARE_FLAG(uint32_t, shard_repl_backlog_len);

using namespace testing;
using namespace std;
using namespace util;

namespace dfly {
namespace journal {

struct EntryPayloadVisitor {
  void operator()(const Entry::Payload& p) {
    out->append(p.cmd).append(" ");
    *out += visit([](const auto& args) { return absl::StrJoin(args, " "); }, p.args);
  }

  string* out;
};

// Extract payload from entry in string form.
std::string ExtractPayload(ParsedEntry& entry) {
  return absl::StrJoin(entry.cmd.view(), " ");
}

std::string ExtractPayload(Entry& entry) {
  std::string out;
  EntryPayloadVisitor visitor{&out};
  visitor(entry.payload);
  return out;
}

// Mock non-owned types with underlying storage.
using StoredSlices = vector<vector<string_view>>;
using StoredLists = vector<pair<vector<string>, CmdArgVec>>;

template <typename... Ss> ArgSlice StoreSlice(StoredSlices* vec, Ss... strings) {
  vec->emplace_back(initializer_list<string_view>{strings...});
  return ArgSlice{vec->back().data(), vec->back().size()};
}

template <typename... Ss> CmdArgList StoreList(StoredLists* vec, Ss... strings) {
  vector<string> stored_strings{strings...};
  CmdArgVec out;
  for (auto& s : stored_strings) {
    out.emplace_back(s.data(), s.size());
  }

  vec->emplace_back(std::move(stored_strings), std::move(out));
  auto& arg_vec = vec->back().second;
  return CmdArgList{arg_vec.data(), arg_vec.size()};
}

// Test serializing and de-serializing entries.
TEST(Journal, WriteRead) {
  StoredSlices slices{};
  StoredLists lists{};

  auto slice = [v = &slices](auto... ss) { return StoreSlice(v, ss...); };
  auto list = [v = &lists](auto... ss) { return StoreList(v, ss...); };
  using Payload = Entry::Payload;

  std::vector<Entry> test_entries = {
      {0, Op::COMMAND, 0, nullopt, Payload("MSET", slice("A", "1", "B", "2"))},
      {0, Op::COMMAND, 0, nullopt, Payload("MSET", slice("C", "3"))},
      {1, Op::COMMAND, 0, nullopt, Payload("DEL", list("A", "B"))},
      {2, Op::COMMAND, 1, nullopt, Payload("LPUSH", list("l", "v1", "v2"))},
      {3, Op::COMMAND, 0, nullopt, Payload("MSET", slice("D", "4"))},
      {4, Op::COMMAND, 1, nullopt, Payload("DEL", list("l1"))},
      {5, Op::COMMAND, 2, nullopt, Payload("DEL", list("E", "2"))}};

  // Write all entries to a buffer.
  base::IoBuf buf;
  io::BufSink sink{&buf};

  JournalWriter writer{&sink};
  for (const auto& entry : test_entries) {
    writer.Write(entry);
  }

  // Read them back.
  io::BufSource source{&buf};
  JournalReader reader{&source, 0};

  ParsedEntry res;
  for (unsigned i = 0; i < test_entries.size(); i++) {
    auto& expected = test_entries[i];

    auto ec = reader.ReadEntry(&res);
    ASSERT_FALSE(ec);

    ASSERT_EQ(expected.opcode, res.opcode);
    ASSERT_EQ(expected.txid, res.txid);
    ASSERT_EQ(expected.dbid, res.dbid);
    ASSERT_EQ(ExtractPayload(expected), ExtractPayload(res));
  }
}

TEST(Journal, PendingBuf) {
  PendingBuf pbuf;

  ASSERT_TRUE(pbuf.Empty());
  ASSERT_EQ(pbuf.Size(), 0);

  pbuf.Push("one");
  pbuf.Push(" smallllllllllllllllllllllllllllllll");
  pbuf.Push(" test");

  ASSERT_FALSE(pbuf.Empty());
  ASSERT_EQ(pbuf.Size(), 44);

  {
    auto& sending_buf = pbuf.PrepareSendingBuf();
    ASSERT_EQ(sending_buf.buf.size(), 3);
    ASSERT_EQ(sending_buf.mem_size, 44);

    ASSERT_EQ(sending_buf.buf[0], "one");
    ASSERT_EQ(sending_buf.buf[1], " smallllllllllllllllllllllllllllllll");
    ASSERT_EQ(sending_buf.buf[2], " test");
  }

  const size_t string_num = PendingBuf::Buf::kMaxBufSize + 1000;
  std::vector<std::string> test_data;
  test_data.reserve(string_num);

  absl::InsecureBitGen gen;

  for (size_t i = 0; i < string_num; ++i) {
    auto str = GetRandomHex(gen, 10, 90);
    test_data.push_back(str);
    pbuf.Push(std::move(str));
  }

  const size_t test_data_size =
      std::accumulate(test_data.begin(), test_data.end(), 0,
                      [](size_t size, const auto& s) { return s.size() + size; });

  ASSERT_FALSE(pbuf.Empty());
  ASSERT_EQ(pbuf.Size(), 44 + test_data_size);

  pbuf.Pop();

  ASSERT_FALSE(pbuf.Empty());
  ASSERT_EQ(pbuf.Size(), test_data_size);

  {
    auto& sending_buf = pbuf.PrepareSendingBuf();

    const size_t send_buf_size =
        std::accumulate(test_data.begin(), test_data.begin() + PendingBuf::Buf::kMaxBufSize, 0,
                        [](size_t size, const auto& s) { return s.size() + size; });

    ASSERT_EQ(sending_buf.buf.size(), PendingBuf::Buf::kMaxBufSize);
    ASSERT_EQ(sending_buf.mem_size, send_buf_size);

    for (size_t i = 0; i < sending_buf.buf.size(); ++i) {
      ASSERT_EQ(sending_buf.buf[i], test_data[i]);
    }
  }

  pbuf.Pop();

  test_data.erase(test_data.begin(), test_data.begin() + PendingBuf::Buf::kMaxBufSize);

  const size_t last_buf_size =
      std::accumulate(test_data.begin(), test_data.end(), 0,
                      [](size_t size, const auto& s) { return s.size() + size; });

  ASSERT_FALSE(pbuf.Empty());
  ASSERT_EQ(pbuf.Size(), last_buf_size);

  {
    auto& sending_buf = pbuf.PrepareSendingBuf();

    ASSERT_EQ(sending_buf.buf.size(), 1000);
    ASSERT_EQ(sending_buf.mem_size, last_buf_size);

    for (size_t i = 0; i < sending_buf.buf.size(); ++i) {
      ASSERT_EQ(sending_buf.buf[i], test_data[i]);
    }
  }

  pbuf.Pop();

  ASSERT_TRUE(pbuf.Empty());
  ASSERT_EQ(pbuf.Size(), 0);
}

void AddSetRecord(JournalSlice* slice, string_view value) {
  array<string_view, 2> args{"key", value};
  slice->AddLogRecord(
      Entry{0, Op::COMMAND, 0, nullopt, Entry::Payload{"SET", ArgSlice{args.data(), args.size()}}});
}

TEST(Journal, BacklogSupportsLegacyEntryLimit) {
  absl::FlagSaver flag_saver;
  absl::SetFlag(&FLAGS_shard_repl_backlog_len, 2u);

  JournalSlice slice;
  slice.Init();

  AddSetRecord(&slice, "value");
  AddSetRecord(&slice, "value");

  EXPECT_EQ(slice.GetRingBufferSize(), 2u);
  EXPECT_TRUE(slice.IsLSNInBuffer(1));
  EXPECT_TRUE(slice.IsLSNInBuffer(2));
  EXPECT_GT(slice.GetRingBufferBytes(), 1u);
  const size_t retained_bytes = slice.GetRingBufferBytes();

  AddSetRecord(&slice, "value");

  EXPECT_EQ(slice.GetRingBufferSize(), 2u);
  EXPECT_EQ(slice.GetRingBufferBytes(), retained_bytes);
  EXPECT_FALSE(slice.IsLSNInBuffer(1));
  EXPECT_TRUE(slice.IsLSNInBuffer(2));
  EXPECT_TRUE(slice.IsLSNInBuffer(3));
}

TEST(Journal, BacklogByteLimitOverridesLegacyEntryLimit) {
  absl::FlagSaver flag_saver;
  absl::SetFlag(&FLAGS_shard_repl_backlog_len, 2u);
  absl::SetFlag(&FLAGS_shard_repl_backlog_max_bytes, strings::MemoryBytesFlag{1});

  JournalSlice slice;
  slice.Init();

  AddSetRecord(&slice, "value");
  AddSetRecord(&slice, "value");

  EXPECT_EQ(slice.GetRingBufferSize(), 1u);
  EXPECT_FALSE(slice.IsLSNInBuffer(1));
  EXPECT_TRUE(slice.IsLSNInBuffer(2));
}

TEST(Journal, BacklogTimeLimitOverridesLegacyEntryLimit) {
  absl::FlagSaver flag_saver;
  absl::SetFlag(&FLAGS_shard_repl_backlog_len, 2u);
  absl::SetFlag(&FLAGS_shard_repl_backlog_time_ms, 1u);

  const uint64_t original_time = TEST_current_time_ms;
  auto restore_time = absl::MakeCleanup([original_time] { TEST_current_time_ms = original_time; });

  JournalSlice slice;
  slice.Init();

  TEST_current_time_ms = 1000;
  AddSetRecord(&slice, "value");
  TEST_current_time_ms = 1001;
  AddSetRecord(&slice, "value");

  EXPECT_EQ(slice.GetRingBufferSize(), 1u);
  EXPECT_FALSE(slice.IsLSNInBuffer(1));
  EXPECT_TRUE(slice.IsLSNInBuffer(2));
}

TEST(Journal, BacklogHonorsByteLimitAndReplacesOversizedRecord) {
  absl::FlagSaver flag_saver;
  absl::SetFlag(&FLAGS_shard_repl_backlog_time_ms, 0u);

  JournalSlice probe;
  probe.Init();
  AddSetRecord(&probe, "x");
  const size_t item_bytes = probe.GetRingBufferBytes();
  ASSERT_GT(item_bytes, 1u);

  const size_t max_bytes = 2 * item_bytes - 1;
  absl::SetFlag(&FLAGS_shard_repl_backlog_max_bytes, strings::MemoryBytesFlag{max_bytes});
  JournalSlice slice;
  slice.Init();

  AddSetRecord(&slice, "x");
  AddSetRecord(&slice, "x");
  EXPECT_EQ(slice.GetRingBufferSize(), 1u);
  EXPECT_FALSE(slice.IsLSNInBuffer(1));
  EXPECT_TRUE(slice.IsLSNInBuffer(2));
  EXPECT_LE(slice.GetRingBufferBytes(), max_bytes);

  string large_value(2048, 'x');
  AddSetRecord(&slice, large_value);
  EXPECT_EQ(slice.GetRingBufferSize(), 1u);
  EXPECT_FALSE(slice.IsLSNInBuffer(2));
  EXPECT_TRUE(slice.IsLSNInBuffer(3));

  AddSetRecord(&slice, "x");
  EXPECT_EQ(slice.GetRingBufferSize(), 1u);
  EXPECT_FALSE(slice.IsLSNInBuffer(3));
  EXPECT_TRUE(slice.IsLSNInBuffer(4));
  EXPECT_LE(slice.GetRingBufferBytes(), max_bytes);
}

TEST(Journal, BacklogDefaultByteLimitUsesHalfPercentOfMaxmemory) {
  absl::FlagSaver flag_saver;
  absl::SetFlag(&FLAGS_shard_repl_backlog_time_ms, 1000u);
  absl::SetFlag(&FLAGS_shard_repl_backlog_max_bytes, strings::MemoryBytesFlag{});

  const size_t original_max_memory = max_memory_limit.exchange(200 * 1024, memory_order_relaxed);
  auto restore_max_memory = absl::MakeCleanup(
      [original_max_memory] { max_memory_limit.store(original_max_memory, memory_order_relaxed); });

  JournalSlice slice;
  slice.Init();

  string large_value(2048, 'x');
  AddSetRecord(&slice, large_value);
  AddSetRecord(&slice, "x");

  // 0.5% of 200 KiB is 1 KiB, so the oversized record is evicted.
  EXPECT_FALSE(slice.IsLSNInBuffer(1));
  EXPECT_TRUE(slice.IsLSNInBuffer(2));
}

TEST(Journal, BacklogGrowsBeyondInitialCapacity) {
  absl::FlagSaver flag_saver;
  absl::SetFlag(&FLAGS_shard_repl_backlog_time_ms, 0u);
  absl::SetFlag(&FLAGS_shard_repl_backlog_max_bytes, strings::MemoryBytesFlag{4 * 1024 * 1024});

  JournalSlice slice;
  slice.Init();

  constexpr size_t kRecordCount = 10'000;
  for (size_t i = 0; i < kRecordCount; ++i) {
    AddSetRecord(&slice, "value");
  }

  EXPECT_EQ(slice.GetRingBufferSize(), kRecordCount);
  EXPECT_TRUE(slice.IsLSNInBuffer(1));
  EXPECT_TRUE(slice.IsLSNInBuffer(kRecordCount));
}

TEST(Journal, BacklogDropsOldestWhenMetadataCannotGrow) {
  absl::FlagSaver flag_saver;
  absl::SetFlag(&FLAGS_shard_repl_backlog_time_ms, 0u);

  const string large_value(128, 'x');
  JournalSlice large_probe;
  large_probe.Init();
  AddSetRecord(&large_probe, large_value);
  const size_t large_item_bytes = large_probe.GetRingBufferBytes();

  JournalSlice small_probe;
  small_probe.Init();
  AddSetRecord(&small_probe, "x");
  const size_t small_item_bytes = small_probe.GetRingBufferBytes();
  ASSERT_LT(small_item_bytes, large_item_bytes);

  constexpr size_t kInitialCapacity = 8192;
  absl::SetFlag(&FLAGS_shard_repl_backlog_max_bytes,
                strings::MemoryBytesFlag{kInitialCapacity * large_item_bytes + small_item_bytes});

  JournalSlice slice;
  slice.Init();
  for (size_t i = 0; i < kInitialCapacity; ++i) {
    AddSetRecord(&slice, large_value);
  }

  AddSetRecord(&slice, "x");

  EXPECT_EQ(slice.GetRingBufferSize(), kInitialCapacity);
  EXPECT_FALSE(slice.IsLSNInBuffer(1));
  EXPECT_TRUE(slice.IsLSNInBuffer(kInitialCapacity + 1));
}

TEST(Journal, BacklogCleansExpiredEntriesOnAppend) {
  absl::FlagSaver flag_saver;
  absl::SetFlag(&FLAGS_shard_repl_backlog_time_ms, 1000u);
  absl::SetFlag(&FLAGS_shard_repl_backlog_max_bytes, strings::MemoryBytesFlag{1024 * 1024});

  const uint64_t original_time = TEST_current_time_ms;
  auto restore_time = absl::MakeCleanup([original_time] { TEST_current_time_ms = original_time; });
  TEST_current_time_ms = 1000;

  JournalSlice slice;
  slice.Init();
  AddSetRecord(&slice, "first");
  EXPECT_TRUE(slice.IsLSNInBuffer(1));

  TEST_current_time_ms = 1999;
  EXPECT_TRUE(slice.IsLSNInBuffer(1));

  TEST_current_time_ms = 2000;
  EXPECT_TRUE(slice.IsLSNInBuffer(1));

  AddSetRecord(&slice, "second");
  EXPECT_EQ(slice.GetRingBufferSize(), 1u);
  EXPECT_FALSE(slice.IsLSNInBuffer(1));
  EXPECT_TRUE(slice.IsLSNInBuffer(2));
}

TEST(Journal, BacklogBoundsTimeBasedCleanup) {
  absl::FlagSaver flag_saver;
  absl::SetFlag(&FLAGS_shard_repl_backlog_time_ms, 1000u);
  absl::SetFlag(&FLAGS_shard_repl_backlog_max_bytes, strings::MemoryBytesFlag{1024 * 1024});

  const uint64_t original_time = TEST_current_time_ms;
  auto restore_time = absl::MakeCleanup([original_time] { TEST_current_time_ms = original_time; });

  JournalSlice slice;
  slice.Init();

  TEST_current_time_ms = 1000;
  constexpr size_t kExpiredEntries = 101;
  for (size_t i = 0; i < kExpiredEntries; ++i) {
    AddSetRecord(&slice, "expired");
  }

  TEST_current_time_ms = 2000;
  AddSetRecord(&slice, "current");

  EXPECT_EQ(slice.GetRingBufferSize(), 2u);
  EXPECT_FALSE(slice.IsLSNInBuffer(1));
  EXPECT_FALSE(slice.IsLSNInBuffer(kExpiredEntries - 1));
  EXPECT_TRUE(slice.IsLSNInBuffer(kExpiredEntries));
  EXPECT_TRUE(slice.IsLSNInBuffer(kExpiredEntries + 1));
}

}  // namespace journal
}  // namespace dfly
