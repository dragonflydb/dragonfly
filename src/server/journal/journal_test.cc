// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/flags/flag.h>
#include <absl/flags/reflection.h>

#include <random>
#include <string>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/detail/gen_utils.h"
#include "journal_slice.h"
#include "server/common.h"
#include "server/journal/pending_buf.h"
#include "server/journal/serializer.h"
#include "server/journal/types.h"
#include "server/serializer_commons.h"
#include "util/fibers/fibers.h"

using namespace testing;
using namespace std;
using namespace util;

ABSL_DECLARE_FLAG(uint64_t, shard_repl_backlog_max_bytes_soft);
ABSL_DECLARE_FLAG(uint32_t, shard_repl_backlog_len);

namespace dfly {
namespace journal {
template <typename T> string ConCat(const T& list) {
  string res;
  for (auto arg : list) {
    res += string_view{arg.data(), arg.size()};
    res += ' ';
  }
  return res;
}

template <> string ConCat(const CmdArgList& list) {
  string res;
  for (auto arg : list) {
    res += facade::ToSV(arg);
    res += ' ';
  }
  return res;
}

struct EntryPayloadVisitor {
  void operator()(const Entry::Payload& p) {
    out->append(p.cmd).append(" ");
    *out += visit([](const auto& args) { return ConCat(args); }, p.args);
  }

  string* out;
};

// Extract payload from entry in string form.
std::string ExtractPayload(ParsedEntry& entry) {
  std::string out = ConCat(entry.cmd);

  if (!out.empty())
    out.pop_back();

  return out;
}

std::string ExtractPayload(Entry& entry) {
  std::string out;
  EntryPayloadVisitor visitor{&out};
  visitor(entry.payload);

  if (!out.empty())
    out.pop_back();

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
      {0, Op::COMMAND, 0, 2, nullopt, Payload("MSET", slice("A", "1", "B", "2"))},
      {0, Op::COMMAND, 0, 2, nullopt, Payload("MSET", slice("C", "3"))},
      {1, Op::COMMAND, 0, 2, nullopt, Payload("DEL", list("A", "B"))},
      {2, Op::COMMAND, 1, 1, nullopt, Payload("LPUSH", list("l", "v1", "v2"))},
      {3, Op::COMMAND, 0, 1, nullopt, Payload("MSET", slice("D", "4"))},
      {4, Op::COMMAND, 1, 1, nullopt, Payload("DEL", list("l1"))},
      {5, Op::COMMAND, 2, 1, nullopt, Payload("DEL", list("E", "2"))}};

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

void RunLimitTest(size_t bytes, size_t count, std::function<void(JournalSlice&)> f) {
  absl::FlagSaver fs;

  absl::SetFlag(&FLAGS_shard_repl_backlog_max_bytes_soft, bytes);
  absl::SetFlag(&FLAGS_shard_repl_backlog_len, count);

  JournalSlice slice;
  slice.Init();
  f(slice);
}

TEST(JournalSlice, ByteLimit) {
  RunLimitTest(500, 10240, [](JournalSlice& slice) {
    std::string cmd(50, 'x');

    for (TxId i : std::views::iota(1, 20)) {
      const Entry entry{i, Op::COMMAND, 0, 0, std::nullopt, Entry::Payload{cmd, ShardArgs{}}};
      slice.AddLogRecord(entry);
    }

    EXPECT_LE(slice.GetRingBufferBytes(), 500);
  });
}

TEST(JournalSlice, ItemTooLarge) {
  constexpr auto large_entry_size = 1024;
  constexpr auto max_bytes = 1200;
  RunLimitTest(max_bytes, 10240, [&](JournalSlice& slice) {
    for (const TxId i : std::views::iota(0, 10)) {
      const Entry entry{i, Op::COMMAND, 0, 0, std::nullopt, Entry::Payload{"xxxxxx", ShardArgs{}}};
      slice.AddLogRecord(entry);
    }

    const std::string cmd(large_entry_size, 'x');
    const Entry entry{0, Op::COMMAND, 0, 0, std::nullopt, Entry::Payload{cmd, ShardArgs{}}};
    slice.AddLogRecord(entry);

    // Some items evicted from the front
    EXPECT_LE(slice.GetRingBufferSize(), 10);
    // The slice grows over the limit temporarily
    EXPECT_GE(slice.GetRingBufferBytes(), max_bytes);

    // the size converges, eventually evicting the large entry
    for (const TxId i : std::views::iota(0, 25)) {
      const Entry e2{i, Op::COMMAND, 0, 0, std::nullopt, Entry::Payload{"xxxxxx", ShardArgs{}}};
      slice.AddLogRecord(e2);
    }

    // Now the slice can hold everything
    EXPECT_GE(slice.GetRingBufferSize(), 20);
    EXPECT_LE(slice.GetRingBufferBytes(), max_bytes);
  });
}

TEST(JournalSlice, BothBytesAndCountLimited) {
  RunLimitTest(300, 3, [](JournalSlice& slice) {
    for (const TxId i : std::views::iota(0, 10)) {
      const Entry entry{i, Op::COMMAND, 0, 0, std::nullopt, Entry::Payload{"x", ShardArgs{}}};
      slice.AddLogRecord(entry);
      EXPECT_LE(slice.GetRingBufferSize(), 3);
      EXPECT_LE(slice.GetRingBufferBytes(), 300);
    }
  });
}

}  // namespace journal
}  // namespace dfly
