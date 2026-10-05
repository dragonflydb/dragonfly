// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/aof.h"

#include <absl/base/internal/endian.h>

#include "base/gtest.h"

namespace dfly {

using namespace std;

class AofTest : public ::testing::Test {
 protected:
  static constexpr uint64_t kUid = 0x1122334455667788;

  // Concatenated payloads of blocks, each verified against kUid.
  static string Payloads(const vector<AofSealedBlock>& blocks) {
    string res;
    for (const auto& b : blocks) {
      auto hdr = DecodeAofBlock(b.bytes, kUid);
      EXPECT_TRUE(hdr);
      EXPECT_EQ(hdr->total_block_bytes, b.bytes.size());
      res += b.bytes.substr(kAofBlockHeaderSize);
    }
    return res;
  }

  vector<AofSealedBlock> TakeSealed() {
    return exchange(sealed_, {});
  }

  vector<AofSealedBlock> sealed_;
  AofBlockBuilder builder_{kUid, [this](AofSealedBlock b) { sealed_.push_back(std::move(b)); }};
};

TEST_F(AofTest, SegmentHeader) {
  AofSegmentHeader hdr{3, 8, 42, kUid};
  string bytes = EncodeAofSegmentHeader(hdr);
  ASSERT_EQ(bytes.size(), kAofSegmentHeaderSize);

  auto decoded = DecodeAofSegmentHeader(bytes);
  ASSERT_TRUE(decoded);
  EXPECT_EQ(decoded->shard_id, 3u);
  EXPECT_EQ(decoded->shard_count, 8u);
  EXPECT_EQ(decoded->seq, 42u);
  EXPECT_EQ(decoded->segment_uid, kUid);

  string bad_magic = bytes;
  bad_magic[0] = 'X';
  EXPECT_FALSE(DecodeAofSegmentHeader(bad_magic));

  string bad_crc = bytes;
  bad_crc[20] ^= 1;
  EXPECT_FALSE(DecodeAofSegmentHeader(bad_crc));
  EXPECT_FALSE(DecodeAofSegmentHeader(bytes.substr(0, 63)));
}

TEST_F(AofTest, BlockCrc) {
  string rec = "some record";
  builder_.Append(rec, 7);
  builder_.Seal();
  auto blocks = TakeSealed();
  ASSERT_EQ(blocks.size(), 1u);
  const string& bytes = blocks[0].bytes;

  // crc32c(segment_uid | payload | total_block_bytes | first_lsn | n_records | flags)
  string input(8, '\0');
  absl::little_endian::Store64(input.data(), kUid);
  input += rec;
  input += bytes.substr(0, 8);    // total_block_bytes
  input += bytes.substr(12, 12);  // first_lsn, n_records
  input += bytes[24];             // flags
  uint32_t expected = static_cast<uint32_t>(absl::ComputeCrc32c(input));
  EXPECT_EQ(absl::little_endian::Load32(bytes.data() + 8), expected);

  EXPECT_TRUE(DecodeAofBlock(bytes, kUid));
  EXPECT_FALSE(DecodeAofBlock(bytes, kUid + 1));
}

TEST_F(AofTest, SmallRecords) {
  builder_.Append("a", 10);
  builder_.Seal();
  auto blocks = TakeSealed();
  ASSERT_EQ(blocks.size(), 1u);
  EXPECT_EQ(blocks[0].flags, 0);
  EXPECT_EQ(blocks[0].first_lsn, 10u);
  EXPECT_EQ(blocks[0].n_records, 1u);
  EXPECT_EQ(blocks[0].bytes.size(), kAofBlockHeaderSize + 1);

  builder_.Append("bb", 11);
  builder_.Append("ccc", 12);
  builder_.Append("dddd", 13);
  EXPECT_TRUE(TakeSealed().empty());  // no seal per record
  builder_.Seal();
  blocks = TakeSealed();
  ASSERT_EQ(blocks.size(), 1u);
  EXPECT_EQ(blocks[0].first_lsn, 11u);
  EXPECT_EQ(blocks[0].n_records, 3u);
  EXPECT_EQ(Payloads(blocks), "bbcccdddd");
}

TEST_F(AofTest, ExactFill) {
  builder_.Append(string(100, 'x'), 1);
  builder_.Append(string(kAofBlockBytes - 100, 'y'), 2);
  auto blocks = TakeSealed();
  ASSERT_EQ(blocks.size(), 1u);
  EXPECT_EQ(blocks[0].flags, 0);
  EXPECT_EQ(blocks[0].n_records, 2u);

  builder_.Append("z", 3);
  builder_.Seal();
  blocks = TakeSealed();
  ASSERT_EQ(blocks.size(), 1u);
  EXPECT_EQ(blocks[0].flags, 0);
  EXPECT_EQ(blocks[0].first_lsn, 3u);
  EXPECT_EQ(Payloads(blocks), "z");
}

TEST_F(AofTest, SpanningRecord) {
  string small(100, 's');
  string huge(2 * kAofBlockBytes + 50, '\0');
  for (size_t i = 0; i < huge.size(); ++i)
    huge[i] = 'a' + i % 26;

  builder_.Append(small, 1);
  builder_.Append(huge, 2);
  builder_.Seal();
  auto blocks = TakeSealed();
  ASSERT_EQ(blocks.size(), 3u);

  EXPECT_EQ(blocks[0].flags, kEndsWithPartial);
  EXPECT_EQ(blocks[1].flags, kEndsWithPartial | kStartsWithContinuation);
  EXPECT_EQ(blocks[2].flags, kStartsWithContinuation);
  EXPECT_EQ(blocks[0].n_records, 1u);
  EXPECT_EQ(blocks[1].n_records, 0u);
  EXPECT_EQ(blocks[2].n_records, 1u);
  EXPECT_EQ(blocks[0].first_lsn, 1u);
  for (size_t i = 1; i < blocks.size(); ++i)
    EXPECT_EQ(blocks[i].first_lsn, blocks[i - 1].first_lsn + blocks[i - 1].n_records);

  EXPECT_EQ(Payloads(blocks), small + huge);
}

TEST_F(AofTest, SealEmpty) {
  builder_.Seal();
  EXPECT_TRUE(TakeSealed().empty());
}

}  // namespace dfly
