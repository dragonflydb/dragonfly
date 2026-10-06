// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/aof.h"

#include <absl/base/internal/endian.h>
#include <absl/strings/str_cat.h>

#include <filesystem>
#include <fstream>

#include "base/gtest.h"
#include "util/fibers/pool.h"

#ifdef __linux__
#include "server/journal/aof_segment_writer.h"
#endif

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
  // total_block_bytes
  input += bytes.substr(0, 8);
  // first_lsn, n_records
  input += bytes.substr(12, 12);
  // flags
  input += bytes[24];
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
  // No seal per record.
  EXPECT_TRUE(TakeSealed().empty());
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

#ifdef __linux__

class AofSegmentWriterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = base::GetTestTempPath("aof");
    filesystem::remove_all(dir_);
    filesystem::create_directories(dir_);
    pp_.reset(util::fb2::Pool::IOUring(16, 1));
    pp_->Run();
  }

  void TearDown() override {
    pp_->Stop();
  }

  string ReadSegment(uint64_t seq) {
    // convenience
    ifstream in(absl::StrCat(dir_, "/", AofSegmentWriter::SegmentName(2, seq)), ios::binary);
    return string(istreambuf_iterator<char>(in), {});
  }

  string dir_;
  unique_ptr<util::ProactorPool> pp_;
};

TEST_F(AofSegmentWriterTest, WriteAndReadBack) {
  string small(100, 's');
  string huge(kAofBlockBytes, 'h');
  pp_->at(0)->Await([&] {
    AofSegmentWriter writer(dir_, 2, 4);
    ASSERT_FALSE(writer.Open(0));
    writer.AddRecord(small, 1);
    // Seals the first block with huge partial.
    writer.AddRecord(huge, 2);

    // Only the open block remains.
    writer.WaitPending(small.size());
    EXPECT_EQ(writer.WrittenLsn(), 1u);

    ASSERT_FALSE(writer.Shutdown());
    EXPECT_EQ(writer.WrittenLsn(), 2u);
  });

  EXPECT_FALSE(
      filesystem::exists(absl::StrCat(dir_, "/", AofSegmentWriter::SegmentName(2, 0), ".tmp")));
  string file = ReadSegment(0);
  auto seg = DecodeAofSegmentHeader(file);
  ASSERT_TRUE(seg);
  EXPECT_EQ(seg->shard_id, 2u);
  EXPECT_EQ(seg->shard_count, 4u);
  EXPECT_EQ(seg->seq, 0u);

  string_view rest = string_view(file).substr(kAofSegmentHeaderSize);
  string payload;
  vector<AofBlockHeader> blocks;
  while (!rest.empty()) {
    auto hdr = DecodeAofBlock(rest, seg->segment_uid);
    ASSERT_TRUE(hdr);
    blocks.push_back(*hdr);
    payload += rest.substr(kAofBlockHeaderSize, hdr->total_block_bytes - kAofBlockHeaderSize);
    rest.remove_prefix(hdr->total_block_bytes);
  }
  ASSERT_EQ(blocks.size(), 2u);
  EXPECT_EQ(blocks[0].flags, kEndsWithPartial);
  EXPECT_EQ(blocks[1].flags, kStartsWithContinuation);
  EXPECT_EQ(blocks[1].first_lsn, 2u);
  EXPECT_EQ(payload, small + huge);
}

TEST_F(AofSegmentWriterTest, OpenKeepsExistingSegment) {
  pp_->at(0)->Await([&] {
    AofSegmentWriter first(dir_, 2, 4);
    ASSERT_FALSE(first.Open(0));
    first.AddRecord("x", 1);
    ASSERT_FALSE(first.Shutdown());

    AofSegmentWriter second(dir_, 2, 4);
    EXPECT_EQ(second.Open(0), errc::file_exists);
  });
  EXPECT_FALSE(
      filesystem::exists(absl::StrCat(dir_, "/", AofSegmentWriter::SegmentName(2, 0), ".tmp")));
  EXPECT_EQ(ReadSegment(0).size(), kAofSegmentHeaderSize + kAofBlockHeaderSize + 1);
}

#endif  // __linux__

}  // namespace dfly
