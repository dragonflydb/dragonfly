// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "facade/reply_builder.h"

#include <absl/cleanup/cleanup.h>
#include <absl/container/fixed_array.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <double-conversion/double-to-string.h>

#include "absl/strings/escaping.h"
#include "base/logging.h"
#include "core/heap_size.h"
#include "facade/error.h"
#include "util/fibers/proactor_base.h"

using namespace std;
using absl::StrAppend;
using namespace double_conversion;

namespace facade {

namespace {

constexpr char kCRLF[] = "\r\n";
constexpr char kErrPref[] = "-ERR ";
constexpr char kSimplePref[] = "+";
constexpr char kNullStringR2[] = "$-1\r\n";
constexpr char kNullStringR3[] = "_\r\n";

constexpr unsigned kConvFlags =
    DoubleToStringConverter::UNIQUE_ZERO | DoubleToStringConverter::EMIT_POSITIVE_EXPONENT_SIGN;

DoubleToStringConverter dfly_conv(kConvFlags, "inf", "nan", 'e', -6, 21, 6, 0);

}  // namespace

SinkReplyBuilder::ReplyAggregator::~ReplyAggregator() {
  rb->batched_ = prev;
  if (!prev)
    rb->Flush();
}

SinkReplyBuilder::ReplyScope::~ReplyScope() {
  rb->scoped_ = prev;
  if (!prev)
    rb->FinishScope();
}

void SinkReplyBuilder::SendError(ErrorReply error) {
  if (error.status)
    return SendError(*error.status);
  SendError(error.ToSv(), error.kind);
}

void SinkReplyBuilder::SendError(OpStatus status) {
  if (status == OpStatus::OK)
    return SendOk();
  SendError(StatusToMsg(status));
}

void SinkReplyBuilder::CloseConnection() {
  if (!ec_)
    ec_ = std::make_error_code(std::errc::connection_aborted);
}

char* SinkReplyBuilder::ReservePiece(size_t size) {
  if (buffer_.AppendLen() <= size)
    Flush();

  char* dest = reinterpret_cast<char*>(buffer_.AppendBuffer().data());

  // Start new vec for piece if last one dones't point at buffer tail
  if (vecs_.empty() || ((char*)vecs_.back().iov_base) + vecs_.back().iov_len != dest)
    NextVec({dest, 0});

  return dest;
}

void SinkReplyBuilder::CommitPiece(size_t size) {
  buffer_.CommitWrite(size);
  vecs_.back().iov_len += size;
  total_size_ += size;
}

void SinkReplyBuilder::WritePiece(std::string_view str) {
  char* dest = ReservePiece(str.size());
  memcpy(dest, str.data(), str.size());
  CommitPiece(str.size());
}

void SinkReplyBuilder::WriteRef(std::string_view str) {
  NextVec(str);
  total_size_ += str.size();
}

void SinkReplyBuilder::Flush() {
  auto& reply_stats = tl_facade_stats->reply_stats;

  send_active_ = true;
  uint64_t before_ns = util::fb2::ProactorBase::GetMonotonicTimeNs();
  reply_stats.io_write_cnt++;
  reply_stats.io_write_bytes += total_size_;

  if (auto ec = sink_->Write(vecs_.data(), vecs_.size()); ec)
    ec_ = ec;

  uint64_t after_ns = util::fb2::ProactorBase::GetMonotonicTimeNs();
  reply_stats.send_stats.count++;
  reply_stats.send_stats.total_duration += (after_ns - before_ns) / 1'000;
  send_active_ = false;

  if (buffer_.InputLen() * 2 > buffer_.Capacity())  // If needed, grow backing buffer
    buffer_.Reserve(std::min(kMaxBufferSize, buffer_.Capacity() * 2));

  total_size_ = 0;
  buffer_.Clear();
  vecs_.clear();
  guaranteed_pieces_ = 0;
}

void SinkReplyBuilder::FinishScope() {
  if (!batched_ || total_size_ * 2 >= kMaxBufferSize)
    return Flush();

  // Check if we have enough space to copy all refs to buffer
  size_t ref_bytes = total_size_ - buffer_.InputLen();
  if (ref_bytes > buffer_.AppendLen())
    return Flush();

  // Copy all extenral references to buffer to safely keep batching
  for (size_t i = guaranteed_pieces_; i < vecs_.size(); i++) {
    auto ib = buffer_.InputBuffer();
    if (vecs_[i].iov_base >= ib.data() && vecs_[i].iov_base <= ib.data() + ib.size())
      continue;  // this is a piece

    DCHECK_LE(buffer_.AppendLen(), vecs_[i].iov_len);
    void* dest = buffer_.AppendBuffer().data();
    memcpy(dest, vecs_[i].iov_base, vecs_[i].iov_len);
    buffer_.CommitWrite(vecs_[i].iov_len);
    vecs_[i].iov_base = dest;
  }
  guaranteed_pieces_ = vecs_.size();  // all vecs are pieces
}

void SinkReplyBuilder::NextVec(std::string_view str) {
  if (vecs_.size() >= IOV_MAX - 2)
    Flush();
  vecs_.push_back(iovec{const_cast<char*>(str.data()), str.size()});
}

MCReplyBuilder::MCReplyBuilder(::io::Sink* sink) : SinkReplyBuilder(sink), noreply_(false) {
}

void MCReplyBuilder::SendValue(std::string_view key, std::string_view value, uint64_t mc_ver,
                               uint32_t mc_flag) {
  ReplyScope scope(this);
  Write("VALUE ", key, " ", absl::StrCat(mc_flag), " ", absl::StrCat(value.size()));
  if (mc_ver)
    Write(" ", absl::StrCat(mc_ver));
  Write(value, kCRLF);
}

void MCReplyBuilder::SendSimpleString(std::string_view str) {
  if (noreply_)
    return;

  ReplyScope scope(this);
  Write(str, kCRLF);
}

void MCReplyBuilder::SendStored() {
  SendSimpleString("STORED");
}

void MCReplyBuilder::SendLong(long val) {
  SendSimplePiece(absl::StrCat(val));
}

void MCReplyBuilder::SendError(string_view str, std::string_view type) {
  SendSimplePiece(absl::StrCat("SERVER_ERROR ", str));
}

void MCReplyBuilder::SendProtocolError(std::string_view str) {
  SendSimplePiece(absl::StrCat("CLIENT_ERROR ", str));
}

void MCReplyBuilder::SendClientError(string_view str) {
  SendSimplePiece(absl::StrCat("CLIENT_ERROR", str));
}

void MCReplyBuilder::SendSetSkipped() {
  SendSimpleString("NOT_STORED");
}

void MCReplyBuilder::SendNotFound() {
  SendSimpleString("NOT_FOUND");
}

void MCReplyBuilder::SendSimplePiece(std::string&& str) {
  ReplyScope scope(this);
  WritePiece(str);
  WritePiece(kCRLF);
}

void RedisReplyBuilderBase::SendNull() {
  ReplyScope scope(this);
  resp3_ ? Write(kNullStringR3) : Write(kNullStringR2);
}

void RedisReplyBuilderBase::SendSimpleString(std::string_view str) {
  ReplyScope scope(this);
  Write(kSimplePref, str, kCRLF);
}

void RedisReplyBuilderBase::SendBulkString(std::string_view str) {
  ReplyScope scope(this);
  WriteIntWithPrefix('$', str.size());
  Write(kCRLF, str, kCRLF);
}

void RedisReplyBuilderBase::SendLong(long val) {
  ReplyScope scope(this);
  WriteIntWithPrefix(':', val);
  Write(kCRLF);
}

void RedisReplyBuilderBase::SendDouble(double val) {
  char buf[DoubleToStringConverter::kBase10MaximalLength + 1];
  static_assert(ABSL_ARRAYSIZE(buf) < kMaxInlineSize, "Write temporary string from buf inline");
  string_view val_str = FormatDouble(val, buf, ABSL_ARRAYSIZE(buf));

  if (!resp3_)
    return SendBulkString(val_str);

  ReplyScope scope(this);
  Write(",", val_str, kCRLF);
}

void RedisReplyBuilderBase::SendNullArray() {
  ReplyScope scope(this);
  Write("*-1", kCRLF);
}

constexpr static const char START_SYMBOLS2[4] = {'*', '~', '%', '>'};
static_assert(START_SYMBOLS2[RedisReplyBuilderBase::MAP] == '%' &&
              START_SYMBOLS2[RedisReplyBuilderBase::SET] == '~');

void RedisReplyBuilderBase::StartCollection(unsigned len, CollectionType ct) {
  if (!IsResp3()) {  // RESP2 supports only arrays
    if (ct == MAP)
      len *= 2;
    ct = ARRAY;
  }
  ReplyScope scope(this);
  WriteIntWithPrefix(START_SYMBOLS2[ct], len);
  WritePiece(kCRLF);
}

void RedisReplyBuilderBase::WriteIntWithPrefix(char prefix, int64_t val) {
  char* dest = ReservePiece(absl::numbers_internal::kFastToBufferSize + 1);
  char* next = dest;
  *next++ = prefix;
  next = absl::numbers_internal::FastIntToBuffer(val, next);
  CommitPiece(next - dest);
}

void RedisReplyBuilderBase::SendError(std::string_view str, std::string_view type) {
  ReplyScope scope(this);

  if (type.empty()) {
    type = str;
    if (type == kSyntaxErr)
      type = kSyntaxErrType;
  }
  tl_facade_stats->reply_stats.err_count[type]++;

  if (str[0] != '-')
    WritePiece(kErrPref);
  WritePiece(str);
  WritePiece(kCRLF);
}

void RedisReplyBuilderBase::SendProtocolError(std::string_view str) {
  SendError(absl::StrCat("-ERR Protocol error: ", str), "protocol_error");
}

char* RedisReplyBuilderBase::FormatDouble(double d, char* dest, unsigned len) {
  StringBuilder sb(dest, len);
  CHECK(dfly_conv.ToShortest(d, &sb));
  return sb.Finalize();
}

void RedisReplyBuilderBase::SendVerbatimString(std::string_view str, VerbatimFormat format) {
  DCHECK(format <= VerbatimFormat::MARKDOWN);
  if (!IsResp3())
    return SendBulkString(str);

  ReplyScope scope(this);
  WriteIntWithPrefix('=', str.size() + 4);
  Write(kCRLF);
  WritePiece(format == VerbatimFormat::MARKDOWN ? "mkd:" : "txt:");
  Write(str);
  WritePiece(kCRLF);
}

void RedisReplyBuilder::SendSimpleStrArr(const facade::ArgRange& strs) {
  ReplyScope scope(this);
  StartArray(strs.Size());
  for (std::string_view str : strs)
    SendSimpleString(str);
}

void RedisReplyBuilder::SendBulkStrArr(const facade::ArgRange& strs, CollectionType ct) {
  ReplyScope scope(this);
  StartCollection(ct == CollectionType::MAP ? strs.Size() / 2 : strs.Size(), ct);
  for (std::string_view str : strs)
    SendBulkString(str);
}

void RedisReplyBuilder::SendScoredArray(absl::Span<const std::pair<std::string, double>> arr,
                                        bool with_scores) {
  ReplyScope scope(this);
  StartArray((with_scores && !IsResp3()) ? arr.size() * 2 : arr.size());
  for (const auto& [str, score] : arr) {
    if (with_scores && IsResp3())
      StartArray(2);
    SendBulkString(str);
    if (with_scores)
      SendDouble(score);
  }
}

void RedisReplyBuilder::SendStored() {
  SendSimpleString("OK");
}

void RedisReplyBuilder::SendSetSkipped() {
  SendSimpleString("SKIPPED");
}

void RedisReplyBuilder::StartArray(unsigned len) {
  StartCollection(len, CollectionType::ARRAY);
}

void RedisReplyBuilder::SendEmptyArray() {
  StartArray(0);
}

std::string RedisReplyBuilder::SerializeCommand(std::string_view cmd) {
  return absl::StrCat(cmd, kCRLF);
}

}  // namespace facade
