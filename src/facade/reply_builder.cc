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

#ifdef __APPLE__
#ifndef UIO_MAXIOV
// Some versions of MacOSX dont have IOV_MAX
#define UIO_MAXIOV 1024
#endif
#endif

using namespace std;
using absl::StrAppend;
using namespace double_conversion;

namespace facade {

namespace {

constexpr char kCRLF[] = "\r\n";
constexpr char kSimplePref[] = "+";
constexpr char kLengthPrefix[] = "$";
constexpr char kDoublePref[] = ",";
constexpr char kLongPref[] = ":";
constexpr char kNullStringR2[] = "$-1\r\n";
constexpr char kNullStringR3[] = "_\r\n";

constexpr unsigned kConvFlags =
    DoubleToStringConverter::UNIQUE_ZERO | DoubleToStringConverter::EMIT_POSITIVE_EXPONENT_SIGN;

DoubleToStringConverter dfly_conv(kConvFlags, "inf", "nan", 'e', -6, 21, 6, 0);

template <typename T> size_t piece_size(const T& v) {
  if constexpr (is_array_v<T>)
    return ABSL_ARRAYSIZE(v) - 1;  // expect null terminated
  else if constexpr (is_integral_v<T>)
    return absl::numbers_internal::kFastToBufferSize;
  else  // string_view
    return v.size();
}

template <size_t S> char* write_piece(const char (&arr)[S], char* dest) {
  return (char*)memcpy(dest, arr, S - 1) + (S - 1);
}

template <typename T> enable_if_t<is_integral_v<T>, char*> write_piece(T num, char* dest) {
  static_assert(!is_same_v<T, char>, "Use arrays for single chars");
  return absl::numbers_internal::FastIntToBuffer(num, dest);
}

char* write_piece(string_view str, char* dest) {
  return (char*)memcpy(dest, str.data(), str.size()) + str.size();
}

}  // namespace

thread_local SinkReplyBuilder::PendingList SinkReplyBuilder::pending_list;

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
    return SendSimpleString("OK");
  SendError(StatusToMsg(status));
}

void SinkReplyBuilder::CloseConnection() {
  if (!ec_)
    ec_ = std::make_error_code(std::errc::connection_aborted);
}

template <typename... Ts> void SinkReplyBuilder::WritePieces(Ts&&... pieces) {
  if (size_t required = (piece_size(pieces) + ...); buffer_.AppendLen() <= required)
    Flush(required);

  auto iovec_end = [](const iovec& v) { return reinterpret_cast<char*>(v.iov_base) + v.iov_len; };

  // Ensure last iovec points to buffer segment
  char* dest = reinterpret_cast<char*>(buffer_.AppendBuffer().data());
  if (vecs_.empty()) {
    vecs_.push_back(iovec{dest, 0});
  } else if (iovec_end(vecs_.back()) != dest) {
    if (vecs_.size() >= IOV_MAX - 2)
      Flush();
    dest = reinterpret_cast<char*>(buffer_.AppendBuffer().data());
    vecs_.push_back(iovec{dest, 0});
  }

  DCHECK(iovec_end(vecs_.back()) == dest);
  char* ptr = dest;
  ([&]() { ptr = write_piece(pieces, ptr); }(), ...);

  size_t written = ptr - dest;
  buffer_.CommitWrite(written);
  vecs_.back().iov_len += written;
  total_size_ += written;
}

void SinkReplyBuilder::WriteRef(std::string_view str) {
  if (vecs_.size() >= IOV_MAX - 2)
    Flush();
  vecs_.push_back(iovec{const_cast<char*>(str.data()), str.size()});
  total_size_ += str.size();
}

void SinkReplyBuilder::Flush(size_t expected_buffer_cap) {
  if (!vecs_.empty())
    Send();

  // Grow backing buffer if was at least half full and still below it's max size
  if (buffer_.InputLen() * 2 > buffer_.Capacity() && buffer_.Capacity() * 2 <= kMaxBufferSize)
    expected_buffer_cap = max(expected_buffer_cap, buffer_.Capacity() * 2);

  total_size_ = 0;
  buffer_.Clear();
  vecs_.clear();
  guaranteed_pieces_ = 0;

  DCHECK_LE(expected_buffer_cap, kMaxBufferSize);  // big strings should be enqueued as iovecs
  if (expected_buffer_cap > buffer_.Capacity())
    buffer_.Reserve(expected_buffer_cap);
}

void SinkReplyBuilder::Send() {
  DCHECK(sink_ != nullptr);
  DCHECK(!vecs_.empty());
  auto& reply_stats = tl_facade_stats->reply_stats;

  send_active_ = true;
  PendingPin pin(util::fb2::ProactorBase::GetMonotonicTimeNs());

  pending_list.push_back(pin);

  reply_stats.io_write_cnt++;
  reply_stats.io_write_bytes += total_size_;
  DVLOG(2) << "Writing " << total_size_ << " bytes";
  if (auto ec = sink_->Write(vecs_.data(), vecs_.size()); ec)
    ec_ = ec;

  auto it = PendingList::s_iterator_to(pin);
  pending_list.erase(it);

  uint64_t after_ns = util::fb2::ProactorBase::GetMonotonicTimeNs();
  reply_stats.send_stats.count++;
  reply_stats.send_stats.total_duration += (after_ns - pin.timestamp_ns) / 1'000;
  DVLOG(2) << "Finished writing " << total_size_ << " bytes";
  send_active_ = false;
}

void SinkReplyBuilder::FinishScope() {
  replies_recorded_++;

  if (!batched_ || total_size_ * 2 >= kMaxBufferSize)
    return Flush();

  // Check if we have enough space to copy all refs to buffer
  size_t ref_bytes = total_size_ - buffer_.InputLen();
  if (ref_bytes > buffer_.AppendLen())
    return Flush(ref_bytes);

  // Copy all external references to buffer to safely keep batching
  for (size_t i = guaranteed_pieces_; i < vecs_.size(); i++) {
    auto ib = buffer_.InputBuffer();
    if (vecs_[i].iov_base >= ib.data() && vecs_[i].iov_base <= ib.data() + ib.size())
      continue;  // this is a piece

    DCHECK_LE(vecs_[i].iov_len, buffer_.AppendLen());
    void* dest = buffer_.AppendBuffer().data();
    memcpy(dest, vecs_[i].iov_base, vecs_[i].iov_len);
    buffer_.CommitWrite(vecs_[i].iov_len);
    vecs_[i].iov_base = dest;
  }
  guaranteed_pieces_ = vecs_.size();  // all vecs are pieces
}

MCReplyBuilder::MCReplyBuilder(::io::Sink* sink) : SinkReplyBuilder(sink), all_(0) {
}

void MCReplyBuilder::SendValue(std::string_view key, std::string_view value, uint64_t mc_ver,
                               uint32_t mc_flag) {
  ReplyScope scope(this);
  if (flag_.meta) {
    string flags;
    if (flag_.return_mcflag)
      absl::StrAppend(&flags, " f", mc_flag);
    if (flag_.return_version)
      absl::StrAppend(&flags, " c", mc_ver);
    if (flag_.return_value) {
      WritePieces("VA ", value.size(), flags, kCRLF, value, kCRLF);
    } else {
      WritePieces("HD ", flags, kCRLF);
    }
  } else {
    WritePieces("VALUE ", key, " ", mc_flag, " ", value.size());
    if (mc_ver)
      WritePieces(" ", mc_ver);

    if (value.size() <= kMaxInlineSize) {
      WritePieces(kCRLF, value, kCRLF);
    } else {
      WritePieces(kCRLF);
      WriteRef(value);
      WritePieces(kCRLF);
    }
  }
}

void MCReplyBuilder::SendSimpleString(std::string_view str) {
  if (flag_.noreply)
    return;

  ReplyScope scope(this);
  WritePieces(str, kCRLF);
}

void MCReplyBuilder::SendStored() {
  SendSimpleString(flag_.meta ? "HD" : "STORED");
}

void MCReplyBuilder::SendLong(long val) {
  SendSimpleString(absl::StrCat(val));
}

void MCReplyBuilder::SendError(string_view str, std::string_view type) {
  last_error_ = str;
  SendSimpleString(absl::StrCat("SERVER_ERROR ", str));
}

void MCReplyBuilder::SendProtocolError(std::string_view str) {
  SendSimpleString(absl::StrCat("CLIENT_ERROR ", str));
}

void MCReplyBuilder::SendClientError(string_view str) {
  SendSimpleString(absl::StrCat("CLIENT_ERROR ", str));
}

void MCReplyBuilder::SendSetSkipped() {
  SendSimpleString(flag_.meta ? "NS" : "NOT_STORED");
}

void MCReplyBuilder::SendNotFound() {
  SendSimpleString(flag_.meta ? "NF" : "NOT_FOUND");
}

void MCReplyBuilder::SendGetEnd() {
  if (!flag_.meta)
    SendSimpleString("END");
}

void MCReplyBuilder::SendMiss() {
  if (flag_.meta)
    SendSimpleString("EN");
}

void MCReplyBuilder::SendDeleted() {
  SendSimpleString(flag_.meta ? "HD" : "DELETED");
}

void MCReplyBuilder::SendRaw(std::string_view str) {
  ReplyScope scope(this);
  WriteRef(str);
}

void RedisReplyBuilderBase::SendNull() {
  ReplyScope scope(this);
  IsResp3() ? WritePieces(kNullStringR3) : WritePieces(kNullStringR2);
}

void RedisReplyBuilderBase::SendSimpleString(std::string_view str) {
  ReplyScope scope(this);
  if (str.size() <= kMaxInlineSize * 2)
    return WritePieces(kSimplePref, str, kCRLF);

  WritePieces(kSimplePref);
  WriteRef(str);
  WritePieces(kCRLF);
}

void RedisReplyBuilderBase::SendBulkString(std::string_view str) {
  ReplyScope scope(this);
  if (str.size() <= kMaxInlineSize)
    return WritePieces(kLengthPrefix, uint32_t(str.size()), kCRLF, str, kCRLF);

  DVLOG(1) << "SendBulk " << str.size();
  WritePieces(kLengthPrefix, uint32_t(str.size()), kCRLF);
  WriteRef(str);
  WritePieces(kCRLF);
}

void RedisReplyBuilderBase::SendLong(long val) {
  ReplyScope scope(this);
  WritePieces(kLongPref, val, kCRLF);
}

void RedisReplyBuilderBase::SendDouble(double val) {
  char buf[DoubleToStringConverter::kBase10MaximalLength + 8];  // +8 to be on the safe side.
  static_assert(ABSL_ARRAYSIZE(buf) < kMaxInlineSize, "Write temporary string from buf inline");
  string_view val_str = FormatDouble(val, buf, ABSL_ARRAYSIZE(buf));

  if (!IsResp3())
    return SendBulkString(val_str);

  ReplyScope scope(this);
  WritePieces(kDoublePref, val_str, kCRLF);
}

void RedisReplyBuilderBase::SendNullArray() {
  ReplyScope scope(this);
  WritePieces("*-1", kCRLF);
}

constexpr static const char START_SYMBOLS2[4][2] = {"*", "~", "%", ">"};
static_assert(START_SYMBOLS2[RedisReplyBuilderBase::MAP][0] == '%' &&
              START_SYMBOLS2[RedisReplyBuilderBase::SET][0] == '~');

void RedisReplyBuilderBase::StartCollection(unsigned len, CollectionType ct) {
  if (!IsResp3()) {  // RESP2 supports only arrays
    if (ct == MAP)
      len *= 2;
    ct = ARRAY;
  }
  ReplyScope scope(this);
  WritePieces(START_SYMBOLS2[ct], len, kCRLF);
}

void RedisReplyBuilderBase::SendError(std::string_view str, std::string_view type) {
  ReplyScope scope(this);

  if (type.empty()) {
    type = str;
    if (type == kSyntaxErr)
      type = kSyntaxErrType;
  }
  tl_facade_stats->reply_stats.err_count[type]++;
  last_error_ = str;

  if (str[0] != '-')
    WritePieces("-ERR ", str, kCRLF);
  else
    WritePieces(str, kCRLF);
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
  WritePieces("=", str.size() + 4, kCRLF, format == VerbatimFormat::MARKDOWN ? "mkd:" : "txt:");
  if (str.size() <= kMaxInlineSize)
    WritePieces(str);
  else
    WriteRef(str);
  WritePieces(kCRLF);
}

std::string RedisReplyBuilderBase::SerializeCommand(std::string_view command) {
  return string{command} + kCRLF;
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

void RedisReplyBuilder::SendScoredArray(ScoredArray arr, bool with_scores) {
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

void RedisReplyBuilder::SendLabeledScoredArray(std::string_view arr_label, ScoredArray arr) {
  ReplyScope scope(this);

  StartArray(2);

  SendBulkString(arr_label);
  StartArray(arr.size());
  for (const auto& [str, score] : arr) {
    StartArray(2);
    SendBulkString(str);
    SendDouble(score);
  }
}

void RedisReplyBuilder::SendStored() {
  SendSimpleString("OK");
}

void RedisReplyBuilder::SendSetSkipped() {
  SendNull();
}

void RedisReplyBuilder::StartArray(unsigned len) {
  StartCollection(len, CollectionType::ARRAY);
}

void RedisReplyBuilder::SendEmptyArray() {
  StartArray(0);
}

}  // namespace facade
