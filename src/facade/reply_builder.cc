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

inline iovec constexpr IoVec(std::string_view s) {
  iovec r{const_cast<char*>(s.data()), s.size()};
  return r;
}

constexpr char kCRLF[] = "\r\n";
constexpr char kErrPref[] = "-ERR ";
constexpr char kSimplePref[] = "+";
constexpr char kLengthPrefix[] = "$";
constexpr char kDoublePref[] = ",";
constexpr char kLongPref[] = ":";
constexpr char kNullStringR2[] = "$-1\r\n";
constexpr char kNullStringR3[] = "_\r\n";

constexpr unsigned kConvFlags =
    DoubleToStringConverter::UNIQUE_ZERO | DoubleToStringConverter::EMIT_POSITIVE_EXPONENT_SIGN;

DoubleToStringConverter dfly_conv(kConvFlags, "inf", "nan", 'e', -6, 21, 6, 0);

const char* NullString(bool resp3) {
  return resp3 ? "_\r\n" : "$-1\r\n";
}

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

SinkReplyBuilder::MGetResponse::~MGetResponse() {
  while (storage_list) {
    auto* next = storage_list->next;
    delete[] reinterpret_cast<char*>(storage_list);
    storage_list = next;
  }
}

SinkReplyBuilder::SinkReplyBuilder(::io::Sink* sink, Type t)
    : sink_(sink),
      should_batch_(false),
      should_aggregate_(false),
      has_replied_(true),
      send_active_(false),
      type_(t) {
}

void SinkReplyBuilder::CloseConnection() {
  if (!ec_)
    ec_ = std::make_error_code(std::errc::connection_aborted);
}

void SinkReplyBuilder::Send(const iovec* v, uint32_t len) {
  has_replied_ = true;
  DCHECK(sink_);
  constexpr size_t kMaxBatchSize = 1024;

  size_t bsize = 0;
  for (unsigned i = 0; i < len; ++i) {
    bsize += v[i].iov_len;
  }

  // Allow batching with up to kMaxBatchSize of data.
  if ((should_batch_ || should_aggregate_) && (batch_.size() + bsize < kMaxBatchSize)) {
    batch_.reserve(batch_.size() + bsize);
    for (unsigned i = 0; i < len; ++i) {
      std::string_view src((char*)v[i].iov_base, v[i].iov_len);
      DVLOG(3) << "Appending to stream " << absl::CHexEscape(src);
      batch_.append(src.data(), src.size());
    }
    DVLOG(2) << "Batched " << bsize << " bytes";
    return;
  }

  int64_t before_ns = util::fb2::ProactorBase::GetMonotonicTimeNs();
  error_code ec;
  send_active_ = true;
  tl_facade_stats->reply_stats.io_write_cnt++;
  tl_facade_stats->reply_stats.io_write_bytes += bsize;
  DVLOG(2) << "Writing " << bsize + batch_.size() << " bytes of len " << len;

  if (batch_.empty()) {
    ec = sink_->Write(v, len);
  } else {
    DVLOG(3) << "Sending batch to stream :" << absl::CHexEscape(batch_);

    tl_facade_stats->reply_stats.io_write_bytes += batch_.size();
    if (len == UIO_MAXIOV) {
      ec = sink_->Write(io::Buffer(batch_));
      if (!ec) {
        ec = sink_->Write(v, len);
      }
    } else {
      iovec tmp[len + 1];
      tmp[0].iov_base = batch_.data();
      tmp[0].iov_len = batch_.size();
      copy(v, v + len, tmp + 1);
      ec = sink_->Write(tmp, len + 1);
    }
    batch_.clear();
  }
  send_active_ = false;
  int64_t after_ns = util::fb2::ProactorBase::GetMonotonicTimeNs();
  tl_facade_stats->reply_stats.send_stats.count++;
  tl_facade_stats->reply_stats.send_stats.total_duration += (after_ns - before_ns) / 1'000;

  if (ec) {
    DVLOG(1) << "Error writing to stream: " << ec.message();
    ec_ = ec;
  }
}

void SinkReplyBuilder::SendRaw(std::string_view raw) {
  iovec v = {IoVec(raw)};

  Send(&v, 1);
}

void SinkReplyBuilder::ExpectReply() {
  has_replied_ = false;
}

void SinkReplyBuilder::SendError(ErrorReply error) {
  if (error.status)
    return SendError(*error.status);

  SendError(error.ToSv(), error.kind);
}

void SinkReplyBuilder::SendError(OpStatus status) {
  if (status == OpStatus::OK) {
    SendOk();
  } else {
    SendError(StatusToMsg(status));
  }
}

void SinkReplyBuilder::StartAggregate() {
  DVLOG(1) << "StartAggregate";
  should_aggregate_ = true;
}

void SinkReplyBuilder::StopAggregate() {
  DVLOG(1) << "StopAggregate";
  should_aggregate_ = false;

  if (should_batch_)
    return;

  FlushBatch();
}

void SinkReplyBuilder::SetBatchMode(bool batch) {
  DVLOG(1) << "SetBatchMode(" << (batch ? "true" : "false") << ")";
  should_batch_ = batch;
}

void SinkReplyBuilder::FlushBatch() {
  if (batch_.empty())
    return;

  error_code ec = sink_->Write(io::Buffer(batch_));
  batch_.clear();
  if (ec) {
    DVLOG(1) << "Error flushing to stream: " << ec.message();
    ec_ = ec;
  }
}

size_t SinkReplyBuilder::UsedMemory() const {
  return dfly::HeapSize(batch_);
}

SinkReplyBuilder2::ReplyAggregator::~ReplyAggregator() {
  rb->batched_ = prev;
  if (!prev)
    rb->Flush();
}

SinkReplyBuilder2::ReplyScope::~ReplyScope() {
  rb->scoped_ = prev;
  if (!prev)
    rb->FinishScope();
}

void SinkReplyBuilder2::SendError(ErrorReply error) {
  if (error.status)
    return SendError(*error.status);
  SendError(error.ToSv(), error.kind);
}

void SinkReplyBuilder2::SendError(OpStatus status) {
  if (status == OpStatus::OK)
    return SendSimpleString("OK");
  SendError(StatusToMsg(status));
}

void SinkReplyBuilder2::CloseConnection() {
  if (!ec_)
    ec_ = std::make_error_code(std::errc::connection_aborted);
}

template <typename... Ts> void SinkReplyBuilder2::WritePieces(Ts&&... pieces) {
  if (size_t required = (piece_size(pieces) + ...); buffer_.AppendLen() <= required)
    Flush(required);

  // Ensure last iovec points to buffer segment
  char* dest = reinterpret_cast<char*>(buffer_.AppendBuffer().data());
  if (vecs_.empty() || ((char*)vecs_.back().iov_base) + vecs_.back().iov_len != dest)
    NextVec({dest, 0});

  dest = reinterpret_cast<char*>(buffer_.AppendBuffer().data());
  char* ptr = dest;
  ([&]() { ptr = write_piece(pieces, ptr); }(), ...);

  size_t written = ptr - dest;
  buffer_.CommitWrite(written);
  vecs_.back().iov_len += written;
  total_size_ += written;
}

void SinkReplyBuilder2::WriteRef(std::string_view str) {
  NextVec(str);
  total_size_ += str.size();
}

void SinkReplyBuilder2::Flush(size_t expected_buffer_cap) {
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

void SinkReplyBuilder2::Send() {
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
}

void SinkReplyBuilder2::FinishScope() {
  if (!batched_ || total_size_ * 2 >= kMaxBufferSize)
    return Flush();

  // Check if we have enough space to copy all refs to buffer
  size_t ref_bytes = total_size_ - buffer_.InputLen();
  if (ref_bytes > buffer_.AppendLen())
    return Flush(ref_bytes);

  // Copy all extenral references to buffer to safely keep batching
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

void SinkReplyBuilder2::NextVec(std::string_view str) {
  if (vecs_.size() >= IOV_MAX - 2)
    Flush();
  vecs_.push_back(iovec{const_cast<char*>(str.data()), str.size()});
}

MCReplyBuilder::MCReplyBuilder(::io::Sink* sink) : SinkReplyBuilder(sink, MC), noreply_(false) {
}

void MCReplyBuilder::SendSimpleString(std::string_view str) {
  if (noreply_)
    return;

  iovec v[2] = {IoVec(str), IoVec(kCRLF)};

  Send(v, ABSL_ARRAYSIZE(v));
}

void MCReplyBuilder::SendStored() {
  SendSimpleString("STORED");
}

void MCReplyBuilder::SendLong(long val) {
  char buf[32];
  char* next = absl::numbers_internal::FastIntToBuffer(val, buf);
  SendSimpleString(string_view(buf, next - buf));
}

void MCReplyBuilder::SendMGetResponse(MGetResponse resp) {
  string header;
  for (unsigned i = 0; i < resp.resp_arr.size(); ++i) {
    if (resp.resp_arr[i]) {
      const auto& src = *resp.resp_arr[i];
      absl::StrAppend(&header, "VALUE ", src.key, " ", src.mc_flag, " ", src.value.size());
      if (src.mc_ver) {
        absl::StrAppend(&header, " ", src.mc_ver);
      }

      absl::StrAppend(&header, "\r\n");
      iovec v[] = {IoVec(header), IoVec(src.value), IoVec(kCRLF)};
      Send(v, ABSL_ARRAYSIZE(v));
      header.clear();
    }
  }
  SendSimpleString("END");
}

void MCReplyBuilder::SendError(string_view str, std::string_view type) {
  SendSimpleString(absl::StrCat("SERVER_ERROR ", str));
}

void MCReplyBuilder::SendProtocolError(std::string_view str) {
  SendSimpleString(absl::StrCat("CLIENT_ERROR ", str));
}

bool MCReplyBuilder::NoReply() const {
  return noreply_;
}

void MCReplyBuilder::SendClientError(string_view str) {
  iovec v[] = {IoVec("CLIENT_ERROR "), IoVec(str), IoVec(kCRLF)};
  Send(v, ABSL_ARRAYSIZE(v));
}

void MCReplyBuilder::SendSetSkipped() {
  SendSimpleString("NOT_STORED");
}

void MCReplyBuilder::SendNotFound() {
  SendSimpleString("NOT_FOUND");
}

MCReplyBuilder2::MCReplyBuilder2(::io::Sink* sink) : SinkReplyBuilder2(sink), noreply_(false) {
}

void MCReplyBuilder2::SendValue(std::string_view key, std::string_view value, uint64_t mc_ver,
                                uint32_t mc_flag) {
  ReplyScope scope(this);
  WritePieces("VALUE ", key, " ", mc_flag, " ", value.size());
  if (mc_ver)
    WritePieces(" ", mc_ver);

  if (value.size() <= kMaxInlineSize) {
    WritePieces(value, kCRLF);
  } else {
    WriteRef(value);
    WritePieces(kCRLF);
  }
}

void MCReplyBuilder2::SendSimpleString(std::string_view str) {
  if (noreply_)
    return;

  ReplyScope scope(this);
  WritePieces(str, kCRLF);
}

void MCReplyBuilder2::SendStored() {
  SendSimpleString("STORED");
}

void MCReplyBuilder2::SendLong(long val) {
  SendSimpleString(absl::StrCat(val));
}

void MCReplyBuilder2::SendError(string_view str, std::string_view type) {
  SendSimpleString(absl::StrCat("SERVER_ERROR ", str));
}

void MCReplyBuilder2::SendProtocolError(std::string_view str) {
  SendSimpleString(absl::StrCat("CLIENT_ERROR ", str));
}

void MCReplyBuilder2::SendClientError(string_view str) {
  SendSimpleString(absl::StrCat("CLIENT_ERROR", str));
}

void MCReplyBuilder2::SendSetSkipped() {
  SendSimpleString("NOT_STORED");
}

void MCReplyBuilder2::SendNotFound() {
  SendSimpleString("NOT_FOUND");
}

char* RedisReplyBuilder::FormatDouble(double val, char* dest, unsigned dest_len) {
  StringBuilder sb(dest, dest_len);
  CHECK(dfly_conv.ToShortest(val, &sb));
  return sb.Finalize();
}

RedisReplyBuilder::RedisReplyBuilder(::io::Sink* sink) : SinkReplyBuilder(sink, REDIS) {
}

void RedisReplyBuilder::SetResp3(bool is_resp3) {
  is_resp3_ = is_resp3;
}

void RedisReplyBuilder::SendError(string_view str, string_view err_type) {
  VLOG(1) << "Error: " << str;

  if (err_type.empty()) {
    err_type = str;
    if (err_type == kSyntaxErr)
      err_type = kSyntaxErrType;
    else if (err_type == kWrongTypeErr)
      err_type = kWrongTypeErrType;
    else if (err_type == kScriptNotFound)
      err_type = kScriptErrType;
  }

  tl_facade_stats->reply_stats.err_count[err_type]++;

  if (str[0] == '-') {
    iovec v[] = {IoVec(str), IoVec(kCRLF)};
    Send(v, ABSL_ARRAYSIZE(v));
    return;
  }

  iovec v[] = {IoVec(kErrPref), IoVec(str), IoVec(kCRLF)};
  Send(v, ABSL_ARRAYSIZE(v));
}

void RedisReplyBuilder::SendProtocolError(std::string_view str) {
  SendError(absl::StrCat("-ERR Protocol error: ", str), "protocol_error");
}

void RedisReplyBuilder::SendSimpleString(std::string_view str) {
  iovec v[3] = {IoVec(kSimplePref), IoVec(str), IoVec(kCRLF)};

  Send(v, ABSL_ARRAYSIZE(v));
}

void RedisReplyBuilder::SendStored() {
  SendSimpleString("OK");
}

void RedisReplyBuilder::SendSetSkipped() {
  SendNull();
}

void RedisReplyBuilder::SendNull() {
  iovec v[] = {IoVec(NullString(is_resp3_))};

  Send(v, ABSL_ARRAYSIZE(v));
}

void RedisReplyBuilder::SendBulkString(std::string_view str) {
  char tmp[absl::numbers_internal::kFastToBufferSize + 3];
  tmp[0] = '$';  // Format length
  char* next = absl::numbers_internal::FastIntToBuffer(uint32_t(str.size()), tmp + 1);
  *next++ = '\r';
  *next++ = '\n';

  std::string_view lenpref{tmp, size_t(next - tmp)};

  // 3 parts: length, string and CRLF.
  iovec v[3] = {IoVec(lenpref), IoVec(str), IoVec(kCRLF)};

  return Send(v, ABSL_ARRAYSIZE(v));
}

void RedisReplyBuilder::SendVerbatimString(std::string_view str, VerbatimFormat format) {
  if (!is_resp3_)
    return SendBulkString(str);

  char tmp[absl::numbers_internal::kFastToBufferSize + 7];
  tmp[0] = '=';
  // + 4 because format is three byte, and need to be followed by a ":"
  char* next = absl::numbers_internal::FastIntToBuffer(uint32_t(str.size() + 4), tmp + 1);
  *next++ = '\r';
  *next++ = '\n';

  DCHECK(format <= VerbatimFormat::MARKDOWN);
  if (format == VerbatimFormat::TXT)
    strcpy(next, "txt:");
  else if (format == VerbatimFormat::MARKDOWN)
    strcpy(next, "mkd:");
  next += 4;
  std::string_view lenpref{tmp, size_t(next - tmp)};
  iovec v[3] = {IoVec(lenpref), IoVec(str), IoVec(kCRLF)};
  return Send(v, ABSL_ARRAYSIZE(v));
}

void RedisReplyBuilder::SendLong(long num) {
  string str = absl::StrCat(":", num, kCRLF);
  SendRaw(str);
}

void RedisReplyBuilder::SendScoredArray(absl::Span<const std::pair<std::string, double>> arr,
                                        bool with_scores) {
  ReplyAggregator agg(this);
  if (!with_scores) {
    auto cb = [&](size_t indx) -> string_view { return arr[indx].first; };

    SendStringArrInternal(arr.size(), std::move(cb), CollectionType::ARRAY);
    return;
  }

  char buf[DoubleToStringConverter::kBase10MaximalLength * 3];  // to be on the safe side.

  if (!is_resp3_) {  // RESP2 formats withscores as a flat array.
    auto cb = [&](size_t indx) -> string_view {
      if (indx % 2 == 0)
        return arr[indx / 2].first;

      // NOTE: we reuse the same buffer, assuming that SendStringArrInternal does not reference
      // previous string_views. The assumption holds for small strings like
      // doubles because SendStringArrInternal employs small string optimization.
      // It's a bit hacky but saves allocations.
      return FormatDouble(arr[indx / 2].second, buf, sizeof(buf));
    };

    SendStringArrInternal(arr.size() * 2, std::move(cb), CollectionType::ARRAY);
    return;
  }

  // Resp3 formats withscores as array of (key, score) pairs.
  // TODO: to implement efficient serializing by extending SendStringArrInternal to support
  // 2-level arrays.
  StartArray(arr.size());
  for (const auto& p : arr) {
    StartArray(2);
    SendBulkString(p.first);
    SendDouble(p.second);
  }
}

void RedisReplyBuilder::SendDouble(double val) {
  char buf[64];

  char* start = FormatDouble(val, buf, sizeof(buf));

  if (!is_resp3_) {
    SendBulkString(start);
  } else {
    // RESP3
    SendRaw(absl::StrCat(",", start, kCRLF));
  }
}

void RedisReplyBuilder::SendMGetResponse(MGetResponse resp) {
  DCHECK(!resp.resp_arr.empty());

  size_t size = resp.resp_arr.size();

  size_t vec_len = std::min<size_t>(32, size);

  constexpr size_t kBatchLen = 32 * 2 + 2;  // (blob_size, blob) * 32 + 2 spares
  iovec vec_batch[kBatchLen];

  // for all the meta data to fill the vec batch. 10 digits for the blob size and 6 for
  // $, \r, \n, \r, \n
  absl::FixedArray<char, 64> meta((vec_len + 2) * 16);  // 2 for header and next item meta data.

  char* next = meta.data();
  char* cur_meta = next;
  *next++ = '*';
  next = absl::numbers_internal::FastIntToBuffer(size, next);
  *next++ = '\r';
  *next++ = '\n';

  unsigned vec_indx = 0;
  const char* nullstr = NullString(is_resp3_);
  size_t nulllen = strlen(nullstr);
  auto get_pending_metabuf = [&] { return string_view{cur_meta, size_t(next - cur_meta)}; };

  for (unsigned i = 0; i < size; ++i) {
    DCHECK_GE(meta.end() - next, 16);  // We have at least 16 bytes for the meta data.
    if (resp.resp_arr[i]) {
      string_view blob = resp.resp_arr[i]->value;

      *next++ = '$';
      next = absl::numbers_internal::FastIntToBuffer(blob.size(), next);
      *next++ = '\r';
      *next++ = '\n';
      DCHECK_GT(next - cur_meta, 0);

      vec_batch[vec_indx++] = IoVec(get_pending_metabuf());
      vec_batch[vec_indx++] = IoVec(blob);
      cur_meta = next;  // we combine the CRLF with the next item meta data.
      *next++ = '\r';
      *next++ = '\n';
    } else {
      memcpy(next, nullstr, nulllen);
      next += nulllen;
    }

    if (vec_indx >= (kBatchLen - 2) || (meta.end() - next < 16)) {
      // we have space for at least one iovec because in the worst case we reached (kBatchLen - 3)
      // and then filled 2 vectors in the previous iteration.
      DCHECK_LE(vec_indx, kBatchLen - 1);

      // if we do not have enough space in the meta buffer, we add the meta data to the
      // vector batch and reset it.
      if (meta.end() - next < 16) {
        vec_batch[vec_indx++] = IoVec(get_pending_metabuf());
        next = meta.data();
        cur_meta = next;
      }

      Send(vec_batch, vec_indx);
      if (ec_)
        return;

      vec_indx = 0;
      size_t meta_len = next - cur_meta;
      memcpy(meta.data(), cur_meta, meta_len);
      cur_meta = meta.data();
      next = cur_meta + meta_len;
    }
  }

  if (next - cur_meta > 0) {
    vec_batch[vec_indx++] = IoVec(get_pending_metabuf());
  }
  if (vec_indx > 0)
    Send(vec_batch, vec_indx);
}

void RedisReplyBuilder::SendSimpleStrArr(StrSpan arr) {
  string res = absl::StrCat("*", arr.Size(), kCRLF);
  for (string_view str : arr)
    StrAppend(&res, "+", str, kCRLF);

  SendRaw(res);
}

void RedisReplyBuilder::SendNullArray() {
  SendRaw("*-1\r\n");
}

void RedisReplyBuilder::SendEmptyArray() {
  StartArray(0);
}

void RedisReplyBuilder::SendStringArr(StrSpan arr, CollectionType type) {
  if (type == ARRAY && arr.Size() == 0) {
    SendRaw("*0\r\n");
    return;
  }

  auto cb = [&](size_t i) {
    return visit([i](auto& span) { return facade::ToSV(span[i]); }, arr.span);
  };
  SendStringArrInternal(arr.Size(), std::move(cb), type);
}

void RedisReplyBuilder::StartArray(unsigned len) {
  StartCollection(len, ARRAY);
}

constexpr static string_view START_SYMBOLS[] = {"*", "~", "%", ">"};
static_assert(START_SYMBOLS[RedisReplyBuilder::MAP] == "%" &&
              START_SYMBOLS[RedisReplyBuilder::SET] == "~");

void RedisReplyBuilder::StartCollection(unsigned len, CollectionType type) {
  if (!is_resp3_) {  // Flatten for Resp2
    if (type == MAP)
      len *= 2;
    type = ARRAY;
  }

  DVLOG(2) << "StartCollection(" << len << ", " << type << ")";

  // We do not want to send multiple packets for small responses because these
  // trigger TCP-related artifacts (e.g. Nagle's algorithm) that slow down the delivery of the whole
  // response.
  bool prev = should_aggregate_;
  should_aggregate_ |= (len > 0);
  SendRaw(absl::StrCat(START_SYMBOLS[type], len, kCRLF));
  should_aggregate_ = prev;
}

// This implementation a bit complicated because it uses vectorized
// send to send an array. The problem with that is the OS limits vector length to UIO_MAXIOV.
// Therefore, to make it robust we send the array in batches.
// We limit the vector length, and when it fills up we flush it to the socket and continue
// iterating.
void RedisReplyBuilder::SendStringArrInternal(
    size_t size, absl::FunctionRef<std::string_view(unsigned)> producer, CollectionType type) {
  size_t header_len = size;
  string_view type_char = "*";
  if (is_resp3_) {
    type_char = START_SYMBOLS[type];
    if (type == MAP)
      header_len /= 2;  // Each key value pair counts as one.
  }

  if (header_len == 0) {
    SendRaw(absl::StrCat(type_char, "0\r\n"));
    return;
  }

  // We limit iovec capacity, vectorized length is limited upto UIO_MAXIOV (Send returns EMSGSIZE).
  size_t vec_cap = std::min<size_t>(UIO_MAXIOV, size * 2);
  absl::FixedArray<iovec, 16> vec(vec_cap);
  absl::FixedArray<char, 128> meta(std::max<size_t>(vec_cap * 64, 128u));

  char* start = meta.data();
  char* next = start;

  // at most 35 chars.
  auto serialize_len = [&](char prefix, size_t len) {
    *next++ = prefix;
    next = absl::numbers_internal::FastIntToBuffer(len, next);  // at most 32 chars
    *next++ = '\r';
    *next++ = '\n';
  };

  serialize_len(type_char[0], header_len);
  unsigned vec_indx = 0;
  string_view src;

#define FLUSH_IOVEC()           \
  do {                          \
    Send(vec.data(), vec_indx); \
    if (ec_)                    \
      return;                   \
    vec_indx = 0;               \
    next = meta.data();         \
  } while (false)

  for (unsigned i = 0; i < size; ++i) {
    DCHECK_LT(vec_indx, vec_cap);

    src = producer(i);
    serialize_len('$', src.size());

    // copy data either by referencing via an iovec or copying inline into meta buf.
    constexpr size_t kSSOLen = 32;
    if (src.size() > kSSOLen) {
      // reference metadata blob before referencing another vector.
      DCHECK_GT(next - start, 0);
      vec[vec_indx++] = IoVec(string_view{start, size_t(next - start)});
      if (vec_indx >= vec_cap) {
        FLUSH_IOVEC();
      }

      DCHECK_LT(vec_indx, vec.size());
      vec[vec_indx++] = IoVec(src);
      if (vec_indx >= vec_cap) {
        FLUSH_IOVEC();
      }
      start = next;
    } else if (src.size() > 0) {
      // NOTE!: this is not just optimization. producer may returns a string_piece that will
      // be overriden for the next call, so we must do this for correctness.
      memcpy(next, src.data(), src.size());
      next += src.size();
    }

    // how much buffer we need to perform the next iteration.
    constexpr ptrdiff_t kMargin = kSSOLen + 3 /* $\r\n */ + 2 /*length*/ + 2 /* \r\n */;

    // Keep at least kMargin bytes for a small string as well as its length.
    if (kMargin >= meta.end() - next) {
      // Flush the iovec array.
      vec[vec_indx++] = IoVec(string_view{start, size_t(next - start)});
      FLUSH_IOVEC();
      start = next;
    }
    *next++ = '\r';
    *next++ = '\n';
  }

  vec[vec_indx].iov_base = start;
  vec[vec_indx].iov_len = next - start;
  Send(vec.data(), vec_indx + 1);
}

void RedisReplyBuilder2Base::SendNull() {
  ReplyScope scope(this);
  has_replied_ = true;
  resp3_ ? WritePieces(kNullStringR3) : WritePieces(kNullStringR2);
}

void RedisReplyBuilder2Base::SendSimpleString(std::string_view str) {
  ReplyScope scope(this);
  has_replied_ = true;
  if (str.size() <= kMaxInlineSize * 2)
    return WritePieces(kSimplePref, str, kCRLF);

  WritePieces(kSimplePref);
  WriteRef(str);
  WritePieces(kCRLF);
}

void RedisReplyBuilder2Base::SendBulkString(std::string_view str) {
  ReplyScope scope(this);
  has_replied_ = true;
  if (str.size() <= kMaxInlineSize)
    return WritePieces(kLengthPrefix, uint32_t(str.size()), kCRLF, str, kCRLF);

  WritePieces(kLengthPrefix, uint32_t(str.size()), kCRLF);
  WriteRef(str);
  WritePieces(kCRLF);
}

void RedisReplyBuilder2Base::SendLong(long val) {
  ReplyScope scope(this);
  has_replied_ = true;
  WritePieces(kLongPref, val, kCRLF);
}

void RedisReplyBuilder2Base::SendDouble(double val) {
  has_replied_ = true;
  char buf[DoubleToStringConverter::kBase10MaximalLength + 8];  // +8 to be on the safe side.
  static_assert(ABSL_ARRAYSIZE(buf) < kMaxInlineSize, "Write temporary string from buf inline");
  string_view val_str = FormatDouble(val, buf, ABSL_ARRAYSIZE(buf));

  if (!resp3_)
    return SendBulkString(val_str);

  ReplyScope scope(this);
  WritePieces(kDoublePref, val_str, kCRLF);
}

void RedisReplyBuilder2Base::SendNullArray() {
  ReplyScope scope(this);
  has_replied_ = true;
  WritePieces("*-1", kCRLF);
}

constexpr static const char START_SYMBOLS2[4][2] = {"*", "~", "%", ">"};
static_assert(START_SYMBOLS2[RedisReplyBuilder2Base::MAP][0] == '%' &&
              START_SYMBOLS2[RedisReplyBuilder2Base::SET][0] == '~');

void RedisReplyBuilder2Base::StartCollection(unsigned len, CollectionType ct) {
  has_replied_ = true;
  if (!IsResp3()) {  // RESP2 supports only arrays
    if (ct == MAP)
      len *= 2;
    ct = ARRAY;
  }
  ReplyScope scope(this);
  WritePieces(START_SYMBOLS2[ct], len, kCRLF);
}

void RedisReplyBuilder2Base::SendError(std::string_view str, std::string_view type) {
  ReplyScope scope(this);
  has_replied_ = true;

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

void RedisReplyBuilder2Base::SendProtocolError(std::string_view str) {
  SendError(absl::StrCat("-ERR Protocol error: ", str), "protocol_error");
}

char* RedisReplyBuilder2Base::FormatDouble(double d, char* dest, unsigned len) {
  StringBuilder sb(dest, len);
  CHECK(dfly_conv.ToShortest(d, &sb));
  return sb.Finalize();
}

void RedisReplyBuilder2Base::SendVerbatimString(std::string_view str, VerbatimFormat format) {
  has_replied_ = true;
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

std::string RedisReplyBuilder2Base::SerializeCommand(std::string_view command) {
  return string{command} + kCRLF;
}

void RedisReplyBuilder2::SendSimpleStrArr2(const facade::ArgRange& strs) {
  ReplyScope scope(this);
  StartArray(strs.Size());
  for (std::string_view str : strs)
    SendSimpleString(str);
}

void RedisReplyBuilder2::SendBulkStrArr(const facade::ArgRange& strs, CollectionType ct) {
  ReplyScope scope(this);
  StartCollection(ct == CollectionType::MAP ? strs.Size() / 2 : strs.Size(), ct);
  for (std::string_view str : strs)
    SendBulkString(str);
}

void RedisReplyBuilder2::SendScoredArray(absl::Span<const std::pair<std::string, double>> arr,
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

void RedisReplyBuilder2::SendStored() {
  SendSimpleString("OK");
}

void RedisReplyBuilder2::SendSetSkipped() {
  SendNull();
}

void RedisReplyBuilder2::StartArray(unsigned len) {
  StartCollection(len, CollectionType::ARRAY);
}

void RedisReplyBuilder2::SendEmptyArray() {
  StartArray(0);
}

void RedisReplyBuilder2::SendMGetResponse(SinkReplyBuilder::MGetResponse resp) {
  ReplyScope scope(this);
  StartArray(resp.resp_arr.size());
  for (const auto& entry : resp.resp_arr) {
    if (entry)
      SendBulkString(entry->value);
    else
      SendNull();
  }
}

}  // namespace facade
