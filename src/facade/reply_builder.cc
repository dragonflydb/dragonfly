// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "facade/reply_builder.h"

#include <absl/container/fixed_array.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <double-conversion/double-to-string.h>

#include "base/logging.h"
#include "facade/error.h"

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

constexpr unsigned kConvFlags =
    DoubleToStringConverter::UNIQUE_ZERO | DoubleToStringConverter::EMIT_POSITIVE_EXPONENT_SIGN;

DoubleToStringConverter dfly_conv(kConvFlags, "inf", "nan", 'e', -6, 21, 6, 0);

}  // namespace

SinkReplyBuilder::SinkReplyBuilder(::io::Sink* sink) : sink_(sink) {
}

void SinkReplyBuilder::CloseConnection() {
  if (!ec_)
    ec_ = std::make_error_code(std::errc::connection_aborted);
}

void SinkReplyBuilder::Send(const iovec* v, uint32_t len) {
  DCHECK(sink_);

  if (should_batch_) {
    size_t total_size = batch_.size();
    for (unsigned i = 0; i < len; ++i) {
      total_size += v[i].iov_len;
    }

    if (total_size < 8192) {  // Allow batching with up to 8K of data.
      for (unsigned i = 0; i < len; ++i) {
        std::string_view src((char*)v[i].iov_base, v[i].iov_len);
        DVLOG(2) << "Appending to stream " << src;
        batch_.append(src.data(), src.size());
      }
      return;
    }
  }

  error_code ec;
  ++io_write_cnt_;

  for (unsigned i = 0; i < len; ++i) {
    io_write_bytes_ += v[i].iov_len;
  }

  if (batch_.empty()) {
    ec = sink_->Write(v, len);
  } else {
    DVLOG(1) << "Sending batch to stream " << sink_ << "\n" << batch_;

    io_write_bytes_ += batch_.size();

    iovec tmp[len + 1];
    tmp[0].iov_base = batch_.data();
    tmp[0].iov_len = batch_.size();
    copy(v, v + len, tmp + 1);
    ec = sink_->Write(tmp, len + 1);
    batch_.clear();
  }

  if (ec) {
    ec_ = ec;
  }
}

void SinkReplyBuilder::SendRaw(std::string_view raw) {
  iovec v = {IoVec(raw)};

  Send(&v, 1);
}

void SinkReplyBuilder::SendRawVec(absl::Span<const std::string_view> msg_vec) {
  absl::FixedArray<iovec, 16> arr(msg_vec.size());

  for (unsigned i = 0; i < msg_vec.size(); ++i) {
    arr[i].iov_base = const_cast<char*>(msg_vec[i].data());
    arr[i].iov_len = msg_vec[i].size();
  }
  Send(arr.data(), msg_vec.size());
}

MCReplyBuilder::MCReplyBuilder(::io::Sink* sink) : SinkReplyBuilder(sink) {
}

void MCReplyBuilder::SendSimpleString(std::string_view str) {
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

void MCReplyBuilder::SendMGetResponse(const OptResp* resp, uint32_t count) {
  string header;
  for (unsigned i = 0; i < count; ++i) {
    if (resp[i]) {
      const auto& src = *resp[i];
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
  SendSimpleString("ERROR");
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

char* RedisReplyBuilder::FormatDouble(double val, char* dest, unsigned dest_len) {
  StringBuilder sb(dest, dest_len);
  CHECK(dfly_conv.ToShortest(val, &sb));
  return sb.Finalize();
}

RedisReplyBuilder::RedisReplyBuilder(::io::Sink* sink) : SinkReplyBuilder(sink) {
}

void RedisReplyBuilder::SetResp3(bool is_resp3) {
  is_resp3_ = is_resp3;
}

void RedisReplyBuilder::SendError(string_view str, string_view err_type) {
  if (err_type.empty()) {
    err_type = str;
    if (err_type == kSyntaxErr)
      err_type = kSyntaxErrType;
  }

  err_count_[err_type]++;

  if (str[0] == '-') {
    iovec v[] = {IoVec(str), IoVec(kCRLF)};
    Send(v, ABSL_ARRAYSIZE(v));
  } else {
    iovec v[] = {IoVec(kErrPref), IoVec(str), IoVec(kCRLF)};
    Send(v, ABSL_ARRAYSIZE(v));
  }
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

const char* RedisReplyBuilder::NullString() {
  if (is_resp3_) {
    return "_\r\n";
  }
  return "$-1\r\n";
}

void RedisReplyBuilder::SendNull() {
  iovec v[] = {IoVec(NullString())};

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

std::string_view RedisReplyBuilder::StatusToMsg(OpStatus status) {
  switch (status) {
    case OpStatus::OK:
      return "OK";
    case OpStatus::KEY_NOTFOUND:
      return kKeyNotFoundErr;
    case OpStatus::WRONG_TYPE:
      return kWrongTypeErr;
    case OpStatus::OUT_OF_RANGE:
      return kIndexOutOfRange;
    case OpStatus::INVALID_FLOAT:
      return kInvalidFloatErr;
    case OpStatus::INVALID_INT:
      return kInvalidIntErr;
    case OpStatus::SYNTAX_ERR:
      return kSyntaxErr;
    case OpStatus::OUT_OF_MEMORY:
      return kOutOfMemory;
    case OpStatus::BUSY_GROUP:
      return "-BUSYGROUP Consumer Group name already exists";
    case OpStatus::INVALID_NUMERIC_RESULT:
      return kInvalidNumericResult;
    default:
      LOG(ERROR) << "Unsupported status " << status;
      return "Internal error";
  }
}

void RedisReplyBuilder::SendError(OpStatus status) {
  if (status == OpStatus::OK) {
    SendOk();
  } else {
    SendError(StatusToMsg(status));
  }
}

void RedisReplyBuilder::SendLong(long num) {
  string str = absl::StrCat(":", num, kCRLF);
  SendRaw(str);
}

void RedisReplyBuilder::SendScoredArray(const std::vector<std::pair<std::string, double>>& arr,
                                        bool with_scores) {
  if (!with_scores) {
    StartArray(arr.size());
    for (const auto& p : arr) {
      SendBulkString(p.first);
    }
    return;
  }
  if (!is_resp3_) {  // RESP2 formats withscores as a flat array.
    StartArray(arr.size() * 2);
    for (const auto& p : arr) {
      SendBulkString(p.first);
      SendDouble(p.second);
    }
    return;
  }
  // Resp3 formats withscores as array of (key, score) pairs.
  StartArray(arr.size());
  for (const auto& p : arr) {
    StartArray(2);
    SendBulkString(p.first);
    SendDouble(p.second);
  }
}

void RedisReplyBuilder::SendDouble(double val) {
  char buf[64];

  StringBuilder sb(buf, sizeof(buf));
  CHECK(dfly_conv.ToShortest(val, &sb));

  if (!is_resp3_) {
    SendBulkString(sb.Finalize());
  } else {
    // RESP3
    string str = absl::StrCat(",", sb.Finalize(), kCRLF);
    SendRaw(str);
  }
}

void RedisReplyBuilder::SendMGetResponse(const OptResp* resp, uint32_t count) {
  string res = absl::StrCat("*", count, kCRLF);
  for (size_t i = 0; i < count; ++i) {
    if (resp[i]) {
      StrAppend(&res, "$", resp[i]->value.size(), kCRLF);
      res.append(resp[i]->value).append(kCRLF);
    } else {
      res.append(NullString());
    }
  }

  SendRaw(res);
}

void RedisReplyBuilder::SendSimpleStrArr(const std::string_view* arr, uint32_t count) {
  string res = absl::StrCat("*", count, kCRLF);

  for (size_t i = 0; i < count; ++i) {
    StrAppend(&res, "+", arr[i], kCRLF);
  }

  SendRaw(res);
}

void RedisReplyBuilder::SendNullArray() {
  SendRaw("*-1\r\n");
}

void RedisReplyBuilder::SendEmptyArray() {
  StartArray(0);
}

void RedisReplyBuilder::SendStringArr(absl::Span<const std::string_view> arr) {
  if (arr.empty()) {
    SendRaw("*0\r\n");
    return;
  }

  SendStringCollection(arr.data(), arr.size(), Resp3Type::ARRAY);
}

// This implementation a bit complicated because it uses vectorized
// send to send an array. The problem with that is the OS limits vector length
// to low numbers (around 1024). Therefore, to make it robust we send the array in batches.
// We limit the vector length to 256 and when it fills up we flush it to the socket and continue
// iterating.
void RedisReplyBuilder::SendStringArr(absl::Span<const string> arr) {
  SendStringCollection(arr.data(), arr.size(), Resp3Type::ARRAY);
}

void RedisReplyBuilder::SendStringArrayAsMap(absl::Span<const std::string_view> arr) {
  SendStringCollection(arr.data(), arr.size(), Resp3Type::MAP);
}

void RedisReplyBuilder::SendStringArrayAsMap(absl::Span<const std::string> arr) {
  SendStringCollection(arr.data(), arr.size(), Resp3Type::MAP);
}

void RedisReplyBuilder::SendStringArrayAsSet(absl::Span<const std::string_view> arr) {
  SendStringCollection(arr.data(), arr.size(), Resp3Type::SET);
}

void RedisReplyBuilder::SendStringArrayAsSet(absl::Span<const std::string> arr) {
  SendStringCollection(arr.data(), arr.size(), Resp3Type::SET);
}

void RedisReplyBuilder::StartArray(unsigned len) {
  SendRaw(absl::StrCat("*", len, kCRLF));
}

void RedisReplyBuilder::StartMap(unsigned num_pairs) {
  if (is_resp3_) {
    SendRaw(absl::StrCat("%", num_pairs, kCRLF));
    return;
  }
  // Flatten for Resp2.
  StartArray(num_pairs * 2);
}

void RedisReplyBuilder::StartSet(unsigned num_elements) {
  if (is_resp3_) {
    SendRaw(absl::StrCat("~", num_elements, kCRLF));
  }
  // Flatten for Resp2.
  StartArray(num_elements);
}

void RedisReplyBuilder::SendStringCollection(StrPtr str_ptr, uint32_t len, Resp3Type type) {
  string type_char = "*";
  size_t header_len = len;
  if (is_resp3_) {
    switch (type) {
      case Resp3Type::ARRAY:
        break;
      case Resp3Type::MAP:
        type_char[0] = '%';
        header_len = 0.5 * len;  // Each key value pair counts as one.
        break;
      case Resp3Type::SET:
        type_char[0] = '~';
        break;
    }
  }

  if (header_len == 0) {
    SendRaw(absl::StrCat(type_char, "0\r\n"));
    return;
  }

  // When vector length is too long, Send returns EMSGSIZE.
  size_t vec_len = std::min<size_t>(256u, len);

  absl::FixedArray<iovec, 16> vec(vec_len * 2 + 2);
  absl::FixedArray<char, 64> meta((vec_len + 1) * 16);
  char* next = meta.data();

  *next++ = type_char[0];
  next = absl::numbers_internal::FastIntToBuffer(header_len, next);
  *next++ = '\r';
  *next++ = '\n';
  vec[0] = IoVec(string_view{meta.data(), size_t(next - meta.data())});
  char* start = next;

  unsigned vec_indx = 1;
  string_view src;
  for (unsigned i = 0; i < len; ++i) {
    if (holds_alternative<const string_view*>(str_ptr)) {
      src = get<const string_view*>(str_ptr)[i];
    } else {
      src = get<const string*>(str_ptr)[i];
    }
    *next++ = '$';
    next = absl::numbers_internal::FastIntToBuffer(src.size(), next);
    *next++ = '\r';
    *next++ = '\n';
    vec[vec_indx] = IoVec(string_view{start, size_t(next - start)});
    DCHECK_GT(next - start, 0);

    start = next;
    ++vec_indx;

    vec[vec_indx] = IoVec(src);

    *next++ = '\r';
    *next++ = '\n';
    ++vec_indx;

    if (vec_indx + 1 >= vec.size()) {
      if (i < len - 1 || vec_indx == vec.size()) {
        Send(vec.data(), vec_indx);
        if (ec_)
          return;

        vec_indx = 0;
        start = meta.data();
        next = start + 2;
        start[0] = '\r';
        start[1] = '\n';
      }
    }
  }

  vec[vec_indx].iov_base = start;
  vec[vec_indx].iov_len = 2;
  Send(vec.data(), vec_indx + 1);
}

void ReqSerializer::SendCommand(std::string_view str) {
  VLOG(1) << "SendCommand: " << str;

  iovec v[] = {IoVec(str), IoVec(kCRLF)};
  ec_ = sink_->Write(v, ABSL_ARRAYSIZE(v));
}

}  // namespace facade
