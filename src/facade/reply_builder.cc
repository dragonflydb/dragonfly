// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "facade/reply_builder.h"

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include "base/logging.h"
#include "facade/error.h"

using namespace std;
using absl::StrAppend;

namespace facade {

namespace {

inline iovec constexpr IoVec(std::string_view s) {
  iovec r{const_cast<char*>(s.data()), s.size()};
  return r;
}

constexpr char kCRLF[] = "\r\n";
constexpr char kErrPref[] = "-ERR ";
constexpr char kSimplePref[] = "+";

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
    // TODO: to introduce flushing when too much data is batched.
    for (unsigned i = 0; i < len; ++i) {
      std::string_view src((char*)v[i].iov_base, v[i].iov_len);
      DVLOG(2) << "Appending to stream " << sink_ << " " << src;
      batch_.append(src.data(), src.size());
    }
    return;
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
  iovec v[msg_vec.size()];

  for (unsigned i = 0; i < msg_vec.size(); ++i) {
    v[i].iov_base = const_cast<char*>(msg_vec[i].data());
    v[i].iov_len = msg_vec[i].size();
  }
  Send(v, msg_vec.size());
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
      absl::StrAppend(&header, "VALUE ", src.key, " ", src.mc_flag, " ",
                      src.value.size());
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

RedisReplyBuilder::RedisReplyBuilder(::io::Sink* sink) : SinkReplyBuilder(sink) {
}

void RedisReplyBuilder::SendError(string_view str, std::string_view type) {
  err_count_[type.empty() ? str : type]++;

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

void RedisReplyBuilder::SendNull() {
  constexpr char kNullStr[] = "$-1\r\n";

  iovec v[] = {IoVec(kNullStr)};

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

void RedisReplyBuilder::SendError(OpStatus status) {
  switch (status) {
    case OpStatus::OK:
      SendOk();
      break;
    case OpStatus::KEY_NOTFOUND:
      SendError(kKeyNotFoundErr);
      break;
    case OpStatus::WRONG_TYPE:
      SendError(kWrongTypeErr);
      break;
    default:
      LOG(ERROR) << "Unsupported status " << status;
      SendError("Internal error");
      break;
  }
}

void RedisReplyBuilder::SendLong(long num) {
  string str = absl::StrCat(":", num, kCRLF);
  SendRaw(str);
}

void RedisReplyBuilder::SendDouble(double val) {
  SendBulkString(absl::StrCat(val));
}

void RedisReplyBuilder::SendMGetResponse(const OptResp* resp, uint32_t count) {
  string res = absl::StrCat("*", count, kCRLF);
  for (size_t i = 0; i < count; ++i) {
    if (resp[i]) {
      StrAppend(&res, "$", resp[i]->value.size(), kCRLF);
      res.append(resp[i]->value).append(kCRLF);
    } else {
      res.append("$-1\r\n");
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

void RedisReplyBuilder::SendStringArr(absl::Span<const std::string_view> arr) {
  string res = absl::StrCat("*", arr.size(), kCRLF);

  for (size_t i = 0; i < arr.size(); ++i) {
    StrAppend(&res, "$", arr[i].size(), kCRLF);
    res.append(arr[i]).append(kCRLF);
  }
  SendRaw(res);
}

void RedisReplyBuilder::SendStringArr(absl::Span<const string> arr) {
  string res = absl::StrCat("*", arr.size(), kCRLF);

  for (size_t i = 0; i < arr.size(); ++i) {
    StrAppend(&res, "$", arr[i].size(), kCRLF);
    res.append(arr[i]).append(kCRLF);
  }
  SendRaw(res);
}

void RedisReplyBuilder::StartArray(unsigned len) {
  SendRaw(absl::StrCat("*", len, kCRLF));
}

void ReqSerializer::SendCommand(std::string_view str) {
  VLOG(1) << "SendCommand: " << str;

  iovec v[] = {IoVec(str), IoVec(kCRLF)};
  ec_ = sink_->Write(v, ABSL_ARRAYSIZE(v));
}

}  // namespace dfly
