// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/reply_builder.h"

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include "base/logging.h"

using namespace std;
using absl::StrAppend;

namespace dfly {

namespace {

inline iovec constexpr IoVec(std::string_view s) {
  iovec r{const_cast<char*>(s.data()), s.size()};
  return r;
}

constexpr char kCRLF[] = "\r\n";
constexpr char kErrPref[] = "-ERR ";
constexpr char kSimplePref[] = "+";

}  // namespace

BaseSerializer::BaseSerializer(io::Sink* sink) : sink_(sink) {
}

void BaseSerializer::Send(const iovec* v, uint32_t len) {
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
  if (batch_.empty()) {
    ec = sink_->Write(v, len);
  } else {
    DVLOG(1) << "Sending batch to stream " << sink_ << "\n" << batch_;

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

void BaseSerializer::SendDirect(std::string_view raw) {
  iovec v = {IoVec(raw)};

  Send(&v, 1);
}

void RespSerializer::SendNull() {
  constexpr char kNullStr[] = "$-1\r\n";

  iovec v[] = {IoVec(kNullStr)};

  Send(v, ABSL_ARRAYSIZE(v));
}

void RespSerializer::SendSimpleString(std::string_view str) {
  iovec v[3] = {IoVec(kSimplePref), IoVec(str), IoVec(kCRLF)};

  Send(v, ABSL_ARRAYSIZE(v));
}

void RespSerializer::SendBulkString(std::string_view str) {
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

void MemcacheSerializer::SendStored() {
  SendDirect("STORED\r\n");
}

void MemcacheSerializer::SendError() {
  SendDirect("ERROR\r\n");
}

ReplyBuilder::ReplyBuilder(Protocol protocol, ::io::Sink* sink) : protocol_(protocol) {
  if (protocol == Protocol::REDIS) {
    serializer_.reset(new RespSerializer(sink));
  } else {
    DCHECK(protocol == Protocol::MEMCACHE);
    serializer_.reset(new MemcacheSerializer(sink));
  }
}

void ReplyBuilder::SendStored() {
  if (protocol_ == Protocol::REDIS) {
    as_resp()->SendSimpleString("OK");
  } else {
    as_mc()->SendStored();
  }
}

void ReplyBuilder::SendMCClientError(string_view str) {
  DCHECK(protocol_ == Protocol::MEMCACHE);

  iovec v[] = {IoVec("CLIENT_ERROR"), IoVec(str), IoVec(kCRLF)};
  serializer_->Send(v, ABSL_ARRAYSIZE(v));
}

void ReplyBuilder::EndMultilineReply() {
  if (protocol_ == Protocol::MEMCACHE) {
    serializer_->SendDirect("END\r\n");
  }
}

void ReplyBuilder::SendError(string_view str) {
  DCHECK(protocol_ == Protocol::REDIS);

  if (str[0] == '-') {
    iovec v[] = {IoVec(str), IoVec(kCRLF)};
    serializer_->Send(v, ABSL_ARRAYSIZE(v));
  } else {
    iovec v[] = {IoVec(kErrPref), IoVec(str), IoVec(kCRLF)};
    serializer_->Send(v, ABSL_ARRAYSIZE(v));
  }
}

void ReplyBuilder::SendError(OpStatus status) {
  DCHECK(protocol_ == Protocol::REDIS);

  switch (status) {
    case OpStatus::OK:
      SendOk();
      break;
    default:
      LOG(ERROR) << "Unsupported status " << status;
      SendError("Internal error");
      break;
  }
}

void ReplyBuilder::SendGetReply(std::string_view key, uint32_t flags, std::string_view value) {
  if (protocol_ == Protocol::REDIS) {
    as_resp()->SendBulkString(value);
  } else {
    string first = absl::StrCat("VALUE ", key, " ", flags, " ", value.size(), "\r\n");
    iovec v[] = {IoVec(first), IoVec(value), IoVec(kCRLF)};
    serializer_->Send(v, ABSL_ARRAYSIZE(v));
  }
}

void ReplyBuilder::SendGetNotFound() {
  if (protocol_ == Protocol::REDIS) {
    as_resp()->SendNull();
  }
}

void ReplyBuilder::SendLong(long num) {
  string str = absl::StrCat(":", num, kCRLF);
  as_resp()->SendDirect(str);
}

void ReplyBuilder::SendMGetResponse(const StrOrNil* arr, uint32_t count) {
  string res = absl::StrCat("*", count, kCRLF);
  for (size_t i = 0; i < count; ++i) {
    if (arr[i]) {
      StrAppend(&res, "$", arr[i]->size(), kCRLF);
      res.append(*arr[i]).append(kCRLF);
    } else {
      res.append("$-1\r\n");
    }
  }

  as_resp()->SendDirect(res);
}

void ReplyBuilder::SendSimpleStrArr(const std::string_view* arr, uint32_t count) {
  CHECK(protocol_ == Protocol::REDIS);
  string res = absl::StrCat("*", count, kCRLF);

  for (size_t i = 0; i < count; ++i) {
    StrAppend(&res, "+", arr[i], kCRLF);
  }

  serializer_->SendDirect(res);
}

}  // namespace dfly
