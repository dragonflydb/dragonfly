// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
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

RespSerializer::RespSerializer(io::Sink* stream) : sink_(stream) {
}

void RespSerializer::Send(const iovec* v, uint32_t len) {
  error_code ec = sink_->Write(v, len);
  if (ec) {
    ec_ = ec;
  }
}

void RespSerializer::SendDirect(std::string_view raw) {
  iovec v = {IoVec(raw)};

  Send(&v, 1);
}

void ReplyBuilder::SendBulkString(std::string_view str) {
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

void ReplyBuilder::SendError(std::string_view str) {
  if (str[0] == '-') {
    iovec v[] = {IoVec(str), IoVec(kCRLF)};
    return Send(v, ABSL_ARRAYSIZE(v));
  } else {
    iovec v[] = {IoVec(kErrPref), IoVec(str), IoVec(kCRLF)};
    return Send(v, ABSL_ARRAYSIZE(v));
  }
}

void ReplyBuilder::SendError(OpStatus status) {
  switch (status) {
    case OpStatus::OK:
      SendOk();
      break;
    case OpStatus::KEY_NOTFOUND:
      SendError("no such key");
      break;
    default:
      LOG(ERROR) << "Unsupported status " << status;
      SendError("Internal error");
      break;
  }
}

void ReplyBuilder::SendNull() {
  constexpr char kNullStr[] = "$-1\r\n";

  iovec v[] = {IoVec(kNullStr)};

  Send(v, ABSL_ARRAYSIZE(v));
}

void ReplyBuilder::SendSimpleString(std::string_view str) {
  iovec v[3] = {IoVec(kSimplePref), IoVec(str), IoVec(kCRLF)};

  Send(v, ABSL_ARRAYSIZE(v));
}

}  // namespace dfly
