// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/resp_parser.h"

#include "base/logging.h"

namespace dfly {

RESPObj::RESPObj(RESPObj&& other) noexcept
    : reply_(other.reply_), needs_to_free_(other.needs_to_free_) {
  other.reply_ = nullptr;
  other.needs_to_free_ = false;
}

RESPObj& RESPObj::operator=(RESPObj&& other) noexcept {
  std::swap(needs_to_free_, other.needs_to_free_);
  std::swap(reply_, other.reply_);
  return *this;
}

RESPObj::~RESPObj() {
  if (needs_to_free_)
    freeReplyObject(reply_);
}

io::Result<RESPObj, GenericError> RESPParser::Feed(const char* data, size_t len) {
  auto status = redisReaderFeed(reader_, data, len);
  if (status != REDIS_OK) {
    LOG(ERROR) << "RESP parser error: " << status << " description: " << reader_->errstr
               << " data: " << std::string_view{data, len};
    return nonstd::make_unexpected(GenericError(reader_->errstr));
  }
  void* reply_obj = nullptr;
  status = redisReaderGetReply(reader_, &reply_obj);
  if (status != REDIS_OK) {
    LOG(ERROR) << "RESP parser error: " << status << " description: " << reader_->errstr
               << " data: " << data;
    return nonstd::make_unexpected(GenericError(reader_->errstr));
  }

  return RESPObj(static_cast<redisReply*>(reply_obj), reply_obj != nullptr);
}

}  // namespace dfly
