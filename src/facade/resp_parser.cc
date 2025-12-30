// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/resp_parser.h"

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

RESPObj::Type RESPObj::GetType() const {
  DCHECK(reply_);
  return static_cast<Type>(reply_->type);
}

std::optional<RESPObj> RESPParser::Feed(const char* data, size_t len) {
  int status = REDIS_OK;
  if (len != 0) {  // if no new data we check is previoud data produced a reply
    status = redisReaderFeed(reader_, data, len);
    if (status != REDIS_OK) {
      LOG(ERROR) << "RESP parser error: " << status << " description: " << reader_->errstr
                 << " data: " << std::string_view{data, len};
      return std::nullopt;
    }
  }
  void* reply_obj = nullptr;
  status = redisReaderGetReply(reader_, &reply_obj);
  if (status != REDIS_OK) {
    LOG(ERROR) << "RESP parser error: " << status << " description: " << reader_->errstr
               << " data: " << data;
    return std::nullopt;
  }

  return RESPObj(static_cast<redisReply*>(reply_obj), reply_obj != nullptr);
}

}  // namespace dfly
