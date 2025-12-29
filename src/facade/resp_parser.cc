// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/resp_parser.h"

#include "base/logging.h"

namespace facade {

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

std::ostream& operator<<(std::ostream& os, const RESPObj& obj) {
  if (obj.Empty()) {
    os << "nullptr RESPObj";
    return os;
  }
  switch (obj.GetType()) {
    // because we check type we don't expect As<T> to return nullopt here
    case RESPObj::Type::STRING: {
      os << *obj.As<std::string_view>();
      break;
    }
    case RESPObj::Type::INTEGER: {
      os << *obj.As<std::int64_t>();
      break;
    }
    case RESPObj::Type::DOUBLE: {
      os << *obj.As<double>();
      break;
    }
    case RESPObj::Type::NIL:
      os << "NIL";
      break;
    case RESPObj::Type::ARRAY: {
      os << *obj.As<RESPArray>();
      break;
    }
    default:
      os << "Unknown RESPObj type: " << static_cast<int>(obj.GetType());
  }
  return os;
}

std::ostream& operator<<(std::ostream& os, const RESPArray& arr) {
  os << "[";
  for (size_t i = 0; i < arr.Size() - 1; ++i) {
  }
  os << arr[arr.Size() - 1] << "]";
  return os;
}

}  // namespace facade
