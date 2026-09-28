// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/resp_parser.h"

#include <mimalloc.h>

#include <cstring>

#include "base/logging.h"
#include "util/fibers/fibers.h"

extern "C" {
#include "redis/hiredis.h"
}

namespace facade {

namespace {

// Scopes zmalloc onto the backing heap so redisReader outliving its shard doesn't UAF the
// shard's data heap. Asserts no fiber preemption, since that could expose the swap to others.
class BackingHeapScope {
 public:
  BackingHeapScope()
      : prev_heap_(zmalloc_get_threadlocal_heap()), prev_skip_(zmalloc_skip_accounting_tl) {
    zmalloc_set_threadlocal_heap(mi_heap_get_backing());
    zmalloc_skip_accounting_tl = true;
  }

  ~BackingHeapScope() {
    zmalloc_set_threadlocal_heap(prev_heap_);
    zmalloc_skip_accounting_tl = prev_skip_;
  }

  BackingHeapScope(const BackingHeapScope&) = delete;
  BackingHeapScope& operator=(const BackingHeapScope&) = delete;

 private:
  util::FiberAtomicGuard fiber_guard_;
  void* prev_heap_;
  bool prev_skip_;
};

}  // namespace

RESPParser::RESPParser() : RESPParser(Limits{}) {
}

RESPParser::RESPParser(Limits limits) {
  Reset(limits);
}

void RESPParser::Reset() {
  Reset(Limits{});
}

void RESPParser::Reset(Limits limits) {
  {
    BackingHeapScope backing_heap;
    redisReaderFree(reader_);
    reader_ = redisReaderCreate();
  }
  CHECK(reader_);

  reader_->maxelements = limits.max_array_len;
}

RESPParser::~RESPParser() {
  BackingHeapScope backing_heap;
  redisReaderFree(reader_);
}

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
  if (needs_to_free_) {
    BackingHeapScope backing_heap;
    freeReplyObject(reply_);
  }
}

RESPObj::Type RESPObj::GetType() const {
  DCHECK(reply_);
  return static_cast<Type>(reply_->type);
}

size_t RESPObj::Size() const {
  if (!reply_)
    return 0;
  Type type = GetType();
  return (type == Type::ARRAY || type == Type::MAP || type == Type::SET) ? reply_->elements : 1;
}

std::optional<RESPObj> RESPParser::Feed(const char* data, size_t len, size_t* consumed) {
  if (consumed) {
    *consumed = 0;
  }

  // The peer controls this data, so only log a bounded prefix of it.
  auto log_error = [this, data, len](int status) {
    constexpr size_t kMaxLoggedData = 256;
    std::string_view sv = data ? std::string_view{data, len} : std::string_view{};
    LOG(ERROR) << "RESP parser error: " << status << " description: " << reader_->errstr
               << " data: " << sv.substr(0, kMaxLoggedData);
  };

  const size_t buffered_before = reader_->len - reader_->pos;
  int status = REDIS_OK;
  bool feed_failed = false;
  void* reply_obj = nullptr;
  {
    BackingHeapScope backing_heap;
    if (len != 0) {  // if no new data we check is previoud data produced a reply
      status = redisReaderFeed(reader_, data, len);
      feed_failed = status != REDIS_OK;
    }
    if (!feed_failed) {
      status = redisReaderGetReply(reader_, &reply_obj);
    }
  }

  if (feed_failed) {
    log_error(status);
    return std::nullopt;
  }
  if (consumed) {
    const size_t buffered_after = reader_->len - reader_->pos;
    DCHECK_LE(buffered_after, buffered_before + len);
    *consumed = buffered_before + len - buffered_after;
  }
  if (status != REDIS_OK) {
    log_error(status);
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
    case RESPObj::Type::INTEGER: {
      os << *obj.As<std::int64_t>();
      break;
    }
    case RESPObj::Type::DOUBLE: {
      os << *obj.As<double>();
      break;
    }
    case RESPObj::Type::ARRAY: {
      os << *obj.As<RESPArray>();
      break;
    }
    case RESPObj::Type::MAP:
      [[fallthrough]];
    case RESPObj::Type::SET: {
      os << *obj.As<RESPArray>();
      break;
    }
    case RESPObj::Type::STRING:
      [[fallthrough]];
    case RESPObj::Type::NIL:
      [[fallthrough]];
    case RESPObj::Type::ERROR:
      [[fallthrough]];
    case RESPObj::Type::REPLY_STATUS: {
      os << *obj.As<std::string_view>();
      break;
    }
    default:
      os << "Unknown RESPObj type: " << static_cast<int>(obj.GetType());
  }
  return os;
}

std::ostream& operator<<(std::ostream& os, const RESPArray& arr) {
  os << "[";
  for (int64_t i = 0; i < (int64_t)arr.Size() - 1; ++i) {
    os << arr[i] << ", ";
  }
  os << arr[arr.Size() - 1] << "]";
  return os;
}

}  // namespace facade
