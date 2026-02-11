// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/resp_parser.h"

#include <cstring>
#include <mutex>

#include "base/logging.h"

extern "C" {
#include "redis/read.h"
}

namespace facade {

namespace {

// File-scope storage for original createNil and createArray from the default hiredis functions.
void* (*g_original_create_nil)(const redisReadTask*) = nullptr;
void* (*g_original_create_array)(const redisReadTask*, size_t) = nullptr;

// Custom createNil callback that preserves the distinction between null bulk strings ($-1)
// and null arrays (*-1). The default hiredis createNil discards task->type and always
// creates REDIS_REPLY_NIL, losing the information that the nil came from an aggregate type.
// For aggregate nils we create the reply as the original aggregate type with SIZE_MAX elements.
void* CreateNilPreservingType(const redisReadTask* task) {
  int type = task->type;
  bool is_aggregate = (type == REDIS_REPLY_ARRAY || type == REDIS_REPLY_MAP ||
                       type == REDIS_REPLY_SET || type == REDIS_REPLY_PUSH);

  if (is_aggregate) {
    // Use the default createArray with 0 elements — it allocates via the correct
    // allocator (zmalloc) and sets up parent linkage.
    void* obj = g_original_create_array(task, 0);
    if (obj == nullptr)
      return nullptr;
    // SIZE_MAX sentinel for "null aggregate".
    static_cast<redisReply*>(obj)->elements = SIZE_MAX;
    return obj;
  }

  // Non-aggregate nil ($-1): delegate to the original createNil.
  return g_original_create_nil(task);
}

// Custom function table: identical to the default except for createNil.
redisReplyObjectFunctions g_custom_functions = {};

void InitCustomFunctions() {
  static std::once_flag initialized;
  std::call_once(initialized, [] {
    // Extract default function pointers from a temporary default reader.
    redisReader* tmp = redisReaderCreate();
    g_custom_functions = *tmp->fn;
    g_original_create_nil = tmp->fn->createNil;
    g_original_create_array = tmp->fn->createArray;
    redisReaderFree(tmp);

    // Override createNil with our version that preserves aggregate nil type info.
    g_custom_functions.createNil = CreateNilPreservingType;
  });
}

}  // namespace

RESPParser::RESPParser() {
  InitCustomFunctions();
  reader_ = redisReaderCreateWithFunctions(&g_custom_functions);
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
  if (needs_to_free_)
    freeReplyObject(reply_);
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
