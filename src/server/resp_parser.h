// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <memory>
#include <optional>

#include "io/io.h"
#include "redis/hiredis.h"
#include "server/common.h"

namespace dfly {

class RESPArray;

class RESPObj {
 public:
  enum class Type {
    STRING = REDIS_REPLY_STRING,
    ARRAY = REDIS_REPLY_ARRAY,
    INTEGER = REDIS_REPLY_INTEGER,
    NIL = REDIS_REPLY_NIL,
    DOUBLE = REDIS_REPLY_DOUBLE,
  };

  RESPObj(redisReply* reply, bool needs_to_free) : reply_(reply), needs_to_free_(needs_to_free) {
  }

  RESPObj(const RESPObj&) = delete;
  RESPObj& operator=(const RESPObj&) = delete;

  RESPObj(RESPObj&& other) noexcept;
  RESPObj& operator=(RESPObj&& other) noexcept;

  ~RESPObj();

  bool Empty() const {
    return reply_ == nullptr;
  }

  Type GetType() const {
    return static_cast<Type>(reply_->type);
  }

  template <class T> std::optional<T> As() const {
    if constexpr (std::is_constructible_v<T, std::string_view>) {
      if (reply_->type == REDIS_REPLY_STRING) {
        return T{std::string_view{reply_->str, reply_->len}};
      }
    } else if constexpr (std::is_integral_v<T>) {
      if (reply_->type == REDIS_REPLY_INTEGER) {
        return static_cast<T>(reply_->integer);
      }
    } else if constexpr (std::is_floating_point_v<T>) {
      if (reply_->type == REDIS_REPLY_DOUBLE) {
        return static_cast<T>(reply_->dval);
      }
    } else if constexpr (std::is_same_v<T, RESPArray>) {
      if (reply_->type == REDIS_REPLY_ARRAY) {
        return RESPArray(reply_->element, reply_->elements);
      }
    }

    // TODO add other types and errors processing
    return std::nullopt;
  }

 private:
  redisReply* reply_ = nullptr;
  bool needs_to_free_ = true;
};

class RESPArray {
 public:
  RESPArray(redisReply** elements, size_t size) : elements_(elements), size_(size) {
  }

  size_t size() const {
    return size_;
  }

  RESPObj operator[](size_t index) {
    return RESPObj(elements_[index], false);
  }

 private:
  redisReply** elements_;
  size_t size_;
};

class RESPParser {
 public:
  RESPParser() : reader_(redisReaderCreate()) {
  }
  ~RESPParser() {
    redisReaderFree(reader_);
  }

  io::Result<RESPObj, GenericError> Feed(const char* data, size_t len);

 private:
  redisReader* reader_;
};

}  // namespace dfly
