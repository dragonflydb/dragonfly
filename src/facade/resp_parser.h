// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <cassert>
#include <memory>
#include <optional>
#include <tuple>

#include "io/io.h"
extern "C" {
#include "redis/hiredis.h"
}

namespace facade {

class RESPArray;
class RESPIterator;

class RESPObj {
 public:
  enum class Type {
    STRING = REDIS_REPLY_STRING,
    ARRAY = REDIS_REPLY_ARRAY,
    INTEGER = REDIS_REPLY_INTEGER,
    NIL = REDIS_REPLY_NIL,
    DOUBLE = REDIS_REPLY_DOUBLE,
  };
  RESPObj() = default;
  RESPObj(redisReply* reply, bool needs_to_free) : reply_(reply), needs_to_free_(needs_to_free) {
  }

  RESPObj(const RESPObj& other) : reply_(other.reply_), needs_to_free_(false) {
  }
  RESPObj& operator=(const RESPObj& other) = delete;

  RESPObj(RESPObj&& other) noexcept;
  RESPObj& operator=(RESPObj&& other) noexcept;

  ~RESPObj();

  bool Empty() const {
    return reply_ == nullptr;
  }

  size_t Size() const;

  Type GetType() const;

  template <class T> std::optional<T> As() const;

 private:
  redisReply* reply_ = nullptr;
  bool needs_to_free_ = true;
};

class RESPArray {
 public:
  RESPArray(redisReply* arr_obj = nullptr) : arr_obj_(arr_obj) {
  }

  size_t Size() const {
    return arr_obj_->elements;
  }

  bool Empty() const {
    return Size() == 0;
  }

  RESPObj operator[](size_t index) const {
    return RESPObj(arr_obj_->element[index], false);
  }

 private:
  redisReply* arr_obj_ = nullptr;
};

class RESPParser {
 public:
  RESPParser() : reader_(redisReaderCreate()) {
  }
  ~RESPParser() {
    redisReaderFree(reader_);
  }

  std::optional<RESPObj> Feed(const char* data, size_t len);

 private:
  redisReader* reader_;
};

std::ostream& operator<<(std::ostream& os, const RESPObj& obj);
std::ostream& operator<<(std::ostream& os, const RESPArray& arr);

class RESPIterator {
 public:
  RESPIterator() = default;
  RESPIterator(const RESPObj& obj) : obj_(obj) {
  }

  RESPIterator(RESPIterator&&) = default;
  RESPIterator& operator=(RESPIterator&&) = default;

  bool HasNext() const {
    return index_ < obj_.Size();
  }

  bool HasError() const {
    return index_ == std::numeric_limits<decltype(index_)>::max();
  }

  // Consume next values and return as tuple or single value
  // if extraction fails, set error state
  template <class T = std::string_view, class... Ts> auto Next() {
    std::conditional_t<sizeof...(Ts) == 0, T, std::tuple<T, Ts...>> res{};
    bool success = true;
    if constexpr (sizeof...(Ts) == 0) {
      success = Check(&res);
    } else {
      success = std::apply([this](auto&... args) { return Check<T, Ts...>(&args...); }, res);
    }
    SetError(!success);
    return res;
  }

  // increase index only if all args are successfully extracted
  template <class Arg, class... Args> bool Check(Arg* arg, Args*... args) {
    auto tmp_index = index_;
    if (index_ + sizeof...(Args) < obj_.Size()) {
      if (auto arr = obj_.As<RESPArray>(); arr.has_value()) {
        if (GetEntry(*arr, index_++, arg) && (GetEntry(*arr, index_++, args) && ...)) {
          return true;
        }
      } else if (auto val = obj_.As<Arg>(); val.has_value()) {
        assert(sizeof...(Args) == 0 && index_ == 0);
        *arg = std::move(*val);
        return true;
      }
    }
    index_ = tmp_index;
    return false;
  }

  void SetError(bool set = true) {
    if (set)
      index_ = std::numeric_limits<decltype(index_)>::max();
  }

 private:
  template <class Arg> bool GetEntry(const RESPArray& arr, size_t idx, Arg* arg) {
    if (auto val = arr[idx].As<Arg>(); val.has_value()) {
      *arg = std::move(*val);

      return true;
    }
    return false;
  }

 private:
  RESPObj obj_;
  size_t index_ = 0;
};

template <class T> std::optional<T> RESPObj::As() const {
  if (!reply_) {
    return std::nullopt;
  }
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
      return RESPArray(reply_);
    }
  } else if constexpr (std::is_same_v<T, RESPObj>) {
    return RESPObj(reply_, false);
  } else if constexpr (std::is_same_v<T, RESPIterator>) {
    return RESPIterator(RESPObj(reply_, false));
  }

  // TODO add other types and errors processing
  return std::nullopt;
}

}  // namespace facade
