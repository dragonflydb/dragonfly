// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <iosfwd>
#include <string_view>
#include <utility>

namespace facade {

enum class OpStatus : uint16_t {
  OK,
  KEY_EXISTS,
  KEY_NOTFOUND,
  KEY_MOVED,
  SKIPPED,
  INVALID_VALUE,
  CORRUPTED_HLL,
  OUT_OF_RANGE,
  WRONG_TYPE,
  WRONG_JSON_TYPE,
  TIMED_OUT,
  OUT_OF_MEMORY,
  INVALID_FLOAT,
  INVALID_INT,
  SYNTAX_ERR,
  BUSY_GROUP,
  STREAM_ID_SMALL,
  INVALID_NUMERIC_RESULT,
  CANCELLED,
  AT_LEAST_ONE_KEY,
  MEMBER_NOTFOUND,
  INVALID_JSON_PATH,
  INVALID_JSON,
  IO_ERROR,
  NAN_OR_INF_DURING_INCR,
  BLOOM_FILTER_LOAD_IN_PROGRESS,
};

class OpResultBase {
 public:
  OpResultBase(OpStatus st = OpStatus::OK) : st_(st) {
  }

  constexpr explicit operator bool() const {
    return st_ == OpStatus::OK;
  }

  OpStatus status() const {
    return st_;
  }

  bool operator==(OpStatus st) const {
    return st_ == st;
  }

  bool ok() const {
    return st_ == OpStatus::OK;
  }

  const char* DebugFormat() const;

 private:
  OpStatus st_;
};

template <typename V> class OpResult : public OpResultBase {
 public:
  using Type = V;

  OpResult(V&& v) : v_(std::move(v)) {
  }

  OpResult(const V& v) : v_(v) {
  }

  using OpResultBase::OpResultBase;

  const V& value() const {
    return v_;
  }

  V& value() {
    return v_;
  }

  V value_or(V v) const {
    return status() == OpStatus::OK ? v_ : v;
  }

  V* operator->() {
    return &v_;
  }

  V& operator*() & {
    return v_;
  }

  V&& operator*() && {
    return std::move(v_);
  }

  const V* operator->() const {
    return &v_;
  }

  const V& operator*() const& {
    return v_;
  }

 private:
  V v_{};
};

template <> class OpResult<void> : public OpResultBase {
 public:
  using OpResultBase::OpResultBase;
};

inline bool operator==(OpStatus st, const OpResultBase& ob) {
  return ob.operator==(st);
}

std::string_view StatusToMsg(OpStatus status);

}  // namespace facade

namespace std {

std::ostream& operator<<(std::ostream& os, facade::OpStatus op);

template <typename T> std::ostream& operator<<(std::ostream& os, const facade::OpResult<T>& res) {
  return os << res.status();
}

}  // namespace std
