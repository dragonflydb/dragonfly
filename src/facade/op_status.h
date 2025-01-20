// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <ostream>

namespace facade {

enum class OpStatus : uint16_t {
  OK,
  KEY_EXISTS,
  KEY_NOTFOUND,
  KEY_MOVED,
  SKIPPED,
  INVALID_VALUE,
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

template <typename V> class OpResultTyped : public OpResult<V> {
 public:
  OpResultTyped(V v) : OpResult<V>(std::move(v)) {
  }

  OpResultTyped(OpStatus st = OpStatus::OK) : OpResult<V>(st) {
  }

  void setType(int type) {
    type_ = type;
  }

  int type() const {
    return type_;
  }

 private:
  int type_ = -1;
};

inline bool operator==(OpStatus st, const OpResultBase& ob) {
  return ob.operator==(st);
}

std::string_view StatusToMsg(OpStatus status);

}  // namespace facade

namespace std {

template <typename T> std::ostream& operator<<(std::ostream& os, const facade::OpResult<T>& res) {
  os << res.status();
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const facade::OpStatus op) {
  os << int(op);
  return os;
}

}  // namespace std
