// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <ostream>

namespace dfly {

enum class OpStatus : uint16_t {
  OK,
  KEY_EXISTS,
  KEY_NOTFOUND,
  SKIPPED,
  INVALID_VALUE,
  OUT_OF_RANGE,
  WRONG_TYPE,
  TIMED_OUT,
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

 private:
  OpStatus st_;
};

template <typename V> class OpResult : public OpResultBase {
 public:
  OpResult(V v) : v_(std::move(v)) {
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

  const V* operator->() const {
    return &v_;
  }

  const V& operator*() const {
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

}  // namespace dfly

namespace std {

template <typename T> std::ostream& operator<<(std::ostream& os, const dfly::OpResult<T>& res) {
  os << int(res.status());
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const dfly::OpStatus op) {
  os << int(op);
  return os;
}

}  // namespace std
