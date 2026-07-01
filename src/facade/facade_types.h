// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cassert>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <variant>

#include "common/arg_range.h"
#include "common/backed_args.h"
#include "facade/op_status.h"

namespace facade {

#if defined(__clang__)
#if defined(__has_feature)
#if __has_feature(address_sanitizer)
constexpr size_t kSanitizerOverhead = 24u;
#else
constexpr size_t kSanitizerOverhead = 0u;
#endif
#endif
#else
#ifdef __SANITIZE_ADDRESS__
constexpr size_t kSanitizerOverhead = 24u;
#else
constexpr size_t kSanitizerOverhead = 0u;
#endif
#endif

enum class Protocol : uint8_t { MEMCACHE = 1, REDIS = 2 };
enum class CollectionType : uint8_t { ARRAY, SET, MAP, PUSH };

using MutableSlice = std::string_view;
using CmdArgVec = std::vector<std::string_view>;
using cmn::ArgSlice;
using CmdArgList = cmn::ArgSlice;
using cmn::ArgRange;

class ParsedArgs {
 public:
  ParsedArgs() = default;

  // References backed arguments. The object must outlive this ParsedArgs.
  ParsedArgs(const cmn::BackedArguments& bargs, uint32_t offset = 0)  // NOLINT
      : args_(WrapperBacked{&bargs, offset}) {
  }

  ParsedArgs(ArgSlice slice)  // NOLINT google-explicit-constructor
      : args_(slice) {
  }

  ParsedArgs(const ParsedArgs& other) = default;
  ParsedArgs& operator=(const ParsedArgs& bargs) = default;

  size_t size() const {
    return std::visit([](const auto& args) { return args.size(); }, args_);
  }

  bool empty() const {
    return size() == 0;
  }

  ParsedArgs Tail(unsigned offset = 1) const {
    return std::visit([offset](const auto& args) { return args.Tail(offset); }, args_);
  }

  std::string_view Front() const {
    return std::visit([](const auto& args) { return args.front(); }, args_);
  }

  std::string_view operator[](size_t i) const {
    return std::visit([i](const auto& args) { return args.at(i); }, args_);
  }

  // Index-based const iterator, so ParsedArgs can be iterated (e.g. as a journal
  // Payload alternative) without exposing its underlying span/BackedArguments.
  class const_iterator {
   public:
    using iterator_category = std::input_iterator_tag;
    using value_type = std::string_view;
    using difference_type = ptrdiff_t;
    using pointer = const std::string_view*;
    using reference = std::string_view;

    const_iterator(const ParsedArgs* args, size_t index) : args_(args), index_(index) {
    }

    std::string_view operator*() const {
      return (*args_)[index_];
    }

    const_iterator& operator++() {
      ++index_;
      return *this;
    }

    const_iterator operator++(int) {
      const_iterator copy = *this;
      ++index_;
      return copy;
    }

    bool operator==(const const_iterator& o) const {
      return index_ == o.index_;
    }

    bool operator!=(const const_iterator& o) const {
      return index_ != o.index_;
    }

   private:
    const ParsedArgs* args_;
    size_t index_;
  };

  const_iterator begin() const {
    return const_iterator{this, 0};
  }

  const_iterator end() const {
    return const_iterator{this, size()};
  }

 private:
  struct WrapperBacked {
    WrapperBacked(const cmn::BackedArguments* args, uint32_t index = 0)  // NOLINT
        : args_(args), index_(index) {
      assert(index <= args->size());
    }

    const cmn::BackedArguments* args_;
    uint32_t index_ = 0;

    ParsedArgs Tail(unsigned offset = 1) const {
      ParsedArgs res(*args_);
      WrapperBacked* wb = std::get_if<WrapperBacked>(&res.args_);
      wb->index_ = index_ + offset;
      return res;
    };

    size_t size() const {
      return args_->size() - index_;
    }

    std::string_view front() const {
      return args_->at(index_);
    }

    std::string_view at(size_t i) const {
      return args_->at(index_ + i);
    }
  };

  struct Slice : public ArgSlice {
    using ArgSlice::ArgSlice;
    Slice(ArgSlice other) : ArgSlice(other) {  // NOLINT
    }

    std::string_view at(size_t i) const {
      return ArgSlice::operator[](i);
    }

    ParsedArgs Tail(unsigned offset = 1) const {
      return ParsedArgs{subspan(offset)};
    }
  };
  std::variant<Slice, WrapperBacked> args_;
};

inline std::string_view ToSV(std::string_view slice) {
  return slice;
}

inline std::string_view ToSV(const std::string& slice) {
  return slice;
}

inline std::string_view ToSV(std::string&& slice) = delete;

inline std::string_view ArgS(ArgSlice args, size_t i) {
  return args[i];
}

struct ErrorReply {
  explicit ErrorReply(std::string&& msg, std::string_view kind = {})
      : message{std::move(msg)}, kind{kind} {
  }
  explicit ErrorReply(std::string_view msg, std::string_view kind = {}) : message{msg}, kind{kind} {
  }
  explicit ErrorReply(const char* msg,
                      std::string_view kind = {})  // to resolve ambiguity of constructors above
      : message{std::string_view{msg}}, kind{kind} {
  }

  ErrorReply(OpStatus status)  // NOLINT google-explicit-constructor)
      : status{status} {
  }

  std::string_view ToSv() const {
    return std::visit(cmn::kToSV, message);
  }

  std::variant<std::string, std::string_view> message;
  std::string_view kind;
  std::optional<OpStatus> status{std::nullopt};
};

struct MemcacheCmdFlags {
  MemcacheCmdFlags() : raw(0) {
  }

  union {
    uint16_t raw = 0;
    struct {
      uint16_t no_reply : 1;  // q
      uint16_t meta : 1;

      // meta flags
      uint16_t base64 : 1;              // b
      uint16_t return_flags : 1;        // f
      uint16_t return_value : 1;        // v
      uint16_t return_ttl : 1;          // t
      uint16_t return_access_time : 1;  // l
      uint16_t return_hit : 1;          // h
      uint16_t return_cas : 1;          // c
    };
  };
};

static_assert(sizeof(MemcacheCmdFlags) == 2);

constexpr unsigned long long operator""_MB(unsigned long long x) {
  return 1024L * 1024L * x;
}

constexpr unsigned long long operator""_KB(unsigned long long x) {
  return 1024L * x;
}

void ResetStats();

// Constants for socket bufring.
constexpr uint16_t kRecvSockGid = 0;

// Size of the buffer in bufring (kRecvSockGid).
constexpr size_t kRecvBufSize = 1500;

}  // namespace facade

namespace std {
ostream& operator<<(ostream& os, cmn::ArgSlice args);
ostream& operator<<(ostream& os, const facade::ParsedArgs& args);
ostream& operator<<(ostream& os, facade::Protocol protocol);

}  // namespace std
