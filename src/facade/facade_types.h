// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>
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
  ParsedArgs(const cmn::BackedArguments& bargs)  // NOLINT google-explicit-constructor
      : args_(&bargs) {
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

  ParsedArgs Tail() const {
    return std::visit([](const auto& args) { return args.Tail(); }, args_);
  }

  std::string_view Front() const {
    return std::visit([](const auto& args) { return args.front(); }, args_);
  }

  ArgSlice ToSlice(CmdArgVec* scratch) const {
    return std::visit([scratch](const auto& args) { return args.ToSlice(scratch); }, args_);
  }

  void ToVec(CmdArgVec* vec) const {
    std::visit([vec](const auto& args) { return args.ToVec(vec); }, args_);
  }

 private:
  struct WrapperBacked {
    WrapperBacked(const cmn::BackedArguments* args) : args_(args) {  // NOLINT
    }

    const cmn::BackedArguments* args_;
    uint32_t index_ = 0;

    ParsedArgs Tail() const {
      ParsedArgs res(*args_);
      WrapperBacked* wb = std::get_if<WrapperBacked>(&res.args_);
      wb->index_ = index_ + 1;
      return res;
    };

    size_t size() const {
      return args_->size() - index_;
    }

    std::string_view front() const {
      return args_->at(index_);
    }

    ArgSlice ToSlice(CmdArgVec* scratch) const {
      ToVec(scratch);
      return *scratch;
    }

    void ToVec(CmdArgVec* scratch) const {
      auto sub_view = args_->view() | std::views::drop(index_);
      static_assert(std::ranges::sized_range<decltype(sub_view)>);

      scratch->reserve(std::ranges::distance(sub_view));
      std::ranges::copy(sub_view, std::back_inserter(*scratch));
    }
  };

  struct Slice : public ArgSlice {
    using ArgSlice::ArgSlice;
    Slice(ArgSlice other) : ArgSlice(other) {  // NOLINT
    }

    ParsedArgs Tail() const {
      return ParsedArgs{subspan(1)};
    }

    ArgSlice ToSlice(void* /*scratch*/) const {
      return *this;
    }

    void ToVec(CmdArgVec* vec) const {
      vec->assign(begin(), end());
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
ostream& operator<<(ostream& os, facade::Protocol protocol);

}  // namespace std
