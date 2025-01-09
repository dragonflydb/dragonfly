// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/types/span.h>

#include <optional>
#include <string>
#include <string_view>
#include <variant>

#include "base/iterator.h"
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

using MutableSlice = std::string_view;
using CmdArgList = absl::Span<const std::string_view>;
using CmdArgVec = std::vector<std::string_view>;
using ArgSlice = absl::Span<const std::string_view>;
using OwnedArgSlice = absl::Span<const std::string>;

inline std::string_view ToSV(std::string_view slice) {
  return slice;
}

inline std::string_view ToSV(const std::string& slice) {
  return slice;
}

inline std::string_view ToSV(std::string&& slice) = delete;

constexpr auto kToSV = [](auto&& v) { return ToSV(std::forward<decltype(v)>(v)); };

inline std::string_view ArgS(ArgSlice args, size_t i) {
  return args[i];
}

struct ArgRange {
  ArgRange(ArgRange&&) = default;
  ArgRange(const ArgRange&) = default;
  ArgRange(ArgRange& range) : ArgRange((const ArgRange&)range) {
  }

  template <typename T> ArgRange(T&& span) : span(std::forward<T>(span)) {
  }

  size_t Size() const {
    return std::visit([](const auto& span) { return span.size(); }, span);
  }

  auto Range() const {
    return base::it::Wrap(kToSV, span);
  }

  auto begin() const {
    return Range().first;
  }

  auto end() const {
    return Range().second;
  }

  std::string_view operator[](size_t idx) const {
    return std::visit([idx](const auto& span) { return facade::ToSV(span[idx]); }, span);
  }

  std::variant<ArgSlice, OwnedArgSlice> span;
};
struct ConnectionStats {
  size_t read_buf_capacity = 0;                // total capacity of input buffers
  uint64_t dispatch_queue_entries = 0;         // total number of dispatch queue entries
  size_t dispatch_queue_bytes = 0;             // total size of all dispatch queue entries
  size_t dispatch_queue_subscriber_bytes = 0;  // total size of all publish messages

  size_t pipeline_cmd_cache_bytes = 0;

  uint64_t io_read_cnt = 0;
  size_t io_read_bytes = 0;

  uint64_t command_cnt = 0;
  uint64_t pipelined_cmd_cnt = 0;
  uint64_t pipelined_cmd_latency = 0;  // in microseconds
  uint64_t conn_received_cnt = 0;

  uint32_t num_conns = 0;
  uint32_t num_blocked_clients = 0;
  uint64_t num_migrations = 0;

  // Number of events when the pipeline queue was over the limit and was throttled.
  uint64_t pipeline_throttle_count = 0;

  ConnectionStats& operator+=(const ConnectionStats& o);
};

struct ReplyStats {
  struct SendStats {
    int64_t count = 0;
    int64_t total_duration = 0;

    SendStats& operator+=(const SendStats& other) {
      static_assert(sizeof(SendStats) == 16u);

      count += other.count;
      total_duration += other.total_duration;
      return *this;
    }
  };

  // Send() operations that are written to sockets
  SendStats send_stats;

  size_t io_write_cnt = 0;
  size_t io_write_bytes = 0;
  absl::flat_hash_map<std::string, uint64_t> err_count;
  size_t script_error_count = 0;

  ReplyStats& operator+=(const ReplyStats& other);
};

struct FacadeStats {
  ConnectionStats conn_stats;
  ReplyStats reply_stats;

  FacadeStats& operator+=(const FacadeStats& other) {
    conn_stats += other.conn_stats;
    reply_stats += other.reply_stats;
    return *this;
  }
};

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
  ErrorReply(OpStatus status) : message{}, kind{}, status{status} {
  }

  std::string_view ToSv() const {
    return std::visit(kToSV, message);
  }

  std::variant<std::string, std::string_view> message;
  std::string_view kind;
  std::optional<OpStatus> status{std::nullopt};
};

constexpr inline unsigned long long operator""_MB(unsigned long long x) {
  return 1024L * 1024L * x;
}

constexpr inline unsigned long long operator""_KB(unsigned long long x) {
  return 1024L * x;
}

extern __thread FacadeStats* tl_facade_stats;

void ResetStats();

// Constants for socket bufring.
constexpr uint16_t kRecvSockGid = 0;
constexpr size_t kRecvBufSize = 128;

}  // namespace facade

namespace std {
ostream& operator<<(ostream& os, facade::CmdArgList args);
ostream& operator<<(ostream& os, facade::Protocol protocol);

}  // namespace std
