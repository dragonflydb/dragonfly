// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/types/span.h>
#include <absl/container/flat_hash_map.h>

namespace facade {

enum class Protocol : uint8_t {
  MEMCACHE = 1,
  REDIS = 2
};

using MutableSlice = absl::Span<char>;
using CmdArgList = absl::Span<MutableSlice>;
using CmdArgVec = std::vector<MutableSlice>;


struct ConnectionStats {
  absl::flat_hash_map<std::string, uint64_t> err_count;
  absl::flat_hash_map<std::string, uint64_t> cmd_count;

  uint32_t num_conns = 0;
  uint32_t num_replicas = 0;
  size_t read_buf_capacity = 0;
  size_t io_read_cnt = 0;
  size_t io_read_bytes = 0;
  size_t io_write_cnt = 0;
  size_t io_write_bytes = 0;
  size_t command_cnt = 0;
  size_t pipelined_cmd_cnt = 0;

  ConnectionStats& operator+=(const ConnectionStats& o);
};

inline MutableSlice ToMSS(absl::Span<uint8_t> span) {
  return MutableSlice{reinterpret_cast<char*>(span.data()), span.size()};
}

inline std::string_view ArgS(CmdArgList args, size_t i) {
  auto arg = args[i];
  return std::string_view(arg.data(), arg.size());
}

constexpr inline unsigned long long operator""_MB(unsigned long long x) {
  return 1024L * 1024L * x;
}

constexpr inline unsigned long long operator""_KB(unsigned long long x) {
  return 1024L * x;
}

}  // namespace facade


namespace std {
ostream& operator<<(ostream& os, facade::CmdArgList args);

}  // namespace std
