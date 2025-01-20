// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/inlined_vector.h>

#include <memory>
#include <utility>
#include <vector>

#include "facade/resp_expr.h"

namespace facade {

/**
 * @brief Zero-copy (best-effort) parser.
 * Note: The client-mode parsing is buggy and should not be used in production.
 *       Currently we only use server-mode parsing in production and client-mode in tests.
 *       It works because tests do not do any incremental parsing.
 *
 */
class RedisParser {
 public:
  enum Result : uint8_t {
    OK,
    INPUT_PENDING,
    BAD_ARRAYLEN,
    BAD_BULKLEN,
    BAD_STRING,
    BAD_INT,
    BAD_DOUBLE
  };
  using Buffer = RespExpr::Buffer;

  explicit RedisParser(uint32_t max_arr_len = UINT32_MAX, bool server_mode = true)
      : server_mode_(server_mode), max_arr_len_(max_arr_len) {
  }

  /**
   * @brief Parses str into res. "consumed" stores number of bytes consumed from str.
   *
   * A caller should not invalidate str if the parser returns RESP_OK as long as he continues
   * accessing res. However, if parser returns INPUT_PENDING a caller may discard consumed
   * part of str because parser caches the intermediate state internally according to 'consumed'
   * result.
   *
   *
   */

  Result Parse(Buffer str, uint32_t* consumed, RespVec* res);

  void SetClientMode() {
    server_mode_ = false;
  }

  size_t parselen_hint() const {
    return bulk_len_;
  }

  size_t stash_size() const {
    return stash_.size();
  }
  const std::vector<std::unique_ptr<RespVec>>& stash() const {
    return stash_;
  }

  size_t UsedMemory() const;

 private:
  using ResultConsumed = std::pair<Result, uint32_t>;

  // Returns true if this is a RESP message, false if INLINE.
  bool InitStart(char prefix_b, RespVec* res);
  void StashState(RespVec* res);

  // Skips the first character (*).
  ResultConsumed ConsumeArrayLen(Buffer str);
  ResultConsumed ParseArg(Buffer str);
  ResultConsumed ConsumeBulk(Buffer str);
  ResultConsumed ParseInline(Buffer str);
  ResultConsumed ParseLen(Buffer str, int64_t* res);

  void HandleFinishArg();
  void ExtendLastString(Buffer str);

  enum State : uint8_t {
    INLINE_S,
    ARRAY_LEN_S,
    MAP_LEN_S,
    PARSE_ARG_TYPE,  // Parse [$:+-]
    PARSE_ARG_S,     // Parse string\r\n
    BULK_STR_S,
    SLASH_N_S,
    CMD_COMPLETE_S,
  };

  State state_ = CMD_COMPLETE_S;
  bool is_broken_token_ = false;  // true, if a token (inline or bulk) is broken during the parsing.
  bool server_mode_ = true;
  uint8_t small_len_ = 0;
  char arg_c_ = 0;

  uint32_t bulk_len_ = 0;
  uint32_t last_stashed_level_ = 0, last_stashed_index_ = 0;
  uint32_t max_arr_len_;

  // Points either to the result passed by the caller or to the stash.
  RespVec* cached_expr_ = nullptr;

  // expected expression length, pointer to expression vector.
  // For server mode, the length is at most 1.
  absl::InlinedVector<std::pair<uint32_t, RespVec*>, 4> parse_stack_;
  std::vector<std::unique_ptr<RespVec>> stash_;

  using Blob = std::vector<uint8_t>;
  std::vector<Blob> buf_stash_;
  std::array<char, 32> small_buf_;
};

}  // namespace facade
