// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/inlined_vector.h>
#include <stddef.h>

#include <cstdint>
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
  enum Result { OK, INPUT_PENDING, BAD_ARRAYLEN, BAD_BULKLEN, BAD_STRING, BAD_INT, BAD_DOUBLE };
  using Buffer = RespExpr::Buffer;

  explicit RedisParser(bool server_mode = true) : server_mode_(server_mode) {
  }

  /**
   * @brief Parses str into res. "consumed" stores number of bytes consumed from str.
   *
   * A caller should not invalidate str if the parser returns RESP_OK as long as he continues
   * accessing res. However, if parser returns MORE_INPUT a caller may discard consumed
   * part of str because parser caches the intermediate state internally according to 'consumed'
   * result.
   *
   * Note: A parser does not always guarantee progress, i.e. if a small buffer was passed it may
   * returns MORE_INPUT with consumed == 0.
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

 private:
  void InitStart(uint8_t prefix_b, RespVec* res);
  void StashState(RespVec* res);

  // Skips the first character (*).
  Result ConsumeArrayLen(Buffer str);
  Result ParseArg(Buffer str);
  Result ConsumeBulk(Buffer str);
  Result ParseInline(Buffer str);

  // Updates last_consumed_
  Result ParseNum(Buffer str, int64_t* res);
  void HandleFinishArg();
  void ExtendLastString(Buffer str);

  enum State : uint8_t {
    INIT_S = 0,
    INLINE_S,
    ARRAY_LEN_S,
    MAP_LEN_S,
    PARSE_ARG_S,  // Parse [$:+-]string\r\n
    BULK_STR_S,
    FINISH_ARG_S,
    CMD_COMPLETE_S,
  };

  State state_ = INIT_S;
  Result last_result_ = OK;

  uint32_t last_consumed_ = 0;
  uint32_t bulk_len_ = 0;
  uint32_t last_stashed_level_ = 0, last_stashed_index_ = 0;

  // expected expression length, pointer to expression vector.
  absl::InlinedVector<std::pair<uint32_t, RespVec*>, 4> parse_stack_;
  std::vector<std::unique_ptr<RespVec>> stash_;

  using BlobPtr = std::unique_ptr<uint8_t[]>;
  std::vector<BlobPtr> buf_stash_;
  RespVec* cached_expr_ = nullptr;
  bool is_broken_token_ = false;
  bool server_mode_ = true;
};

}  // namespace facade
