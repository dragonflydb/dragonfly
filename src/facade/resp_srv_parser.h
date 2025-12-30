// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/types/span.h>

#include <memory>
#include <utility>
#include <vector>

#include "common/backed_args.h"

namespace facade {

/**
 * @brief RESP server-side parser.
 */
class RespSrvParser {
 public:
  enum Result : uint8_t {
    OK,
    INPUT_PENDING,
    BAD_ARRAYLEN,
    BAD_BULKLEN,
    BAD_STRING,
  };
  using Buffer = absl::Span<const uint8_t>;
  explicit RespSrvParser(uint32_t max_arr_len = UINT32_MAX, uint32_t max_bulk_len = UINT32_MAX)
      : max_arr_len_(max_arr_len), max_bulk_len_(max_bulk_len) {
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

  Result Parse(Buffer str, uint32_t* consumed, cmn::BackedArguments* dest);

  size_t parselen_hint() const {
    return bulk_len_;
  }

  size_t UsedMemory() const;

 private:
  using ResultConsumed = std::pair<Result, uint32_t>;

  // Skips the first character (*).
  ResultConsumed ConsumeArrayLen(Buffer str, cmn::BackedArguments* args);
  ResultConsumed ParseArg(Buffer str, cmn::BackedArguments* args);
  ResultConsumed ConsumeBulk(Buffer str, cmn::BackedArguments* args);
  ResultConsumed ParseInline(Buffer str, cmn::BackedArguments* args);
  ResultConsumed ParseLen(Buffer str, int64_t* res);

  void HandleFinishArg();

  enum State : uint8_t {
    INLINE_S,
    ARRAY_LEN_S,
    PARSE_ARG_TYPE,
    PARSE_ARG_S,  // Parse string\r\n
    BULK_STR_S,
    SLASH_N_S,
    CMD_COMPLETE_S,
  };

  State state_ = CMD_COMPLETE_S;
  uint8_t small_len_ = 0;

  uint32_t bulk_len_ = 0, arg_len_ = 0;
  uint32_t max_arr_len_;
  uint32_t max_bulk_len_;

  std::string buf_stash_;
  std::array<char, 32> small_buf_;
};

}  // namespace facade
