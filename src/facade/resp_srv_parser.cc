// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "facade/resp_srv_parser.h"

#include <absl/strings/escaping.h>
#include <absl/strings/numbers.h>

#include "base/logging.h"
#include "common/heap_size.h"

namespace facade {

using namespace std;

auto RespSrvParser::Parse(Buffer str, uint32_t* consumed, cmn::BackedArguments* args) -> Result {
  DCHECK(!str.empty());
  *consumed = 0;
  DVLOG(2) << "Parsing: "
           << absl::CHexEscape(string_view{reinterpret_cast<const char*>(str.data()), str.size()});

  if (state_ == CMD_COMPLETE_S) {
    args->clear();
    buf_stash_.clear();

    if (str[0] == '*') {
      // We recognized a non-INLINE state, starting with '$'
      str.remove_prefix(1);
      *consumed += 1;
      state_ = ARRAY_LEN_S;
      if (str.empty())
        return INPUT_PENDING;
    } else {  // INLINE mode, aka PING\n
      state_ = INLINE_S;
    }
  }

  ResultConsumed resultc{OK, 0};
  do {
    switch (state_) {
      case ARRAY_LEN_S:
        resultc = ConsumeArrayLen(str, args);
        break;
      case PARSE_ARG_TYPE:
        if (str[0] != '$')  // server side only supports bulk strings.
          return BAD_BULKLEN;
        resultc.second = 1;
        state_ = PARSE_ARG_S;
        break;
      case PARSE_ARG_S:
        resultc = ParseArg(str, args);
        break;
      case INLINE_S:
        resultc = ParseInline(str, args);
        break;
      case BULK_STR_S:
        resultc = ConsumeBulk(str, args);
        break;
      case SLASH_N_S:
        if (str[0] != '\n') {
          resultc.first = BAD_STRING;
        } else {
          resultc = {OK, 1};
          HandleFinishArg();
        }
        break;
      default:
        LOG(FATAL) << "Unexpected state " << int(state_);
    }

    *consumed += resultc.second;
    str.remove_prefix(exchange(resultc.second, 0));
  } while (state_ != CMD_COMPLETE_S && resultc.first == OK && !str.empty());

  if (state_ != CMD_COMPLETE_S) {
    if (resultc.first == OK) {
      resultc.first = INPUT_PENDING;
    }

    if (resultc.first == INPUT_PENDING) {
      if (!str.empty()) {
        LOG(DFATAL) << "Did not consume all input: "
                    << absl::CHexEscape({reinterpret_cast<const char*>(str.data()), str.size()})
                    << ", state: " << int(state_) << " smallbuf: "
                    << absl::CHexEscape(
                           {reinterpret_cast<const char*>(small_buf_.data()), small_len_});
      }
    }
    return resultc.first;
  }

  return resultc.first;
}

auto RespSrvParser::ParseInline(Buffer str, cmn::BackedArguments* args) -> ResultConsumed {
  DCHECK(!str.empty());

  const uint8_t* ptr = str.begin();
  const uint8_t* end = str.end();
  const uint8_t* token_start = ptr;

  auto find_token_end = [](const uint8_t* ptr, const uint8_t* end) {
    while (ptr != end && *ptr > 32)
      ++ptr;
    return ptr;
  };

  if (!buf_stash_.empty()) {
    ptr = find_token_end(ptr, end);
    size_t len = ptr - token_start;

    buf_stash_.append(reinterpret_cast<const char*>(token_start), len);
    if (ptr == end) {
      return {INPUT_PENDING, ptr - token_start};
    }

    args->PushArg(buf_stash_);
    buf_stash_.clear();
  }

  while (ptr != end) {
    // For inline input we only require \n.
    if (*ptr == '\n') {
      if (args->empty()) {
        ++ptr;
        continue;  // skip empty line
      }
      break;
    }

    if (*ptr <= 32) {  // skip ws/control chars
      ++ptr;
      continue;
    }

    // token start
    DCHECK(buf_stash_.empty());

    token_start = ptr;
    ptr = find_token_end(ptr, end);
    if (ptr != end) {
      args->PushArg(
          string_view{reinterpret_cast<const char*>(token_start), size_t(ptr - token_start)});
    }
  }

  uint32_t last_consumed = ptr - str.data();
  if (ptr == end) {                       // we have not finished parsing.
    bool is_broken_token = ptr[-1] > 32;  // we stopped in the middle of the token.
    if (is_broken_token) {
      DCHECK(buf_stash_.empty());
      buf_stash_.append(reinterpret_cast<const char*>(token_start), size_t(ptr - token_start));
    } else if (args->empty()) {
      state_ = CMD_COMPLETE_S;  // have not found anything besides whitespace.
    }
    return {INPUT_PENDING, last_consumed};
  }

  DCHECK_EQ('\n', *ptr);

  ++last_consumed;  // consume \n as well.
  state_ = CMD_COMPLETE_S;

  return {OK, last_consumed};
}

// Parse lines like:'$5\r\n' or '*2\r\n'. The first character is already consumed by the caller.
auto RespSrvParser::ParseLen(Buffer str, int64_t* res) -> ResultConsumed {
  DCHECK(!str.empty());

  const char* s = reinterpret_cast<const char*>(str.data());
  const char* pos = reinterpret_cast<const char*>(memchr(s, '\n', str.size()));
  if (!pos) {
    if (str.size() + small_len_ < small_buf_.size()) {
      memcpy(&small_buf_[small_len_], str.data(), str.size());
      small_len_ += str.size();
      return {INPUT_PENDING, str.size()};
    }
    LOG(WARNING) << "Unexpected format " << string_view{s, str.size()};
    return ResultConsumed{BAD_ARRAYLEN, 0};
  }

  unsigned consumed = pos - s + 1;
  if (small_len_ > 0) {
    if (small_len_ + consumed >= small_buf_.size()) {
      return ResultConsumed{BAD_ARRAYLEN, consumed};
    }
    memcpy(&small_buf_[small_len_], str.data(), consumed);
    small_len_ += consumed;
    s = small_buf_.data();
    pos = s + small_len_ - 1;
    small_len_ = 0;
  }

  if (pos[-1] != '\r') {
    return {BAD_ARRAYLEN, consumed};
  }

  // Skip 2 last characters (\r\n).
  string_view len_token{s, size_t(pos - 1 - s)};
  bool success = absl::SimpleAtoi(len_token, res);

  if (success && *res >= -1) {
    return ResultConsumed{OK, consumed};
  }

  LOG(ERROR) << "Failed to parse len " << absl::CHexEscape(len_token) << " "
             << absl::CHexEscape(string_view{reinterpret_cast<const char*>(str.data()), str.size()})
             << " " << consumed << " " << int(s == small_buf_.data());
  return ResultConsumed{BAD_ARRAYLEN, consumed};
}

auto RespSrvParser::ConsumeArrayLen(Buffer str, cmn::BackedArguments* args) -> ResultConsumed {
  int64_t len;

  ResultConsumed res = ParseLen(str, &len);
  if (res.first != OK) {
    return res;
  }

  if (len <= 0) {
    return {BAD_ARRAYLEN, res.second};
  }

  if (len > max_arr_len_) {
    LOG(WARNING) << "Multibulk len is too large " << len;

    return {BAD_ARRAYLEN, res.second};
  }

  state_ = PARSE_ARG_TYPE;
  arg_len_ = len;
  args->Reserve(len, 0);
  return {OK, res.second};
}

auto RespSrvParser::ParseArg(Buffer str, cmn::BackedArguments* args) -> ResultConsumed {
  DCHECK(!str.empty());

  int64_t len;

  ResultConsumed res = ParseLen(str, &len);
  if (res.first != OK) {
    return res;
  }

  if (len > 0 && static_cast<uint64_t>(len) > max_bulk_len_) {
    LOG_EVERY_T(WARNING, 1) << "Threshold reached with bulk len: " << len
                            << ", consider increasing max_bulk_len";
    return {BAD_BULKLEN, res.second};
  }

  if (len < 0) {
    return {BAD_BULKLEN, res.second};
  }

  bulk_len_ = len;
  state_ = BULK_STR_S;
  args->PushArg(size_t(len));

  return {OK, res.second};
}

auto RespSrvParser::ConsumeBulk(Buffer str, cmn::BackedArguments* args) -> ResultConsumed {
  DCHECK_EQ(small_len_, 0);
  uint32_t consumed = 0;

  if (str.size() >= bulk_len_) {
    consumed = bulk_len_;
    if (bulk_len_) {
      char* last_arg = args->data(args->size() - 1);  // Get pointer to last argument.
      DCHECK_GE(args->elem_len(args->size() - 1), bulk_len_);
      char* start = last_arg + (args->elem_len(args->size() - 1) - bulk_len_);
      memcpy(start, str.data(), bulk_len_);
      str.remove_prefix(exchange(bulk_len_, 0));
    }

    if (str.size() >= 2) {
      if (str[0] != '\r' || str[1] != '\n') {
        return {BAD_STRING, consumed};
      }
      HandleFinishArg();
      return {OK, consumed + 2};
    }

    if (str.size() == 1) {
      if (str[0] != '\r') {
        return {BAD_STRING, consumed};
      }
      state_ = SLASH_N_S;
      consumed++;
    }
    return {INPUT_PENDING, consumed};
  }

  DCHECK(bulk_len_);
  DCHECK_GE(args->elem_len(args->size() - 1), bulk_len_);
  size_t len = std::min<size_t>(str.size(), bulk_len_);
  char* last_arg = args->data(args->size() - 1);  // Get pointer to last argument.
  char* start = last_arg + (args->elem_len(args->size() - 1) - bulk_len_);
  memcpy(start, str.data(), len);
  consumed = len;
  bulk_len_ -= len;

  return {INPUT_PENDING, consumed};
}

void RespSrvParser::HandleFinishArg() {
  state_ = (--arg_len_ == 0) ? CMD_COMPLETE_S : PARSE_ARG_TYPE;

  small_len_ = 0;
}

size_t RespSrvParser::UsedMemory() const {
  return cmn::HeapSize(buf_stash_);
}

}  // namespace facade
