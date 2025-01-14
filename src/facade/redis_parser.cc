// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "facade/redis_parser.h"

#include <absl/strings/escaping.h>
#include <absl/strings/numbers.h>

#include "base/logging.h"
#include "core/heap_size.h"

namespace facade {

using namespace std;
constexpr static long kMaxBulkLen = 256 * (1ul << 20);  // 256MB.

auto RedisParser::Parse(Buffer str, uint32_t* consumed, RespExpr::Vec* res) -> Result {
  DCHECK(!str.empty());
  *consumed = 0;
  res->clear();

  DVLOG(2) << "Parsing: "
           << absl::CHexEscape(string_view{reinterpret_cast<const char*>(str.data()), str.size()});

  if (state_ == CMD_COMPLETE_S) {
    if (InitStart(str[0], res)) {
      // We recognized a non-INLINE state, starting with a special char.
      str.remove_prefix(1);
      *consumed += 1;
      if (server_mode_ && state_ == PARSE_ARG_S) {  // server requests start with ARRAY_LEN_S.
        state_ = CMD_COMPLETE_S;                    // reject and reset the state.
        return BAD_ARRAYLEN;
      }
      if (str.empty())
        return INPUT_PENDING;
    }
  } else {  // INLINE mode, aka PING\n
    // We continue parsing in the middle.
    if (!cached_expr_)
      cached_expr_ = res;
  }
  DCHECK(state_ != CMD_COMPLETE_S);

  ResultConsumed resultc{OK, 0};

  do {
    switch (state_) {
      case MAP_LEN_S:
      case ARRAY_LEN_S:
        resultc = ConsumeArrayLen(str);
        break;
      case PARSE_ARG_TYPE:
        arg_c_ = str[0];
        if (server_mode_ && arg_c_ != '$')  // server side only supports bulk strings.
          return BAD_BULKLEN;
        resultc.second = 1;
        state_ = PARSE_ARG_S;
        break;
      case PARSE_ARG_S:
        resultc = ParseArg(str);
        break;
      case INLINE_S:
        DCHECK(parse_stack_.empty());
        resultc = ParseInline(str);
        break;
      case BULK_STR_S:
        resultc = ConsumeBulk(str);
        break;
      case SLASH_N_S:
        if (str[0] != '\n') {
          resultc.first = BAD_STRING;
        } else {
          resultc = {OK, 1};
          if (arg_c_ == '_') {
            cached_expr_->emplace_back(RespExpr::NIL);
            cached_expr_->back().u = Buffer{};
          }
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
      DCHECK(str.empty());
      StashState(res);
    }
    return resultc.first;
  }

  if (resultc.first == OK) {
    DCHECK(cached_expr_);
    DCHECK_EQ(0, small_len_);

    if (res != cached_expr_) {
      DCHECK(!stash_.empty());

      *res = *cached_expr_;
    }
  }

  return resultc.first;
}

bool RedisParser::InitStart(char prefix_b, RespExpr::Vec* res) {
  buf_stash_.clear();
  stash_.clear();
  cached_expr_ = res;
  parse_stack_.clear();
  last_stashed_level_ = 0;
  last_stashed_index_ = 0;

  switch (prefix_b) {
    case '$':
    case ':':
    case '+':
    case '-':
    case '_':  // Resp3 NULL
    case ',':  // Resp3 DOUBLE
      state_ = PARSE_ARG_S;
      parse_stack_.emplace_back(1, cached_expr_);  // expression of length 1.
      arg_c_ = prefix_b;
      return true;
    case '*':
    case '~':  // Resp3 SET
      state_ = ARRAY_LEN_S;
      return true;
    case '%':  // Resp3 MAP
      state_ = MAP_LEN_S;
      return true;
  }

  state_ = INLINE_S;
  return false;
}

void RedisParser::StashState(RespExpr::Vec* res) {
  if (cached_expr_->empty() && stash_.empty()) {
    cached_expr_ = nullptr;
    return;
  }

  if (cached_expr_ == res) {
    stash_.emplace_back(new RespExpr::Vec(*res));
    cached_expr_ = stash_.back().get();
  }

  DCHECK_LT(last_stashed_level_, stash_.size());
  while (true) {
    auto& cur = *stash_[last_stashed_level_];

    for (; last_stashed_index_ < cur.size(); ++last_stashed_index_) {
      auto& e = cur[last_stashed_index_];
      if (RespExpr::STRING == e.type) {
        Buffer& ebuf = get<Buffer>(e.u);
        if (ebuf.empty() && last_stashed_index_ + 1 == cur.size())
          break;
        if (!ebuf.empty() && !e.has_support) {
          Blob blob(ebuf.size());
          memcpy(blob.data(), ebuf.data(), ebuf.size());
          ebuf = Buffer{blob.data(), blob.size()};
          buf_stash_.push_back(std::move(blob));
          e.has_support = true;
        }
      }
    }

    if (last_stashed_level_ + 1 == stash_.size())
      break;
    ++last_stashed_level_;
    last_stashed_index_ = 0;
  }
}

auto RedisParser::ParseInline(Buffer str) -> ResultConsumed {
  DCHECK(!str.empty());

  uint8_t* ptr = str.begin();
  uint8_t* end = str.end();
  uint8_t* token_start = ptr;

  auto find_token_end = [](uint8_t* ptr, uint8_t* end) {
    while (ptr != end && *ptr > 32)
      ++ptr;
    return ptr;
  };

  if (is_broken_token_) {
    ptr = find_token_end(ptr, end);
    size_t len = ptr - token_start;

    ExtendLastString(Buffer(token_start, len));
    if (ptr == end) {
      return {INPUT_PENDING, ptr - token_start};
    }
    is_broken_token_ = false;
  }

  while (ptr != end) {
    // For inline input we only require \n.
    if (*ptr == '\n') {
      if (cached_expr_->empty()) {
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
    DCHECK(!is_broken_token_);

    token_start = ptr;
    ptr = find_token_end(ptr, end);

    cached_expr_->emplace_back(RespExpr::STRING);
    cached_expr_->back().u = Buffer{token_start, size_t(ptr - token_start)};
  }

  uint32_t last_consumed = ptr - str.data();
  if (ptr == end) {  // we have not finished parsing.
    if (cached_expr_->empty()) {
      state_ = CMD_COMPLETE_S;  // have not found anything besides whitespace.
    } else {
      is_broken_token_ = ptr[-1] > 32;  // we stopped in the middle of the token.
    }
    return {INPUT_PENDING, last_consumed};
  }

  DCHECK_EQ('\n', *ptr);

  ++last_consumed;  // consume \n as well.
  state_ = CMD_COMPLETE_S;

  return {OK, last_consumed};
}

// Parse lines like:'$5\r\n' or '*2\r\n'. The first character is already consumed by the caller.
auto RedisParser::ParseLen(Buffer str, int64_t* res) -> ResultConsumed {
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

  // Skip the first character and 2 last ones (\r\n).
  string_view len_token{s, size_t(pos - 1 - s)};
  bool success = absl::SimpleAtoi(len_token, res);

  if (success && *res >= -1) {
    return ResultConsumed{OK, consumed};
  }

  LOG(WARNING) << "Failed to parse len " << len_token;
  return ResultConsumed{BAD_ARRAYLEN, consumed};
}

auto RedisParser::ConsumeArrayLen(Buffer str) -> ResultConsumed {
  int64_t len;

  ResultConsumed res = ParseLen(str, &len);
  if (res.first != OK) {
    return res;
  }

  if (state_ == MAP_LEN_S) {
    // Map starts with %N followed by an array of 2*N elements.
    // Even elements are keys, odd elements are values.
    len *= 2;
  }

  if (len > max_arr_len_) {
    LOG(WARNING) << "Multibulk len is too large " << len;

    return {BAD_ARRAYLEN, res.second};
  }

  if (server_mode_ && (parse_stack_.size() > 0 || !cached_expr_->empty()))
    return {BAD_STRING, res.second};

  if (len <= 0) {
    if (len < 0) {
      cached_expr_->emplace_back(RespExpr::NIL_ARRAY);
      cached_expr_->back().u.emplace<RespVec*>(nullptr);  // nil
    } else {
      static RespVec empty_vec;
      cached_expr_->emplace_back(RespExpr::ARRAY);
      cached_expr_->back().u = &empty_vec;
    }
    if (parse_stack_.empty()) {
      state_ = CMD_COMPLETE_S;
    } else {
      HandleFinishArg();
    }

    return {OK, res.second};
  }

  if (state_ == PARSE_ARG_S) {
    DCHECK(!server_mode_);

    cached_expr_->emplace_back(RespExpr::ARRAY);
    stash_.emplace_back(new RespExpr::Vec());
    RespExpr::Vec* arr = stash_.back().get();
    arr->reserve(len);
    cached_expr_->back().u = arr;
    cached_expr_ = arr;
  }
  state_ = PARSE_ARG_TYPE;

  DVLOG(1) << "PushStack: (" << len << ", " << cached_expr_ << ")";
  parse_stack_.emplace_back(len, cached_expr_);

  return {OK, res.second};
}

auto RedisParser::ParseArg(Buffer str) -> ResultConsumed {
  DCHECK(!str.empty());

  if (arg_c_ == '$') {
    int64_t len;

    ResultConsumed res = ParseLen(str, &len);
    if (res.first != OK) {
      return res;
    }

    if (len == -1) {  // Resp2 NIL
      cached_expr_->emplace_back(RespExpr::NIL);
      cached_expr_->back().u = Buffer{};
      HandleFinishArg();
    } else {
      DVLOG(1) << "String(" << len << ")";
      LOG_IF(WARNING, len > kMaxBulkLen) << "Large bulk len: " << len;

      cached_expr_->emplace_back(RespExpr::STRING);
      cached_expr_->back().u = Buffer{};
      bulk_len_ = len;
      state_ = BULK_STR_S;
    }

    return {OK, res.second};
  }

  DCHECK(!server_mode_);

  if (arg_c_ == '_') {  // Resp3 NIL
    // "_\r\n", with '_' consumed into arg_c_.
    DCHECK_LT(small_len_, 2u);  // must be because we never fill here with more than 2 bytes.
    DCHECK_GE(str.size(), 1u);

    if (str[0] != '\r' || (str.size() > 1 && str[1] != '\n')) {
      return {BAD_STRING, 0};
    }

    if (str.size() == 1) {
      state_ = SLASH_N_S;
      return {INPUT_PENDING, 1};
    }

    cached_expr_->emplace_back(RespExpr::NIL);
    cached_expr_->back().u = Buffer{};
    HandleFinishArg();
    return {OK, 2};
  }

  if (arg_c_ == '*') {
    return ConsumeArrayLen(str);
  }

  char* s = reinterpret_cast<char*>(str.data());
  char* eol = reinterpret_cast<char*>(memchr(s, '\n', str.size()));

  // TODO: in client mode we still may not consume everything (see INPUT_PENDING below).
  // It's not a problem, because we need consume all the input only in server mode.

  if (arg_c_ == '+' || arg_c_ == '-') {  // Simple string or error.
    DCHECK(!server_mode_);
    if (!eol) {
      Result r = str.size() < 256 ? INPUT_PENDING : BAD_STRING;
      return {r, 0};
    }

    if (eol[-1] != '\r')
      return {BAD_STRING, 0};

    cached_expr_->emplace_back(arg_c_ == '+' ? RespExpr::STRING : RespExpr::ERROR);
    cached_expr_->back().u = Buffer{reinterpret_cast<uint8_t*>(s), size_t((eol - 1) - s)};
  } else if (arg_c_ == ':') {
    DCHECK(!server_mode_);
    if (!eol) {
      Result r = str.size() < 32 ? INPUT_PENDING : BAD_INT;
      return {r, 0};
    }
    int64_t ival;
    std::string_view tok{s, size_t((eol - s) - 1)};

    if (eol[-1] != '\r' || !absl::SimpleAtoi(tok, &ival))
      return {BAD_INT, 0};

    cached_expr_->emplace_back(RespExpr::INT64);
    cached_expr_->back().u = ival;
  } else if (arg_c_ == ',') {
    DCHECK(!server_mode_);
    if (!eol) {
      Result r = str.size() < 32 ? INPUT_PENDING : BAD_DOUBLE;
      return {r, 0};
    }
    double_t dval;
    std::string_view tok{s, size_t((eol - s) - 1)};

    if (eol[-1] != '\r' || !absl::SimpleAtod(tok, &dval))
      return {BAD_INT, 0};

    cached_expr_->emplace_back(RespExpr::DOUBLE);
    cached_expr_->back().u = dval;
  } else {
    return {BAD_STRING, 0};
  }

  HandleFinishArg();

  return {OK, (eol - s) + 1};
}

auto RedisParser::ConsumeBulk(Buffer str) -> ResultConsumed {
  DCHECK_EQ(small_len_, 0);
  uint32_t consumed = 0;
  auto& bulk_str = get<Buffer>(cached_expr_->back().u);

  if (str.size() >= bulk_len_) {
    consumed = bulk_len_;
    if (bulk_len_) {
      // is_broken_token_ can be false, if we just parsed the bulk length but have
      // not parsed the token itself.
      if (is_broken_token_) {
        memcpy(bulk_str.end(), str.data(), bulk_len_);
        bulk_str = Buffer{bulk_str.data(), bulk_str.size() + bulk_len_};
      } else {
        bulk_str = str.subspan(0, bulk_len_);
      }
      str.remove_prefix(exchange(bulk_len_, 0));
      is_broken_token_ = false;
    }

    if (str.size() >= 2) {
      if (str[0] != '\r' || str[1] != '\n') {
        return {BAD_STRING, consumed};
      }
      HandleFinishArg();
      return {OK, consumed + 2};
    } else if (str.size() == 1) {
      if (str[0] != '\r') {
        return {BAD_STRING, consumed};
      }
      state_ = SLASH_N_S;
      consumed++;
    }
    return {INPUT_PENDING, consumed};
  }

  DCHECK(bulk_len_);
  size_t len = std::min<size_t>(str.size(), bulk_len_);

  if (is_broken_token_) {
    memcpy(bulk_str.end(), str.data(), len);
    bulk_str = Buffer{bulk_str.data(), bulk_str.size() + len};
    DVLOG(1) << "Extending bulk stash to size " << bulk_str.size();
  } else {
    DVLOG(1) << "New bulk stash size " << bulk_len_;
    vector<uint8_t> nb(bulk_len_);
    memcpy(nb.data(), str.data(), len);
    bulk_str = Buffer{nb.data(), len};
    buf_stash_.emplace_back(std::move(nb));
    is_broken_token_ = true;
    cached_expr_->back().has_support = true;
  }
  consumed = len;
  bulk_len_ -= len;

  return {INPUT_PENDING, consumed};
}

void RedisParser::HandleFinishArg() {
  DCHECK(!parse_stack_.empty());
  DCHECK_GT(parse_stack_.back().first, 0u);

  state_ = PARSE_ARG_TYPE;
  while (true) {
    --parse_stack_.back().first;
    if (parse_stack_.back().first != 0)
      break;
    auto* arr = parse_stack_.back().second;
    DVLOG(1) << "PopStack (" << arr << ")";
    parse_stack_.pop_back();  // pop 0.
    if (parse_stack_.empty()) {
      state_ = CMD_COMPLETE_S;
      break;
    }
    cached_expr_ = parse_stack_.back().second;
  }
  small_len_ = 0;
}

void RedisParser::ExtendLastString(Buffer str) {
  DCHECK(!cached_expr_->empty() && cached_expr_->back().type == RespExpr::STRING);
  DCHECK(!buf_stash_.empty());

  Buffer& last_str = get<Buffer>(cached_expr_->back().u);

  DCHECK(last_str.data() == buf_stash_.back().data());

  vector<uint8_t> nb(last_str.size() + str.size());
  memcpy(nb.data(), last_str.data(), last_str.size());
  memcpy(nb.data() + last_str.size(), str.data(), str.size());
  last_str = RespExpr::Buffer{nb.data(), last_str.size() + str.size()};
  buf_stash_.back() = std::move(nb);
}

size_t RedisParser::UsedMemory() const {
  return dfly::HeapSize(parse_stack_) + dfly::HeapSize(stash_) + dfly::HeapSize(buf_stash_);
}

}  // namespace facade
