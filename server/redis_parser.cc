// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/redis_parser.h"

#include <absl/strings/numbers.h>

#include "base/logging.h"

namespace dfly {

using namespace std;

namespace {

constexpr int kMaxArrayLen = 1024;
constexpr int64_t kMaxBulkLen = 64 * (1ul << 20);  // 64MB.

}  // namespace

auto RedisParser::Parse(Buffer str, uint32_t* consumed, RespExpr::Vec* res) -> Result {
  *consumed = 0;
  res->clear();

  if (str.size() < 2) {
    return INPUT_PENDING;
  }

  if (state_ == CMD_COMPLETE_S) {
    state_ = INIT_S;
  }

  if (state_ == INIT_S) {
    InitStart(str[0], res);
  }

  if (!cached_expr_)
    cached_expr_ = res;

  while (state_ != CMD_COMPLETE_S) {
    last_consumed_ = 0;
    switch (state_) {
      case ARRAY_LEN_S:
        last_result_ = ConsumeArrayLen(str);
        break;
      case PARSE_ARG_S:
        if (str.size() < 4) {
          last_result_ = INPUT_PENDING;
        } else {
          last_result_ = ParseArg(str);
        }
        break;
      case INLINE_S:
        DCHECK(parse_stack_.empty());
        last_result_ = ParseInline(str);
        break;
      case BULK_STR_S:
        last_result_ = ConsumeBulk(str);
        break;
      case FINISH_ARG_S:
        HandleFinishArg();
        break;
      default:
        LOG(FATAL) << "Unexpected state " << int(state_);
    }

    *consumed += last_consumed_;

    if (last_result_ != OK) {
      break;
    }
    str.remove_prefix(last_consumed_);
  }

  if (last_result_ == INPUT_PENDING) {
    StashState(res);
  } else if (last_result_ == OK) {
    DCHECK(cached_expr_);
    if (res != cached_expr_) {
      DCHECK(!stash_.empty());

      *res = *cached_expr_;
    }
  }

  return last_result_;
}

void RedisParser::InitStart(uint8_t prefix_b, RespExpr::Vec* res) {
  buf_stash_.clear();
  stash_.clear();
  cached_expr_ = res;
  parse_stack_.clear();
  last_stashed_level_ = 0;
  last_stashed_index_ = 0;

  switch (prefix_b) {
    case '$':
      state_ = PARSE_ARG_S;
      parse_stack_.emplace_back(1, cached_expr_);  // expression of length 1.
      break;
    case '*':
      state_ = ARRAY_LEN_S;
      break;
    default:
      state_ = INLINE_S;
      break;
  }
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
          BlobPtr ptr(new uint8_t[ebuf.size()]);
          memcpy(ptr.get(), ebuf.data(), ebuf.size());
          ebuf = Buffer{ptr.get(), ebuf.size()};
          buf_stash_.push_back(std::move(ptr));
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

auto RedisParser::ParseInline(Buffer str) -> Result {
  DCHECK(!str.empty());

  uint8_t* ptr = str.begin();
  uint8_t* end = str.end();
  uint8_t* token_start = ptr;

  if (is_broken_token_) {
    while (ptr != end && *ptr > 32)
      ++ptr;

    size_t len = ptr - token_start;

    ExtendLastString(Buffer(token_start, len));
    if (ptr != end) {
      is_broken_token_ = false;
    }
  }

  auto is_finish = [&] { return ptr == end || *ptr == '\n'; };

  while (true) {
    while (!is_finish() && *ptr <= 32) {
      ++ptr;
    }
    // We do not test for \r in order to accept 'nc' input.
    if (is_finish())
      break;

    DCHECK(!is_broken_token_);

    token_start = ptr;
    while (ptr != end && *ptr > 32)
      ++ptr;

    cached_expr_->emplace_back(RespExpr::STRING);
    cached_expr_->back().u = Buffer{token_start, size_t(ptr - token_start)};
  }

  last_consumed_ = ptr - str.data();
  if (ptr == end) {  // we have not finished parsing.
    if (ptr[-1] > 32) {
      // we stopped in the middle of the token.
      is_broken_token_ = true;
    }

    return INPUT_PENDING;
  } else {
    ++last_consumed_;  // consume the delimiter as well.
  }
  state_ = CMD_COMPLETE_S;

  return OK;
}

auto RedisParser::ParseNum(Buffer str, int64_t* res) -> Result {
  if (str.size() < 4) {
    return INPUT_PENDING;
  }

  char* s = reinterpret_cast<char*>(str.data() + 1);
  char* pos = reinterpret_cast<char*>(memchr(s, '\n', str.size() - 1));
  if (!pos) {
    return str.size() < 32 ? INPUT_PENDING : BAD_INT;
  }
  if (pos[-1] != '\r') {
    return BAD_INT;
  }

  bool success = absl::SimpleAtoi(std::string_view{s, size_t(pos - s - 1)}, res);
  if (!success) {
    return BAD_INT;
  }
  last_consumed_ = (pos - s) + 2;

  return OK;
}

auto RedisParser::ConsumeArrayLen(Buffer str) -> Result {
  int64_t len;

  Result res = ParseNum(str, &len);
  switch (res) {
    case INPUT_PENDING:
      return INPUT_PENDING;
    case BAD_INT:
      return BAD_ARRAYLEN;
    case OK:
      if (len < -1 || len > kMaxArrayLen)
        return BAD_ARRAYLEN;
      break;
    default:
      LOG(ERROR) << "Unexpected result " << res;
  }

  // Already parsed array expression somewhere. Server should accept only single-level expressions.
  if (!parse_stack_.empty())
    return BAD_STRING;

  // Similarly if our cached expr is not empty.
  if (!cached_expr_->empty())
    return BAD_STRING;

  if (len <= 0) {
    cached_expr_->emplace_back(len == -1 ? RespExpr::NIL_ARRAY : RespExpr::ARRAY);
    if (len < 0)
      cached_expr_->back().u.emplace<RespVec*>(nullptr);  // nil
    else {
      static RespVec empty_vec;
      cached_expr_->back().u = &empty_vec;
    }
    state_ = (parse_stack_.empty()) ? CMD_COMPLETE_S : FINISH_ARG_S;

    return OK;
  }

  parse_stack_.emplace_back(len, cached_expr_);
  DCHECK(cached_expr_->empty());
  state_ = PARSE_ARG_S;

  return OK;
}

auto RedisParser::ParseArg(Buffer str) -> Result {
  char c = str[0];
  if (c == '$') {
    int64_t len;

    Result res = ParseNum(str, &len);
    switch (res) {
      case INPUT_PENDING:
        return INPUT_PENDING;
      case BAD_INT:
        return BAD_ARRAYLEN;
      case OK:
        if (len < -1 || len > kMaxBulkLen)
          return BAD_ARRAYLEN;
        break;
      default:
        LOG(ERROR) << "Unexpected result " << res;
    }

    if (len < 0) {
      state_ = FINISH_ARG_S;
      cached_expr_->emplace_back(RespExpr::NIL);
    } else {
      cached_expr_->emplace_back(RespExpr::STRING);
      bulk_len_ = len;
      state_ = BULK_STR_S;
    }
    cached_expr_->back().u = Buffer{};

    return OK;
  }

  return BAD_BULKLEN;
}

auto RedisParser::ConsumeBulk(Buffer str) -> Result {
  auto& bulk_str = get<Buffer>(cached_expr_->back().u);

  if (str.size() >= bulk_len_ + 2) {
    if (str[bulk_len_] != '\r' || str[bulk_len_ + 1] != '\n') {
      return BAD_STRING;
    }

    if (bulk_len_) {
      if (is_broken_token_) {
        memcpy(bulk_str.end(), str.data(), bulk_len_);
        bulk_str = Buffer{bulk_str.data(), bulk_str.size() + bulk_len_};
      } else {
        bulk_str = str.subspan(0, bulk_len_);
      }
    }
    is_broken_token_ = false;
    state_ = FINISH_ARG_S;
    last_consumed_ = bulk_len_ + 2;
    bulk_len_ = 0;

    return OK;
  }

  if (str.size() >= 32) {
    DCHECK(bulk_len_);
    size_t len = std::min<size_t>(str.size(), bulk_len_);

    if (is_broken_token_) {
      memcpy(bulk_str.end(), str.data(), len);
      bulk_str = Buffer{bulk_str.data(), bulk_str.size() + len};
      DVLOG(1) << "Extending bulk stash to size " << bulk_str.size();
    } else {
      DVLOG(1) << "New bulk stash size " << bulk_len_;
      std::unique_ptr<uint8_t[]> nb(new uint8_t[bulk_len_]);
      memcpy(nb.get(), str.data(), len);
      bulk_str = Buffer{nb.get(), len};
      buf_stash_.emplace_back(move(nb));
      is_broken_token_ = true;
      cached_expr_->back().has_support = true;
    }
    last_consumed_ = len;
    bulk_len_ -= len;
  }

  return INPUT_PENDING;
}

void RedisParser::HandleFinishArg() {
  DCHECK(!parse_stack_.empty());
  DCHECK_GT(parse_stack_.back().first, 0u);

  while (true) {
    --parse_stack_.back().first;
    state_ = PARSE_ARG_S;
    if (parse_stack_.back().first != 0)
      break;

    parse_stack_.pop_back();  // pop 0.
    if (parse_stack_.empty()) {
      state_ = CMD_COMPLETE_S;
      break;
    }
    cached_expr_ = parse_stack_.back().second;
  }
}

void RedisParser::ExtendLastString(Buffer str) {
  DCHECK(!cached_expr_->empty() && cached_expr_->back().type == RespExpr::STRING);
  DCHECK(!buf_stash_.empty());

  Buffer& last_str = get<Buffer>(cached_expr_->back().u);

  DCHECK(last_str.data() == buf_stash_.back().get());

  std::unique_ptr<uint8_t[]> nb(new uint8_t[last_str.size() + str.size()]);
  memcpy(nb.get(), last_str.data(), last_str.size());
  memcpy(nb.get() + last_str.size(), str.data(), str.size());
  last_str = RespExpr::Buffer{nb.get(), last_str.size() + str.size()};
  buf_stash_.back() = std::move(nb);
}

}  // namespace dfly
