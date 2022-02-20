// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/memcache_parser.h"

#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>

namespace dfly {
using namespace std;

namespace {

pair<string_view, MemcacheParser::CmdType> cmd_map[] = {
    {"set", MemcacheParser::SET},         {"add", MemcacheParser::ADD},
    {"replace", MemcacheParser::REPLACE}, {"append", MemcacheParser::APPEND},
    {"prepend", MemcacheParser::PREPEND}, {"cas", MemcacheParser::CAS},
    {"get", MemcacheParser::GET},         {"gets", MemcacheParser::GETS},
    {"gat", MemcacheParser::GAT},         {"gats", MemcacheParser::GATS},
};

MemcacheParser::CmdType From(string_view token) {
  for (const auto& k_v : cmd_map) {
    if (token == k_v.first)
      return k_v.second;
  }
  return MemcacheParser::INVALID;
}

MemcacheParser::Result ParseStore(const std::string_view* tokens, unsigned num_tokens,
                                  MemcacheParser::Command* res) {

  unsigned opt_pos = 3;
  if (res->type == MemcacheParser::CAS) {
    if (num_tokens <= opt_pos)
      return MemcacheParser::PARSE_ERROR;
    ++opt_pos;
  }

  uint32_t flags;
  if (!absl::SimpleAtoi(tokens[0], &flags) || !absl::SimpleAtoi(tokens[1], &res->expire_ts) ||
      !absl::SimpleAtoi(tokens[2], &res->bytes_len))
    return MemcacheParser::BAD_INT;

  if (res->type == MemcacheParser::CAS && !absl::SimpleAtoi(tokens[3], &res->cas_unique)) {
    return MemcacheParser::BAD_INT;
  }

  res->flags = flags;
  if (num_tokens == opt_pos + 1) {
    if (tokens[opt_pos] == "noreply") {
      res->no_reply = true;
    } else {
      return MemcacheParser::PARSE_ERROR;
    }
  } else if (num_tokens > opt_pos + 1) {
    return MemcacheParser::PARSE_ERROR;
  }

  return MemcacheParser::OK;
}

MemcacheParser::Result ParseRetrieve(const std::string_view* tokens, unsigned num_tokens,
                                    MemcacheParser::Command* res) {
  unsigned key_pos = 0;
  if (res->type == MemcacheParser::GAT || res->type == MemcacheParser::GATS) {
    if (!absl::SimpleAtoi(tokens[0], &res->expire_ts)) {
      return MemcacheParser::BAD_INT;
    }
    ++key_pos;
  }
  res->key = tokens[key_pos++];
  while (key_pos < num_tokens) {
    res->keys_ext.push_back(tokens[key_pos++]);
  }

  return MemcacheParser::OK;
}

}  // namespace

auto MemcacheParser::Parse(string_view str, uint32_t* consumed, Command* res) -> Result {
  auto pos = str.find('\n');
  *consumed = 0;
  if (pos == string_view::npos) {
    // TODO: it's over simplified since we may process gets command that is not limited to
    // 300 characters.
    return str.size() > 300 ? PARSE_ERROR : INPUT_PENDING;
  }
  if (pos == 0 || str[pos - 1] != '\r') {
    return PARSE_ERROR;
  }
  *consumed = pos + 1;

  // cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n
  // get <key>*\r\n
  string_view tokens[8];
  unsigned num_tokens = 0;
  uint32_t cur = 0;

  while (cur < pos && str[cur] == ' ')
    ++cur;
  uint32_t s = cur;
  for (; cur < pos; ++cur) {
    if (str[cur] == ' ' || str[cur] == '\r') {
      if (cur != s) {
        tokens[num_tokens++] = str.substr(s, cur - s);
        if (num_tokens == ABSL_ARRAYSIZE(tokens)) {
          ++cur;
          s = cur;
          break;
        }
      }
      s = cur + 1;
    }
  }
  if (num_tokens == 0)
    return PARSE_ERROR;

  while (cur < pos - 1) {
    if (str[cur] != ' ')
      return PARSE_ERROR;
    ++cur;
  }

  res->type = From(tokens[0]);
  if (res->type == INVALID) {
    return UNKNOWN_CMD;
  }

  if (res->type <= CAS) {  // Store command
    if (num_tokens < 5 || tokens[1].size() > 250) {
      return MemcacheParser::PARSE_ERROR;
    }

    // memcpy(single_key_, tokens[0].data(), tokens[0].size());  // we copy the key
    res->key = string_view{tokens[1].data(), tokens[1].size()};

    return ParseStore(tokens + 2, num_tokens - 2, res);
  }

  return ParseRetrieve(tokens + 1, num_tokens - 1, res);
};

}  // namespace dfly