// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/memcache_parser.h"

#include <absl/container/flat_hash_map.h>
#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>

#include "base/stl_util.h"

namespace dfly {
using namespace std;
using MP = MemcacheParser;

namespace {

MP::CmdType From(string_view token) {
  static absl::flat_hash_map<string_view, MP::CmdType> cmd_map{
      {"set", MP::SET},       {"add", MP::ADD},         {"replace", MP::REPLACE},
      {"append", MP::APPEND}, {"prepend", MP::PREPEND}, {"cas", MP::CAS},
      {"get", MP::GET},       {"gets", MP::GETS},       {"gat", MP::GAT},
      {"gats", MP::GATS},     {"stats", MP::STATS},     {"incr", MP::INCR},
      {"decr", MP::DECR},     {"delete", MP::DELETE},   {"flush_all", MP::FLUSHALL},
      {"quit", MP::QUIT},
  };

  auto it = cmd_map.find(token);
  if (it == cmd_map.end())
    return MP::INVALID;

  return it->second;
}

MP::Result ParseStore(const std::string_view* tokens, unsigned num_tokens, MP::Command* res) {
  unsigned opt_pos = 3;
  if (res->type == MP::CAS) {
    if (num_tokens <= opt_pos)
      return MP::PARSE_ERROR;
    ++opt_pos;
  }

  uint32_t flags;
  if (!absl::SimpleAtoi(tokens[0], &flags) || !absl::SimpleAtoi(tokens[1], &res->expire_ts) ||
      !absl::SimpleAtoi(tokens[2], &res->bytes_len))
    return MP::BAD_INT;

  if (res->type == MP::CAS && !absl::SimpleAtoi(tokens[3], &res->cas_unique)) {
    return MP::BAD_INT;
  }

  res->flags = flags;
  if (num_tokens == opt_pos + 1) {
    if (tokens[opt_pos] == "noreply") {
      res->no_reply = true;
    } else {
      return MP::PARSE_ERROR;
    }
  } else if (num_tokens > opt_pos + 1) {
    return MP::PARSE_ERROR;
  }

  return MP::OK;
}

MP::Result ParseValueless(const std::string_view* tokens, unsigned num_tokens, MP::Command* res) {
  unsigned key_pos = 0;
  if (res->type == MP::GAT || res->type == MP::GATS) {
    if (!absl::SimpleAtoi(tokens[0], &res->expire_ts)) {
      return MP::BAD_INT;
    }
    ++key_pos;
  }
  res->key = tokens[key_pos++];

  if (key_pos < num_tokens && base::_in(res->type, {MP::STATS, MP::FLUSHALL}))
    return MP::PARSE_ERROR;  // we do not support additional arguments for now.

  if (res->type == MP::INCR || res->type == MP::DECR) {
    if (key_pos == num_tokens)
      return MP::PARSE_ERROR;

    if (!absl::SimpleAtoi(tokens[key_pos], &res->delta))
      return MP::BAD_DELTA;
    ++key_pos;
  }

  while (key_pos < num_tokens) {
    res->keys_ext.push_back(tokens[key_pos++]);
  }

  if (res->type >= MP::DELETE) {  // write commands
    if (!res->keys_ext.empty() && res->keys_ext.back() == "noreply") {
      res->no_reply = true;
      res->keys_ext.pop_back();
    }
  }

  return MP::OK;
}

}  // namespace

auto MP::Parse(string_view str, uint32_t* consumed, Command* cmd) -> Result {
  auto pos = str.find('\n');
  *consumed = 0;
  if (pos == string_view::npos) {
    // TODO: it's over simplified since we may process GET/GAT command that is not limited to
    // 300 characters.
    return str.size() > 300 ? PARSE_ERROR : INPUT_PENDING;
  }

  if (pos == 0) {
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
  for (; cur <= pos; ++cur) {
    if (absl::ascii_isspace(str[cur])) {
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

  cmd->type = From(tokens[0]);
  if (cmd->type == INVALID) {
    return UNKNOWN_CMD;
  }

  if (cmd->type <= CAS) {  // Store command
    if (num_tokens < 5 || tokens[1].size() > 250) {
      return MP::PARSE_ERROR;
    }

    // memcpy(single_key_, tokens[0].data(), tokens[0].size());  // we copy the key
    cmd->key = string_view{tokens[1].data(), tokens[1].size()};

    return ParseStore(tokens + 2, num_tokens - 2, cmd);
  }

  if (num_tokens == 1) {
    if (base::_in(cmd->type, {MP::STATS, MP::FLUSHALL, MP::QUIT}))
      return MP::OK;
    return MP::PARSE_ERROR;
  }

  return ParseValueless(tokens + 1, num_tokens - 1, cmd);
};

}  // namespace dfly