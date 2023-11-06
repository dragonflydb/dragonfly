// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "facade/memcache_parser.h"

#include <absl/container/flat_hash_map.h>
#include <absl/container/inlined_vector.h>
#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>
#include <absl/types/span.h>

#include "base/logging.h"
#include "base/stl_util.h"

namespace facade {
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
      {"quit", MP::QUIT},     {"version", MP::VERSION},
  };

  auto it = cmd_map.find(token);
  if (it == cmd_map.end())
    return MP::INVALID;

  return it->second;
}

using TokensView = absl::Span<std::string_view>;

MP::Result ParseStore(TokensView tokens, MP::Command* res) {
  const size_t num_tokens = tokens.size();
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

MP::Result ParseValueless(TokensView tokens, MP::Command* res) {
  const size_t num_tokens = tokens.size();
  size_t key_pos = 0;
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
  cmd->no_reply = false;  // re-initialize
  auto pos = str.find("\r\n");
  *consumed = 0;
  if (pos == string_view::npos) {
    return INPUT_PENDING;
  }

  if (pos == 0) {
    return PARSE_ERROR;
  }
  *consumed = pos + 2;

  std::string_view tokens_expression = str.substr(0, pos);

  // cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n
  // get <key>*\r\n
  absl::InlinedVector<std::string_view, 32> tokens =
      absl::StrSplit(tokens_expression, ' ', absl::SkipWhitespace());

  const size_t num_tokens = tokens.size();

  if (num_tokens == 0)
    return PARSE_ERROR;

  cmd->type = From(tokens[0]);
  if (cmd->type == INVALID) {
    return UNKNOWN_CMD;
  }

  if (cmd->type <= CAS) {                            // Store command
    if (num_tokens < 5 || tokens[1].size() > 250) {  // key length limit
      return MP::PARSE_ERROR;
    }

    cmd->key = string_view{tokens[1].data(), tokens[1].size()};

    TokensView tokens_view{tokens.begin() + 2, num_tokens - 2};
    return ParseStore(tokens_view, cmd);
  }

  if (num_tokens == 1) {
    if (base::_in(cmd->type, {MP::STATS, MP::FLUSHALL, MP::QUIT, MP::VERSION})) {
      return MP::OK;
    }
    return MP::PARSE_ERROR;
  }

  TokensView tokens_view{tokens.begin() + 1, num_tokens - 1};
  return ParseValueless(tokens_view, cmd);
};

}  // namespace facade
