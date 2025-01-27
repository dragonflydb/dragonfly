// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "facade/memcache_parser.h"

#include <absl/container/flat_hash_map.h>
#include <absl/container/inlined_vector.h>
#include <absl/strings/ascii.h>
#include <absl/strings/escaping.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>
#include <absl/types/span.h>

#include "base/logging.h"
#include "base/stl_util.h"
#include "facade/facade_types.h"

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

  if (token.size() == 2) {
    // META_COMMANDS
    if (token[0] != 'm')
      return MP::INVALID;
    switch (token[1]) {
      case 's':
        return MP::META_SET;
      case 'g':
        return MP::META_GET;
      case 'd':
        return MP::META_DEL;
      case 'a':
        return MP::META_ARITHM;
      case 'n':
        return MP::META_NOOP;
      case 'e':
        return MP::META_DEBUG;
    }
    return MP::INVALID;
  }

  if (token.size() > 2) {
    auto it = cmd_map.find(token);
    if (it == cmd_map.end())
      return MP::INVALID;
    return it->second;
  }
  return MP::INVALID;
}

MP::Result ParseStore(ArgSlice tokens, MP::Command* res) {
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

MP::Result ParseValueless(ArgSlice tokens, MP::Command* res) {
  const size_t num_tokens = tokens.size();
  size_t key_pos = 0;
  if (res->type == MP::GAT || res->type == MP::GATS) {
    if (!absl::SimpleAtoi(tokens[0], &res->expire_ts)) {
      return MP::BAD_INT;
    }
    ++key_pos;
  }

  // We support only `flushall` or `flushall 0`
  if (key_pos < num_tokens && res->type == MP::FLUSHALL) {
    int delay = 0;
    if (key_pos + 1 == num_tokens && absl::SimpleAtoi(tokens[key_pos], &delay) && delay == 0)
      return MP::OK;
    return MP::PARSE_ERROR;
  }

  res->key = tokens[key_pos++];

  if (key_pos < num_tokens && res->type == MP::STATS)
    return MP::PARSE_ERROR;  // we don't support additional arguments to stats for now

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

bool ParseMetaMode(char m, MP::Command* res) {
  if (res->type == MP::SET) {
    switch (m) {
      case 'E':
        res->type = MP::ADD;
        break;
      case 'A':
        res->type = MP::APPEND;
        break;
      case 'R':
        res->type = MP::REPLACE;
        break;
      case 'P':
        res->type = MP::PREPEND;
        break;
      case 'S':
        break;
      default:
        return false;
    }
    return true;
  }

  if (res->type == MP::INCR) {
    switch (m) {
      case 'I':
      case '+':
        break;
      case 'D':
      case '-':
        res->type = MP::DECR;
        break;
      default:
        return false;
    }
    return true;
  }
  return false;
}

// See https://raw.githubusercontent.com/memcached/memcached/refs/heads/master/doc/protocol.txt
MP::Result ParseMeta(ArgSlice tokens, MP::Command* res) {
  DCHECK(!tokens.empty());

  if (res->type == MP::META_DEBUG) {
    LOG(ERROR) << "meta debug not yet implemented";
    return MP::PARSE_ERROR;
  }

  if (tokens[0].size() > 250)
    return MP::PARSE_ERROR;

  res->meta = true;
  res->key = tokens[0];
  res->bytes_len = 0;
  res->flags = 0;
  res->expire_ts = 0;

  tokens.remove_prefix(1);

  // We emulate the behavior by returning the high level commands.
  // TODO: we should reverse the interface in the future, so that a high level command
  // will be represented in MemcacheParser::Command by a meta command with flags.
  // high level commands should not be part of the interface in the future.
  switch (res->type) {
    case MP::META_GET:
      res->type = MP::GET;
      break;
    case MP::META_DEL:
      res->type = MP::DELETE;
      break;
    case MP::META_SET:
      if (tokens.empty()) {
        return MP::PARSE_ERROR;
      }
      if (!absl::SimpleAtoi(tokens[0], &res->bytes_len))
        return MP::BAD_INT;

      res->type = MP::SET;
      tokens.remove_prefix(1);
      break;
    case MP::META_ARITHM:
      res->type = MP::INCR;
      res->delta = 1;
      break;
    default:
      return MP::PARSE_ERROR;
  }

  for (size_t i = 0; i < tokens.size(); ++i) {
    string_view token = tokens[i];

    switch (token[0]) {
      case 'T':
        if (!absl::SimpleAtoi(token.substr(1), &res->expire_ts))
          return MP::BAD_INT;
        break;
      case 'b':
        if (token.size() != 1)
          return MP::PARSE_ERROR;
        if (!absl::Base64Unescape(res->key, &res->blob))
          return MP::PARSE_ERROR;
        res->key = res->blob;
        res->base64 = true;
        break;
      case 'F':
        if (!absl::SimpleAtoi(token.substr(1), &res->flags))
          return MP::BAD_INT;
        break;
      case 'M':
        if (token.size() != 2 || !ParseMetaMode(token[1], res))
          return MP::PARSE_ERROR;
        break;
      case 'D':
        if (!absl::SimpleAtoi(token.substr(1), &res->delta))
          return MP::BAD_INT;
        break;
      case 'q':
        res->no_reply = true;
        break;
      case 'f':
        res->return_flags = true;
        break;
      case 'v':
        res->return_value = true;
        break;
      case 't':
        res->return_ttl = true;
        break;
      case 'l':
        res->return_access_time = true;
        break;
      case 'h':
        res->return_hit = true;
        break;
      case 'c':
        res->return_version = true;
        break;
      default:
        LOG(WARNING) << "unknown meta flag: " << token;  // not yet implemented
        return MP::PARSE_ERROR;
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
    // We need more data to parse the command. For get/gets commands this line can be very long.
    // we limit maximum buffer capacity in the higher levels using max_client_iobuf_len.
    return INPUT_PENDING;
  }

  if (pos == 0) {
    return PARSE_ERROR;
  }
  *consumed = pos + 2;

  string_view tokens_expression = str.substr(0, pos);

  // cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n
  // get <key>*\r\n
  // ms <key> <datalen> <flags>*\r\n
  absl::InlinedVector<string_view, 32> tokens =
      absl::StrSplit(tokens_expression, ' ', absl::SkipWhitespace());

  if (tokens.empty())
    return PARSE_ERROR;

  cmd->type = From(tokens[0]);
  if (cmd->type == INVALID) {
    return UNKNOWN_CMD;
  }

  ArgSlice tokens_view{tokens};
  tokens_view.remove_prefix(1);

  if (cmd->type <= CAS) {                                    // Store command
    if (tokens_view.size() < 4 || tokens[0].size() > 250) {  // key length limit
      return MP::PARSE_ERROR;
    }

    cmd->key = tokens_view[0];

    tokens_view.remove_prefix(1);
    return ParseStore(tokens_view, cmd);
  }

  if (cmd->type >= META_SET) {
    return tokens_view.empty() ? MP::PARSE_ERROR : ParseMeta(tokens_view, cmd);
  }

  if (tokens_view.empty()) {
    if (base::_in(cmd->type, {MP::STATS, MP::FLUSHALL, MP::QUIT, MP::VERSION, MP::META_NOOP})) {
      return MP::OK;
    }
    return MP::PARSE_ERROR;
  }

  return ParseValueless(tokens_view, cmd);
};

}  // namespace facade
