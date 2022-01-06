// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/command_registry.h"

#include "absl/strings/str_cat.h"
#include "base/bits.h"
#include "base/logging.h"
#include "server/conn_context.h"

using namespace std;

namespace dfly {

using absl::StrAppend;
using absl::StrCat;

CommandId::CommandId(const char* name, uint32_t mask, int8_t arity, int8_t first_key,
                     int8_t last_key, int8_t step)
    : name_(name), opt_mask_(mask), arity_(arity), first_key_(first_key), last_key_(last_key),
      step_key_(step) {
  if (mask & CO::ADMIN) {
    opt_mask_ |= CO::NOSCRIPT;
  }
}

uint32_t CommandId::OptCount(uint32_t mask) {
  return absl::popcount(mask);
}

CommandRegistry::CommandRegistry() {
  CommandId cd("COMMAND", CO::RANDOM | CO::LOADING | CO::STALE, 0, 0, 0, 0);

  cd.SetHandler([this](const auto& args, auto* cntx) { return Command(args, cntx); });
  const char* nm = cd.name();
  cmd_map_.emplace(nm, std::move(cd));
}

void CommandRegistry::Command(CmdArgList args, ConnectionContext* cntx) {
  size_t sz = cmd_map_.size();
  string resp = StrCat("*", sz, "\r\n");

  for (const auto& val : cmd_map_) {
    const CommandId& cd = val.second;
    StrAppend(&resp, "*6\r\n$", strlen(cd.name()), "\r\n", cd.name(), "\r\n");
    StrAppend(&resp, ":", int(cd.arity()), "\r\n");
    StrAppend(&resp, "*", CommandId::OptCount(cd.opt_mask()), "\r\n");

    for (uint32_t i = 0; i < 32; ++i) {
      unsigned obit = (1u << i);
      if (cd.opt_mask() & obit) {
        const char* name = CO::OptName(CO::CommandOpt{obit});
        StrAppend(&resp, "+", name, "\r\n");
      }
    }

    StrAppend(&resp, ":", cd.first_key_pos(), "\r\n");
    StrAppend(&resp, ":", cd.last_key_pos(), "\r\n");
    StrAppend(&resp, ":", cd.key_arg_step(), "\r\n");
  }

  cntx->SendRespBlob(resp);
}

CommandRegistry& CommandRegistry::operator<<(CommandId cmd) {
  string_view k = cmd.name();
  CHECK(cmd_map_.emplace(k, std::move(cmd)).second) << k;

  return *this;
}

namespace CO {

const char* OptName(CO::CommandOpt fl) {
  using namespace CO;

  switch (fl) {
    case WRITE:
      return "write";
    case READONLY:
      return "readonly";
    case DENYOOM:
      return "denyoom";
    case FAST:
      return "fast";
    case STALE:
      return "stale";
    case LOADING:
      return "loading";
    case RANDOM:
      return "random";
    case ADMIN:
      return "admin";
    case NOSCRIPT:
      return "noscript";
    case GLOBAL_TRANS:
      return "global-trans";
  }
  return "unknown";
}

}  // namespace CO

}  // namespace dfly
