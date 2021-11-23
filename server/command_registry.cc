// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
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
}

uint32_t CommandId::OptCount(uint32_t mask) {
  return absl::popcount(mask);
}

CommandRegistry::CommandRegistry() {
  CommandId cd("COMMAND", CO::RANDOM | CO::LOADING | CO::STALE, 0, 0, 0, 0);
  cd.AssignCallback([this](const auto& args, auto* cntx) { return Command(args, cntx); });
  const char* nm = cd.name();
  cmd_map_.emplace(nm, std::move(cd));
}

void CommandRegistry::Command(CmdArgList args, ConnectionContext* cntx) {
  size_t sz = cmd_map_.size();
  string resp = absl::StrCat("*", sz, "\r\n");

  for (const auto& val : cmd_map_) {
    const CommandId& cd = val.second;
    StrAppend(&resp, "*6\r\n$", strlen(cd.name()), "\r\n", cd.name(), "\r\n");
    StrAppend(&resp, ":", int(cd.arity()), "\r\n", "*", CommandId::OptCount(cd.opt_mask()), "\r\n");
    uint32_t opt_bit = 1;

    for (uint32_t i = 1; i < 32; ++i, opt_bit <<= 1) {
      if (cd.opt_mask() & opt_bit) {
        const char* name = CO::OptName(CO::CommandOpt{opt_bit});
        StrAppend(&resp, "+", name, "\r\n");
      }
    }

    StrAppend(&resp, ":", cd.first_key_pos(), "\r\n");
    StrAppend(&resp, ":", cd.last_key_pos(), "\r\n");
    StrAppend(&resp, ":", cd.key_arg_step(), "\r\n");
  }

  cntx->SendRespBlob(resp);
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
  }
  return "";
}

}  // namespace CO

}  // namespace dfly
