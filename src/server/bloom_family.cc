// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/bloom_family.h"

#include "facade/cmd_arg_parser.h"
#include "facade/error.h"
#include "server/command_registry.h"
#include "server/conn_context.h"

namespace dfly {

// Bloom interface based on SBFs:
// https://www.sciencedirect.com/science/article/abs/pii/S0020019006003127 See
// https://www.alibabacloud.com/help/en/tair/developer-reference/bloom for the API documentation.
// See c-project for the implementation of bloom filters
// https://github.com/armon/bloomd as well as https://github.com/jvirkki/libbloom

using namespace facade;

namespace {}  // namespace

void BloomFamily::Reserve(CmdArgList args, ConnectionContext* cntx) {
  cntx->SendError(kSyntaxErr);
}

void BloomFamily::Add(CmdArgList args, ConnectionContext* cntx) {
  cntx->SendError(kSyntaxErr);
}

void BloomFamily::Exists(CmdArgList args, ConnectionContext* cntx) {
  cntx->SendError(kSyntaxErr);
}

using CI = CommandId;

#define HFUNC(x) SetHandler(&BloomFamily::x)

void BloomFamily::Register(CommandRegistry* registry) {
  registry->StartFamily();

  *registry << CI{"BF.RESERVE", CO::WRITE | CO::DENYOOM | CO::FAST, -4, 1, 1, acl::BLOOM}.HFUNC(
                   Reserve)
            << CI{"BF.ADD", CO::WRITE | CO::DENYOOM | CO::FAST, 3, 1, 1, acl::BLOOM}.HFUNC(Add)
            << CI{"BF.EXISTS", CO::READONLY | CO::FAST, 3, 1, 1, acl::BLOOM}.HFUNC(Exists);
};

}  // namespace dfly
