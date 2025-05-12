// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tdigest_family.h"

#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"

namespace dfly {

using CI = CommandId;

#define HFUNC(x) SetHandler(&TDigestFamily::x)

void TDigestFamily::Register(CommandRegistry* registry) {
  registry->StartFamily();

  *registry << CI{"TS.Create", 0 /*FIX THOSE*/, -4, 1, 1, acl::TDIGEST}.HFUNC(Create)
            << CI{"TS.Del", 0 /*FIX THOSE*/, -4, 1, 1, acl::TDIGEST}.HFUNC(Del)
            << CI{"TS.CreateRule", 0 /*FIX THOSE*/, -4, 1, 1, acl::TDIGEST}.HFUNC(CreateRule)
            << CI{"TS.DeleteRule", 0 /*FIX THOSE*/, -4, 1, 1, acl::TDIGEST}.HFUNC(DeleteRule)
            << CI{"TS.Alter", 0 /*FIX THOSE*/, -4, 1, 1, acl::TDIGEST}.HFUNC(Alter)
            << CI{"TS.MCreate", 0 /*FIX THOSE*/, -4, 1, 1, acl::TDIGEST}.HFUNC(MCreate)
            << CI{"TS.Add", 0 /*FIX THOSE*/, -4, 1, 1, acl::TDIGEST}.HFUNC(Add)
            << CI{"TS.Get", 0 /*FIX THOSE*/, -4, 1, 1, acl::TDIGEST}.HFUNC(Get)
            << CI{"TS.Range", 0 /*FIX THOSE*/, -4, 1, 1, acl::TDIGEST}.HFUNC(Range)
            << CI{"TS.MAdd", 0 /*FIX THOSE*/, -4, 1, 1, acl::TDIGEST}.HFUNC(MAdd)
            << CI{"TS.MGet", 0 /*FIX THOSE*/, -4, 1, 1, acl::TDIGEST}.HFUNC(MGet)
            << CI{"TS.MRange", 0 /*FIX THOSE*/, -4, 1, 1, acl::TDIGEST}.HFUNC(MRange)
            << CI{"TS.MQueryIndex", 0 /*FIX THOSE*/, -4, 1, 1, acl::TDIGEST}.HFUNC(MQueryIndex)
            << CI{"TS.Info", 0 /*FIX THOSE*/, -4, 1, 1, acl::TDIGEST}.HFUNC(Info);
};

}  // namespace dfly
