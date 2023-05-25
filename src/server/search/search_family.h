// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <string>

#include "base/mutex.h"
#include "server/common.h"

namespace dfly {
class CommandRegistry;
class ConnectionContext;

class SearchFamily {
  static void FtCreate(CmdArgList args, ConnectionContext* cntx);
  static void FtSearch(CmdArgList args, ConnectionContext* cntx);

 public:
  static void Register(CommandRegistry* registry);

  struct IndexData {
    enum DataType { HASH, JSON };

    // Get numeric OBJ_ code
    uint8_t GetObjCode() const;

    std::string prefix{};
    DataType type{HASH};
  };

 private:
  static Mutex indices_mu_;
  static absl::flat_hash_map<std::string, IndexData> indices_;
};

}  // namespace dfly
