// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/service_interface.h"

#include <absl/strings/str_cat.h>

#include "facade/facade_types.h"

namespace facade {

std::string ServiceInterface::ContextInfo::Format() const {
  char buf[16] = {0};
  std::string res = absl::StrCat("db=", db_index);

  unsigned index = 0;

  // Letters as clients parse them: A closing, P pub/sub, b blocked, t tracking, N none.
  if (conn_closing)
    buf[index++] = 'A';

  if (has_subscribers)
    buf[index++] = 'P';

  if (is_blocked)
    buf[index++] = 'b';

  if (is_tracking)
    buf[index++] = 't';

  absl::StrAppend(&res, " flags=", index ? buf : "N");
  return res;
}

DispatchResult ServiceInterface::DispatchCommandSimple(
    ParsedCommand* cmd, AsyncPreference mode,
    absl::FunctionRef<void(ParsedCommand*)>* pre_dispatch_cb) {
  return DispatchCommand(ParsedArgs{*cmd}, cmd, mode, pre_dispatch_cb);
}

}  // namespace facade
