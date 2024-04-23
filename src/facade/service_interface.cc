// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/service_interface.h"

#include <absl/strings/str_cat.h>

namespace facade {

std::string ServiceInterface::ContextInfo::Format() const {
  char buf[16] = {0};
  std::string res = absl::StrCat("db=", db_index);

  unsigned index = 0;

  if (async_dispatch)
    buf[index++] = 'a';

  if (conn_closing)
    buf[index++] = 't';

  if (subscribers)
    buf[index++] = 'P';

  if (blocked)
    buf[index++] = 'b';

  if (index)
    absl::StrAppend(&res, " flags=", buf);
  return res;
}

}  // namespace facade
