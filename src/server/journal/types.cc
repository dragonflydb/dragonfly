// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/types.h"

namespace dfly::journal {

std::string Entry::ToString() const {
  std::string rv = absl::StrCat("{dbid=", dbid);
  std::visit(
      [&rv](const auto& payload) {
        if constexpr (std::is_same_v<std::decay_t<decltype(payload)>, std::monostate>) {
          absl::StrAppend(&rv, ", empty");
        } else {
          const auto& [cmd, args] = payload;
          absl::StrAppend(&rv, ", cmd='");
          absl::StrAppend(&rv, cmd);
          absl::StrAppend(&rv, "', args=[");
          for (size_t i = 0; i < args.size(); i++) {
            absl::StrAppend(&rv, "'");
            absl::StrAppend(&rv, facade::ToSV(args[i]));
            absl::StrAppend(&rv, "'");
            if (i + 1 != args.size())
              absl::StrAppend(&rv, ", ");
          }
          absl::StrAppend(&rv, "]");
        }
      },
      payload);

  rv += "}";
  return rv;
}

}  // namespace dfly::journal
