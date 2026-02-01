#pragma once

#include <cstddef>

namespace dfly::search {

struct RenewableQuota {
  void Check() const;

  const size_t max_usec;
};

}  // namespace dfly::search
