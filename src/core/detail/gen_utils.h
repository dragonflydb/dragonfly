#pragma once

#include <absl/random/random.h>

#include <string>

namespace dfly::detail {

std::string GetRandomHex(absl::InsecureBitGen& gen, size_t len, size_t len_deviation = 0);

}  // namespace dfly::detail
