#include "core/detail/gen_utils.h"

#include <absl/strings/str_cat.h>

namespace dfly::detail {

std::string GetRandomHex(absl::InsecureBitGen& gen, size_t len, size_t len_deviation) {
  static_assert(std::is_same<uint64_t, decltype(gen())>::value);
  if (len_deviation) {
    len += (gen() % len_deviation);
  }

  std::string res(len, '\0');
  size_t indx = 0;

  for (size_t i = 0; i < len / 16; ++i) {  // 2 chars per byte
    absl::numbers_internal::FastHexToBufferZeroPad16(gen(), res.data() + indx);
    indx += 16;
  }

  if (indx < res.size()) {
    char buf[32];
    absl::numbers_internal::FastHexToBufferZeroPad16(gen(), buf);

    for (unsigned j = 0; indx < res.size(); indx++, j++) {
      res[indx] = buf[j];
    }
  }

  return res;
}
}  // namespace dfly::detail
