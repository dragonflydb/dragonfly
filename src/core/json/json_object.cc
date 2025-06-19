// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/json/json_object.h"

#include "base/logging.h"

using namespace jsoncons;
using namespace std;

namespace dfly {

optional<JsonType> JsonFromString(string_view input, PMR_NS::memory_resource* mr) {
  error_code ec;
  auto JsonErrorHandler = [](json_errc ec, const ser_context&) {
    VLOG(1) << "Error while decode JSON: " << make_error_code(ec).message();
    return false;
  };

  // The maximum allowed JSON nesting depth is 64.
  // The limit was reduced from 256 to 64. This change is reasonable, as most documents contain
  // no more than 20-30 levels of nesting. In the test case, over 128 levels were used, causing
  // the parser to enter a long stall due to excessive resource consumption. Even a limit of 128
  // does not mitigate the issue. A limit of 64 is a sensible compromise.
  // See https://github.com/dragonflydb/dragonfly/issues/5028
  const uint32_t json_nesting_depth_limit = 64;

  /* The maximum possible JSON nesting depth is either the specified json_nesting_depth_limit or
     half of the input size. Since nesting a JSON object requires at least 2 characters. */
  auto parser_options = jsoncons::json_options{}.max_nesting_depth(
      std::min(json_nesting_depth_limit, uint32_t(input.size() / 2)));

  json_decoder<JsonType> decoder(std::pmr::polymorphic_allocator<char>{mr});
  json_parser parser(parser_options, JsonErrorHandler);

  parser.update(input);
  parser.finish_parse(decoder, ec);

  if (!ec && decoder.is_valid()) {
    return decoder.get_result();
  }
  return nullopt;
}

}  // namespace dfly
