// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/json/json_object.h"

#include "base/flags.h"
#include "base/logging.h"

using namespace jsoncons;
using namespace std;

ABSL_FLAG(uint32_t, json_nesting_depth_limit, 128,
          "The maximum allowed nesting depth for JSON documents. "
          "Any attempt to create a document with a nesting depth greater than the specified limit "
          "will result in an error.");

namespace dfly {

optional<JsonType> JsonFromString(string_view input, PMR_NS::memory_resource* mr) {
  error_code ec;
  auto JsonErrorHandler = [](json_errc ec, const ser_context&) {
    VLOG(1) << "Error while decode JSON: " << make_error_code(ec).message();
    return false;
  };

  /* The maximum possible JSON nesting depth is either the specified depth_limit or half of the
     input size. Since nesting a JSON object requires at least 2 characters. */
  const uint32_t depth_limit = absl::GetFlag(FLAGS_json_nesting_depth_limit);
  auto parser_options =
      jsoncons::json_options{}.max_nesting_depth(std::min(depth_limit, uint32_t(input.size() / 2)));

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
