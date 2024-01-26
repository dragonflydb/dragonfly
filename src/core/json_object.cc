// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/json_object.h"

#include "base/logging.h"
#include "core/compact_object.h"

using namespace jsoncons;
using namespace std;

namespace dfly {

optional<JsonType> JsonFromString(string_view input) {
  error_code ec;
  auto JsonErrorHandler = [](json_errc ec, const ser_context&) {
    VLOG(1) << "Error while decode JSON: " << make_error_code(ec).message();
    return false;
  };

  json_decoder<JsonType> decoder(
      std::pmr::polymorphic_allocator<char>{CompactObj::memory_resource()});
  json_parser parser(basic_json_decode_options<char>{}, JsonErrorHandler);

  parser.update(input);
  parser.finish_parse(decoder, ec);

  if (decoder.is_valid()) {
    return decoder.get_result();
  }
  return nullopt;
}

}  // namespace dfly
