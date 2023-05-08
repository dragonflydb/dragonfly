// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/search.h"

#include "core/search/query_driver.h"

using namespace std;

namespace dfly::search {

AstExpr ParseQuery(std::string_view query) {
  QueryDriver driver{};
  driver.ResetScanner();
  driver.SetInput(std::string{query});
  try {
    (void)Parser (&driver)();
  } catch (...) {
    // TODO: return detailed error info
    return {};
  }
  return driver.Get();
}

}  // namespace dfly::search
