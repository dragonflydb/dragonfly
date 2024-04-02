// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/string_or_view.h"

namespace dfly {
using namespace std;

StringOrView StringOrView::FromString(string s) {
  StringOrView sov;
  sov.val_ = std::move(s);
  return sov;
}

StringOrView StringOrView::FromView(string_view sv) {
  StringOrView sov;
  sov.val_ = sv;
  return sov;
}

bool StringOrView::operator==(const StringOrView& o) const {
  return *this == o.view();
}

bool StringOrView::operator==(std::string_view o) const {
  return view() == o;
}

bool StringOrView::operator!=(const StringOrView& o) const {
  return *this != o.view();
}

bool StringOrView::operator!=(std::string_view o) const {
  return !(*this == o);
}

string_view StringOrView::view() const {
  return visit([](const auto& s) -> std::string_view { return s; }, val_);
}

void StringOrView::MakeOwned() {
  if (std::holds_alternative<std::string_view>(val_))
    val_ = std::string{std::get<std::string_view>(val_)};
}

}  // namespace dfly
