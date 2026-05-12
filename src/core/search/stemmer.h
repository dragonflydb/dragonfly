// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>
#include <string_view>

struct sb_stemmer;

namespace dfly::search {

class Stemmer {
 public:
  explicit Stemmer(std::string_view language);
  ~Stemmer();

  Stemmer(const Stemmer&) = delete;
  Stemmer& operator=(const Stemmer&) = delete;
  Stemmer(Stemmer&& other) noexcept;
  Stemmer& operator=(Stemmer&& other) noexcept;

  // Not thread-safe: libstemmer reuses an internal buffer per call.
  std::string Stem(std::string_view token);

 private:
  sb_stemmer* stemmer_;
};

}  // namespace dfly::search
