// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <optional>
#include <string>
#include <string_view>

struct sb_stemmer;

namespace dfly::search {

class Stemmer {
 public:
  // Accepts forms recognised by sb_stemmer_new: English name or 2/3-letter ISO 639 code,
  // lowercase. Returns nullopt for unsupported language.
  static std::optional<Stemmer> TryCreate(std::string_view language);

  // CHECK-fails on invalid language. Use TryCreate when the language is user-supplied.
  explicit Stemmer(std::string_view language);
  ~Stemmer();

  Stemmer(const Stemmer&) = delete;
  Stemmer& operator=(const Stemmer&) = delete;
  Stemmer(Stemmer&& other) noexcept;
  Stemmer& operator=(Stemmer&& other) noexcept;

  // Not thread-safe: libstemmer reuses an internal buffer per call.
  std::string Stem(std::string_view token);

 private:
  explicit Stemmer(sb_stemmer* handle) noexcept : stemmer_{handle} {
  }

  sb_stemmer* stemmer_;
};

// Lazily-populated cache of Stemmer instances keyed by language. Used to honor
// LANGUAGE_FIELD per-doc overrides without paying allocation cost on every doc.
// Not thread-safe; keep one per shard, like Stemmer itself.
// unique_ptr storage keeps returned pointers stable across rehashes.
class StemmerPool {
 public:
  // Returns a stemmer for the language; nullptr if libstemmer doesn't ship one.
  Stemmer* Get(std::string_view language);

 private:
  absl::flat_hash_map<std::string, std::unique_ptr<Stemmer>> pool_;
};

}  // namespace dfly::search
