// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/stemmer.h"

#include <glog/logging.h>
#include <libstemmer.h>

#include <utility>

namespace dfly::search {

std::optional<Stemmer> Stemmer::TryCreate(std::string_view language) {
  auto* s = sb_stemmer_new(std::string(language).c_str(), "UTF_8");
  if (!s)
    return std::nullopt;
  return Stemmer{s};
}

Stemmer::Stemmer(std::string_view language)
    : stemmer_(sb_stemmer_new(std::string(language).c_str(), "UTF_8")) {
  CHECK(stemmer_) << "Unsupported stemmer language: " << language;
}

Stemmer::~Stemmer() {
  sb_stemmer_delete(stemmer_);
}

Stemmer::Stemmer(Stemmer&& other) noexcept : stemmer_(other.stemmer_) {
  other.stemmer_ = nullptr;
}

Stemmer& Stemmer::operator=(Stemmer&& other) noexcept {
  std::swap(stemmer_, other.stemmer_);
  return *this;
}

std::string Stemmer::Stem(std::string_view token) {
  if (token.empty())
    return {};
  const sb_symbol* result = sb_stemmer_stem(
      stemmer_, reinterpret_cast<const sb_symbol*>(token.data()), static_cast<int>(token.size()));
  if (!result)
    return std::string{token};
  int len = sb_stemmer_length(stemmer_);
  return {reinterpret_cast<const char*>(result), static_cast<size_t>(len)};
}

Stemmer* StemmerPool::Get(std::string_view language) {
  if (auto it = pool_.find(language); it != pool_.end())
    return it->second.get();
  auto stem = Stemmer::TryCreate(language);
  if (!stem)
    return nullptr;
  auto [it, _] =
      pool_.try_emplace(std::string{language}, std::make_unique<Stemmer>(std::move(*stem)));
  return it->second.get();
}

}  // namespace dfly::search
