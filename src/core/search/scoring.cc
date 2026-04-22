// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/scoring.h"

namespace dfly::search {

double ScoreDocument(ScorerType scorer, const ScoringContext& ctx,
                     const std::vector<ScoringTermInfo>& terms) {
  auto sc_func = [scorer]() {
    switch (scorer) {
      case ScorerType::BM25STD:
        return &BM25Std;
      case ScorerType::TFIDF:
        return &TfIdf;
      case ScorerType::TFIDF_DOCNORM:
        return &TfIdfDocNorm;
    }
    return static_cast<decltype(&BM25Std)>(nullptr);  // unreachable
  }();

  double score = 0.0;
  for (const auto& term : terms)
    score += sc_func(ctx, term);
  return score;
}

}  // namespace dfly::search
