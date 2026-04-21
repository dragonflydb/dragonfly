// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/scoring.h"

namespace dfly::search {

double ScoreDocument(ScorerType scorer, const ScoringContext& ctx,
                     const std::vector<ScoringTermInfo>& terms) {
  double score = 0.0;
  switch (scorer) {
    case ScorerType::BM25STD:
      for (const auto& term : terms)
        score += BM25Std(ctx, term);
      break;
  }
  return score;
}

}  // namespace dfly::search
