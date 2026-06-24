// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/scoring.h"

namespace dfly::search {

double ScoreDocument(ScorerFn scorer, const ScoringContext& ctx,
                     const std::vector<ScoringTermInfo>& terms) {
  double score = 0.0;
  for (const auto& term : terms)
    score += scorer(ctx, term);
  return score;
}

ScorerFn RawScorer(const ScorerSpec& scorer) {
  switch (scorer.kind) {
    case ScorerKind::BM25STD:
    case ScorerKind::BM25STD_NORM:
    case ScorerKind::BM25STD_TANH:
      return &BM25Std;
    case ScorerKind::TFIDF:
      return &TfIdf;
    case ScorerKind::TFIDF_DOCNORM:
      return &TfIdfDocNorm;
  }
  return &BM25Std;
}

double ScoreDocument(const ScorerSpec& scorer, const ScoringContext& ctx,
                     const std::vector<ScoringTermInfo>& terms) {
  double score = ScoreDocument(RawScorer(scorer), ctx, terms);
  if (scorer.kind == ScorerKind::BM25STD_TANH)
    score = std::tanh(score / scorer.bm25std_tanh_factor);
  return score;
}

void GlobalScoringStats::Merge(const ShardScoringStats& shard) {
  num_docs += shard.num_docs;
  for (const auto& [field, stats] : shard.field_stats) {
    auto& dst = field_stats[field];
    dst.num_docs += stats.num_docs;
    dst.total_docs_len += stats.total_docs_len;
  }
  for (const auto& [field, terms] : shard.term_stats) {
    auto& dst = term_stats[field];
    for (const auto& [term, count] : terms)
      dst[term] += count;
  }
}

double GlobalScoringStats::GetFieldAvgDocLen(std::string_view field_ident) const {
  auto it = field_stats.find(field_ident);
  if (it == field_stats.end() || it->second.num_docs == 0)
    return 0.0;
  return static_cast<double>(it->second.total_docs_len) / it->second.num_docs;
}

size_t GlobalScoringStats::GetTermDocs(std::string_view field_ident, std::string_view term) const {
  auto field_it = term_stats.find(field_ident);
  if (field_it == term_stats.end())
    return 0;
  auto term_it = field_it->second.find(term);
  return term_it == field_it->second.end() ? 0 : term_it->second;
}

}  // namespace dfly::search
