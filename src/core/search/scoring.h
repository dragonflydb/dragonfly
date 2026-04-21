// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <algorithm>
#include <cmath>
#include <string>
#include <string_view>
#include <vector>

#include "core/search/base.h"

namespace dfly::search {

class FieldIndices;
struct TextIndex;

// Supported scorer types
enum class ScorerType : int {
  BM25STD,        // Standard Okapi BM25 (default)
  TFIDF,          // Classic TF * IDF (no document length normalization)
  TFIDF_DOCNORM,  // TFIDF with document length normalization
};

// Per-term information needed for scoring a single document
struct ScoringTermInfo {
  uint32_t term_freq = 0;        // How many times this term appears in the document
  size_t term_docs = 0;          // Number of documents containing this term
  uint32_t field_doc_len = 0;    // Document length in THIS field (sum of TF)
  double field_avg_doc_len = 0;  // Average document length for THIS field
};

// Context passed to scorer for a single document
struct ScoringContext {
  size_t num_docs = 0;  // Total documents in index
};

// Compute BM25STD score for a single term in a document.
//
// Formula: IDF * f * (k1 + 1) / (f + k1 * (1 - b + b * docLen / avgDocLen))
// where IDF = ln(1 + (N - n + 0.5) / (n + 0.5))
//
// Parameters: b = 0.75, k1 = 1.2
inline double BM25Std(const ScoringContext& ctx, const ScoringTermInfo& term) {
  constexpr double b = 0.75;
  constexpr double k1 = 1.2;

  double f = term.term_freq;
  if (f == 0)
    return 0.0;

  // IDF: ln(1 + (N - n + 0.5) / (n + 0.5))
  double N = ctx.num_docs;
  double n = term.term_docs;
  // Clamp N >= n to avoid negative IDF during transient states
  N = std::max(N, n);
  double idf = std::log(1.0 + (N - n + 0.5) / (n + 0.5));

  // TF saturation: f * (k1 + 1) / (f + k1 * (1 - b + b * fieldDocLen / fieldAvgDocLen))
  double avg = term.field_avg_doc_len > 0 ? term.field_avg_doc_len : 1.0;
  double tf = f * (k1 + 1.0) / (f + k1 * (1.0 - b + b * term.field_doc_len / avg));

  return idf * tf;
}

// Compute TFIDF score for a single term in a document.
//
// Formula: f * IDF
// where IDF = ln(N / n), clamped to be non-negative.
//
// Note: returns 0 when a term appears in every document (N == n, no discriminating power).
// This differs from BM25STD, which adds a "+1" inside the log to keep the score positive.
inline double TfIdf(const ScoringContext& ctx, const ScoringTermInfo& term) {
  double f = term.term_freq;
  if (f == 0 || term.term_docs == 0)
    return 0.0;

  double N = ctx.num_docs;
  double n = term.term_docs;
  // Clamp N >= n to avoid negative IDF during transient states
  N = std::max(N, n);
  double idf = std::log(N / n);
  return f * idf;
}

// Compute TFIDF with document length normalization for a single term.
//
// Formula: (f * IDF) / fieldDocLen
inline double TfIdfDocNorm(const ScoringContext& ctx, const ScoringTermInfo& term) {
  double score = TfIdf(ctx, term);
  if (score == 0.0 || term.field_doc_len == 0)
    return score;
  return score / term.field_doc_len;
}

// Compute score for a document matched against multiple terms.
// Returns sum of per-term scores.
double ScoreDocument(ScorerType scorer, const ScoringContext& ctx,
                     const std::vector<ScoringTermInfo>& terms);

}  // namespace dfly::search
