// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/inlined_vector.h>

#include <functional>
#include <optional>
#include <string_view>

#include "core/search/parser.hh"

namespace dfly::search {

// A lexer token and its byte span in the query text.
struct RawToken {
  Parser::symbol_type sym;
  size_t begin = 0, end = 0;
  bool param = false;  // spelled with a leading '$' (a param or a $-keyword)
};

// Token filter between the lexer and the parser for words with punctuation inside (example.com).
// The lexer emits one GLUE per separator character. A separator run between two word atoms with
// no whitespace around it reaches the parser as one GLUE; any other run is dropped. Inside a glued
// word, keywords become plain terms and numbers that took a leading `.` or `+` are split again.
class WordGlue {
 public:
  using Source = std::function<RawToken()>;

  // `input` is the text `source` lexes; it must outlive the parse.
  WordGlue(Source source, std::string_view input) : source_(std::move(source)), input_(input) {
  }

  Parser::symbol_type Next();

 private:
  struct Tok : RawToken {
    bool star = false;  // a `.5` or `+5` number with a glued '*'
  };

  Tok NextTok();
  void FillWord();
  void ConvertJoinedKeywords();
  void SplitNumbers(bool glued);
  bool IsSep(const Tok& t) const;
  int MakeTerm(Tok* t) const;

  Source source_;
  std::string_view input_;
  absl::InlinedVector<Tok, 8> buf_;  // the current whitespace-free word
  size_t head_ = 0;
  std::optional<Tok> pending_;  // the token after a separator run
  std::optional<Tok> carry_;    // the token after the current word
  int prev_kind_ = Parser::token::TOK_YYEOF;
  size_t prev_end_ = 0;
  unsigned brackets_ = 0;
};

}  // namespace dfly::search
