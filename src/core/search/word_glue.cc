// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/word_glue.h"

#include <absl/strings/ascii.h>

#include <memory>
#include <string>

namespace dfly::search {

namespace {

// type_get() with raw token numbers works on every supported bison version.
using TokenId = Parser::token;
constexpr int kNoToken = -2;

int Kind(const Parser::symbol_type& sym) {
  return sym.type_get();
}

bool IsAtom(int k) {
  return k == TokenId::TOK_TERM || k == TokenId::TOK_PREFIX || k == TokenId::TOK_SUFFIX ||
         k == TokenId::TOK_INFIX || k == TokenId::TOK_UINT32 || k == TokenId::TOK_DOUBLE;
}

bool IsKeyword(int k) {
  return k == TokenId::TOK_KNN || k == TokenId::TOK_AS || k == TokenId::TOK_EF_RUNTIME ||
         k == TokenId::TOK_VECTOR_RANGE;
}

// Length of the `.`/`+` prefix that a number like `.5` or `+5` took by longest match.
size_t SepPrefix(const RawToken& t) {
  return Kind(t.sym) == TokenId::TOK_DOUBLE ? t.sym.value.as<std::string>().find_first_not_of(".+")
                                            : 0;
}

bool IsPlainKeyword(const RawToken& t) {
  return IsKeyword(Kind(t.sym)) && !t.param;
}

}  // namespace

// Outside [..] a comma is a separator; inside it keeps separating the range bounds.
bool WordGlue::IsSep(const Tok& t) const {
  return Kind(t.sym) == TokenId::TOK_GLUE || (Kind(t.sym) == TokenId::TOK_COMMA && brackets_ == 0);
}

// Lexes the next token, or a whole whitespace-free word, into the empty buf_.
void WordGlue::FillWord() {
  if (carry_)
    buf_.push_back(std::move(*carry_));
  else
    buf_.push_back(Tok{source_()});
  carry_.reset();
  auto in_word = [this](const Tok& t) {
    return IsAtom(Kind(t.sym)) || IsKeyword(Kind(t.sym)) || IsSep(t);
  };
  if (!in_word(buf_[0]))
    return;
  while (true) {
    Tok n{source_()};
    Tok& last = buf_.back();
    bool adjacent = n.begin == last.end;
    if (adjacent && Kind(n.sym) == TokenId::TOK_STAR && brackets_ == 0 && SepPrefix(last) > 0 &&
        !last.star) {
      last.end = n.end;  // `x.5*`: the number becomes a prefix atom of the word
      last.star = true;
      continue;
    }
    if (!in_word(n) || !adjacent) {
      carry_.emplace(std::move(n));
      break;
    }
    if (IsSep(n) && IsSep(last)) {  // one token per separator run keeps buf_ small
      last.end = n.end;
      continue;
    }
    buf_.push_back(std::move(n));
  }
  if (buf_.size() == 1 && !buf_[0].star)
    return;
  if (brackets_ == 0)
    ConvertJoinedKeywords();

  // The word is glued if a separator, or the `.` of `x.5*`, sits between two atoms.
  bool glued = false, star = false;
  size_t last_atom = 0;
  for (size_t i = 0; i < buf_.size(); ++i) {
    star |= buf_[i].star;
    glued |= buf_[i].star && i > 0 && IsAtom(Kind(buf_[i - 1].sym));
    last_atom = IsAtom(Kind(buf_[i].sym)) ? i : last_atom;
  }
  for (size_t i = 1; i < last_atom; ++i)
    glued |= IsAtom(Kind(buf_[i - 1].sym)) && IsSep(buf_[i]);
  if (brackets_ == 0 && (glued || star))
    SplitNumbers(glued);
}

// Outside [..], a keyword joined by separators to another word part (foo.as) is a plain term.
void WordGlue::ConvertJoinedKeywords() {
  const ptrdiff_t size = buf_.size();
  auto joined = [&](ptrdiff_t i, ptrdiff_t step) {
    ptrdiff_t j = i + step;
    while (j >= 0 && j < size && IsSep(buf_[j]))
      j += step;
    if (j < 0 || j >= size)
      return false;
    if (j == i + step)  // `as.5`: the separator is inside the number
      return step > 0 && SepPrefix(buf_[j]) > 0;
    return IsAtom(Kind(buf_[j].sym)) || IsPlainKeyword(buf_[j]);
  };
  for (ptrdiff_t i = 0; i < size; ++i) {
    if (IsPlainKeyword(buf_[i]) && (joined(i, -1) || joined(i, 1)))
      MakeTerm(&buf_[i]);
  }
}

// Splits `.5` into GLUE + number; an integer re-joins a following TERM or PREFIX (`.5as`).
void WordGlue::SplitNumbers(bool glued) {
  bool split = false;
  for (const Tok& t : buf_)
    split |= SepPrefix(t) > 0;
  if (!split)
    return;

  auto w = std::move(buf_);
  buf_.clear();
  for (size_t i = 0; i < w.size(); ++i) {
    size_t k = glued || w[i].star ? SepPrefix(w[i]) : 0;  // `.5*.5` keeps the second `.5`
    if (k == 0) {
      buf_.push_back(std::move(w[i]));
      continue;
    }
    auto loc = w[i].sym.location;
    size_t b = w[i].begin, e = w[i].end;
    std::string num = w[i].sym.value.as<std::string>().substr(k);
    bool integer = num.find('.') == std::string::npos;
    buf_.push_back(Tok{{Parser::make_GLUE(loc), b, b + k}});
    if (w[i].star) {
      if (!integer)  // `+5.5*`, like `5.5*`
        throw Parser::syntax_error(loc, "number with '*'");
      buf_.push_back(Tok{{Parser::make_PREFIX(num, loc), b + k, e}});
      continue;
    }
    auto next = i + 1 < w.size() && integer ? Kind(w[i + 1].sym) : kNoToken;
    // Without its `.`/`+`, the number would lex into other tokens: `5*y`, `5w'q'`, `inf5`.
    bool wild = integer && i + 1 == w.size() && carry_ && carry_->begin == e &&
                Kind(carry_->sym) == TokenId::TOK_WILDCARD;
    bool digit = (next == TokenId::TOK_UINT32 || next == TokenId::TOK_DOUBLE) && !w[i + 1].param &&
                 absl::ascii_isdigit(w[i + 1].sym.value.as<std::string>()[0]);
    if (wild || digit || next == TokenId::TOK_SUFFIX || next == TokenId::TOK_INFIX)
      throw Parser::syntax_error(loc, "number glued to the next word part");
    if (next != kNoToken && IsPlainKeyword(w[i + 1]))  // `5as` is one atom
      next = MakeTerm(&w[i + 1]);
    if (next == TokenId::TOK_DOUBLE && absl::ascii_isalpha(w[i + 1].sym.value.as<std::string>()[0]))
      next = TokenId::TOK_TERM;  // `5inf`
    if ((next == TokenId::TOK_TERM && !w[i + 1].param) || next == TokenId::TOK_PREFIX) {
      num += w[i + 1].sym.value.as<std::string>();
      e = w[++i].end;
      buf_.push_back(Tok{
          {next == TokenId::TOK_TERM ? Parser::make_TERM(num, loc) : Parser::make_PREFIX(num, loc),
           b + k, e}});
    } else {
      buf_.push_back(Tok{{Parser::make_DOUBLE(num, loc), b + k, e}});
    }
  }
}

// Replaces a keyword token with a TERM of its source spelling.
int WordGlue::MakeTerm(Tok* t) const {
  auto loc = t->sym.location;
  std::string text{input_.substr(t->begin, t->end - t->begin)};
  std::destroy_at(&t->sym);  // symbol_type has no assignment
  std::construct_at(&t->sym, Parser::make_TERM(std::move(text), loc));
  return TokenId::TOK_TERM;
}

WordGlue::Tok WordGlue::NextTok() {
  if (pending_) {
    Tok t = std::move(*pending_);
    pending_.reset();
    return t;
  }
  if (head_ == buf_.size()) {
    buf_.clear();
    head_ = 0;
    FillWord();
  }
  return std::move(buf_[head_++]);
}

Parser::symbol_type WordGlue::Next() {
  while (true) {
    Tok t = NextTok();
    if (!IsSep(t)) {
      if (Kind(t.sym) == TokenId::TOK_LBRACKET)
        ++brackets_;
      else if (Kind(t.sym) == TokenId::TOK_RBRACKET && brackets_ > 0)
        --brackets_;
      prev_kind_ = Kind(t.sym);
      prev_end_ = t.end;
      return std::move(t.sym);
    }

    bool glue = IsAtom(prev_kind_) && prev_end_ == t.begin && brackets_ == 0;
    bool at_star = prev_kind_ == TokenId::TOK_STAR && prev_end_ == t.begin;
    size_t run_end = t.end;
    while (true) {
      Tok n = NextTok();
      if (!IsSep(n)) {
        pending_.emplace(std::move(n));
        break;
      }
      glue &= n.begin == run_end;
      run_end = n.end;
    }
    const Tok& n = *pending_;
    // `*.com`: a lone star next to a separator is not a word, reject instead of skipping.
    if (at_star || (Kind(n.sym) == TokenId::TOK_STAR && n.begin == run_end))
      throw Parser::syntax_error(t.sym.location, "separator next to '*'");
    if (glue && IsAtom(Kind(n.sym)) && n.begin == run_end)
      return Parser::make_GLUE(t.sym.location);
    // Otherwise the run is dropped and pending_ is returned next.
  }
}

}  // namespace dfly::search
