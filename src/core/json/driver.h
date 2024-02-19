// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <string>

#include "src/core/json/path.h"

namespace dfly {
namespace json {

class Lexer;
class location;  // from jsonpath_grammar.hh

class Driver {
 public:
  Driver();
  virtual ~Driver();

  Lexer* lexer() {
    return lexer_.get();
  }

  void SetInput(std::string str);
  void ResetScanner();
  virtual void Error(const location& l, const std::string& msg) = 0;

  void AddIdentifier(const std::string& identifier) {
    AddSegment(PathSegment(SegmentType::IDENTIFIER, identifier));
  }

  void AddFunction(std::string_view fname);

  void AddWildcard() {
    AddSegment(PathSegment(SegmentType::WILDCARD));
  }

  void AddSegment(PathSegment segment) {
    path_.push_back(std::move(segment));
  }

  Path TakePath() {
    return std::move(path_);
  }

 private:
  Path path_;
  std::string cur_str_;
  std::unique_ptr<Lexer> lexer_;
};

}  // namespace json
}  // namespace dfly
