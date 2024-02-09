// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <string>

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

 private:
  std::string cur_str_;
  std::unique_ptr<Lexer> lexer_;
};

}  // namespace json
}  // namespace dfly
