// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/facade_types.h"

namespace facade {

class CommandId {
 public:
  using CmdArgList = facade::CmdArgList;

  /**
   * @brief Construct a new Command Id object
   *
   * When creating a new command use the https://github.com/redis/redis/tree/unstable/src/commands
   * files to find the right arguments.
   *
   * @param name
   * @param mask
   * @param arity -     positive if command has fixed number of required arguments
   *                    negative if command has minimum number of required arguments, but may have
   * more.
   * @param first_key - position of first key in argument list
   * @param last_key  - position of last key in argument list,
   *                    -1 means the last key index is (arg_length - 1), -2 means that the last key
   * index is (arg_length - 2).
   * @param step -      step count for locating repeating keys
   */
  CommandId(const char* name, uint32_t mask, int8_t arity, int8_t first_key, int8_t last_key,
            int8_t step);

  std::string_view name() const {
    return name_;
  }

  int arity() const {
    return arity_;
  }

  uint32_t opt_mask() const {
    return opt_mask_;
  }

  int8_t first_key_pos() const {
    return first_key_;
  }

  int8_t last_key_pos() const {
    return last_key_;
  }

  int8_t key_arg_step() const {
    return step_key_;
  }

  static uint32_t OptCount(uint32_t mask);

 protected:
  std::string_view name_;

  uint32_t opt_mask_;
  int8_t arity_;
  int8_t first_key_;
  int8_t last_key_;
  int8_t step_key_;
};

}  // namespace facade
