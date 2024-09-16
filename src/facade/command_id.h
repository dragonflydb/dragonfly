// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string_view>

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
   * @param acl_categories - bitfield for acl categories of the command
   */
  CommandId(const char* name, uint32_t mask, int8_t arity, int8_t first_key, int8_t last_key,
            uint32_t acl_categories);

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

  uint32_t acl_categories() const {
    return acl_categories_;
  }

  void SetFamily(size_t fam) {
    family_ = fam;
  }

  void SetBitIndex(uint64_t bit) {
    bit_index_ = bit;
  }

  size_t GetFamily() const {
    return family_;
  }

  uint64_t GetBitIndex() const {
    return bit_index_;
  }

  // Returns true if the command can only be used by admin connections, false
  // otherwise.
  bool IsRestricted() const {
    return restricted_;
  }

  void SetRestricted(bool restricted) {
    restricted_ = restricted;
  }

  void SetFlag(uint32_t flag) {
    opt_mask_ |= flag;
  }

  static uint32_t OptCount(uint32_t mask);

  // PUBLISH/SUBSCRIBE/UNSUBSCRIBE variant
  bool IsPubSub() const {
    return is_pub_sub_;
  }

  // PSUBSCRIBE/PUNSUBSCRIBE variant
  bool IsPSub() const {
    return is_p_sub_;
  }

 protected:
  std::string name_;

  uint32_t opt_mask_;
  int8_t arity_;
  int8_t first_key_;
  int8_t last_key_;

  // Acl categories
  uint32_t acl_categories_;
  // Acl commands indices
  size_t family_;
  uint64_t bit_index_;

  // Whether the command can only be used by admin connections.
  bool restricted_ = false;

  bool is_pub_sub_ = false;
  bool is_p_sub_ = false;
};

}  // namespace facade
