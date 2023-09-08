// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/acl/user.h"

#include <openssl/sha.h>

#include "absl/strings/escaping.h"

namespace dfly::acl {

namespace {
std::string StringSHA256(std::string_view password) {
  std::string hash;
  hash.resize(SHA256_DIGEST_LENGTH);
  SHA256(reinterpret_cast<const unsigned char*>(password.data()), password.size(),
         reinterpret_cast<unsigned char*>(hash.data()));
  return hash;
}

}  // namespace

User::User() {
  commands_ = std::vector<uint64_t>(NumberOfFamilies(), 0);
}

void User::Update(UpdateRequest&& req) {
  if (req.password) {
    SetPasswordHash(*req.password, req.is_hashed);
  }

  for (auto [sign, category] : req.categories) {
    if (sign == Sign::PLUS) {
      SetAclCategories(category);
      continue;
    }
    UnsetAclCategories(category);
  }

  for (auto [sign, index, bit_index] : req.commands) {
    if (sign == Sign::PLUS) {
      SetAclCommands(index, bit_index);
      continue;
    }
    UnsetAclCommands(index, bit_index);
  }

  if (req.is_active) {
    SetIsActive(*req.is_active);
  }
}

void User::SetPasswordHash(std::string_view password, bool is_hashed) {
  if (password == "nopass") {
    return;
  }

  if (is_hashed) {
    password_hash_ = absl::HexStringToBytes(password);
    return;
  }
  password_hash_ = StringSHA256(password);
}

bool User::HasPassword(std::string_view password) const {
  if (!password_hash_) {
    if (password == "nopass") {
      return true;
    }
    return false;
  }
  // hash password and compare
  return *password_hash_ == StringSHA256(password);
}

void User::SetAclCategories(uint32_t cat) {
  acl_categories_ |= cat;
}

void User::UnsetAclCategories(uint32_t cat) {
  SetAclCategories(cat);
  acl_categories_ ^= cat;
}

void User::SetAclCommands(size_t index, uint64_t bit_index) {
  commands_[index] |= bit_index;
}

void User::UnsetAclCommands(size_t index, uint64_t bit_index) {
  SetAclCommands(index, bit_index);
  commands_[index] ^= bit_index;
}

uint32_t User::AclCategory() const {
  return acl_categories_;
}

std::vector<uint64_t> User::AclCommands() const {
  return commands_;
}

const std::vector<uint64_t>& User::AclCommandsRef() const {
  return commands_;
}

void User::SetIsActive(bool is_active) {
  is_active_ = is_active;
}

bool User::IsActive() const {
  return is_active_;
}

static const std::string_view default_pass = "nopass";

std::string_view User::Password() const {
  return password_hash_ ? *password_hash_ : default_pass;
}

}  // namespace dfly::acl
