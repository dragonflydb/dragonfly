// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/acl/user.h"

#include <openssl/sha.h>

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
  // acl_categories_ = AclCat::ACL_CATEGORY_ADMIN;
}

void User::Update(UpdateRequest&& req) {
  if (req.password) {
    SetPasswordHash(*req.password);
  }

  for (auto [sign, category] : req.categories) {
    if (sign == Sign::PLUS) {
      SetAclCategories(category);
      continue;
    }
    UnsetAclCategories(category);
  }

  if (req.is_active) {
    SetIsActive(*req.is_active);
  }
}

void User::SetPasswordHash(std::string_view password) {
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

void User::SetAclCategories(uint64_t cat) {
  acl_categories_ |= cat;
}

void User::UnsetAclCategories(uint64_t cat) {
  SetAclCategories(cat);
  acl_categories_ ^= cat;
}

uint32_t User::AclCategory() const {
  return acl_categories_;
}

// For ACL commands
// void SetAclCommand()
// void AclCommand() const;

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
