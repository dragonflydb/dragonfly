// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/acl/user.h"

#include <openssl/sha.h>

#include <limits>

#include "absl/container/flat_hash_set.h"
#include "absl/strings/escaping.h"
#include "core/overloaded.h"

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

void User::Update(UpdateRequest&& req, const CategoryToIdxStore& cat_to_id,
                  const ReverseCategoryIndexTable& reverse_cat,
                  const CategoryToCommandsIndexStore& cat_to_commands) {
  for (auto& pass : req.passwords) {
    if (pass.nopass) {
      SetNopass();
      continue;
    }
    if (pass.unset) {
      UnsetPassword(pass.password);
      continue;
    }
    if (pass.reset_password) {
      password_hashes_.clear();
      continue;
    }
    SetPasswordHash(pass.password, pass.is_hashed);
  }

  auto cat_visitor = [&, this](UpdateRequest::CategoryValueType cat) {
    auto [sign, category] = cat;
    if (sign == Sign::PLUS) {
      SetAclCategoriesAndIncrSeq(category, cat_to_id, reverse_cat, cat_to_commands);
      return;
    }
    UnsetAclCategoriesAndIncrSeq(category, cat_to_id, reverse_cat, cat_to_commands);
  };

  auto cmd_visitor = [this](UpdateRequest::CommandsValueType cmd) {
    auto [sign, index, bit_index] = cmd;
    if (sign == Sign::PLUS) {
      SetAclCommandsAndIncrSeq(index, bit_index);
      return;
    }
    UnsetAclCommandsAndIncrSeq(index, bit_index);
  };

  Overloaded visitor{cat_visitor, cmd_visitor};

  for (auto req : req.updates) {
    std::visit(visitor, req);
  }

  if (!req.keys.empty()) {
    SetKeyGlobs(std::move(req.keys));
  }

  if (!req.pub_sub.empty()) {
    SetPubSub(std::move(req.pub_sub));
  }

  if (req.is_active) {
    SetIsActive(*req.is_active);
  }

  SetNamespace(req.ns);
}

void User::SetPasswordHash(std::string_view password, bool is_hashed) {
  nopass_ = false;
  if (is_hashed) {
    std::string binary;
    if (absl::HexStringToBytes(password, &binary)) {
      password_hashes_.insert(binary);
    } else {
      LOG(ERROR) << "Invalid password hash: " << password;
    }
    return;
  }
  password_hashes_.insert(StringSHA256(password));
}

void User::UnsetPassword(std::string_view password) {
  password_hashes_.erase(StringSHA256(password));
}

void User::SetNamespace(const std::string& ns) {
  namespace_ = ns;
}

const std::string& User::Namespace() const {
  return namespace_;
}

bool User::HasPassword(std::string_view password) const {
  if (nopass_) {
    return true;
  }
  return password_hashes_.contains(StringSHA256(password));
}

void User::SetAclCategoriesAndIncrSeq(uint32_t cat, const CategoryToIdxStore& cat_to_id,
                                      const ReverseCategoryIndexTable& reverse_cat,
                                      const CategoryToCommandsIndexStore& cat_to_commands) {
  acl_categories_ |= cat;
  if (cat == acl::ALL) {
    SetAclCommands(std::numeric_limits<size_t>::max(), 0);
  } else {
    auto id = cat_to_id.at(cat);
    std::string_view name = reverse_cat[id];
    const auto& commands_group = cat_to_commands.at(name);
    for (size_t fam_id = 0; fam_id < commands_group.size(); ++fam_id) {
      SetAclCommands(fam_id, commands_group[fam_id]);
    }
  }

  CategoryChange change{cat};
  cat_changes_[change] = ChangeMetadata{Sign::PLUS, seq_++};
}

void User::UnsetAclCategoriesAndIncrSeq(uint32_t cat, const CategoryToIdxStore& cat_to_id,
                                        const ReverseCategoryIndexTable& reverse_cat,
                                        const CategoryToCommandsIndexStore& cat_to_commands) {
  acl_categories_ ^= cat;
  if (cat == acl::ALL) {
    UnsetAclCommands(std::numeric_limits<size_t>::max(), 0);
  } else {
    auto id = cat_to_id.at(cat);
    std::string_view name = reverse_cat[id];
    const auto& commands_group = cat_to_commands.at(name);
    for (size_t fam_id = 0; fam_id < commands_group.size(); ++fam_id) {
      UnsetAclCommands(fam_id, commands_group[fam_id]);
    }
  }

  CategoryChange change{cat};
  cat_changes_[change] = ChangeMetadata{Sign::MINUS, seq_++};
}

void User::SetAclCommands(size_t index, uint64_t bit_index) {
  if (index == std::numeric_limits<size_t>::max()) {
    for (auto& family : commands_) {
      family = ALL_COMMANDS;
    }
    return;
  }
  commands_[index] |= bit_index;
}

void User::SetAclCommandsAndIncrSeq(size_t index, uint64_t bit_index) {
  SetAclCommands(index, bit_index);
  CommandChange change{index, bit_index};
  cmd_changes_[change] = ChangeMetadata{Sign::PLUS, seq_++};
}

void User::UnsetAclCommands(size_t index, uint64_t bit_index) {
  if (index == std::numeric_limits<size_t>::max()) {
    for (auto& family : commands_) {
      family = NONE_COMMANDS;
    }
    return;
  }
  SetAclCommands(index, bit_index);
  commands_[index] ^= bit_index;
}

void User::UnsetAclCommandsAndIncrSeq(size_t index, uint64_t bit_index) {
  UnsetAclCommands(index, bit_index);
  CommandChange change{index, bit_index};
  cmd_changes_[change] = ChangeMetadata{Sign::MINUS, seq_++};
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

const absl::flat_hash_set<std::string>& User::Passwords() const {
  return password_hashes_;
}

bool User::HasNopass() const {
  return nopass_;
}

const AclKeys& User::Keys() const {
  return keys_;
}

const AclPubSub& User::PubSub() const {
  return pub_sub_;
}

const User::CategoryChanges& User::CatChanges() const {
  return cat_changes_;
}

const User::CommandChanges& User::CmdChanges() const {
  return cmd_changes_;
}

void User::SetKeyGlobs(std::vector<UpdateKey> keys) {
  for (auto& key : keys) {
    if (key.all_keys) {
      keys_.key_globs.clear();
      keys_.all_keys = true;
    } else if (key.reset_keys) {
      keys_.key_globs.clear();
      keys_.all_keys = false;
    } else {
      keys_.key_globs.push_back({std::move(key.key), key.op});
    }
  }
}

void User::SetPubSub(std::vector<UpdatePubSub> pub_sub) {
  for (auto& pattern : pub_sub) {
    if (pattern.all_channels) {
      pub_sub_.globs.clear();
      pub_sub_.all_channels = true;
    } else if (pattern.reset_channels) {
      pub_sub_.globs.clear();
      pub_sub_.all_channels = false;
    } else {
      pub_sub_.globs.push_back({std::move(pattern.pattern), pattern.has_asterisk});
    }
  }
}

void User::SetNopass() {
  nopass_ = true;
  password_hashes_.clear();
}

}  // namespace dfly::acl
