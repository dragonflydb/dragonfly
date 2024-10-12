#pragma once

#include <absl/types/span.h>

#include <cstdio>
#include <optional>
#include <string_view>

#include "base/pmr/memory_resource.h"

extern "C" {
#include "redis/rax.h"
}

namespace dfly::search {

// absl::flat_hash_map/std::unordered_map compatible tree map based on rax tree.
// Allocates all objects on heap (with custom memory resource) as rax tree operates fully on
// pointers.
template <typename V> struct RaxTreeMap {
  struct FindIterator;

  // Simple seeking iterator
  struct SeekIterator {
    friend struct FindIterator;

    SeekIterator() {
      raxStart(&it_, nullptr);
      it_.node = nullptr;
    }

    ~SeekIterator() {
      raxStop(&it_);
    }

    SeekIterator(SeekIterator&&) = delete;       // self-referential
    SeekIterator(const SeekIterator&) = delete;  // self-referential

    SeekIterator(rax* tree, const char* op, std::string_view key) {
      raxStart(&it_, tree);
      raxSeek(&it_, op, to_key_ptr(key), key.size());
      operator++();
    }

    explicit SeekIterator(rax* tree) : SeekIterator(tree, "^", std::string_view{nullptr, 0}) {
    }

    bool operator==(const SeekIterator& rhs) const {
      return it_.node == rhs.it_.node;
    }

    bool operator!=(const SeekIterator& rhs) const {
      return !operator==(rhs);
    }

    SeekIterator& operator++() {
      if (!raxNext(&it_)) {
        raxStop(&it_);
        it_.node = nullptr;
      }
      return *this;
    }

    std::pair<std::string_view, V&> operator*() const {
      return {std::string_view{reinterpret_cast<const char*>(it_.key), it_.key_len},
              *reinterpret_cast<V*>(it_.data)};
    }

   private:
    raxIterator it_;
  };

  // Result of find() call. Inherits from pair to mimic iterator interface, not incrementable.
  struct FindIterator : public std::optional<std::pair<std::string_view, V&>> {
    bool operator==(const SeekIterator& rhs) const {
      if (this->has_value() != !bool(rhs.it_.flags & RAX_ITER_EOF))
        return false;
      if (!this->has_value())
        return true;
      return (*this)->first ==
             std::string_view{reinterpret_cast<const char*>(rhs.it_.key), rhs.it_.key_len};
    }

    bool operator!=(const SeekIterator& rhs) const {
      return !operator==(rhs);
    }
  };

 public:
  explicit RaxTreeMap(PMR_NS::memory_resource* mr) : tree_(raxNew()), mr_(mr) {
  }

  size_t size() const {
    return raxSize(tree_);
  }

  auto begin() const {
    return SeekIterator{tree_};
  }

  auto end() const {
    return SeekIterator{};
  }

  auto lower_bound(std::string_view key) const {
    return SeekIterator{tree_, ">=", key};
  }

  FindIterator find(std::string_view key) const {
    if (void* ptr = raxFind(tree_, to_key_ptr(key), key.size()); ptr != raxNotFound)
      return FindIterator{std::pair<std::string_view, V&>(key, *reinterpret_cast<V*>(ptr))};
    return FindIterator{std::nullopt};
  }

  template <typename... Args>
  std::pair<FindIterator, bool> try_emplace(std::string_view key, Args&&... args);

  void erase(FindIterator it) {
    V* old = nullptr;
    raxRemove(tree_, to_key_ptr(it->first.data()), it->first.size(),
              reinterpret_cast<void**>(&old));
    mr_->deallocate(old, sizeof(V), alignof(V));
  }

 private:
  static unsigned char* to_key_ptr(std::string_view key) {
    return reinterpret_cast<unsigned char*>(const_cast<char*>(key.data()));
  }

  rax* tree_;
  PMR_NS::memory_resource* mr_;
};

template <typename V>
template <typename... Args>
std::pair<typename RaxTreeMap<V>::FindIterator, bool> RaxTreeMap<V>::try_emplace(
    std::string_view key, Args&&... args) {
  if (auto it = find(key); it)
    return {it, false};

  void* ptr = mr_->allocate(sizeof(V), alignof(V));
  V* data = new (ptr) V(std::forward<Args>(args)...);
  assert(uint64_t(ptr) == uint64_t(data));  // we free by the latter

  V* old = nullptr;
  raxInsert(tree_, to_key_ptr(key), key.size(), data, reinterpret_cast<void**>(&old));
  assert(old == nullptr);

  auto it = std::make_optional(std::pair<std::string_view, V&>(key, *data));
  return std::make_pair(FindIterator{it}, true);
}

}  // namespace dfly::search
