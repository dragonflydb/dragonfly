#pragma once

#include <cassert>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "base/pmr/memory_resource.h"

extern "C" {
#include "redis/rax.h"
}

namespace dfly::search {

// absl::flat_hash_map/std::unordered_map compatible tree map based on rax tree.
// Allocates all objects on heap (with custom memory resource) as rax tree operates fully on
// pointers.
// TODO: Add full support for polymorphic allocators, including rax trie node allocations
template <typename V> struct RaxTreeMap {
  struct FindIterator;

  // Simple seeking iterator
  struct SeekIterator {
    SeekIterator() {
      it_.rt = nullptr;
    }

    SeekIterator(rax* tree, const char* op, std::string_view key) {
      raxStart(&it_, tree);
      if (raxSeek(&it_, op, to_key_ptr(key), key.size())) {  // Successfuly seeked
        operator++();
      } else {
        InvalidateIterator();
      }
    }

    explicit SeekIterator(rax* tree) : SeekIterator(tree, "^", std::string_view{nullptr, 0}) {
    }

    /* Remove copy/move constructors to avoid double iterator invalidation */
    SeekIterator(SeekIterator&&) = delete;
    SeekIterator(const SeekIterator&) = delete;
    SeekIterator& operator=(SeekIterator&&) = delete;
    SeekIterator& operator=(const SeekIterator&) = delete;

    ~SeekIterator() {
      if (IsValid()) {
        InvalidateIterator();
      }
    }

    bool operator==(const SeekIterator& rhs) const {
      if (!IsValid() || !rhs.IsValid())
        return !IsValid() && !rhs.IsValid();
      return it_.node == rhs.it_.node;
    }

    bool operator!=(const SeekIterator& rhs) const {
      return !operator==(rhs);
    }

    SeekIterator& operator++() {
      int next_result = raxNext(&it_);
      if (!next_result) {  // OOM or we reached the end of the tree
        InvalidateIterator();
      }
      return *this;
    }

    /* After operator++() the first value (string_view) is invalid. So make sure your copied it to
     * string */
    std::pair<std::string_view, V&> operator*() const {
      assert(IsValid() && it_.node && it_.node->iskey && it_.data);
      return {std::string_view{reinterpret_cast<const char*>(it_.key), it_.key_len},
              *reinterpret_cast<V*>(it_.data)};
    }

    bool IsValid() const {
      return it_.rt;
    }

   private:
    void InvalidateIterator() {
      raxStop(&it_);
      it_.rt = nullptr;
    }

    raxIterator it_;
  };

  // Result of find() call. Inherits from pair to mimic iterator interface, not incrementable.
  struct FindIterator : public std::optional<std::pair<std::string, V&>> {
    bool operator==(const SeekIterator& rhs) const {
      if (!this->has_value() || !rhs.IsValid())
        return !this->has_value() && !rhs.IsValid();
      return (*this)->first == (*rhs).first;
    }

    bool operator!=(const SeekIterator& rhs) const {
      return !operator==(rhs);
    }
  };

 public:
  explicit RaxTreeMap(PMR_NS::memory_resource* mr) : tree_(raxNew()), alloc_(mr) {
  }

  ~RaxTreeMap() {
    using Allocator = decltype(alloc_);

    auto free_callback = [](void* data, void* context) {
      Allocator* allocator = static_cast<Allocator*>(context);
      V* ptr = static_cast<V*>(data);
      std::allocator_traits<Allocator>::destroy(*allocator, ptr);
      allocator->deallocate(ptr, 1);
    };

    raxFreeWithCallbackAndArgument(tree_, free_callback, &alloc_);
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
      return FindIterator{std::pair<std::string, V&>(std::string(key), *reinterpret_cast<V*>(ptr))};

    return FindIterator{std::nullopt};
  }

  template <typename... Args>
  std::pair<FindIterator, bool> try_emplace(std::string_view key, Args&&... args);

  void erase(FindIterator it) {
    V* old = nullptr;
    raxRemove(tree_, to_key_ptr(it->first.data()), it->first.size(),
              reinterpret_cast<void**>(&old));
    std::allocator_traits<decltype(alloc_)>::destroy(alloc_, old);
    alloc_.deallocate(old, 1);
  }

  auto& get_allocator() const {
    return alloc_;
  }

 private:
  static unsigned char* to_key_ptr(std::string_view key) {
    return reinterpret_cast<unsigned char*>(const_cast<char*>(key.data()));
  }

  rax* tree_;
  PMR_NS::polymorphic_allocator<V> alloc_;
};

template <typename V>
template <typename... Args>
std::pair<typename RaxTreeMap<V>::FindIterator, bool> RaxTreeMap<V>::try_emplace(
    std::string_view key, Args&&... args) {
  if (auto it = find(key); it)
    return {it, false};

  V* ptr = alloc_.allocate(1);
  std::allocator_traits<decltype(alloc_)>::construct(alloc_, ptr, std::forward<Args>(args)...);

  V* old = nullptr;
  raxInsert(tree_, to_key_ptr(key), key.size(), ptr, reinterpret_cast<void**>(&old));
  assert(!old);

  auto it = std::make_optional(std::pair<std::string, V&>(std::string(key), *ptr));
  return std::make_pair(std::move(FindIterator{it}), true);
}

}  // namespace dfly::search
