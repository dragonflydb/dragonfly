// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

extern "C" {
#include "redis/quicklist.h"
}

#include <functional>
#include <optional>
#include <string>

namespace dfly {

class QList {
 public:
  enum Where { TAIL, HEAD };

  struct Entry {
    Entry(const char* value, size_t length) : value{value}, length{length} {
    }
    Entry(long long longval) : value{nullptr}, longval{longval} {
    }

    const char* value;
    union {
      size_t length;
      long long longval;
    };
  };

  using IterateFunc = std::function<bool(Entry)>;
  enum InsertOpt { BEFORE, AFTER };

  QList();
  QList(int fill, int compress);
  ~QList();

  size_t Size() const {
    return count_;
  }

  void Push(std::string_view value, Where where);
  void AppendListpack(unsigned char* zl);
  void AppendPlain(unsigned char* zl);
  void Insert(std::string_view pivot, std::string_view elem, InsertOpt opt);

  size_t MallocUsed() const;

  // Peeks at the head or tail of the list. Precondition: list is not empty.
  std::string Peek(Where where) const;

  std::optional<std::string> Get(long index) const;

  void Iterate(IterateFunc cb, long start, long end) const;

 private:
  bool AllowCompression() const {
    return compress_ != 0;
  }

  // Returns false if used existing head, true if new head created.
  bool PushHead(std::string_view value);

  // Returns false if used existing head, true if new head created.
  bool PushTail(std::string_view value);
  void InsertPlainNode(quicklistNode* old_node, std::string_view, bool after);
  void InsertNode(quicklistNode* old_node, quicklistNode* new_node, bool after);
  void Compress(quicklistNode* node);

  quicklistNode* head_ = nullptr;
  quicklistNode* tail_ = nullptr;
  uint32_t count_ = 0;                   /* total count of all entries in all listpacks */
  uint32_t len_ = 0;                     /* number of quicklistNodes */
  signed int fill_ : QL_FILL_BITS;       /* fill factor for individual nodes */
  unsigned int compress_ : QL_COMP_BITS; /* depth of end nodes not to compress;0=off */
  unsigned int bookmark_count_ : QL_BM_BITS;
};

}  // namespace dfly
