// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/functional/function_ref.h>

#include <cstdint>
#include <string>

#include "core/collection_entry.h"

#define QL_FILL_BITS 16
#define QL_COMP_BITS 16
#define QL_BM_BITS 4

/* quicklist node encodings */
#define QUICKLIST_NODE_ENCODING_RAW 1
#define QUICKLIST_NODE_ENCODING_LZF 2
#define QLIST_NODE_ENCODING_LZ4 3

/* quicklist node container formats */
#define QUICKLIST_NODE_CONTAINER_PLAIN 1
#define QUICKLIST_NODE_CONTAINER_PACKED 2

namespace dfly {

class PageUsage;

// Heuristic: for values smaller than 2 KiB we prefer the compact listpack
// representation. 2048 was chosen as a conservative threshold that matches
// common quicklist usage patterns and avoids creating very large listpacks
// that are costly to reallocate or compress.
inline bool ShouldStoreAsListPack(size_t size) {
  return size < 2048;
}

class QList {
 public:
  enum Where : uint8_t { TAIL, HEAD };
  enum COMPR_METHOD : uint8_t { LZF = 0, LZ4 = 1 };

  /* Node is a 40 byte struct describing a listpack for a quicklist.
   * We use bit fields keep the Node at 40 bytes.
   * count: 16 bits, max 65536 (max lp bytes is 65k, so max count actually < 32k).
   * encoding: 2 bits, RAW=1, LZF=2.
   * container: 2 bits, PLAIN=1 (a single item as char array), PACKED=2 (listpack with multiple
   * items). recompress: 1 bit, bool, true if node is temporary decompressed for usage.
   * attempted_compress: 1 bit, boolean, used for verifying during testing.
   * dont_compress: 1 bit, boolean, used for preventing compression of entry.
   * */

  struct Node {
    Node* prev;
    Node* next;
    unsigned char* entry;
    size_t sz : 48;    /* entry size in bytes */
    size_t count : 16; /* count of items in listpack */

    uint16_t encoding : 2;           /* RAW==1 or LZF==2 */
    uint16_t container : 2;          /* PLAIN==1 or PACKED==2 */
    uint16_t recompress : 1;         /* was this node previous compressed? */
    uint16_t attempted_compress : 1; /* node can't compress; too small */
    uint16_t dont_compress : 1;      /* prevent compression of entry that will be used later */
    uint16_t reserved1 : 9;          /* reserved for future use */

    uint16_t reserved2; /* more bits to steal for future usage */
    uint32_t reserved3; /* more bits to steal for future usage */

    bool IsCompressed() const {
      return encoding != QUICKLIST_NODE_ENCODING_RAW;
    }

    size_t GetLZF(void** data) const;
  };

  using Entry = CollectionEntry;
  class Iterator {
   public:
    // Returns true if the iterator is valid (points to an element).
    bool Valid() const {
      return zi_ != nullptr;
    }

    Entry Get() const;

    // Advances to the next/prev element. Returns false if no more entries.
    bool Next();

   private:
    const QList* owner_ = nullptr;
    Node* current_ = nullptr;
    unsigned char* zi_ = nullptr; /* points to the current element */
    long offset_ = 0;             /* offset in current listpack */
    uint8_t direction_ = 1;

    friend class QList;
  };

  using IterateFunc = absl::FunctionRef<bool(Entry)>;
  enum InsertOpt : uint8_t { BEFORE, AFTER };

  /**
   * fill: The number of entries allowed per internal list node can be specified
   * as a fixed maximum size or a maximum number of elements.
   * For a fixed maximum size, use -5 through -1, meaning:
   * -5: max size: 64 Kb  <-- not recommended for normal workloads
   * -4: max size: 32 Kb  <-- not recommended
   * -3: max size: 16 Kb  <-- probably not recommended
   * -2: max size: 8 Kb   <-- good
   * -1: max size: 4 Kb   <-- good
   * Positive numbers mean store up to _exactly_ that number of elements
   * per list node.
   * The highest performing option is usually -2 (8 Kb size) or -1 (4 Kb size),
   * but if your use case is unique, adjust the settings as necessary.
   *
   *
   * Lists may also be compressed.
   * "compress" is the number of quicklist listpack nodes from *each* side of
   * the list to *exclude* from compression.  The head and tail of the list
   * are always uncompressed for fast push/pop operations.  Settings are:
   * 0: disable all list compression
   * 1: depth 1 means "don't start compressing until after 1 node into the list,
   *    going from either the head or tail"
   *    So: [head]->node->node->...->node->[tail]
   *    [head], [tail] will always be uncompressed; inner nodes will compress.
   * 2: [head]->[next]->node->node->...->node->[prev]->[tail]
   *    2 here means: don't compress head or head->next or tail->prev or tail,
   *    but compress all nodes between them.
   * 3: [head]->[next]->[next]->node->node->...->node->[prev]->[prev]->[tail]
   * etc.
   *
   */
  explicit QList(int fill = -2, int compress = 0);

  QList(QList&&) noexcept;
  QList(const QList&) = delete;
  ~QList();

  QList& operator=(const QList&) = delete;
  QList& operator=(QList&&) noexcept;

  size_t Size() const {
    return count_;
  }

  void Clear() noexcept;

  void Push(std::string_view value, Where where);

  // Returns the popped value. Precondition: list is not empty.
  std::string Pop(Where where);

  void AppendListpack(uint8_t* zl);
  void AppendPlain(uint8_t* zl, size_t sz);

  // Returns true if pivot found and elem inserted, false otherwise.
  bool Insert(std::string_view pivot, std::string_view elem, InsertOpt opt);

  void Insert(Iterator it, std::string_view elem, InsertOpt opt);

  // Returns true if item was replaced, false if index is out of range.
  bool Replace(long index, std::string_view elem);

  size_t MallocUsed(bool slow) const;

  // Iterates over entries from start to end (inclusive).
  void Iterate(IterateFunc cb, long start, long end) const;

  // Returns an iterator to tail or the head of the list.
  // result.Valid() is true if the list is not empty.
  Iterator GetIterator(Where where) const;

  // Returns an iterator at a specific index 'idx',
  // or Invalid iterator if index is out of range.
  // negative index - means counting from the tail.
  // result.Valid() is true if the index is within range.
  Iterator GetIterator(long idx) const;

  uint32_t node_count() const {
    return len_;
  }

  unsigned compress_param() const {
    return compress_;
  }

  Iterator Erase(Iterator it);

  // Returns true if elements were deleted, false if list has not changed.
  // Negative start index is allowed.
  bool Erase(long start, unsigned count);

  // Needed by tests and the rdb code.
  const Node* Head() const {
    return head_;
  }

  const Node* Tail() const {
    return _Tail();
  }

  // Returns nullptr if quicklist does not fit the necessary requirements
  // to be converted to listpack, and listpack otherwise. The ownership over the listpack
  // blob is moved to the caller.
  uint8_t* TryExtractListpack();

  void set_fill(int fill) {
    fill_ = fill;
  }

  void set_compr_method(COMPR_METHOD cm) {
    compr_method_ = static_cast<unsigned>(cm);
  }

  static void SetPackedThreshold(unsigned threshold);

  // Moves nodes away from underused pages by reallocating if the underlying page usage is low.
  // Returns count of nodes reallocated to help in testing.
  size_t DefragIfNeeded(PageUsage* page_usage);

  struct Stats {
    uint64_t compression_attempts = 0;

    // compression attempts with compression ratio that was not good enough to keep.
    // Subset of compression_attempts.
    uint64_t bad_compression_attempts = 0;

    uint64_t decompression_calls = 0;

    // How many bytes we currently keep compressed.
    size_t compressed_bytes = 0;

    // how many bytes we compressed from.
    // Compressed savings are calculated as raw_compressed_bytes - compressed_bytes.
    size_t raw_compressed_bytes = 0;
  };
  static __thread Stats stats;

 private:
  bool AllowCompression() const {
    return compress_ != 0;
  }

  Node* _Tail() const {
    return head_ ? head_->prev : nullptr;
  }

  // Returns newly created plain node.
  Node* InsertPlainNode(Node* old_node, std::string_view, InsertOpt insert_opt);
  void InsertNode(Node* old_node, Node* new_node, InsertOpt insert_opt);
  void Replace(Iterator it, std::string_view elem);

  void Compress(Node* node);

  Node* MergeNodes(Node* node);

  // Deletes one of the nodes and returns the other.
  Node* ListpackMerge(Node* a, Node* b);

  void DelNode(Node* node);
  bool DelPackedIndex(Node* node, uint8_t* p);

  // Initializes iterator's zi_ to point to the element at offset_.
  // Decompresses the node if needed. Assumes current_ is not null.
  void InitIteratorEntry(Iterator* it) const;

  Node* head_ = nullptr;
  size_t malloc_size_ = 0;  // size of the quicklist struct
  uint32_t count_ = 0;      /* total count of all entries in all listpacks */
  uint32_t len_ = 0;        /* number of quicklistNodes */
  int fill_ : QL_FILL_BITS; /* fill factor for individual nodes */
  int compr_method_ : 2;    // 0 - lzf, 1 - lz4
  int reserved1_ : 14;
  unsigned compress_ : QL_COMP_BITS; /* depth of end nodes not to compress;0=off */
  unsigned bookmark_count_ : QL_BM_BITS;
  unsigned reserved2_ : 12;
};

}  // namespace dfly
