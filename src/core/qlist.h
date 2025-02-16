// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

extern "C" {
#include "redis/quicklist.h"
}

#include <absl/functional/function_ref.h>

#include <optional>
#include <string>
#include <variant>

namespace dfly {

class QList {
 public:
  enum Where { TAIL, HEAD };
  enum COMPR_METHOD { LZF = 0, LZ4 = 1 };

  /* Node is a 40 byte struct describing a listpack for a quicklist.
   * We use bit fields keep the Node at 40 bytes.
   * count: 16 bits, max 65536 (max lp bytes is 65k, so max count actually < 32k).
   * encoding: 2 bits, RAW=1, LZF=2.
   * container: 2 bits, PLAIN=1 (a single item as char array), PACKED=2 (listpack with multiple
   * items). recompress: 1 bit, bool, true if node is temporary decompressed for usage.
   * attempted_compress: 1 bit, boolean, used for verifying during testing.
   * dont_compress: 1 bit, boolean, used for preventing compression of entry.
   * extra: 9 bits, free for future use; pads out the remainder of 32 bits
   * NOTE: do not change the ABI of this struct as long as we support --list_experimental_v2=false
   * */

  typedef struct Node {
    struct Node* prev;
    struct Node* next;
    unsigned char* entry;
    size_t sz;                           /* entry size in bytes */
    unsigned int count : 16;             /* count of items in listpack */
    unsigned int encoding : 2;           /* RAW==1 or LZF==2 */
    unsigned int container : 2;          /* PLAIN==1 or PACKED==2 */
    unsigned int recompress : 1;         /* was this node previous compressed? */
    unsigned int attempted_compress : 1; /* node can't compress; too small */
    unsigned int dont_compress : 1;      /* prevent compression of entry that will be used later */
    unsigned int extra : 25;             /* more bits to steal for future usage */
  } Node;

  // Provides wrapper around the references to the listpack entries.
  class Entry {
    std::variant<std::string_view, int64_t> value_;

   public:
    Entry(const char* value, size_t length) : value_{std::string_view(value, length)} {
    }

    explicit Entry(int64_t longval) : value_{longval} {
    }

    // Assumes value is not int64.
    std::string_view view() const {
      return std::get<std::string_view>(value_);
    }

    bool is_int() const {
      return std::holds_alternative<int64_t>(value_);
    }

    int64_t ival() const {
      return std::get<int64_t>(value_);
    }

    bool operator==(std::string_view sv) const;

    friend bool operator==(std::string_view sv, const Entry& entry) {
      return entry == sv;
    }

    std::string to_string() const {
      if (std::holds_alternative<int64_t>(value_)) {
        return std::to_string(std::get<int64_t>(value_));
      }
      return std::string(view());
    }
  };

  class Iterator {
   public:
    Entry Get() const;

    // Returns false if no more entries.
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
  enum InsertOpt { BEFORE, AFTER };

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

  QList(QList&&);
  QList(const QList&) = delete;
  ~QList();

  QList& operator=(const QList&) = delete;
  QList& operator=(QList&&);

  size_t Size() const {
    return count_;
  }

  void Clear();

  void Push(std::string_view value, Where where);

  // Returns the popped value. Precondition: list is not empty.
  std::string Pop(Where where);

  void AppendListpack(unsigned char* zl);
  void AppendPlain(unsigned char* zl, size_t sz);

  // Returns true if pivot found and elem inserted, false otherwise.
  bool Insert(std::string_view pivot, std::string_view elem, InsertOpt opt);

  void Insert(Iterator it, std::string_view elem, InsertOpt opt);

  // Returns true if item was replaced, false if index is out of range.
  bool Replace(long index, std::string_view elem);

  size_t MallocUsed(bool slow) const;

  void Iterate(IterateFunc cb, long start, long end) const;

  // Returns an iterator to tail or the head of the list.
  // To mirror the quicklist interface, the iterator is not valid until Next() is called.
  // TODO: to fix this.
  Iterator GetIterator(Where where) const;

  // Returns an iterator at a specific index 'idx',
  // or Invalid iterator if index is out of range.
  // negative index - means counting from the tail.
  // Requires calling subsequent Next() to initialize the iterator.
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
  bool Erase(const long start, unsigned count);

  // Needed by tests and the rdb code.
  const Node* Head() const {
    return head_;
  }

  const Node* Tail() const {
    return _Tail();
  }

  void set_fill(int fill) {
    fill_ = fill;
  }

  void set_compr_method(COMPR_METHOD cm) {
    compr_method_ = static_cast<unsigned>(cm);
  }

  static void SetPackedThreshold(unsigned threshold);

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

  void OnPreUpdate(Node* node);
  void OnPostUpdate(Node* node);

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
