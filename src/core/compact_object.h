// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/internal/endian.h>

#include <optional>
#include <type_traits>
#include <utility>

#include "base/pmr/memory_resource.h"
#include "common/borrowed_string.h"
#include "common/string_or_view.h"
#include "core/json/json_object.h"
#include "core/mi_memory_resource.h"
#include "core/small_string.h"

typedef struct stream stream;

namespace dfly {

namespace tiering {
struct TieredCoolRecord;
}

constexpr unsigned kEncodingIntSet = 0;
constexpr unsigned kEncodingStrMap2 = 2;  // for set/map encodings of strings using DenseSet
constexpr unsigned kEncodingQL2 = 1;
constexpr unsigned kEncodingListPack = 3;
constexpr unsigned kEncodingJsonCons = 0;
constexpr unsigned kEncodingJsonFlat = 1;

class SBF;
class TOPK;
class CMS;
class CuckooFilter;
struct CuckooFilterOptions;
class PageUsage;

using cmn::StringOrView;
namespace detail {

// Storage for the five Redis collection types (LIST/SET/HASH/ZSET/STREAM).
// The CompactObj tag identifies the type; this struct holds the inner pointer,
// a per-collection size/byte-count, and an encoding byte. All type-aware
// dispatch (Free/Size/MallocUsed/DefragIfNeeded) is performed by CompactObj.
class RobjWrapper {
 public:
  using MemoryResource = PMR_NS::memory_resource;

  RobjWrapper() : sz_(0), encoding_(0), reserved_(0) {
  }

  // Used when sz_ is used to denote memory usage (e.g. OBJ_STREAM).
  void SetSize(uint64_t size) {
    sz_ = size;
  }
  size_t Size() const {
    return sz_;
  }

  void Init(unsigned encoding, void* inner) {
    encoding_ = encoding;
    inner_obj_ = inner;
    sz_ = 0;
  }

  unsigned encoding() const {
    return encoding_;
  }
  void* inner_obj() const {
    return inner_obj_;
  }

  void set_inner_obj(void* ptr) {
    inner_obj_ = ptr;
  }

 private:
  void* inner_obj_ = nullptr;

  // Semantics depend on the collection tag; only OBJ_STREAM currently uses it
  // (tracking bytes used, for memory accounting).
  uint64_t sz_ : 56;

  uint64_t encoding_ : 4;
  uint64_t reserved_ : 4;
} __attribute__((packed));

static_assert(sizeof(RobjWrapper) == 16);

// Raw, large (non-inline) string storage. Used when a string value does not
// fit in CompactObj's inline buffer and is not better represented as INT/SMALL/EXTERNAL.
struct LargeString {
  using MemoryResource = PMR_NS::memory_resource;

  void* ptr;
  uint64_t sz : 56;

  // Hint: outstanding readers may be borrowing `ptr`. Mutations consult
  // TL::pin_map and hand the buffer to its PendingRead instead of freeing.
  uint64_t read_pending : 1;
  uint64_t reserved : 7;

  size_t Size() const {
    return sz;
  }

  size_t MallocUsed() const;

  std::string_view AsView() const {
    return std::string_view{reinterpret_cast<char*>(ptr), sz};
  }

  uint64_t HashCode() const;

  bool Equal(std::string_view sv) const {
    return AsView() == sv;
  }

  // Replace contents with s, growing the underlying allocation if needed.
  // Precondition: !s.empty(). Use Free() for clearing a value.
  void SetString(std::string_view s, MemoryResource* mr);

  // Allocate room for `size` bytes; ptr must be null.
  void ReserveString(size_t size, MemoryResource* mr);

  // Append s. Precondition: existing capacity >= sz + s.size().
  void AppendString(std::string_view s, MemoryResource* mr);

  // Free underlying allocation; resets to {nullptr, 0}.
  void Free(MemoryResource* mr);

  // Re-allocate the backing buffer if its memory page is under-utilized.
  bool DefragIfNeeded(PageUsage* page_usage);

 private:
  void ReallocateString(MemoryResource* mr);
} __attribute__((packed));

static_assert(sizeof(LargeString) == 16);

}  // namespace detail

using CompactObjType = unsigned;

constexpr CompactObjType kInvalidCompactObjType = std::numeric_limits<CompactObjType>::max();

uint32_t JsonEnconding();

class CompactObj {
  static constexpr unsigned kInlineLen = 16;

 public:
  // Maximum input length, in bytes, that we attempt to compress with Huffman encoding.
  // The on-wire blob carries a varint size-delta header (1 or 2 bytes), so 16 KB stays well
  // inside the 15-bit delta budget. Also used by debug tooling that builds a representative
  // symbol histogram from existing data to train the Huffman table.
  static constexpr unsigned kMaxHuffLen = 16 * 1024;

 private:
  void operator=(const CompactObj&) = delete;
  CompactObj(const CompactObj&) = delete;

 protected:
  // 0-16 is reserved for inline lengths of string type.
  enum TagEnum : uint8_t {
    INT_TAG = 17,
    SMALL_TAG = 18,
    CUCKOO_FILTER_TAG = 19,
    EXTERNAL_TAG = 20,
    JSON_TAG = 21,
    SBF_TAG = 22,
    CMS_TAG = 23,
    SDS_TTL_TAG = 24,
    TOPK_TAG = 25,
    LARGE_STR_TAG = 26,  // detail::LargeString — raw, large non-inline string
    LIST_TAG = 27,
    SET_TAG = 28,
    HASH_TAG = 29,
    ZSET_TAG = 30,
    STREAM_TAG = 31,
  };

  // String encoding types.
  // With ascii compression it compresses 8 bytes to 7 but also 7 to 7.
  // Therefore, in order to know the original length we introduce 2 states that
  // correct the length upon decoding. ASCII1_ENC rounds down the decoded length,
  // while ASCII2_ENC rounds it up. See DecodedLen implementation for more info.
  enum EncodingEnum : uint8_t {
    NONE_ENC = 0,
    ASCII1_ENC = 1,
    ASCII2_ENC = 2,
    HUFFMAN_ENC = 3,
  };

 public:
  // Utility class for working with different string encodings (ascii, huffman, etc)
  struct StrEncoding {
    StrEncoding(uint8_t enc, bool is_key) : enc_(static_cast<EncodingEnum>(enc)), is_key_(is_key) {
    }

    size_t DecodedSize(std::string_view blob) const;         // Size of decoded blob
    size_t Decode(std::string_view blob, char* dest) const;  // Decode into dest, return size
    StringOrView Decode(std::string_view blob) const;

    // Decode a byte at offset into dest. Return true if decoded successfully,
    // false if idx is out of bounds.
    bool DecodeByte(std::string_view blob, size_t idx, uint8_t* dest) const;

   private:
    friend class CompactObj;

    // For HUFFMAN_ENC, huff_header is the little-endian 16-bit delta header
    // (decoded_size - compressed_size - 2). For other encodings the header is ignored.
    size_t DecodedSize(size_t compr_size, uint16_t huff_header) const;

    EncodingEnum enc_;
    bool is_key_;
  };

  using MemoryResource = detail::RobjWrapper::MemoryResource;

  // Different representations of external values
  enum class ExternalRep : uint8_t {
    STRING,          // OBJ_STRING, Basic representation with various string encodings
    SERIALIZED_MAP,  // OBJ_HASH, Serialized map
    LIST_NODE        // OBJ_LIST, QList::Node
  };

  explicit CompactObj(bool is_key)
      : is_key_{is_key}, taglen_{0}, encoding_{0} {  // default - empty string
  }

  CompactObj(std::string_view str, bool is_key) : CompactObj(is_key) {
    SetString(str);
  }

  CompactObj(CompactObj&& cs) noexcept : CompactObj(cs.is_key_) {
    operator=(std::move(cs));
  };

  ~CompactObj();

  CompactObj& operator=(CompactObj&& o) noexcept;

  // Returns object size depending on the semantics.
  // For strings - returns the length of the string.
  // For containers - returns number of elements in the container.
  size_t Size() const;

  std::string_view GetSlice(std::string* scratch) const;

  // Read-only fast path. Returns a cmn::BorrowedString iff this CompactObj
  // holds a string value that can be borrowed, otherwise std::nullopt
  // (caller uses GetSlice/GetString/ToString). The borrowed bytes are valid
  // until the pin is released.
  //
  // For NONE_ENC: `encoded` is the user-visible bytes; the reply can stream
  // them directly. For ASCII1/ASCII2_ENC: `encoded` is the packed source
  // (`encoded.size() < decoded_size`) and the reply must decode in chunks.
  //
  // Side effects on success: stamps the LargeString's read_pending bit and
  // registers an internal pin in the thread-local pin map. The returned
  // BorrowedString carries the pin and its release fn; destruction (or
  // explicit Unpin()) releases it.
  std::optional<cmn::BorrowedString> TryBorrow() const;

  // Test helpers for inspecting the pin's refcount / orphaned state through
  // an active BorrowedString. Implemented in compact_object.cc where the
  // internal pin type is visible.
  static uint32_t TEST_PinRefcnt(const cmn::BorrowedString& bs) noexcept;
  static bool TEST_PinOrphaned(const cmn::BorrowedString& bs) noexcept;

  std::string ToString() const {
    std::string res;
    GetString(&res);
    return res;
  }

  uint64_t HashCode() const;
  static uint64_t HashCode(std::string_view str);

  bool HasFlag() const {
    return mask_bits_.mc_flag;
  }

  void SetFlag(bool e) {
    mask_bits_.mc_flag = e;
  }

  bool WasTouched() const {
    return mask_bits_.touched;
  }

  void SetTouched(bool e) {
    mask_bits_.touched = e;
  }

  bool DefragIfNeeded(PageUsage* page_usage);

  void SetOmitDefrag(bool v) {
    mask_bits_.omit_defrag = v;
  }

  bool OmitDefrag() const {
    return mask_bits_.omit_defrag;
  }

  bool HasStashPending() const {
    return mask_bits_.io_pending;
  }

  void SetStashPending(bool b) {
    mask_bits_.io_pending = b;
  }

  bool IsSticky() const {
    return mask_bits_.sticky;
  }

  void SetSticky(bool e) {
    mask_bits_.sticky = e;
  }

  unsigned Encoding() const;
  CompactObjType ObjType() const;

  void* RObjPtr() const {
    return u_.r_obj.inner_obj();
  }

  void SetRObjPtr(void* ptr) {
    u_.r_obj.set_inner_obj(ptr);
  }

  // takes ownership over obj_inner.
  // type should not be OBJ_STRING.
  void InitRobj(CompactObjType type, unsigned encoding, void* obj_inner);

  // Sets the abstract time used by per-member lazy expiry on StringSet/StringMap.
  // The value should typically be obtained via MemberTimeSeconds(now_ms).
  // Safe to call unconditionally — no-op if the underlying encoding is not kEncodingStrMap2.
  void SetMemberTime(uint32_t seconds) const;

  // Returns the abstract time previously set via SetMemberTime, or 0 if encoding
  // is not kEncodingStrMap2.
  uint32_t MemberTime() const;

  // Returns true if any member of the underlying StringSet/StringMap has expiration.
  // Returns false if encoding is not kEncodingStrMap2.
  bool HasMemberExpiration() const;

  // For STR object.
  void SetInt(int64_t val);
  std::optional<int64_t> TryGetInt() const;

  void GetString(std::string* res) const;

  void SetString(std::string_view str);
  void ReserveString(size_t size);
  void AppendString(std::string_view str);

  // Will set this to hold OBJ_JSON, after that it is safe to call GetJson
  // NOTE: in order to avid copy which can be expensive in this case,
  // you need to move an object that created with the function JsonFromString
  // into here, no copying is allowed!
  void SetJson(JsonType&& j);
  void SetJson(const uint8_t* buf, size_t len);
  // Adjusts the size used by json
  void SetJsonSize(int64_t size);
  // Adjusts the size used by a stream
  void AddStreamSize(int64_t size);

  // pre condition - the type here is OBJ_JSON and was set with SetJson
  JsonType* GetJson() const;

  void SetSBF(SBF* sbf) {
    SetMeta(SBF_TAG);
    u_.sbf = sbf;
  }

  void SetSBF(uint64_t initial_capacity, double fp_prob, double grow_factor);
  SBF* GetSBF() const;

  void SetTOPK(TOPK* topk) {
    SetMeta(TOPK_TAG);
    u_.topk = topk;
  }

  void SetTOPK(uint32_t k, uint32_t width, uint32_t depth, double decay);
  TOPK* GetTOPK() const;

  void SetCMS(CMS* cms) {
    SetMeta(CMS_TAG);
    u_.cms = cms;
  }

  void SetCMS(uint32_t width, uint32_t depth);
  CMS* GetCMS() const;

  void SetCuckooFilter(CuckooFilter* cf) {
    SetMeta(CUCKOO_FILTER_TAG);
    u_.cuckoo_filter = cf;
  }

  void SetCuckooFilter(const CuckooFilterOptions& options);
  CuckooFilter* GetCuckooFilter() const;

  // dest must have at least Size() bytes available
  void GetString(char* dest) const;

  bool IsExternal() const {
    return taglen_ == EXTERNAL_TAG;
  }

  // returns true if the value is stored in the cooling storage. Cooling storage has an item both
  // on disk and in memory.
  bool IsCool() const {
    assert(IsExternal());
    return u_.ext_ptr.is_cool;
  }

  void SetExternal(size_t offset, uint32_t sz, ExternalRep rep);
  ExternalRep GetExternalRep() const;

  // Switches to empty, non-external string.
  // Preserves all the attributes.
  void RemoveExternal() {
    encoding_ = NONE_ENC;
    SetMeta(0, mask_);
  }

  // Assigns a cooling record to the object together with its external slice.
  void SetCool(size_t offset, uint32_t serialized_size, ExternalRep rep,
               tiering::TieredCoolRecord* record);

  struct CoolItem {
    uint16_t page_offset;
    size_t serialized_size;
    tiering::TieredCoolRecord* record;
  };

  // Prerequisite: IsCool() is true.
  // Returns the external data of the object incuding its ColdRecord.
  CoolItem GetCool() const;

  // Prequisite: IsCool() is true.
  // Keeps cool record only as external value and discard in-memory part.
  void Freeze(size_t offset, size_t sz);

  std::pair<size_t, size_t> GetExternalSlice() const;

  // Injects either the the raw string (extracted with GetRawString()) or the usual string
  // back to the compact object. In the latter case, encoding is performed.
  // Precondition: The object must be in the EXTERNAL state.
  // Postcondition: The object is an in-memory string.
  void Materialize(std::string_view str, bool is_raw);

  // Returns the approximation of memory used by the object.
  // If slow is true, may use more expensive methods to calculate the precise size.
  size_t MallocUsed(bool slow = false) const;

  // Resets the object to empty state (string).
  void Reset();

  bool IsInline() const {
    return taglen_ <= kInlineLen;
  }

  uint8_t GetFirstByte() const;

  // For HUFFMAN_ENC strings, returns the first 2 bytes of the encoded blob assembled as a
  // little-endian uint16_t. Those 2 bytes carry the delta header that maps compressed length
  // to decoded length. Only meaningful when encoding_ == HUFFMAN_ENC.
  uint16_t GetHuffHeader() const;
  // Returns true if the byte was decoded successfully, false if idx is out of bounds.
  bool GetByteAtIndex(size_t idx, uint8_t* res) const;
  // Returns a pair of booleans: {success, in_place}. success is false if offset is out of bounds
  // in_place is true if the byte was set without needing to rewrite the string.
  std::pair<bool, bool> SetByteAtIndex(size_t idx, uint8_t val);

  struct Stats {
    size_t small_string_bytes = 0;
    uint64_t huff_encode_total = 0, huff_encode_success = 0;
  };

  static Stats GetStatsThreadLocal();
  static void InitThreadLocal(MemoryResource* mr);

  // Iterate the thread-local pin map and reap entries with refcnt==0:
  // free orphaned buffers, erase the map slot. Typically called from
  // EngineShard::Heartbeat.
  static void DrainPendingReads();

  enum HuffmanDomain : uint8_t {
    HUFF_KEYS = 0,
    HUFF_STRING_VALUES = 1,
    // TODO: add more domains.
  };

  static bool InitHuffmanThreadLocal(HuffmanDomain domain, std::string_view hufftable);
  static MemoryResource* memory_resource();  // thread-local.

  template <typename T, typename... Args> static T* AllocateMR(Args&&... args) {
    void* ptr = memory_resource()->allocate(sizeof(T), alignof(T));
    if constexpr (std::is_constructible_v<T, decltype(memory_resource())> && sizeof...(args) == 0)
      return new (ptr) T{memory_resource()};
    else
      return new (ptr) T{std::forward<Args>(args)...};
  }

  template <typename T> static void DeleteMR(void* ptr) {
    T* t = (T*)ptr;
    t->~T();
    memory_resource()->deallocate(ptr, sizeof(T), alignof(T));
  }

  // Return raw (non-decoded) string as two views. First is guaranteed to be non-empty.
  // Precondition: the object is a non-inline string.
  std::array<std::string_view, 2> GetRawString() const;

  StrEncoding GetStrEncoding() const {
    return StrEncoding{encoding_, is_key_};
  }

  bool HasAllocated() const;

  bool TagAllowsEmptyValue() const;

  uint8_t Tag() const {
    return taglen_;
  }

 private:
  // Returns a string_view corresponding to the serialized encoded blob.
  // If opt_dest is provided, it may be used to decode directly into the destination buffer.
  std::string_view GetEncodedBlob(StrEncoding str_encoding, char* opt_dest) const;

 protected:
  void EncodeString(std::string_view str);

  // Requires: HasAllocated() - true.
  void Free();

  bool CmpEncoded(std::string_view sv) const;
  bool CmpNonInline(std::string_view sv) const;

  void SetMeta(uint8_t taglen, uint8_t mask = 0) {
    if (HasAllocated()) {
      Free();
    } else {
      memset(u_.inline_str, 0, kInlineLen);
    }
    taglen_ = taglen;
    mask_ = mask;
  }

  struct ExternalPtr {
    uint32_t serialized_size;
    // page_offset only needs 12 bits (0..4095). We use the remaining 4 bits of the 16-bit
    // container to store flag bits, freeing up a full byte that we redirect to header_bytes.
    uint16_t page_offset : 12;  // 0 for multi-page blobs. != 0 for small blobs.
    uint16_t is_cool : 1;
    uint16_t representation : 2;  // See ExternalRep
    uint16_t is_reserved : 1;
    // For HUFFMAN_ENC strings, holds the first 2 bytes of the encoded blob, which encode
    // the huffman delta header (little-endian) used to recover decoded length. For other
    // encodings, only header_bytes[0] is meaningful (cached first byte).
    uint8_t header_bytes[2];

    // We do not have enough space in the common area to store page_index together with
    // cool_record pointer. Therefore, we moved this field into TieredCoolRecord itself.
    struct Offload {
      uint32_t page_index;
      uint32_t reserved;
    };

    union {
      Offload offload;
      tiering::TieredCoolRecord* cool_record;
    };
  } __attribute__((packed));
  static_assert(sizeof(ExternalPtr) == 16);

  struct SdsTtlString {
    char* sds_ptr;    // SDS string (length via sdslen)
    uint64_t exp_ms;  // absolute expiry time in ms

    std::string_view view() const;
  } __attribute__((packed));

  struct JsonConsT {
    JsonType* json_ptr;
    size_t bytes_used;

    bool DefragIfNeeded(PageUsage* page_usage);
  };

  struct FlatJsonT {
    uint32_t json_len;
    uint8_t* flat_ptr;

    bool DefragIfNeeded(PageUsage* page_usage);
  };

  struct JsonWrapper {
    union {
      JsonConsT cons;
      FlatJsonT flat;
    };

    bool DefragIfNeeded(PageUsage* page_usage);
  };

  // Union of different representations
  union U {
    char inline_str[kInlineLen];

    SmallString small_str;
    detail::RobjWrapper r_obj;
    detail::LargeString large_str;

    // using 'packed' to reduce alignment of U to 1.
    JsonWrapper json_obj __attribute__((packed));
    SBF* sbf __attribute__((packed));
    TOPK* topk __attribute__((packed));
    CMS* cms __attribute__((packed));
    CuckooFilter* cuckoo_filter __attribute__((packed));
    int64_t ival __attribute__((packed));
    ExternalPtr ext_ptr;
    SdsTtlString sds_ttl;

    U() : r_obj() {
    }
  } u_;

  static_assert(sizeof(u_) == 16);

  union {
    uint8_t mask_ = 0;
    struct {
      uint8_t unused : 2;
      uint8_t mc_flag : 1;  // Marks keys that have memcache flags assigned.

      // IO_PENDING is set when the tiered storage has issued an i/o request to save the value.
      // It is cleared when the io request finishes or is cancelled.
      uint8_t io_pending : 1;
      uint8_t sticky : 1;

      // TOUCHED used to determin which items are hot/cold.
      // by checking if the item was touched from the last time we
      // reached this item while travering the database to set items as cold.
      // https://junchengyang.com/publication/nsdi24-SIEVE.pdf
      uint8_t touched : 1;  // used to mark keys that were accessed.

      uint8_t omit_defrag : 1;  // mark object to skip defragmentation.
    } mask_bits_;
  };

  // TODO: use c++20 bitfield initializers
  const bool is_key_ : 1;
  uint8_t taglen_ : 5;    // Either length of inline string or tag of type
  uint8_t encoding_ : 2;  // Encoding of string values
};

struct CompactKey : public CompactObj {
  CompactKey() : CompactObj(true) {
  }

  explicit CompactKey(std::string_view str) : CompactObj{str, true} {
  }

  bool HasExpire() const {
    return taglen_ == SDS_TTL_TAG;
  }

  // Embed expire time directly in the key by converting to SDS_TTL_TAG.
  void SetExpireTime(uint64_t abs_ms);

  // Remove embedded expire time and convert back to optimal string form.
  bool ClearExpireTime();

  // Read the embedded expire time.
  // Returns 0 if there is no embedded expire time, otherwise
  // returns the absolute expire time in ms.
  uint64_t GetExpireTime() const;

  CompactKey& operator=(std::string_view sv) noexcept {
    SetString(sv);
    return *this;
  }

  bool operator==(std::string_view sl) const;

  bool operator!=(std::string_view sl) const {
    return !(*this == sl);
  }

  friend bool operator==(std::string_view sl, const CompactKey& o) {
    return o.operator==(sl);
  }
};

inline bool CompactKey::operator==(std::string_view sv) const {
  if (encoding_)
    return CmpEncoded(sv);

  if (IsInline()) {
    return std::string_view{u_.inline_str, taglen_} == sv;
  }
  return CmpNonInline(sv);
}

struct CompactValue : public CompactObj {
  CompactValue() : CompactObj(false) {
  }

  explicit CompactValue(std::string_view str) : CompactObj{str, false} {
  }
};

std::string_view ObjTypeToString(CompactObjType type);

// Returns kInvalidCompactObjType if sv is not a valid type.
CompactObjType ObjTypeFromString(std::string_view sv);

stream* streamNew();
void freeStream(stream* s);

}  // namespace dfly
