/**
 *  @file       index_dense.hpp
 *  @author     Ash Vardanian
 *  @brief      Single-header Vector Search engine for equi-dimensional dense vectors.
 *  @date       July 26, 2023
 */
#pragma once
#include <stdlib.h>  // `aligned_alloc`

#include "core/search/usearch/index_plugins.hpp"
#include "index.hpp"

#if defined(USEARCH_DEFINED_CPP17)
#include <shared_mutex>  // `std::shared_mutex`
#endif

#include "util/fibers/synchronization.h"

namespace unum {
namespace usearch {

template <typename, typename> class index_dense_gt;

/**
 *  @brief  The "magic" sequence helps infer the type of the file.
 *          USearch indexes start with the "usearch" string.
 */
constexpr char const* default_magic() {
  return "usearch";
}

using index_dense_head_buffer_t = byte_t[64];

static_assert(sizeof(index_dense_head_buffer_t) == 64, "File header should be exactly 64 bytes");

/**
 *  @brief  Serialized binary representations of the USearch index start with metadata.
 *          Metadata is parsed into a `index_dense_head_t`, containing the USearch package version,
 *          and the properties of the index.
 *
 *  It uses: 13 bytes for file versioning, 22 bytes for structural information = 35 bytes.
 *  The following 24 bytes contain binary size of the graph, of the vectors, and the checksum,
 *  leaving 5 bytes at the end vacant.
 */
struct index_dense_head_t {
  // Versioning:
  using magic_t = char[7];
  using version_t = std::uint16_t;

  // Versioning: 7 + 2 * 3 = 13 bytes
  char const* magic;
  misaligned_ref_gt<version_t> version_major;
  misaligned_ref_gt<version_t> version_minor;
  misaligned_ref_gt<version_t> version_patch;

  // Structural: 4 * 3 = 12 bytes
  misaligned_ref_gt<metric_kind_t> kind_metric;
  misaligned_ref_gt<scalar_kind_t> kind_scalar;
  misaligned_ref_gt<scalar_kind_t> kind_key;
  misaligned_ref_gt<scalar_kind_t> kind_compressed_slot;

  // Population: 8 * 3 = 24 bytes
  misaligned_ref_gt<std::uint64_t> count_present;
  misaligned_ref_gt<std::uint64_t> count_deleted;
  misaligned_ref_gt<std::uint64_t> dimensions;
  misaligned_ref_gt<bool> multi;

  index_dense_head_t(byte_t* ptr) noexcept
      : magic((char const*)exchange(ptr, ptr + sizeof(magic_t))),          //
        version_major(exchange(ptr, ptr + sizeof(version_t))),             //
        version_minor(exchange(ptr, ptr + sizeof(version_t))),             //
        version_patch(exchange(ptr, ptr + sizeof(version_t))),             //
        kind_metric(exchange(ptr, ptr + sizeof(metric_kind_t))),           //
        kind_scalar(exchange(ptr, ptr + sizeof(scalar_kind_t))),           //
        kind_key(exchange(ptr, ptr + sizeof(scalar_kind_t))),              //
        kind_compressed_slot(exchange(ptr, ptr + sizeof(scalar_kind_t))),  //
        count_present(exchange(ptr, ptr + sizeof(std::uint64_t))),         //
        count_deleted(exchange(ptr, ptr + sizeof(std::uint64_t))),         //
        dimensions(exchange(ptr, ptr + sizeof(std::uint64_t))),            //
        multi(exchange(ptr, ptr + sizeof(bool))) {
  }
};

struct index_dense_head_result_t {
  index_dense_head_buffer_t buffer;
  index_dense_head_t head;
  error_t error;

  explicit operator bool() const noexcept {
    return !error;
  }
  index_dense_head_result_t failed(error_t message) noexcept {
    error = std::move(message);
    return std::move(*this);
  }
};

/**
 *  @brief  Configuration settings for the construction of dense
 *          equidimensional vector indexes.
 *
 *  Unlike the underlying `index_gt` class, incorporates the
 *  `::expansion_add` and `::expansion_search` parameters passed
 *  separately for the lower-level engine.
 */
struct index_dense_config_t : public index_config_t {
  std::size_t expansion_add = default_expansion_add();
  std::size_t expansion_search = default_expansion_search();

  /**
   *  @brief  Excludes vectors from the serialized file.
   *          This is handy when you want to store the vectors in a separate file.
   *
   *  ! For advanced users only.
   */
  bool exclude_vectors = false;

  /**
   *  @brief  Allows you to store multiple vectors per key.
   *          This is handy when a large document is chunked into many parts.
   *
   *  ! May degrade the performance of iterators.
   */
  bool multi = false;

  /**
   *  @brief  Allows you to reduce RAM consumption by avoiding
   *          reverse-indexing keys-to-vectors, and only keeping
   *          the vectors-to-keys mappings.
   *
   *  ! This configuration parameter doesn't affect the serialized file,
   *  ! and is not preserved between runs. Makes sense for smaller vectors
   *  ! that fit in a couple of cache lines.
   *
   *  The trade-off is that some methods won't be available, like `get`, `rename`,
   *  and `remove`. The basic functionality, like `add` and `search` will work as
   *  expected even with `enable_key_lookups = false`.
   *
   *  If both `!multi && !enable_key_lookups`, the "duplicate entry" checks won't
   *  be performed and no errors will be raised.
   */
  bool enable_key_lookups = true;

  inline index_dense_config_t(index_config_t base) noexcept : index_config_t(base) {
  }

  inline index_dense_config_t(std::size_t c = 0, std::size_t ea = 0, std::size_t es = 0) noexcept
      : index_config_t(c), expansion_add(ea), expansion_search(es) {
  }

  /**
   *  @brief  Validates the configuration settings, updating them in-place.
   *  @return Error message, if any.
   */
  inline error_t validate() noexcept {
    error_t error = index_config_t::validate();
    if (error)
      return error;
    if (expansion_add == 0)
      expansion_add = default_expansion_add();
    if (expansion_search == 0)
      expansion_search = default_expansion_search();
    return {};
  }
};

struct index_dense_clustering_config_t {
  std::size_t min_clusters = 0;
  std::size_t max_clusters = 0;
  enum mode_t {
    merge_smallest_k,
    merge_closest_k,
  } mode = merge_smallest_k;
};

struct index_dense_serialization_config_t {
  bool exclude_vectors = false;
  bool use_64_bit_dimensions = false;
};

struct index_dense_copy_config_t : public index_copy_config_t {
  bool force_vector_copy = true;

  index_dense_copy_config_t() = default;
  index_dense_copy_config_t(index_copy_config_t base) noexcept : index_copy_config_t(base) {
  }
};

struct index_dense_metadata_result_t {
  index_dense_serialization_config_t config;
  index_dense_head_buffer_t head_buffer;
  index_dense_head_t head;
  error_t error;

  explicit operator bool() const noexcept {
    return !error;
  }
  index_dense_metadata_result_t failed(error_t message) noexcept {
    error = std::move(message);
    return std::move(*this);
  }

  index_dense_metadata_result_t() noexcept : config(), head_buffer(), head(head_buffer), error() {
  }

  index_dense_metadata_result_t(index_dense_metadata_result_t&& other) noexcept
      : config(), head_buffer(), head(head_buffer), error(std::move(other.error)) {
    std::memcpy(&config, &other.config, sizeof(other.config));
    std::memcpy(&head_buffer, &other.head_buffer, sizeof(other.head_buffer));
  }

  index_dense_metadata_result_t& operator=(index_dense_metadata_result_t&& other) noexcept {
    std::memcpy(&config, &other.config, sizeof(other.config));
    std::memcpy(&head_buffer, &other.head_buffer, sizeof(other.head_buffer));
    error = std::move(other.error);
    return *this;
  }
};

/**
 *  @brief  Fixes serialized scalar-kind codes for pre-v2.10 versions, until we can upgrade to v3.
 *          The old enum `scalar_kind_t` is defined without explicit constants from 0.
 */
inline scalar_kind_t convert_pre_2_10_scalar_kind(scalar_kind_t scalar_kind) noexcept {
  switch (static_cast<std::underlying_type<scalar_kind_t>::type>(scalar_kind)) {
    case 0:
      return scalar_kind_t::unknown_k;
    case 1:
      return scalar_kind_t::b1x8_k;
    case 2:
      return scalar_kind_t::u40_k;
    case 3:
      return scalar_kind_t::uuid_k;
    case 4:
      return scalar_kind_t::f64_k;
    case 5:
      return scalar_kind_t::f32_k;
    case 6:
      return scalar_kind_t::f16_k;
    case 7:
      return scalar_kind_t::f8_k;
    case 8:
      return scalar_kind_t::u64_k;
    case 9:
      return scalar_kind_t::u32_k;
    case 10:
      return scalar_kind_t::u8_k;
    case 11:
      return scalar_kind_t::i64_k;
    case 12:
      return scalar_kind_t::i32_k;
    case 13:
      return scalar_kind_t::i16_k;
    case 14:
      return scalar_kind_t::i8_k;
    default:
      return scalar_kind;
  }
}

/**
 *  @brief  Fixes the metadata for pre-v2.10 versions, until we can upgrade to v3.
 *          Originates from: https://github.com/unum-cloud/usearch/issues/423
 */
inline void fix_pre_2_10_metadata(index_dense_head_t& head) {
  if (head.version_major == 2 && head.version_minor < 10) {
    head.kind_scalar = convert_pre_2_10_scalar_kind(head.kind_scalar);
    head.kind_key = convert_pre_2_10_scalar_kind(head.kind_key);
    head.kind_compressed_slot = convert_pre_2_10_scalar_kind(head.kind_compressed_slot);
    head.version_minor = 10;
    head.version_patch = 0;
  }
}

/**
 *  @brief  Extracts metadata from a pre-constructed index on disk,
 *          without loading it or mapping the whole binary file.
 */
inline index_dense_metadata_result_t index_dense_metadata_from_path(
    char const* file_path) noexcept {
  index_dense_metadata_result_t result;
  std::unique_ptr<std::FILE, int (*)(std::FILE*)> file(std::fopen(file_path, "rb"), &std::fclose);
  if (!file)
    return result.failed(std::strerror(errno));

  // Read the header
  std::size_t read =
      std::fread(result.head_buffer, sizeof(index_dense_head_buffer_t), 1, file.get());
  if (!read)
    return result.failed(std::feof(file.get()) ? "End of file reached!" : std::strerror(errno));

  // Check if the file immediately starts with the index, instead of vectors
  result.config.exclude_vectors = true;
  if (std::memcmp(result.head_buffer, default_magic(), std::strlen(default_magic())) == 0) {
    fix_pre_2_10_metadata(result.head);
    return result;
  }

  if (std::fseek(file.get(), 0L, SEEK_END) != 0)
    return result.failed("Can't infer file size");

  // Check if it starts with 32-bit
  std::size_t const file_size = std::ftell(file.get());

  std::uint32_t dimensions_u32[2]{0};
  std::memcpy(dimensions_u32, result.head_buffer, sizeof(dimensions_u32));
  std::size_t offset_if_u32 =
      std::size_t(dimensions_u32[0]) * dimensions_u32[1] + sizeof(dimensions_u32);

  std::uint64_t dimensions_u64[2]{0};
  std::memcpy(dimensions_u64, result.head_buffer, sizeof(dimensions_u64));
  std::size_t offset_if_u64 =
      std::size_t(dimensions_u64[0]) * dimensions_u64[1] + sizeof(dimensions_u64);

  // Check if it starts with 32-bit
  if (offset_if_u32 + sizeof(index_dense_head_buffer_t) < file_size) {
    if (std::fseek(file.get(), static_cast<long>(offset_if_u32), SEEK_SET) != 0)
      return result.failed(std::strerror(errno));
    read = std::fread(result.head_buffer, sizeof(index_dense_head_buffer_t), 1, file.get());
    if (!read)
      return result.failed(std::feof(file.get()) ? "End of file reached!" : std::strerror(errno));

    result.config.exclude_vectors = false;
    result.config.use_64_bit_dimensions = false;
    if (std::memcmp(result.head_buffer, default_magic(), std::strlen(default_magic())) == 0) {
      fix_pre_2_10_metadata(result.head);
      return result;
    }
  }

  // Check if it starts with 64-bit
  if (offset_if_u64 + sizeof(index_dense_head_buffer_t) < file_size) {
    if (std::fseek(file.get(), static_cast<long>(offset_if_u64), SEEK_SET) != 0)
      return result.failed(std::strerror(errno));
    read = std::fread(result.head_buffer, sizeof(index_dense_head_buffer_t), 1, file.get());
    if (!read)
      return result.failed(std::feof(file.get()) ? "End of file reached!" : std::strerror(errno));

    // Check if it starts with 64-bit
    result.config.exclude_vectors = false;
    result.config.use_64_bit_dimensions = true;
    if (std::memcmp(result.head_buffer, default_magic(), std::strlen(default_magic())) == 0) {
      fix_pre_2_10_metadata(result.head);
      return result;
    }
  }

  return result.failed("Not a dense USearch index!");
}

/**
 *  @brief  Extracts metadata from a pre-constructed index serialized into an in-memory buffer.
 */
inline index_dense_metadata_result_t index_dense_metadata_from_buffer(
    memory_mapped_file_t const& file, std::size_t offset = 0) noexcept {
  index_dense_metadata_result_t result;

  // Read the header
  if (offset + sizeof(index_dense_head_buffer_t) >= file.size())
    return result.failed("End of file reached!");

  byte_t const* file_data = file.data() + offset;
  std::size_t const file_size = file.size() - offset;
  std::memcpy(&result.head_buffer, file_data, sizeof(index_dense_head_buffer_t));

  // Check if the file immediately starts with the index, instead of vectors
  result.config.exclude_vectors = true;
  if (std::memcmp(result.head_buffer, default_magic(), std::strlen(default_magic())) == 0)
    return result;

  // Check if it starts with 32-bit
  std::uint32_t dimensions_u32[2]{0};
  std::memcpy(dimensions_u32, result.head_buffer, sizeof(dimensions_u32));
  std::size_t offset_if_u32 =
      std::size_t(dimensions_u32[0]) * dimensions_u32[1] + sizeof(dimensions_u32);

  std::uint64_t dimensions_u64[2]{0};
  std::memcpy(dimensions_u64, result.head_buffer, sizeof(dimensions_u64));
  std::size_t offset_if_u64 =
      std::size_t(dimensions_u64[0]) * dimensions_u64[1] + sizeof(dimensions_u64);

  // Check if it starts with 32-bit
  if (offset_if_u32 + sizeof(index_dense_head_buffer_t) < file_size) {
    std::memcpy(&result.head_buffer, file_data + offset_if_u32, sizeof(index_dense_head_buffer_t));
    result.config.exclude_vectors = false;
    result.config.use_64_bit_dimensions = false;
    if (std::memcmp(result.head_buffer, default_magic(), std::strlen(default_magic())) == 0)
      return result;
  }

  // Check if it starts with 64-bit
  if (offset_if_u64 + sizeof(index_dense_head_buffer_t) < file_size) {
    std::memcpy(&result.head_buffer, file_data + offset_if_u64, sizeof(index_dense_head_buffer_t));
    result.config.exclude_vectors = false;
    result.config.use_64_bit_dimensions = true;
    if (std::memcmp(result.head_buffer, default_magic(), std::strlen(default_magic())) == 0)
      return result;
  }

  return result.failed("Not a dense USearch index!");
}

/**
 *  @brief  Oversimplified type-punned index for equidimensional vectors
 *          with automatic @b down-casting, hardware-specific @b SIMD metrics,
 *          and ability to @b remove existing vectors, common in Semantic Caching
 *          applications.
 *
 *  @section Serialization
 *
 *  The serialized binary form of `index_dense_gt` is made up of three parts:
 *      1. Binary matrix, aka the `.bbin` part,
 *      2. Metadata about used metrics, number of used vs free slots,
 *      3. The HNSW index in a binary form.
 *  The first (1.) generally starts with 2 integers - number of rows (vectors) and @b single-byte
 * columns. The second (2.) starts with @b "usearch"-magic-string, used to infer the file type on
 * open. The third (3.) is implemented by the underlying `index_gt` class.
 */
template <typename key_at = default_key_t, typename compressed_slot_at = default_slot_t>  //
class index_dense_gt {
 public:
  using vector_key_t = key_at;
  using key_t = vector_key_t;
  using compressed_slot_t = compressed_slot_at;
  using distance_t = distance_punned_t;
  using metric_t = metric_punned_t;

  using member_ref_t = member_ref_gt<vector_key_t>;
  using member_cref_t = member_cref_gt<vector_key_t>;

  using head_t = index_dense_head_t;
  using head_buffer_t = index_dense_head_buffer_t;
  using head_result_t = index_dense_head_result_t;

  using serialization_config_t = index_dense_serialization_config_t;

  using dynamic_allocator_t = aligned_allocator_gt<byte_t, 64>;
  using tape_allocator_t = memory_mapping_allocator_gt<64>;

 private:
  /// @brief Punned index.
  using index_t = index_gt<                         //
      distance_t, vector_key_t, compressed_slot_t,  //
      dynamic_allocator_t, tape_allocator_t>;
  using index_allocator_t = aligned_allocator_gt<index_t, 64>;

  using member_iterator_t = typename index_t::member_iterator_t;
  using member_citerator_t = typename index_t::member_citerator_t;

  /// @brief Punned metric object.
  class metric_proxy_t {
    index_dense_gt const* index_ = nullptr;

   public:
    metric_proxy_t(index_dense_gt const& index) noexcept : index_(&index) {
    }

    inline distance_t operator()(byte_t const* a, member_cref_t b) const noexcept {
      return f(a, v(b));
    }
    inline distance_t operator()(member_cref_t a, member_cref_t b) const noexcept {
      return f(v(a), v(b));
    }

    inline distance_t operator()(byte_t const* a, member_citerator_t b) const noexcept {
      return f(a, v(b));
    }
    inline distance_t operator()(member_citerator_t a, member_citerator_t b) const noexcept {
      return f(v(a), v(b));
    }

    inline distance_t operator()(byte_t const* a, byte_t const* b) const noexcept {
      return f(a, b);
    }

    inline byte_t const* v(member_cref_t m) const noexcept {
      return index_->vectors_lookup_[get_slot(m)];
    }
    inline byte_t const* v(member_citerator_t m) const noexcept {
      return index_->vectors_lookup_[get_slot(m)];
    }
    inline distance_t f(byte_t const* a, byte_t const* b) const noexcept {
      return index_->metric_(a, b);
    }
  };

  index_dense_config_t config_;
  index_t* typed_ = nullptr;

  using cast_buffer_t = buffer_gt<byte_t, dynamic_allocator_t>;

  /// @brief  Temporary memory for every thread to store a casted vector.
  mutable cast_buffer_t cast_buffer_;
  casts_punned_t casts_;

  /// @brief An instance of a potentially stateful `metric_t` used to initialize copies and forks.
  metric_t metric_;

  using vectors_tape_allocator_t = memory_mapping_allocator_gt<8>;
  /// @brief Allocator for the copied vectors, aligned to widest double-precision scalars.
  vectors_tape_allocator_t vectors_tape_allocator_;

  using vectors_lookup_allocator_t = aligned_allocator_gt<byte_t*, 64>;
  using vectors_lookup_t = buffer_gt<byte_t*, vectors_lookup_allocator_t>;

  /// @brief For every managed `compressed_slot_t` stores a pointer to the allocated vector copy.
  mutable vectors_lookup_t vectors_lookup_;

  using available_threads_allocator_t = aligned_allocator_gt<std::size_t, 64>;
  using available_threads_t = ring_gt<std::size_t, available_threads_allocator_t>;

  /// @brief Originally forms and array of integers [0, threads], marking all as available.
  mutable available_threads_t available_threads_;

  /// @brief Mutex, controlling concurrent access to `available_threads_`.
  mutable std::mutex available_threads_mutex_;

#if defined(USEARCH_DEFINED_CPP17)
  using shared_mutex_t = util::fb2::SharedMutex;
#else
  using shared_mutex_t = unfair_shared_mutex_t;
#endif
  using shared_lock_t = shared_lock_gt<shared_mutex_t>;
  using unique_lock_t = std::unique_lock<shared_mutex_t>;

  struct key_and_slot_t {
    vector_key_t key;
    compressed_slot_t slot;

    bool any_slot() const {
      return slot == default_free_value<compressed_slot_t>();
    }
    static key_and_slot_t any_slot(vector_key_t key) {
      return {key, default_free_value<compressed_slot_t>()};
    }
  };

  struct lookup_key_hash_t {
    using is_transparent = void;
    std::size_t operator()(key_and_slot_t const& k) const noexcept {
      return hash_gt<vector_key_t>{}(k.key);
    }
    std::size_t operator()(vector_key_t const& k) const noexcept {
      return hash_gt<vector_key_t>{}(k);
    }
  };

  struct lookup_key_same_t {
    using is_transparent = void;
    bool operator()(key_and_slot_t const& a, vector_key_t const& b) const noexcept {
      return a.key == b;
    }
    bool operator()(vector_key_t const& a, key_and_slot_t const& b) const noexcept {
      return a == b.key;
    }
    bool operator()(key_and_slot_t const& a, key_and_slot_t const& b) const noexcept {
      return a.key == b.key;
    }
  };

  /// @brief Multi-Map from keys to IDs, and allocated vectors.
  flat_hash_multi_set_gt<key_and_slot_t, lookup_key_hash_t, lookup_key_same_t> slot_lookup_;

  /// @brief Mutex, controlling concurrent access to `slot_lookup_`.
  mutable shared_mutex_t slot_lookup_mutex_;

  /// @brief Ring-shaped queue of deleted entries, to be reused on future insertions.
  ring_gt<compressed_slot_t> free_keys_;

  /// @brief Mutex, controlling concurrent access to `free_keys_`.
  mutable std::mutex free_keys_mutex_;

  /// @brief A constant for the reserved key value, used to mark deleted entries.
  vector_key_t free_key_ = default_free_value<vector_key_t>();

  /// @brief Locks the thread for the duration of the operation.
  struct thread_lock_t {
    index_dense_gt const& parent;
    std::size_t thread_id = 0;
    bool engaged = false;

    ~thread_lock_t() usearch_noexcept_m {
      if (engaged)
        parent.thread_unlock_(thread_id);
    }

    thread_lock_t(thread_lock_t const&) = delete;
    thread_lock_t& operator=(thread_lock_t const&) = delete;

    thread_lock_t(index_dense_gt const& parent, std::size_t thread_id, bool engaged = true) noexcept
        : parent(parent), thread_id(thread_id), engaged(engaged) {
    }
    thread_lock_t(thread_lock_t&& other) noexcept
        : parent(other.parent), thread_id(other.thread_id), engaged(other.engaged) {
      other.engaged = false;
    }
  };

 public:
  using cluster_result_t = typename index_t::cluster_result_t;
  using add_result_t = typename index_t::add_result_t;
  using stats_t = typename index_t::stats_t;
  using match_t = typename index_t::match_t;

  /**
   *  @brief  A search result, containing the found keys and distances.
   *
   *  As the `index_dense_gt` manages the thread-pool on its own, the search result
   *  preserves the thread-lock to avoid undefined behaviors, when other threads
   *  start overwriting the results.
   */
  struct search_result_t : public index_t::search_result_t {
    inline search_result_t(index_dense_gt const& parent) noexcept
        : index_t::search_result_t(), lock_(parent, 0, false) {
    }
    search_result_t failed(error_t message) noexcept {
      this->error = std::move(message);
      return std::move(*this);
    }

   private:
    friend class index_dense_gt;
    thread_lock_t lock_;

    inline search_result_t(typename index_t::search_result_t result, thread_lock_t lock) noexcept
        : index_t::search_result_t(std::move(result)), lock_(std::move(lock)) {
    }
  };

  index_dense_gt() = default;
  index_dense_gt(index_dense_gt&& other)
      : config_(std::move(other.config_)),

        typed_(exchange(other.typed_, nullptr)),      //
        cast_buffer_(std::move(other.cast_buffer_)),  //
        casts_(std::move(other.casts_)),              //
        metric_(std::move(other.metric_)),            //

        vectors_tape_allocator_(std::move(other.vectors_tape_allocator_)),  //
        vectors_lookup_(std::move(other.vectors_lookup_)),                  //

        available_threads_(std::move(other.available_threads_)),  //
        slot_lookup_(std::move(other.slot_lookup_)),              //
        free_keys_(std::move(other.free_keys_)),                  //
        free_key_(std::move(other.free_key_)) {
  }  //

  index_dense_gt& operator=(index_dense_gt&& other) {
    swap(other);
    return *this;
  }

  /**
   *  @brief Swaps the contents of this index with another index.
   *  @param other The other index to swap with.
   */
  void swap(index_dense_gt& other) {
    std::swap(config_, other.config_);

    std::swap(typed_, other.typed_);
    std::swap(cast_buffer_, other.cast_buffer_);
    std::swap(casts_, other.casts_);
    std::swap(metric_, other.metric_);

    std::swap(vectors_tape_allocator_, other.vectors_tape_allocator_);
    std::swap(vectors_lookup_, other.vectors_lookup_);

    std::swap(available_threads_, other.available_threads_);
    std::swap(slot_lookup_, other.slot_lookup_);
    std::swap(free_keys_, other.free_keys_);
    std::swap(free_key_, other.free_key_);
  }

  ~index_dense_gt() {
    if (typed_)
      typed_->~index_t();
    index_allocator_t{}.deallocate(typed_, 1);
    typed_ = nullptr;
  }

  struct state_result_t {
    index_dense_gt index;
    error_t error;

    explicit operator bool() const noexcept {
      return !error;
    }
    state_result_t failed(error_t message) noexcept {
      error = std::move(message);
      return std::move(*this);
    }
    operator index_dense_gt&&() && {
      if (error)
        usearch_raise_runtime_error(error.what());
      return std::move(index);
    }
  };
  using copy_result_t = state_result_t;

  /**
   *  @brief Constructs an instance of ::index_dense_gt.
   *  @param[in] metric One of the provided or an @b ad-hoc metric, type-punned.
   *  @param[in] config The index configuration (optional).
   *  @param[in] free_key The key used for freed vectors (optional).
   *  @return An instance of ::index_dense_gt or error, wrapped in a `state_result_t`.
   *
   *  ! If the `metric` isn't provided in this method, it has to be set with
   *  ! the `change_metric` method before the index can be used. Alternatively,
   *  ! if you are loading an existing index, the metric will be set automatically.
   */
  static state_result_t make(            //
      metric_t metric = {},              //
      index_dense_config_t config = {},  //
      vector_key_t free_key = default_free_value<vector_key_t>()) {
    if (metric.missing())
      return state_result_t{}.failed("Metric won't be initialized!");
    error_t error = config.validate();
    if (error)
      return state_result_t{}.failed(std::move(error));
    index_t* raw = index_allocator_t{}.allocate(1);
    if (!raw)
      return state_result_t{}.failed("Failed to allocate memory for the index!");

    state_result_t result;
    index_dense_gt& index = result.index;
    index.config_ = config;
    index.free_key_ = free_key;

    // In some cases the metric is not provided, and will be set later.
    if (metric) {
      scalar_kind_t scalar_kind = metric.scalar_kind();
      index.casts_ = casts_punned_t::make(scalar_kind);
      index.metric_ = metric;
    }

    new (raw) index_t(config);
    index.typed_ = raw;
    return result;
  }

  /**
   *  @brief Constructs an instance of ::index_dense_gt from a serialized binary file.
   *  @param[in] path The path to the binary file.
   *  @param[in] view Whether to map the file into memory or load it.
   *  @return An instance of ::index_dense_gt or error, wrapped in a `state_result_t`.
   */
  static state_result_t make(char const* path, bool view = false) {
    state_result_t result;
    serialization_result_t serialization_result =
        view ? result.index.view(path) : result.index.load(path);
    if (!serialization_result)
      return result.failed(std::move(serialization_result.error));
    return result;
  }

  explicit operator bool() const {
    return typed_;
  }
  std::size_t connectivity() const {
    return typed_->connectivity();
  }
  std::size_t size() const {
    return typed_->size() - free_keys_.size();
  }
  std::size_t capacity() const {
    return typed_->capacity();
  }
  std::size_t max_level() const {
    return typed_->max_level();
  }
  index_dense_config_t const& config() const {
    return config_;
  }
  index_limits_t const& limits() const {
    return typed_->limits();
  }
  double inverse_log_connectivity() const {
    return typed_->inverse_log_connectivity();
  }
  std::size_t neighbors_base_bytes() const {
    return typed_->neighbors_base_bytes();
  }
  std::size_t neighbors_bytes() const {
    return typed_->neighbors_bytes();
  }
  bool multi() const {
    return config_.multi;
  }
  std::size_t currently_available_threads() const {
    std::unique_lock<std::mutex> available_threads_lock(available_threads_mutex_);
    return available_threads_.size();
  }

  // The metric and its properties
  metric_t const& metric() const {
    return metric_;
  }
  void change_metric(metric_t metric) {
    metric_ = std::move(metric);
  }

  scalar_kind_t scalar_kind() const {
    return metric_.scalar_kind();
  }
  metric_kind_t metric_kind() const {
    return metric_.metric_kind();
  }
  std::size_t bytes_per_vector() const {
    return metric_.bytes_per_vector();
  }
  std::size_t scalar_words() const {
    return metric_.scalar_words();
  }
  std::size_t dimensions() const {
    return metric_.dimensions();
  }

  // Fetching and changing search criteria
  std::size_t expansion_add() const {
    return config_.expansion_add;
  }
  std::size_t expansion_search() const {
    return config_.expansion_search;
  }
  void change_expansion_add(std::size_t n) {
    config_.expansion_add = n;
  }
  void change_expansion_search(std::size_t n) {
    config_.expansion_search = n;
  }

  member_citerator_t cbegin() const {
    return typed_->cbegin();
  }
  member_citerator_t cend() const {
    return typed_->cend();
  }
  member_iterator_t begin() {
    return typed_->begin();
  }
  member_iterator_t end() {
    return typed_->end();
  }

  stats_t stats() const {
    return typed_->stats();
  }
  stats_t stats(std::size_t level) const {
    return typed_->stats(level);
  }
  stats_t stats(stats_t* stats_per_level, std::size_t max_level) const {
    return typed_->stats(stats_per_level, max_level);
  }

  dynamic_allocator_t const& allocator() const {
    return typed_->dynamic_allocator();
  }
  vector_key_t const& free_key() const {
    return free_key_;
  }

  /**
   *  @brief  A relatively accurate lower bound on the amount of memory consumed by the system.
   *          In practice it's error will be below 10%.
   *
   *  @see    `serialized_length` for the length of the binary serialized representation.
   */
  std::size_t memory_usage() const {
    return                                           //
        typed_->memory_usage(0) +                    //
        typed_->tape_allocator().total_wasted() +    //
        typed_->tape_allocator().total_reserved() +  //
        vectors_tape_allocator_.total_allocated();
  }

  static constexpr std::size_t any_thread() {
    return std::numeric_limits<std::size_t>::max();
  }
  static constexpr distance_t infinite_distance() {
    return std::numeric_limits<distance_t>::max();
  }

  struct aggregated_distances_t {
    std::size_t count = 0;
    distance_t mean = infinite_distance();
    distance_t min = infinite_distance();
    distance_t max = infinite_distance();
  };

  // clang-format off
    add_result_t add(vector_key_t key, b1x8_t const* vector, std::size_t thread = any_thread(), bool copy_vector = true) { return add_(key, vector, thread, copy_vector, casts_.from.b1x8); }
    add_result_t add(vector_key_t key, i8_t const* vector, std::size_t thread = any_thread(), bool copy_vector = true) { return add_(key, vector, thread, copy_vector, casts_.from.i8); }
    add_result_t add(vector_key_t key, f16_t const* vector, std::size_t thread = any_thread(), bool copy_vector = true) { return add_(key, vector, thread, copy_vector, casts_.from.f16); }
    add_result_t add(vector_key_t key, bf16_t const* vector, std::size_t thread = any_thread(), bool copy_vector = true) { return add_(key, vector, thread, copy_vector, casts_.from.bf16); }
    add_result_t add(vector_key_t key, f32_t const* vector, std::size_t thread = any_thread(), bool copy_vector = true) { return add_(key, vector, thread, copy_vector, casts_.from.f32); }
    add_result_t add(vector_key_t key, f64_t const* vector, std::size_t thread = any_thread(), bool copy_vector = true) { return add_(key, vector, thread, copy_vector, casts_.from.f64); }

    search_result_t search(b1x8_t const* vector, std::size_t wanted, std::size_t thread = any_thread(), bool exact = false, size_t expansion = 0) const { return search_(vector, wanted, dummy_predicate_t {}, thread, exact, casts_.from.b1x8, expansion); }
    search_result_t search(i8_t const* vector, std::size_t wanted, std::size_t thread = any_thread(), bool exact = false, size_t expansion = 0) const { return search_(vector, wanted, dummy_predicate_t {}, thread, exact, casts_.from.i8, expansion); }
    search_result_t search(f16_t const* vector, std::size_t wanted, std::size_t thread = any_thread(), bool exact = false, size_t expansion = 0) const { return search_(vector, wanted, dummy_predicate_t {}, thread, exact, casts_.from.f16, expansion); }
    search_result_t search(f32_t const* vector, std::size_t wanted, std::size_t thread = any_thread(), bool exact = false, size_t expansion = 0) const { return search_(vector, wanted, dummy_predicate_t {}, thread, exact, casts_.from.f32, expansion); }
    search_result_t search(f64_t const* vector, std::size_t wanted, std::size_t thread = any_thread(), bool exact = false, size_t expansion = 0) const { return search_(vector, wanted, dummy_predicate_t {}, thread, exact, casts_.from.f64, expansion); }

    template <typename predicate_at> search_result_t filtered_search(b1x8_t const* vector, std::size_t wanted, predicate_at&& predicate, std::size_t thread = any_thread(), bool exact = false, size_t expansion = 0) const { return search_(vector, wanted, std::forward<predicate_at>(predicate), thread, exact, casts_.from.b1x8, expansion); }
    template <typename predicate_at> search_result_t filtered_search(i8_t const* vector, std::size_t wanted, predicate_at&& predicate, std::size_t thread = any_thread(), bool exact = false, size_t expansion = 0) const { return search_(vector, wanted, std::forward<predicate_at>(predicate), thread, exact, casts_.from.i8, expansion); }
    template <typename predicate_at> search_result_t filtered_search(f16_t const* vector, std::size_t wanted, predicate_at&& predicate, std::size_t thread = any_thread(), bool exact = false, size_t expansion = 0) const { return search_(vector, wanted, std::forward<predicate_at>(predicate), thread, exact, casts_.from.f16, expansion); }
    template <typename predicate_at> search_result_t filtered_search(f32_t const* vector, std::size_t wanted, predicate_at&& predicate, std::size_t thread = any_thread(), bool exact = false, size_t expansion = 0) const { return search_(vector, wanted, std::forward<predicate_at>(predicate), thread, exact, casts_.from.f32, expansion); }
    template <typename predicate_at> search_result_t filtered_search(f64_t const* vector, std::size_t wanted, predicate_at&& predicate, std::size_t thread = any_thread(), bool exact = false, size_t expansion = 0) const { return search_(vector, wanted, std::forward<predicate_at>(predicate), thread, exact, casts_.from.f64, expansion); }

    std::size_t get(vector_key_t key, b1x8_t* vector, std::size_t vectors_count = 1) const { return get_(key, vector, vectors_count, casts_.to.b1x8); }
    std::size_t get(vector_key_t key, i8_t* vector, std::size_t vectors_count = 1) const { return get_(key, vector, vectors_count, casts_.to.i8); }
    std::size_t get(vector_key_t key, f16_t* vector, std::size_t vectors_count = 1) const { return get_(key, vector, vectors_count, casts_.to.f16); }
    std::size_t get(vector_key_t key, bf16_t* vector, std::size_t vectors_count = 1) const { return get_(key, vector, vectors_count, casts_.to.bf16); }
    std::size_t get(vector_key_t key, f32_t* vector, std::size_t vectors_count = 1) const { return get_(key, vector, vectors_count, casts_.to.f32); }
    std::size_t get(vector_key_t key, f64_t* vector, std::size_t vectors_count = 1) const { return get_(key, vector, vectors_count, casts_.to.f64); }

    cluster_result_t cluster(b1x8_t const* vector, std::size_t level, std::size_t thread = any_thread()) const { return cluster_(vector, level, thread, casts_.from.b1x8); }
    cluster_result_t cluster(i8_t const* vector, std::size_t level, std::size_t thread = any_thread()) const { return cluster_(vector, level, thread, casts_.from.i8); }
    cluster_result_t cluster(f16_t const* vector, std::size_t level, std::size_t thread = any_thread()) const { return cluster_(vector, level, thread, casts_.from.f16); }
    cluster_result_t cluster(bf16_t const* vector, std::size_t level, std::size_t thread = any_thread()) const { return cluster_(vector, level, thread, casts_.from.bf16); }
    cluster_result_t cluster(f32_t const* vector, std::size_t level, std::size_t thread = any_thread()) const { return cluster_(vector, level, thread, casts_.from.f32); }
    cluster_result_t cluster(f64_t const* vector, std::size_t level, std::size_t thread = any_thread()) const { return cluster_(vector, level, thread, casts_.from.f64); }

    aggregated_distances_t distance_between(vector_key_t key, b1x8_t const* vector, std::size_t thread = any_thread()) const { return distance_between_(key, vector, thread, casts_.to.b1x8); }
    aggregated_distances_t distance_between(vector_key_t key, i8_t const* vector, std::size_t thread = any_thread()) const { return distance_between_(key, vector, thread, casts_.to.i8); }
    aggregated_distances_t distance_between(vector_key_t key, f16_t const* vector, std::size_t thread = any_thread()) const { return distance_between_(key, vector, thread, casts_.to.f16); }
    aggregated_distances_t distance_between(vector_key_t key, bf16_t const* vector, std::size_t thread = any_thread()) const { return distance_between_(key, vector, thread, casts_.to.bf16); }
    aggregated_distances_t distance_between(vector_key_t key, f32_t const* vector, std::size_t thread = any_thread()) const { return distance_between_(key, vector, thread, casts_.to.f32); }
    aggregated_distances_t distance_between(vector_key_t key, f64_t const* vector, std::size_t thread = any_thread()) const { return distance_between_(key, vector, thread, casts_.to.f64); }
  // clang-format on

  /**
   *  @brief  Computes the distance between two managed entities.
   *          If either key maps into more than one vector, will aggregate results
   *          exporting the mean, maximum, and minimum values.
   */
  aggregated_distances_t distance_between(vector_key_t a, vector_key_t b,
                                          std::size_t = any_thread()) const {
    usearch_assert_m(config().enable_key_lookups, "Key lookups are disabled!");
    shared_lock_t lock(slot_lookup_mutex_);
    aggregated_distances_t result;
    if (!multi()) {
      auto a_it = slot_lookup_.find(key_and_slot_t::any_slot(a));
      auto b_it = slot_lookup_.find(key_and_slot_t::any_slot(b));
      bool a_missing = a_it == slot_lookup_.end();
      bool b_missing = b_it == slot_lookup_.end();
      if (a_missing || b_missing)
        return result;

      key_and_slot_t a_key_and_slot = *a_it;
      byte_t const* a_vector = vectors_lookup_[a_key_and_slot.slot];
      key_and_slot_t b_key_and_slot = *b_it;
      byte_t const* b_vector = vectors_lookup_[b_key_and_slot.slot];
      distance_t a_b_distance = metric_(a_vector, b_vector);

      result.mean = result.min = result.max = a_b_distance;
      result.count = 1;
      return result;
    }

    auto a_range = slot_lookup_.equal_range(key_and_slot_t::any_slot(a));
    auto b_range = slot_lookup_.equal_range(key_and_slot_t::any_slot(b));
    bool a_missing = a_range.first == a_range.second;
    bool b_missing = b_range.first == b_range.second;
    if (a_missing || b_missing)
      return result;

    result.min = std::numeric_limits<distance_t>::max();
    result.max = std::numeric_limits<distance_t>::min();
    result.mean = 0;
    result.count = 0;

    while (a_range.first != a_range.second) {
      key_and_slot_t a_key_and_slot = *a_range.first;
      byte_t const* a_vector = vectors_lookup_[a_key_and_slot.slot];
      while (b_range.first != b_range.second) {
        key_and_slot_t b_key_and_slot = *b_range.first;
        byte_t const* b_vector = vectors_lookup_[b_key_and_slot.slot];
        distance_t a_b_distance = metric_(a_vector, b_vector);

        result.mean += a_b_distance;
        result.min = (std::min)(result.min, a_b_distance);
        result.max = (std::max)(result.max, a_b_distance);
        result.count++;

        //
        ++b_range.first;
      }
      ++a_range.first;
    }

    result.mean /= result.count;
    return result;
  }

  /**
   *  @brief  Identifies a node in a given `level`, that is the closest to the `key`.
   */
  cluster_result_t cluster(vector_key_t key, std::size_t level,
                           std::size_t thread = any_thread()) const {
    // Check if such `key` is even present.
    shared_lock_t slots_lock(slot_lookup_mutex_);
    auto key_range = slot_lookup_.equal_range(key_and_slot_t::any_slot(key));
    cluster_result_t result;
    if (key_range.first == key_range.second)
      return result.failed("Key missing!");

    index_cluster_config_t cluster_config;
    thread_lock_t lock = thread_lock_(thread);
    cluster_config.thread = lock.thread_id;
    cluster_config.expansion = config_.expansion_search;
    metric_proxy_t metric{*this};
    vector_key_t free_key_copy = free_key_;
    auto allow = [free_key_copy](member_cref_t const& member) noexcept {
      return member.key != free_key_copy;
    };

    // Find the closest cluster for any vector under that key.
    while (key_range.first != key_range.second) {
      key_and_slot_t key_and_slot = *key_range.first;
      byte_t const* vector_data = vectors_lookup_[key_and_slot.slot];
      cluster_result_t new_result =
          typed_->cluster(vector_data, level, metric, cluster_config, allow);
      if (!new_result)
        return new_result;
      if (new_result.cluster.distance < result.cluster.distance)
        result = std::move(new_result);

      ++key_range.first;
    }
    return result;
  }

  /**
   *  @brief Reserves memory for the index and the keyed lookup.
   *  @return `true` if the memory reservation was successful, `false` otherwise.
   *
   *  ! No update or search operations should be running during this operation.
   */
  bool try_reserve(index_limits_t limits) {
    // The slot lookup system will generally prefer power-of-two sizes.
    if (config_.enable_key_lookups) {
      unique_lock_t lock(slot_lookup_mutex_);
      if (!slot_lookup_.try_reserve(limits.members))
        return false;
      limits.members = slot_lookup_.capacity();
    }

    // Once the `slot_lookup_` grows, let's use its capacity as the new
    // target for the `vectors_lookup_` to synchronize allocations and
    // expensive index re-organizations.
    if (limits.members != vectors_lookup_.size()) {
      vectors_lookup_t new_vectors_lookup(limits.members);
      if (!new_vectors_lookup)
        return false;
      if (vectors_lookup_.size() > 0)
        std::memcpy(new_vectors_lookup.data(), vectors_lookup_.data(),
                    vectors_lookup_.size() * sizeof(byte_t*));
      vectors_lookup_ = std::move(new_vectors_lookup);
    }

    // During reserve, no insertions may be happening, so we can safely overwrite the whole
    // collection.
    std::unique_lock<std::mutex> available_threads_lock(available_threads_mutex_);
    available_threads_.clear();
    if (!available_threads_.reserve(limits.threads()))
      return false;
    for (std::size_t i = 0; i < limits.threads(); i++)
      available_threads_.push(i);

    // Allocate a buffer for the casted vectors.
    cast_buffer_t cast_buffer(limits.threads() * metric_.bytes_per_vector());
    if (!cast_buffer)
      return false;
    cast_buffer_ = std::move(cast_buffer);

    return typed_->reserve(limits);
  }

  void reserve(index_limits_t limits) {
    if (!try_reserve(limits))
      usearch_raise_runtime_error("failed to reserve memory");
  }

  /**
   *  @brief Erases all the vectors from the index.
   *
   *  Will change `size()` to zero, but will keep the same `capacity()`.
   *  Will keep the number of available threads/contexts the same as it was.
   */
  void clear() {
    unique_lock_t lookup_lock(slot_lookup_mutex_);

    std::unique_lock<std::mutex> free_lock(free_keys_mutex_);
    typed_->clear();
    slot_lookup_.clear();
    vectors_lookup_.reset();
    free_keys_.clear();
    vectors_tape_allocator_.reset();
  }

  /**
   *  @brief Erases all members from index, closing files, and returning RAM to OS.
   *
   *  Will change both `size()` and `capacity()` to zero.
   *  Will deallocate all threads/contexts.
   *  If the index is memory-mapped - releases the mapping and the descriptor.
   */
  void reset() {
    unique_lock_t lookup_lock(slot_lookup_mutex_);
    std::unique_lock<std::mutex> free_lock(free_keys_mutex_);
    std::unique_lock<std::mutex> available_threads_lock(available_threads_mutex_);

    if (typed_)
      typed_->reset();
    slot_lookup_.clear();
    vectors_lookup_.reset();
    free_keys_.clear();
    vectors_tape_allocator_.reset();
    available_threads_.reset();
  }

  /**
   *  @brief  Saves serialized binary index representation to a stream.
   */
  template <typename output_callback_at, typename progress_at = dummy_progress_t>
  serialization_result_t save_to_stream(output_callback_at&& output,         //
                                        serialization_config_t config = {},  //
                                        progress_at&& progress = {}) const {
    serialization_result_t result;
    std::uint64_t matrix_rows = 0;
    std::uint64_t matrix_cols = 0;

    // We may not want to put the vectors into the same file
    if (!config.exclude_vectors) {
      // Save the matrix size
      if (!config.use_64_bit_dimensions) {
        std::uint32_t dimensions[2];
        dimensions[0] = static_cast<std::uint32_t>(typed_->size());
        dimensions[1] = static_cast<std::uint32_t>(metric_.bytes_per_vector());
        if (!output(&dimensions, sizeof(dimensions)))
          return result.failed("Failed to serialize into stream");
        matrix_rows = dimensions[0];
        matrix_cols = dimensions[1];
      } else {
        std::uint64_t dimensions[2];
        dimensions[0] = static_cast<std::uint64_t>(typed_->size());
        dimensions[1] = static_cast<std::uint64_t>(metric_.bytes_per_vector());
        if (!output(&dimensions, sizeof(dimensions)))
          return result.failed("Failed to serialize into stream");
        matrix_rows = dimensions[0];
        matrix_cols = dimensions[1];
      }

      // Dump the vectors one after another
      for (std::uint64_t i = 0; i != matrix_rows; ++i) {
        byte_t* vector = vectors_lookup_[i];
        if (!output(vector, matrix_cols))
          return result.failed("Failed to serialize into stream");
      }
    }

    // Augment metadata
    {
      index_dense_head_buffer_t buffer;
      std::memset(buffer, 0, sizeof(buffer));
      index_dense_head_t head{buffer};
      std::memcpy(buffer, default_magic(), std::strlen(default_magic()));

      // Describe software version
      using version_t = index_dense_head_t::version_t;
      head.version_major = static_cast<version_t>(USEARCH_VERSION_MAJOR);
      head.version_minor = static_cast<version_t>(USEARCH_VERSION_MINOR);
      head.version_patch = static_cast<version_t>(USEARCH_VERSION_PATCH);

      // Describes types used
      head.kind_metric = metric_.metric_kind();
      head.kind_scalar = metric_.scalar_kind();
      head.kind_key = unum::usearch::scalar_kind<vector_key_t>();
      head.kind_compressed_slot = unum::usearch::scalar_kind<compressed_slot_t>();

      head.count_present = size();
      head.count_deleted = typed_->size() - size();
      head.dimensions = dimensions();
      head.multi = multi();

      if (!output(&buffer, sizeof(buffer)))
        return result.failed("Failed to serialize into stream");
    }

    // Save the actual proximity graph
    return typed_->save_to_stream(std::forward<output_callback_at>(output),
                                  std::forward<progress_at>(progress));
  }

  /**
   *  @brief  Estimate the binary length (in bytes) of the serialized index.
   */
  std::size_t serialized_length(serialization_config_t config = {}) const {
    std::size_t dimensions_length = 0;
    std::size_t matrix_length = 0;
    if (!config.exclude_vectors) {
      dimensions_length =
          config.use_64_bit_dimensions ? sizeof(std::uint64_t) * 2 : sizeof(std::uint32_t) * 2;
      matrix_length = typed_->size() * metric_.bytes_per_vector();
    }
    return dimensions_length + matrix_length + sizeof(index_dense_head_buffer_t) +
           typed_->serialized_length();
  }

  /**
   *  @brief Parses the index from file to RAM.
   *  @param[in] input The input stream to read from.
   *  @param[in] config Configuration parameters for imports.
   *  @param[in] progress Callback to report the execution progress.
   *  @return Outcome descriptor explicitly convertible to boolean.
   */
  template <typename input_callback_at, typename progress_at = dummy_progress_t>
  serialization_result_t load_from_stream(input_callback_at&& input,           //
                                          serialization_config_t config = {},  //
                                          progress_at&& progress = {}) {
    // Discard all previous memory allocations of `vectors_tape_allocator_`
    index_limits_t old_limits = typed_ ? typed_->limits() : index_limits_t{};
    reset();

    // Infer the new index size
    serialization_result_t result;
    std::uint64_t matrix_rows = 0;
    std::uint64_t matrix_cols = 0;

    // We may not want to load the vectors from the same file, or allow attaching them afterwards
    if (!config.exclude_vectors) {
      // Save the matrix size
      if (!config.use_64_bit_dimensions) {
        std::uint32_t dimensions[2];
        if (!input(&dimensions, sizeof(dimensions)))
          return result.failed("Failed to read 32-bit dimensions of the matrix");
        matrix_rows = dimensions[0];
        matrix_cols = dimensions[1];
      } else {
        std::uint64_t dimensions[2];
        if (!input(&dimensions, sizeof(dimensions)))
          return result.failed("Failed to read 64-bit dimensions of the matrix");
        matrix_rows = dimensions[0];
        matrix_cols = dimensions[1];
      }
      // Load the vectors one after another
      vectors_lookup_ = vectors_lookup_t(matrix_rows);
      if (!vectors_lookup_)
        return result.failed("Failed to allocate memory to address vectors");
      for (std::uint64_t slot = 0; slot != matrix_rows; ++slot) {
        byte_t* vector = vectors_tape_allocator_.allocate(matrix_cols);
        if (!input(vector, matrix_cols))
          return result.failed("Failed to read vectors");
        vectors_lookup_[slot] = vector;
      }
    }

    // Load metadata and choose the right metric
    {
      index_dense_head_buffer_t buffer;
      if (!input(buffer, sizeof(buffer)))
        return result.failed("Failed to read the index ");

      index_dense_head_t head{buffer};
      if (std::memcmp(buffer, default_magic(), std::strlen(default_magic())) != 0)
        return result.failed("Magic header mismatch - the file isn't an index");

      // fix pre-2.10 headers
      fix_pre_2_10_metadata(head);

      // Validate the software version
      if (head.version_major != USEARCH_VERSION_MAJOR)
        return result.failed("File format may be different, please rebuild");

      // Check the types used
      if (head.kind_key != unum::usearch::scalar_kind<vector_key_t>())
        return result.failed("Key type doesn't match, consider rebuilding");
      if (head.kind_compressed_slot != unum::usearch::scalar_kind<compressed_slot_t>())
        return result.failed("Slot type doesn't match, consider rebuilding");

      config_.multi = head.multi;
      metric_ = metric_t::builtin(head.dimensions, head.kind_metric, head.kind_scalar);
      // available_threads_.size() will be updated to old_limits.threads() later in this
      // method, so use that as the number of threads to prepare for.
      cast_buffer_ = cast_buffer_t(old_limits.threads() * metric_.bytes_per_vector());
      if (!cast_buffer_)
        return result.failed("Failed to allocate memory for the casts");
      casts_ = casts_punned_t::make(head.kind_scalar);
    }

    // Pull the actual proximity graph
    if (!typed_) {
      index_t* raw = index_allocator_t{}.allocate(1);
      if (!raw)
        return result.failed("Failed to allocate memory for the index");
      new (raw) index_t(config_);
      typed_ = raw;
    }
    result = typed_->load_from_stream(std::forward<input_callback_at>(input),
                                      std::forward<progress_at>(progress));
    if (!result)
      return result;
    if (typed_->size() != static_cast<std::size_t>(matrix_rows))
      return result.failed("Index size and the number of vectors doesn't match");
    old_limits.members = static_cast<std::size_t>(matrix_rows);
    if (!typed_->try_reserve(old_limits))
      return result.failed("Failed to reserve memory for the index");

    // After the index is loaded, we may have to resize the `available_threads_` to
    // match the limits of the underlying engine.
    available_threads_t available_threads;
    std::size_t max_threads = old_limits.threads();
    if (!available_threads.reserve(max_threads))
      return result.failed("Failed to allocate memory for the available threads!");
    for (std::size_t i = 0; i < max_threads; i++)
      available_threads.push(i);
    available_threads_ = std::move(available_threads);

    reindex_keys_();
    return result;
  }

  /**
   *  @brief Parses the index from file, without loading it into RAM.
   *  @param[in] file The input file to read from.
   *  @param[in] offset The offset in the file to start reading from.
   *  @param[in] config Configuration parameters for imports.
   *  @param[in] progress Callback to report the execution progress.
   *  @return Outcome descriptor explicitly convertible to boolean.
   */
  template <typename progress_at = dummy_progress_t>
  serialization_result_t view(memory_mapped_file_t file,                                   //
                              std::size_t offset = 0, serialization_config_t config = {},  //
                              progress_at&& progress = {}) {
    // Discard all previous memory allocations of `vectors_tape_allocator_`
    index_limits_t old_limits = typed_ ? typed_->limits() : index_limits_t{};
    reset();

    serialization_result_t result = file.open_if_not();
    if (!result)
      return result;

    // Infer the new index size
    std::uint64_t matrix_rows = 0;
    std::uint64_t matrix_cols = 0;
    span_punned_t vectors_buffer;

    // We may not want to fetch the vectors from the same file, or allow attaching them afterwards
    if (!config.exclude_vectors) {
      // Save the matrix size
      if (!config.use_64_bit_dimensions) {
        std::uint32_t dimensions[2];
        if (file.size() - offset < sizeof(dimensions))
          return result.failed("File is corrupted and lacks matrix dimensions");
        std::memcpy(&dimensions, file.data() + offset, sizeof(dimensions));
        matrix_rows = dimensions[0];
        matrix_cols = dimensions[1];
        offset += sizeof(dimensions);
      } else {
        std::uint64_t dimensions[2];
        if (file.size() - offset < sizeof(dimensions))
          return result.failed("File is corrupted and lacks matrix dimensions");
        std::memcpy(&dimensions, file.data() + offset, sizeof(dimensions));
        matrix_rows = dimensions[0];
        matrix_cols = dimensions[1];
        offset += sizeof(dimensions);
      }
      vectors_buffer = {file.data() + offset, static_cast<std::size_t>(matrix_rows * matrix_cols)};
      offset += vectors_buffer.size();
    }

    // Load metadata and choose the right metric
    {
      index_dense_head_buffer_t buffer;
      if (file.size() - offset < sizeof(buffer))
        return result.failed("File is corrupted and lacks a header");

      std::memcpy(buffer, file.data() + offset, sizeof(buffer));

      index_dense_head_t head{buffer};
      if (std::memcmp(buffer, default_magic(), std::strlen(default_magic())) != 0)
        return result.failed("Magic header mismatch - the file isn't an index");

      // fix pre-2.10 headers
      fix_pre_2_10_metadata(head);

      // Validate the software version
      if (head.version_major != USEARCH_VERSION_MAJOR)
        return result.failed("File format may be different, please rebuild");

      // Check the types used
      if (head.kind_key != unum::usearch::scalar_kind<vector_key_t>())
        return result.failed("Key type doesn't match, consider rebuilding");
      if (head.kind_compressed_slot != unum::usearch::scalar_kind<compressed_slot_t>())
        return result.failed("Slot type doesn't match, consider rebuilding");

      config_.multi = head.multi;
      metric_ = metric_t::builtin(head.dimensions, head.kind_metric, head.kind_scalar);
      // available_threads_.size() will be updated to old_limits.threads() later in this
      // method, so use that as the number of threads to prepare for.
      cast_buffer_ = cast_buffer_t(old_limits.threads() * metric_.bytes_per_vector());
      if (!cast_buffer_)
        return result.failed("Failed to allocate memory for the casts");
      casts_ = casts_punned_t::make(head.kind_scalar);
      offset += sizeof(buffer);
    }

    // Pull the actual proximity graph
    if (!typed_) {
      index_t* raw = index_allocator_t{}.allocate(1);
      if (!raw)
        return result.failed("Failed to allocate memory for the index");
      new (raw) index_t(config_);
      typed_ = raw;
    }
    result = typed_->view(std::move(file), offset, std::forward<progress_at>(progress));
    if (!result)
      return result;
    if (typed_->size() != static_cast<std::size_t>(matrix_rows))
      return result.failed("Index size and the number of vectors doesn't match");
    old_limits.members = static_cast<std::size_t>(matrix_rows);
    if (!typed_->try_reserve(old_limits))
      return result.failed("Failed to reserve memory for the index");

    // Address the vectors
    vectors_lookup_ = vectors_lookup_t(matrix_rows);
    if (!vectors_lookup_)
      return result.failed("Failed to allocate memory to address vectors");
    if (!config.exclude_vectors)
      for (std::uint64_t slot = 0; slot != matrix_rows; ++slot)
        vectors_lookup_[slot] = (byte_t*)vectors_buffer.data() + matrix_cols * slot;

    // After the index is loaded, we may have to resize the `available_threads_` to
    // match the limits of the underlying engine.
    available_threads_t available_threads;
    std::size_t max_threads = old_limits.threads();
    if (!available_threads.reserve(max_threads))
      return result.failed("Failed to allocate memory for the available threads!");
    for (std::size_t i = 0; i < max_threads; i++)
      available_threads.push(i);
    available_threads_ = std::move(available_threads);

    reindex_keys_();
    return result;
  }

  /**
   *  @brief Saves the index to a file.
   *  @param[in] file The output file to write to.
   *  @param[in] config Configuration parameters for exports.
   *  @param[in] progress Callback to report the execution progress.
   *  @return Outcome descriptor explicitly convertible to boolean.
   */
  template <typename progress_at = dummy_progress_t>
  serialization_result_t save(output_file_t file, serialization_config_t config = {},
                              progress_at&& progress = {}) const {
    serialization_result_t io_result = file.open_if_not();
    if (!io_result)
      return io_result;

    serialization_result_t stream_result = save_to_stream(
        [&](void const* buffer, std::size_t length) {
          io_result = file.write(buffer, length);
          return !!io_result;
        },
        config, std::forward<progress_at>(progress));

    if (!stream_result) {
      io_result.error.release();
      return stream_result;
    }
    return io_result;
  }

  /**
   *  @brief  Memory-maps the serialized binary index representation from disk,
   *          @b without copying data into RAM, and fetching it on-demand.
   */
  template <typename progress_at = dummy_progress_t>
  serialization_result_t save(memory_mapped_file_t file,           //
                              std::size_t offset = 0,              //
                              serialization_config_t config = {},  //
                              progress_at&& progress = {}) const {
    serialization_result_t io_result = file.open_if_not();
    if (!io_result)
      return io_result;

    serialization_result_t stream_result = save_to_stream(
        [&](void const* buffer, std::size_t length) {
          if (offset + length > file.size())
            return false;
          std::memcpy(file.data() + offset, buffer, length);
          offset += length;
          return true;
        },
        config, std::forward<progress_at>(progress));

    return stream_result;
  }

  /**
   *  @brief Parses the index from file to RAM.
   *  @param[in] file The input file to read from.
   *  @param[in] config Configuration parameters for imports.
   *  @param[in] progress Progress callback.
   *  @return Outcome descriptor explicitly convertible to boolean.
   */
  template <typename progress_at = dummy_progress_t>
  serialization_result_t load(input_file_t file, serialization_config_t config = {},
                              progress_at&& progress = {}) {
    serialization_result_t io_result = file.open_if_not();
    if (!io_result)
      return io_result;

    serialization_result_t stream_result = load_from_stream(
        [&](void* buffer, std::size_t length) {
          io_result = file.read(buffer, length);
          return !!io_result;
        },
        config, std::forward<progress_at>(progress));

    if (!stream_result) {
      io_result.error.release();
      return stream_result;
    }
    return io_result;
  }

  /**
   *  @brief  Memory-maps the serialized binary index representation from disk,
   *          @b without copying data into RAM, and fetching it on-demand.
   */
  template <typename progress_at = dummy_progress_t>
  serialization_result_t load(memory_mapped_file_t file,           //
                              std::size_t offset = 0,              //
                              serialization_config_t config = {},  //
                              progress_at&& progress = {}) {
    serialization_result_t io_result = file.open_if_not();
    if (!io_result)
      return io_result;

    serialization_result_t stream_result = load_from_stream(
        [&](void* buffer, std::size_t length) {
          if (offset + length > file.size())
            return false;
          std::memcpy(buffer, file.data() + offset, length);
          offset += length;
          return true;
        },
        config, std::forward<progress_at>(progress));

    return stream_result;
  }

  template <typename progress_at = dummy_progress_t>
  serialization_result_t save(char const* file_path,               //
                              serialization_config_t config = {},  //
                              progress_at&& progress = {}) const {
    return save(output_file_t(file_path), config, std::forward<progress_at>(progress));
  }

  template <typename progress_at = dummy_progress_t>
  serialization_result_t load(char const* file_path,               //
                              serialization_config_t config = {},  //
                              progress_at&& progress = {}) {
    return load(input_file_t(file_path), config, std::forward<progress_at>(progress));
  }

  /**
   *  @brief Checks if a vector with specified key is present.
   *  @return `true` if the key is present in the index, `false` otherwise.
   */
  bool contains(vector_key_t key) const {
    usearch_assert_m(config().enable_key_lookups, "Key lookups are disabled");
    shared_lock_t lock(slot_lookup_mutex_);
    return slot_lookup_.contains(key_and_slot_t::any_slot(key));
  }

  /**
   *  @brief Count the number of vectors with specified key present.
   *  @return Zero if nothing is found, a positive integer otherwise.
   */
  std::size_t count(vector_key_t key) const {
    usearch_assert_m(config().enable_key_lookups, "Key lookups are disabled");
    shared_lock_t lock(slot_lookup_mutex_);
    return slot_lookup_.count(key_and_slot_t::any_slot(key));
  }

  struct labeling_result_t {
    error_t error{};
    std::size_t completed{};

    explicit operator bool() const noexcept {
      return !error;
    }
    labeling_result_t failed(error_t message) noexcept {
      error = std::move(message);
      return std::move(*this);
    }
  };

  /**
   *  @brief Removes an entry with the specified key from the index.
   *  @param[in] key The key of the entry to remove.
   *  @return The ::labeling_result_t indicating the result of the removal operation.
   *          If the removal was successful, `result.completed` will be `true`.
   *          If the key was not found in the index, `result.completed` will be `false`.
   *          If an error occurred during the removal operation, `result.error` will contain an
   * error message.
   */
  labeling_result_t remove(vector_key_t key) {
    usearch_assert_m(config().enable_key_lookups, "Key lookups are disabled");
    labeling_result_t result;
    if (typed_->is_immutable())
      return result.failed("Can't remove from an immutable index");

    unique_lock_t lookup_lock(slot_lookup_mutex_);
    auto matching_slots = slot_lookup_.equal_range(key_and_slot_t::any_slot(key));
    if (matching_slots.first == matching_slots.second)
      return result;

    // Grow the removed entries ring, if needed
    std::size_t matching_count = std::distance(matching_slots.first, matching_slots.second);
    std::unique_lock<std::mutex> free_lock(free_keys_mutex_);
    std::size_t free_count_old = free_keys_.size();
    if (!free_keys_.reserve(free_count_old + matching_count))
      return result.failed("Can't allocate memory for a free-list");

    // A removed entry would be:
    // - present in `free_keys_`
    // - missing in the `slot_lookup_`
    // - marked in the `typed_` index with a `free_key_`
    for (auto slots_it = matching_slots.first; slots_it != matching_slots.second; ++slots_it) {
      compressed_slot_t slot = (*slots_it).slot;
      free_keys_.push(slot);
      typed_->at(slot).key = free_key_;
    }
    slot_lookup_.erase(key);
    result.completed = matching_count;
    usearch_assert_m(free_keys_.size() == free_count_old + matching_count,
                     "Free keys count mismatch");

    return result;
  }

  /**
   *  @brief Removes multiple entries with the specified keys from the index.
   *  @param[in] keys_begin The beginning of the keys range.
   *  @param[in] keys_end The ending of the keys range.
   *  @return The ::labeling_result_t indicating the result of the removal operation.
   *          `result.completed` will contain the number of keys that were successfully removed.
   *          `result.error` will contain an error message if an error occurred during the removal
   * operation.
   */
  template <typename keys_iterator_at>
  labeling_result_t remove(keys_iterator_at keys_begin, keys_iterator_at keys_end) {
    usearch_assert_m(config().enable_key_lookups, "Key lookups are disabled");

    labeling_result_t result;
    unique_lock_t lookup_lock(slot_lookup_mutex_);
    std::unique_lock<std::mutex> free_lock(free_keys_mutex_);
    // Grow the removed entries ring, if needed
    std::size_t matching_count = 0;
    for (auto keys_it = keys_begin; keys_it != keys_end; ++keys_it)
      matching_count += slot_lookup_.count(key_and_slot_t::any_slot(*keys_it));

    if (!free_keys_.reserve(free_keys_.size() + matching_count))
      return result.failed("Can't allocate memory for a free-list");

    // Remove them one-by-one
    for (auto keys_it = keys_begin; keys_it != keys_end; ++keys_it) {
      vector_key_t key = *keys_it;
      auto matching_slots = slot_lookup_.equal_range(key_and_slot_t::any_slot(key));
      // A removed entry would be:
      // - present in `free_keys_`
      // - missing in the `slot_lookup_`
      // - marked in the `typed_` index with a `free_key_`
      matching_count = 0;
      for (auto slots_it = matching_slots.first; slots_it != matching_slots.second; ++slots_it) {
        compressed_slot_t slot = (*slots_it).slot;
        free_keys_.push(slot);
        typed_->at(slot).key = free_key_;
        ++matching_count;
      }

      slot_lookup_.erase(key);
      result.completed += matching_count;
    }

    return result;
  }

  /**
   *  @brief Renames an entry with the specified key to a new key.
   *  @param[in] from The current key of the entry to rename.
   *  @param[in] to The new key to assign to the entry.
   *  @return The ::labeling_result_t indicating the result of the rename operation.
   *          If the rename was successful, `result.completed` will be `true`.
   *          If the entry with the current key was not found, `result.completed` will be `false`.
   */
  labeling_result_t rename(vector_key_t from, vector_key_t to) {
    labeling_result_t result;
    unique_lock_t lookup_lock(slot_lookup_mutex_);

    if (!multi() && slot_lookup_.contains(key_and_slot_t::any_slot(to)))
      return result.failed("Renaming impossible, the key is already in use");

    // The `from` may map to multiple entries
    while (true) {
      key_and_slot_t key_and_slot_removed;
      if (!slot_lookup_.pop_first(key_and_slot_t::any_slot(from), key_and_slot_removed))
        break;

      key_and_slot_t key_and_slot_replacing{to, key_and_slot_removed.slot};
      slot_lookup_.try_emplace(key_and_slot_replacing);  // This can't fail
      typed_->at(key_and_slot_removed.slot).key = to;
      ++result.completed;
    }

    return result;
  }

  /**
   *  @brief Exports a range of keys for the vectors present in the index.
   *  @param[out] keys Pointer to the array where the keys will be exported.
   *  @param[in] offset The number of keys to skip. Useful for pagination.
   *  @param[in] limit The maximum number of keys to export, that can fit in ::keys.
   */
  void export_keys(vector_key_t* keys, std::size_t offset, std::size_t limit) const {
    shared_lock_t lock(slot_lookup_mutex_);
    offset = (std::min)(offset, slot_lookup_.size());
    slot_lookup_.for_each([&](key_and_slot_t const& key_and_slot) {
      if (offset)
        // Skip the first `offset` entries
        --offset;
      else if (limit) {
        *keys = key_and_slot.key;
        ++keys;
        --limit;
      }
    });
  }

  /**
   *  @brief Copies the ::index_dense_gt @b with all the data in it.
   *  @param config The copy configuration (optional).
   *  @return A copy of the ::index_dense_gt instance.
   */
  copy_result_t copy(index_dense_copy_config_t config = {}) const {
    copy_result_t result = fork();
    if (!result)
      return result;

    auto typed_result = typed_->copy(config);
    if (!typed_result)
      return result.failed(std::move(typed_result.error));

    // Export the free (removed) slot numbers
    index_dense_gt& copy = result.index;
    if (!copy.free_keys_.reserve(free_keys_.size()))
      return result.failed(std::move(typed_result.error));
    for (std::size_t i = 0; i != free_keys_.size(); ++i)
      copy.free_keys_.push(free_keys_[i]);

    // Allocate buffers and move the vectors themselves
    copy.vectors_lookup_ = vectors_lookup_t(vectors_lookup_.size());
    if (!copy.vectors_lookup_)
      return result.failed("Out of memory!");
    if (!config.force_vector_copy && copy.config_.exclude_vectors) {
      std::memcpy(copy.vectors_lookup_.data(), vectors_lookup_.data(),
                  vectors_lookup_.size() * sizeof(byte_t*));
    } else {
      std::size_t slots_count = typed_result.index.size();
      for (std::size_t slot = 0; slot != slots_count; ++slot)
        copy.vectors_lookup_[slot] =
            copy.vectors_tape_allocator_.allocate(copy.metric_.bytes_per_vector());
      if (std::count(copy.vectors_lookup_.begin(), copy.vectors_lookup_.begin() + slots_count,
                     nullptr))
        return result.failed("Out of memory!");
      for (std::size_t slot = 0; slot != slots_count; ++slot)
        std::memcpy(copy.vectors_lookup_[slot], vectors_lookup_[slot], metric_.bytes_per_vector());
    }

    copy.slot_lookup_ = slot_lookup_;  // TODO: Handle out of memory
    *copy.typed_ = std::move(typed_result.index);
    return result;
  }

  /**
   *  @brief Copies the ::index_dense_gt model @b without any data.
   *  @return A similarly configured ::index_dense_gt instance.
   */
  copy_result_t fork() const {
    cast_buffer_t cast_buffer(cast_buffer_.size());
    if (!cast_buffer)
      return state_result_t{}.failed("Failed to allocate memory for the casts!");
    available_threads_t available_threads;
    std::size_t max_threads = limits().threads();
    if (!available_threads.reserve(max_threads))
      return state_result_t{}.failed("Failed to allocate memory for the available threads!");
    for (std::size_t i = 0; i < max_threads; i++)
      available_threads.push(i);
    index_t* raw = index_allocator_t{}.allocate(1);
    if (!raw)
      return state_result_t{}.failed("Failed to allocate memory for the index!");

    copy_result_t result;
    index_dense_gt& other = result.index;
    index_limits_t other_limits = limits();
    other_limits.members = 0;
    other.config_ = config_;
    other.cast_buffer_ = std::move(cast_buffer);
    other.casts_ = casts_;

    other.metric_ = metric_;
    other.available_threads_ = std::move(available_threads);
    other.free_key_ = free_key_;

    new (raw) index_t(config());
    raw->try_reserve(other_limits);
    other.typed_ = raw;
    return result;
  }

  struct compaction_result_t {
    error_t error{};
    std::size_t pruned_edges{};

    explicit operator bool() const noexcept {
      return !error;
    }
    compaction_result_t failed(error_t message) noexcept {
      error = std::move(message);
      return std::move(*this);
    }
  };

  /**
   *  @brief Performs compaction on the index, pruning links to removed entries.
   *  @param executor The executor parallel processing. Default ::dummy_executor_t single-threaded.
   *  @param progress The progress tracker instance to use. Default ::dummy_progress_t reports
   * nothing.
   *  @return The ::compaction_result_t indicating the result of the compaction operation.
   *          `result.pruned_edges` will contain the number of edges that were removed.
   *          `result.error` will contain an error message if an error occurred during the
   * compaction operation.
   */
  template <typename executor_at = dummy_executor_t, typename progress_at = dummy_progress_t>
  compaction_result_t isolate(executor_at&& executor = executor_at{},
                              progress_at&& progress = progress_at{}) {
    compaction_result_t result;
    std::atomic<std::size_t> pruned_edges;
    auto allow = [&](member_cref_t const& member) noexcept {
      bool freed = member.key == free_key_;
      pruned_edges += freed;
      return !freed;
    };
    typed_->isolate(allow, std::forward<executor_at>(executor),
                    std::forward<progress_at>(progress));
    result.pruned_edges = pruned_edges;
    return result;
  }

  class values_proxy_t {
    index_dense_gt const* index_;

   public:
    values_proxy_t(index_dense_gt const& index) noexcept : index_(&index) {
    }
    byte_t const* operator[](compressed_slot_t slot) const noexcept {
      return index_->vectors_lookup_[slot];
    }
    byte_t const* operator[](member_citerator_t it) const noexcept {
      return index_->vectors_lookup_[get_slot(it)];
    }
  };

  /**
   *  @brief Performs compaction on the index, pruning links to removed entries.
   *  @param executor The executor parallel processing. Default ::dummy_executor_t single-threaded.
   *  @param progress The progress tracker instance to use. Default ::dummy_progress_t reports
   * nothing.
   *  @return The ::compaction_result_t indicating the result of the compaction operation.
   *          `result.pruned_edges` will contain the number of edges that were removed.
   *          `result.error` will contain an error message if an error occurred during the
   * compaction operation.
   */
  template <typename executor_at = dummy_executor_t, typename progress_at = dummy_progress_t>
  compaction_result_t compact(executor_at&& executor = executor_at{},
                              progress_at&& progress = progress_at{}) {
    compaction_result_t result;

    vectors_lookup_t new_vectors_lookup(vectors_lookup_.size());
    if (!new_vectors_lookup)
      return result.failed("Out of memory!");

    vectors_tape_allocator_t new_vectors_allocator;

    auto track_slot_change = [&](vector_key_t, compressed_slot_t old_slot,
                                 compressed_slot_t new_slot) {
      byte_t* new_vector = new_vectors_allocator.allocate(metric_.bytes_per_vector());
      byte_t* old_vector = vectors_lookup_[old_slot];
      std::memcpy(new_vector, old_vector, metric_.bytes_per_vector());
      new_vectors_lookup[new_slot] = new_vector;
    };
    typed_->compact(values_proxy_t{*this}, metric_proxy_t{*this}, track_slot_change,
                    std::forward<executor_at>(executor), std::forward<progress_at>(progress));
    vectors_lookup_ = std::move(new_vectors_lookup);
    vectors_tape_allocator_ = std::move(new_vectors_allocator);
    return result;
  }

  template <                                                  //
      typename man_to_woman_at = dummy_key_to_key_mapping_t,  //
      typename woman_to_man_at = dummy_key_to_key_mapping_t,  //
      typename executor_at = dummy_executor_t,                //
      typename progress_at = dummy_progress_t                 //
      >
  join_result_t join(                                      //
      index_dense_gt const& women,                         //
      index_join_config_t config = {},                     //
      man_to_woman_at&& man_to_woman = man_to_woman_at{},  //
      woman_to_man_at&& woman_to_man = woman_to_man_at{},  //
      executor_at&& executor = executor_at{},              //
      progress_at&& progress = progress_at{}) const {
    index_dense_gt const& men = *this;
    return unum::usearch::join(                       //
        *men.typed_, *women.typed_,                   //
        values_proxy_t{men}, values_proxy_t{women},   //
        metric_proxy_t{men}, metric_proxy_t{women},   //
        config,                                       //
        std::forward<man_to_woman_at>(man_to_woman),  //
        std::forward<woman_to_man_at>(woman_to_man),  //
        std::forward<executor_at>(executor),          //
        std::forward<progress_at>(progress));
  }

  struct clustering_result_t {
    error_t error{};
    std::size_t clusters{};
    std::size_t visited_members{};
    std::size_t computed_distances{};

    explicit operator bool() const noexcept {
      return !error;
    }
    clustering_result_t failed(error_t message) noexcept {
      error = std::move(message);
      return std::move(*this);
    }
  };

  /**
   *  @brief  Implements clustering, classifying the given objects (vectors of member keys)
   *          into a given number of clusters.
   *
   *  @param[in] queries_begin Iterator pointing to the first query.
   *  @param[in] queries_end Iterator pointing to the last query.
   *  @param[in] executor Thread-pool to execute the job in parallel.
   *  @param[in] progress Callback to report the execution progress.
   *  @param[in] config Configuration parameters for clustering.
   *
   *  @param[out] cluster_keys Pointer to the array where the cluster keys will be exported.
   *  @param[out] cluster_distances Pointer to the array where the distances to those centroids will
   * be exported.
   */
  template <                                    //
      typename queries_iterator_at,             //
      typename executor_at = dummy_executor_t,  //
      typename progress_at = dummy_progress_t   //
      >
  clustering_result_t cluster(                 //
      queries_iterator_at queries_begin,       //
      queries_iterator_at queries_end,         //
      index_dense_clustering_config_t config,  //
      vector_key_t* cluster_keys,              //
      distance_t* cluster_distances,           //
      executor_at&& executor = executor_at{},  //
      progress_at&& progress = progress_at{}) {
    std::size_t const queries_count = queries_end - queries_begin;

    // Find the first level (top -> down) that has enough nodes to exceed `config.min_clusters`.
    std::size_t level = max_level();
    if (config.min_clusters) {
      for (; level > 1; --level) {
        if (stats(level).nodes > config.min_clusters)
          break;
      }
    } else
      level = 1, config.max_clusters = stats(1).nodes, config.min_clusters = 2;

    clustering_result_t result;
    if (max_level() < 2)
      return result.failed("Index too small to cluster!");

    // A structure used to track the popularity of a specific cluster
    struct cluster_t {
      vector_key_t centroid;
      vector_key_t merged_into;
      std::size_t popularity;
      byte_t* vector;
    };

    auto centroid_id = [](cluster_t const& a, cluster_t const& b) {
      return a.centroid < b.centroid;
    };
    auto higher_popularity = [](cluster_t const& a, cluster_t const& b) {
      return a.popularity > b.popularity;
    };

    std::atomic<std::size_t> visited_members(0);
    std::atomic<std::size_t> computed_distances(0);
    std::atomic<char const*> atomic_error{nullptr};

    using dynamic_allocator_traits_t = std::allocator_traits<dynamic_allocator_t>;
    using clusters_allocator_t =
        typename dynamic_allocator_traits_t::template rebind_alloc<cluster_t>;
    buffer_gt<cluster_t, clusters_allocator_t> clusters(queries_count);
    if (!clusters)
      return result.failed("Out of memory!");

  map_to_clusters:
    // Concurrently perform search until a certain depth
    executor.dynamic(queries_count, [&](std::size_t thread_idx, std::size_t query_idx) {
      auto result = cluster(queries_begin[query_idx], level, thread_idx);
      if (!result) {
        atomic_error = result.error.release();
        return false;
      }

      cluster_keys[query_idx] = result.cluster.member.key;
      cluster_distances[query_idx] = result.cluster.distance;

      // Export in case we need to refine afterwards
      clusters[query_idx].centroid = result.cluster.member.key;
      clusters[query_idx].vector = vectors_lookup_[result.cluster.member.slot];
      clusters[query_idx].merged_into = free_key();
      clusters[query_idx].popularity = 1;

      visited_members += result.visited_members;
      computed_distances += result.computed_distances;
      return true;
    });

    if (atomic_error)
      return result.failed(atomic_error.load());

    // Now once we have identified the closest clusters,
    // we can try reducing their quantity, refining
    std::sort(clusters.begin(), clusters.end(), centroid_id);

    // Transform into run-length encoding, computing the number of unique clusters
    std::size_t unique_clusters = 0;
    {
      std::size_t last_idx = 0;
      for (std::size_t current_idx = 1; current_idx != clusters.size(); ++current_idx) {
        if (clusters[last_idx].centroid == clusters[current_idx].centroid) {
          clusters[last_idx].popularity++;
        } else {
          last_idx++;
          clusters[last_idx] = clusters[current_idx];
        }
      }
      unique_clusters = last_idx + 1;
    }

    // In some cases the queries may be co-located, all mapping into the same cluster on that
    // level. In that case we refine the granularity and dive deeper into clusters:
    if (unique_clusters < config.min_clusters && level > 1) {
      level--;
      goto map_to_clusters;
    }

    std::sort(clusters.data(), clusters.data() + unique_clusters, higher_popularity);

    // If clusters are too numerous, merge the ones that are too close to each other.
    std::size_t merge_cycles = 0;
  merge_nearby_clusters:
    if (unique_clusters > config.max_clusters) {
      cluster_t& merge_source = clusters[unique_clusters - 1];
      std::size_t merge_target_idx = 0;
      distance_t merge_distance = std::numeric_limits<distance_t>::max();

      for (std::size_t candidate_idx = 0; candidate_idx + 1 < unique_clusters; ++candidate_idx) {
        distance_t distance = metric_(merge_source.vector, clusters[candidate_idx].vector);
        if (distance < merge_distance) {
          merge_distance = distance;
          merge_target_idx = candidate_idx;
        }
      }

      merge_source.merged_into = clusters[merge_target_idx].centroid;
      clusters[merge_target_idx].popularity += exchange(merge_source.popularity, 0);

      // The target object may have to be swapped a few times to get to optimal position.
      while (merge_target_idx &&
             clusters[merge_target_idx - 1].popularity < clusters[merge_target_idx].popularity)
        std::swap(clusters[merge_target_idx - 1], clusters[merge_target_idx]), --merge_target_idx;

      unique_clusters--;
      merge_cycles++;
      goto merge_nearby_clusters;
    }

    // Replace evicted clusters
    if (merge_cycles) {
      // Sort dropped clusters by name to accelerate future lookups
      auto clusters_end = clusters.data() + config.max_clusters + merge_cycles;
      std::sort(clusters.data(), clusters_end, centroid_id);

      executor.dynamic(queries_count, [&](std::size_t thread_idx, std::size_t query_idx) {
        vector_key_t& cluster_key = cluster_keys[query_idx];
        distance_t& cluster_distance = cluster_distances[query_idx];

        // Recursively trace replacements of that cluster
        while (true) {
          // To avoid implementing heterogeneous comparisons, lets wrap the `cluster_key`
          cluster_t updated_cluster;
          updated_cluster.centroid = cluster_key;
          updated_cluster =
              *std::lower_bound(clusters.data(), clusters_end, updated_cluster, centroid_id);
          if (updated_cluster.merged_into == free_key())
            break;
          cluster_key = updated_cluster.merged_into;
        }

        cluster_distance = distance_between(cluster_key, queries_begin[query_idx], thread_idx).mean;
        return true;
      });
    }

    result.computed_distances = computed_distances;
    result.visited_members = visited_members;
    result.clusters = unique_clusters;

    (void)progress;
    return result;
  }

 private:
  thread_lock_t thread_lock_(std::size_t thread_id) const usearch_noexcept_m {
    if (thread_id != any_thread())
      return {*this, thread_id, false};

    available_threads_mutex_.lock();
    usearch_assert_m(available_threads_.size(), "No available threads to lock");
    available_threads_.try_pop(thread_id);
    available_threads_mutex_.unlock();
    return {*this, thread_id, true};
  }

  void thread_unlock_(std::size_t thread_id) const usearch_noexcept_m {
    available_threads_mutex_.lock();
    usearch_assert_m(available_threads_.size() < available_threads_.capacity(),
                     "Too many threads unlocked");
    available_threads_.push(thread_id);
    available_threads_mutex_.unlock();
  }

  template <typename scalar_at>
  add_result_t add_(                              //
      vector_key_t key, scalar_at const* vector,  //
      std::size_t thread, bool copy_vector, cast_punned_t const& cast) {
    if (!multi() && config().enable_key_lookups && contains(key))
      return add_result_t{}.failed("Duplicate keys not allowed in high-level wrappers");

    // Cast the vector, if needed for compatibility with `metric_`
    thread_lock_t lock = thread_lock_(thread);
    byte_t const* vector_data = reinterpret_cast<byte_t const*>(vector);
    {
      byte_t* casted_data = cast_buffer_.data() + metric_.bytes_per_vector() * lock.thread_id;
      bool casted = cast(vector_data, dimensions(), casted_data);
      if (casted)
        vector_data = casted_data, copy_vector = true;
    }

    // Check if there are some removed entries, whose nodes we can reuse
    compressed_slot_t free_slot = default_free_value<compressed_slot_t>();
    {
      std::unique_lock<std::mutex> lock(free_keys_mutex_);
      free_keys_.try_pop(free_slot);
    }

    // Perform the insertion or the update
    bool reuse_node = free_slot != default_free_value<compressed_slot_t>();
    auto on_success = [&](member_ref_t member) {
      if (config_.enable_key_lookups) {
        unique_lock_t slot_lock(slot_lookup_mutex_);
        slot_lookup_.try_emplace(key_and_slot_t{key, static_cast<compressed_slot_t>(member.slot)});
      }
      if (copy_vector) {
        if (!reuse_node)
          vectors_lookup_[member.slot] =
              vectors_tape_allocator_.allocate(metric_.bytes_per_vector());
        std::memcpy(vectors_lookup_[member.slot], vector_data, metric_.bytes_per_vector());
      } else
        vectors_lookup_[member.slot] = (byte_t*)vector_data;
    };

    index_update_config_t update_config;
    update_config.thread = lock.thread_id;
    update_config.expansion = config_.expansion_add;

    metric_proxy_t metric{*this};
    return reuse_node  //
               ? typed_->update(typed_->iterator_at(free_slot), key, vector_data, metric,
                                update_config, on_success)
               : typed_->add(key, vector_data, metric, update_config, on_success);
  }

  template <typename scalar_at, typename predicate_at>
  search_result_t search_(scalar_at const* vector, std::size_t wanted, predicate_at&& predicate,
                          std::size_t thread, bool exact, cast_punned_t const& cast,
                          size_t expansion) const {
    // Cast the vector, if needed for compatibility with `metric_`
    thread_lock_t lock = thread_lock_(thread);
    byte_t const* vector_data = reinterpret_cast<byte_t const*>(vector);
    {
      byte_t* casted_data = cast_buffer_.data() + metric_.bytes_per_vector() * lock.thread_id;
      bool casted = cast(vector_data, dimensions(), casted_data);
      if (casted)
        vector_data = casted_data;
    }

    index_search_config_t search_config;
    search_config.thread = lock.thread_id;
    search_config.expansion = expansion;
    search_config.exact = exact;

    vector_key_t free_key_copy = free_key_;
    if (std::is_same<typename std::decay<predicate_at>::type, dummy_predicate_t>::value) {
      auto allow = [free_key_copy](member_cref_t const& member) noexcept {
        return (vector_key_t)member.key != free_key_copy;
      };
      auto typed_result =
          typed_->search(vector_data, wanted, metric_proxy_t{*this}, search_config, allow);
      return search_result_t{std::move(typed_result), std::move(lock)};
    } else {
      auto allow = [free_key_copy, &predicate](member_cref_t const& member) noexcept {
        return (vector_key_t)member.key != free_key_copy && predicate(member.key);
      };
      auto typed_result =
          typed_->search(vector_data, wanted, metric_proxy_t{*this}, search_config, allow);
      return search_result_t{std::move(typed_result), std::move(lock)};
    }
  }

  template <typename scalar_at>
  cluster_result_t cluster_(                       //
      scalar_at const* vector, std::size_t level,  //
      std::size_t thread, cast_punned_t const& cast) const {
    // Cast the vector, if needed for compatibility with `metric_`
    thread_lock_t lock = thread_lock_(thread);
    byte_t const* vector_data = reinterpret_cast<byte_t const*>(vector);
    {
      byte_t* casted_data = cast_buffer_.data() + metric_.bytes_per_vector() * lock.thread_id;
      bool casted = cast(vector_data, dimensions(), casted_data);
      if (casted)
        vector_data = casted_data;
    }

    index_cluster_config_t cluster_config;
    cluster_config.thread = lock.thread_id;
    cluster_config.expansion = config_.expansion_search;

    vector_key_t free_key_copy = free_key_;
    auto allow = [free_key_copy](member_cref_t const& member) noexcept {
      return member.key != free_key_copy;
    };
    return typed_->cluster(vector_data, level, metric_proxy_t{*this}, cluster_config, allow);
  }

  template <typename scalar_at>
  aggregated_distances_t distance_between_(       //
      vector_key_t key, scalar_at const* vector,  //
      std::size_t thread, cast_punned_t const& cast) const {
    // Cast the vector, if needed for compatibility with `metric_`
    thread_lock_t lock = thread_lock_(thread);
    byte_t const* vector_data = reinterpret_cast<byte_t const*>(vector);
    {
      byte_t* casted_data = cast_buffer_.data() + metric_.bytes_per_vector() * lock.thread_id;
      bool casted = cast(vector_data, dimensions(), casted_data);
      if (casted)
        vector_data = casted_data;
    }

    // Check if such `key` is even present.
    usearch_assert_m(config().enable_key_lookups, "Key lookups are disabled!");
    shared_lock_t slots_lock(slot_lookup_mutex_);
    auto key_range = slot_lookup_.equal_range(key_and_slot_t::any_slot(key));
    aggregated_distances_t result;
    if (key_range.first == key_range.second)
      return result;

    result.min = std::numeric_limits<distance_t>::max();
    result.max = std::numeric_limits<distance_t>::min();
    result.mean = 0;
    result.count = 0;

    while (key_range.first != key_range.second) {
      key_and_slot_t key_and_slot = *key_range.first;
      byte_t const* a_vector = vectors_lookup_[key_and_slot.slot];
      byte_t const* b_vector = vector_data;
      distance_t a_b_distance = metric_(a_vector, b_vector);

      result.mean += a_b_distance;
      result.min = (std::min)(result.min, a_b_distance);
      result.max = (std::max)(result.max, a_b_distance);
      result.count++;

      //
      ++key_range.first;
    }

    result.mean /= result.count;
    return result;
  }

  void reindex_keys_() {
    // Estimate number of entries first
    std::size_t count_total = typed_->size();
    std::size_t count_removed = 0;
    for (std::size_t i = 0; i != count_total; ++i) {
      auto member_slot = static_cast<compressed_slot_t>(i);
      member_cref_t member = typed_->at(member_slot);
      count_removed += member.key == free_key_;
    }

    if (!count_removed && !config_.enable_key_lookups)
      return;

    // Pull entries from the underlying `typed_` into either
    // into `slot_lookup_`, or `free_keys_` if they are unused.
    unique_lock_t lock(slot_lookup_mutex_);
    slot_lookup_.clear();
    if (config_.enable_key_lookups)
      slot_lookup_.reserve(count_total - count_removed);
    free_keys_.clear();
    free_keys_.reserve(count_removed);
    for (std::size_t i = 0; i != typed_->size(); ++i) {
      auto member_slot = static_cast<compressed_slot_t>(i);
      member_cref_t member = typed_->at(member_slot);
      if (member.key == free_key_)
        free_keys_.push(member_slot);
      else if (config_.enable_key_lookups)
        slot_lookup_.try_emplace(key_and_slot_t{vector_key_t(member.key), member_slot});
    }
  }

  template <typename scalar_at>
  std::size_t get_(vector_key_t key, scalar_at* reconstructed, std::size_t vectors_limit,
                   cast_punned_t const& cast) const {
    if (!multi()) {
      compressed_slot_t slot;
      // Find the matching ID
      {
        shared_lock_t lock(slot_lookup_mutex_);
        auto it = slot_lookup_.find(key_and_slot_t::any_slot(key));
        if (it == slot_lookup_.end())
          return false;
        slot = (*it).slot;
      }
      // Export the entry
      byte_t const* punned_vector = reinterpret_cast<byte_t const*>(vectors_lookup_[slot]);
      bool casted = cast(punned_vector, dimensions(), (byte_t*)reconstructed);
      if (!casted)
        std::memcpy(reconstructed, punned_vector, metric_.bytes_per_vector());
      return true;
    } else {
      shared_lock_t lock(slot_lookup_mutex_);
      auto equal_range_pair = slot_lookup_.equal_range(key_and_slot_t::any_slot(key));
      std::size_t count_exported = 0;
      for (auto begin = equal_range_pair.first;
           begin != equal_range_pair.second && count_exported != vectors_limit;
           ++begin, ++count_exported) {
        //
        compressed_slot_t slot = (*begin).slot;
        byte_t const* punned_vector = reinterpret_cast<byte_t const*>(vectors_lookup_[slot]);
        byte_t* reconstructed_vector =
            (byte_t*)reconstructed + metric_.bytes_per_vector() * count_exported;
        bool casted = cast(punned_vector, dimensions(), reconstructed_vector);
        if (!casted)
          std::memcpy(reconstructed_vector, punned_vector, metric_.bytes_per_vector());
      }
      return count_exported;
    }
  }
};

using index_dense_t = index_dense_gt<>;
using index_dense_big_t = index_dense_gt<uuid_t, uint40_t>;

/**
 *  @brief  Adapts the Male-Optimal Stable Marriage algorithm for unequal sets
 *          to perform fast one-to-one matching between two large collections
 *          of vectors, using approximate nearest neighbors search.
 *
 *  @param[inout] man_to_woman Container to map ::first keys to ::second.
 *  @param[inout] woman_to_man Container to map ::second keys to ::first.
 *  @param[in] executor Thread-pool to execute the job in parallel.
 *  @param[in] progress Callback to report the execution progress.
 */
template <  //

    typename men_key_at,     //
    typename women_key_at,   //
    typename men_slot_at,    //
    typename women_slot_at,  //

    typename man_to_woman_at = dummy_key_to_key_mapping_t,  //
    typename woman_to_man_at = dummy_key_to_key_mapping_t,  //
    typename executor_at = dummy_executor_t,                //
    typename progress_at = dummy_progress_t                 //
    >
static join_result_t join(                                     //
    index_dense_gt<men_key_at, men_slot_at> const& men,        //
    index_dense_gt<women_key_at, women_slot_at> const& women,  //

    index_join_config_t config = {},                     //
    man_to_woman_at&& man_to_woman = man_to_woman_at{},  //
    woman_to_man_at&& woman_to_man = woman_to_man_at{},  //
    executor_at&& executor = executor_at{},              //
    progress_at&& progress = progress_at{}) {
  return men.join(                                  //
      women, config,                                //
      std::forward<woman_to_man_at>(woman_to_man),  //
      std::forward<man_to_woman_at>(man_to_woman),  //
      std::forward<executor_at>(executor),          //
      std::forward<progress_at>(progress));
}

}  // namespace usearch
}  // namespace unum
