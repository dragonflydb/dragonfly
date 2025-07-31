find_library(LIB_PCRE2 NAMES pcre2-8)
if(LIB_PCRE2)
  set(PCRE2_LIB ${LIB_PCRE2})
else()
  message(STATUS "pcre2-8 not found. Building without PCRE2 support.")
  set(PCRE2_LIB "")
endif()

find_library(LIB_RE2 NAMES re2)
if(LIB_RE2)
  set(RE2_LIB ${LIB_RE2})
else()
  message(STATUS "re2 not found. Building without RE2 support.")
  set(RE2_LIB "")
endif()

if (WITH_SEARCH)
  add_subdirectory(search)
else()
  add_library(dfly_search_core INTERFACE)
endif()

add_subdirectory(json)

add_library(dfly_core allocation_tracker.cc bloom.cc compact_object.cc dense_set.cc
    dragonfly_core.cc extent_tree.cc huff_coder.cc
    interpreter.cc glob_matcher.cc mi_memory_resource.cc qlist.cc sds_utils.cc
    segment_allocator.cc score_map.cc small_string.cc sorted_map.cc task_queue.cc
    tx_queue.cc string_set.cc string_map.cc top_keys.cc detail/bitpacking.cc
        page_usage_stats.cc)

cxx_link(dfly_core base dfly_search_core fibers2 jsonpath
    absl::flat_hash_map absl::str_format absl::random_random redis_lib
    TRDP::lua lua_modules
    OpenSSL::Crypto TRDP::dconv TRDP::lz4 TRDP::hdr_histogram)

add_executable(dash_bench dash_bench.cc)
cxx_link(dash_bench dfly_core redis_test_lib)

cxx_test(dfly_core_test dfly_core TRDP::fast_float ${PCRE2_LIB} ${RE2_LIB} LABELS DFLY)
cxx_test(compact_object_test dfly_core LABELS DFLY)
cxx_test(extent_tree_test dfly_core LABELS DFLY)
cxx_test(dash_test dfly_core file redis_test_lib DATA testdata/ids.txt.zst LABELS DFLY)
cxx_test(interpreter_test dfly_core LABELS DFLY)

cxx_test(string_set_test dfly_core LABELS DFLY)
cxx_test(string_map_test dfly_core LABELS DFLY)
cxx_test(sorted_map_test dfly_core redis_test_lib LABELS DFLY)
cxx_test(bptree_set_test dfly_core LABELS DFLY)
cxx_test(linear_search_map_test dfly_core LABELS DFLY)
cxx_test(score_map_test dfly_core LABELS DFLY)
cxx_test(maps_list_test dfly_core LABELS DFLY)
cxx_test(flatbuffers_test dfly_core TRDP::flatbuffers LABELS DFLY)
cxx_test(bloom_test dfly_core LABELS DFLY)
cxx_test(allocation_tracker_test dfly_core absl::random_random LABELS DFLY)
cxx_test(qlist_test dfly_core DATA testdata/list.txt.zst LABELS DFLY)
cxx_test(zstd_test dfly_core TRDP::zstd LABELS DFLY)
cxx_test(top_keys_test dfly_core LABELS DFLY)
cxx_test(page_usage_stats_test dfly_core LABELS DFLY)

if(LIB_PCRE2)
  target_compile_definitions(dfly_core_test PRIVATE USE_PCRE2=1)
  # target_compile_definitions(dfly_core PUBLIC USE_PCRE2=1)
endif()

if(LIB_RE2)
  target_compile_definitions(dfly_core_test PRIVATE USE_RE2)
endif()
