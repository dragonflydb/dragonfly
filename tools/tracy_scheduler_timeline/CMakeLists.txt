cmake_minimum_required(VERSION 3.20)
project(tracy_scheduler_timeline LANGUAGES CXX)

set(TRACY_ROOT "" CACHE PATH "Path to a standalone Tracy v0.14.1 checkout")
if(NOT EXISTS "${TRACY_ROOT}/server/TracyFileRead.hpp")
  message(FATAL_ERROR "Set TRACY_ROOT to a Tracy v0.14.1 checkout")
endif()

find_package(Threads REQUIRED)
find_library(TBB_LIBRARY NAMES tbb REQUIRED)
find_library(ZSTD_LIBRARY NAMES zstd REQUIRED)
set(PPQSORT_INCLUDE_DIR "" CACHE PATH "Directory containing ppqsort.h")
if(NOT PPQSORT_INCLUDE_DIR)
  file(GLOB ppqsort_candidates "${TRACY_ROOT}/*/build/.cpm-cache/ppqsort/*/include")
  foreach(candidate IN LISTS ppqsort_candidates)
    if(EXISTS "${candidate}/ppqsort.h")
      set(PPQSORT_INCLUDE_DIR "${candidate}")
      break()
    endif()
  endforeach()
endif()
if(NOT EXISTS "${PPQSORT_INCLUDE_DIR}/ppqsort.h")
  message(FATAL_ERROR "Set PPQSORT_INCLUDE_DIR to the directory containing ppqsort.h")
endif()

add_executable(
  tracy_scheduler_timeline
  tracy_scheduler_timeline.cc
  "${TRACY_ROOT}/public/common/tracy_lz4.cpp"
)
target_compile_features(tracy_scheduler_timeline PRIVATE cxx_std_20)
target_include_directories(
  tracy_scheduler_timeline PRIVATE "${TRACY_ROOT}/server" "${PPQSORT_INCLUDE_DIR}"
)
target_link_libraries(
  tracy_scheduler_timeline PRIVATE Threads::Threads "${TBB_LIBRARY}" "${ZSTD_LIBRARY}"
)
