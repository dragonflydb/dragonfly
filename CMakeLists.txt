cmake_minimum_required(VERSION 3.15 FATAL_ERROR)
set(PROJECT_CONTACT romange@gmail.com)

include(CheckCXXCompilerFlag)
include(CheckCCompilerFlag)

enable_testing()

set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

# AFL++ fuzzing support - must be set BEFORE project() command
option(USE_AFL "Enable AFL++ fuzzing" OFF)
if(USE_AFL)
  # Automatically set AFL++ compilers if not already set
  if(NOT CMAKE_C_COMPILER MATCHES "afl-" AND NOT CMAKE_CXX_COMPILER MATCHES "afl-")
    find_program(AFL_CC afl-clang-fast)
    find_program(AFL_CXX afl-clang-fast++)

    if(AFL_CC AND AFL_CXX)
      message(STATUS "AFL++ fuzzing enabled - setting compilers")
      set(CMAKE_C_COMPILER ${AFL_CC})
      set(CMAKE_CXX_COMPILER ${AFL_CXX})
    else()
      message(FATAL_ERROR "USE_AFL=ON but AFL++ compilers not found!\n"
              "Please install AFL++: apt install afl++ or build from source\n"
              "https://github.com/AFLplusplus/AFLplusplus")
    endif()
  endif()
endif()

# Set targets in folders
set_property(GLOBAL PROPERTY USE_FOLDERS ON)
project(DRAGONFLY C CXX)
set(CMAKE_CXX_STANDARD 20)

# Disabled because it has false positives with ref-counted intrusive pointers.
CHECK_CXX_COMPILER_FLAG("-Wuse-after-free" HAS_USE_AFTER_FREE_WARN)
if (HAS_USE_AFTER_FREE_WARN)
    set(CMAKE_CXX_FLAGS "-Wno-use-after-free ${CMAKE_CXX_FLAGS}")
endif()

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "-Wthread-safety -Werror=thread-safety ${CMAKE_CXX_FLAGS}")
endif()

# We can not use here CHECK_CXX_COMPILER_FLAG because systems that do not support sanitizers
# fail during linking time.
set(CMAKE_REQUIRED_FLAGS "-fsanitize=address")
check_cxx_source_compiles("int main() { return 0; }" SUPPORT_ASAN)

set(CMAKE_REQUIRED_FLAGS "-fsanitize=undefined")
check_cxx_source_compiles("int main() { return 0; }" SUPPORT_USAN)
set(CMAKE_REQUIRED_FLAGS "")

# We must define all the required variables from the root cmakefile, otherwise
# they just disappear.
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/helio/cmake" ${CMAKE_MODULE_PATH})
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
option(DF_USE_SSL "Provide support for SSL connections" ON)

find_package(OpenSSL)

# AFL++ configuration - must be before sanitizer checks
if(USE_AFL)
  message(STATUS "AFL++ fuzzing mode active")
  message(STATUS "  C compiler: ${CMAKE_C_COMPILER}")
  message(STATUS "  C++ compiler: ${CMAKE_CXX_COMPILER}")

  # Add USE_AFL as compile definition so #ifdef USE_AFL works in code
  add_compile_definitions(USE_AFL)

  # AFL++ requires specific compiler flags for coverage instrumentation
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g")

  # Force disable sanitizers when fuzzing (AFL++ incompatible with ASAN/UBSAN)
  message(STATUS "Disabling sanitizers (incompatible with AFL++ fuzzing)")
  set(WITH_ASAN OFF CACHE BOOL "Disable ASAN for fuzzing" FORCE)
  set(WITH_USAN OFF CACHE BOOL "Disable UBSAN for fuzzing" FORCE)

  # Disable AWS and GCP for fuzzing builds (not needed, reduces build time)
  message(STATUS "Disabling AWS and GCP integrations for fuzzing")
  set(WITH_AWS OFF CACHE BOOL "Disable AWS for fuzzing" FORCE)
  set(WITH_GCP OFF CACHE BOOL "Disable GCP for fuzzing" FORCE)
endif()

option(WITH_ASAN "Enable -fsanitize=address" OFF)
if (SUPPORT_ASAN AND WITH_ASAN)
  message(STATUS "address sanitizer enabled")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address")
endif()

option(WITH_USAN "Enable -fsanitize=undefined" OFF)
if (SUPPORT_USAN AND WITH_USAN)
  message(STATUS "ub sanitizer enabled")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=undefined")
endif()

# Split debug info into .dwo files so the linker handles much less data.
# Both GCC and Clang support this. GDB resolves .dwo references transparently.
CHECK_CXX_COMPILER_FLAG("-gsplit-dwarf" HAS_SPLIT_DWARF_CXX)
CHECK_C_COMPILER_FLAG("-gsplit-dwarf" HAS_SPLIT_DWARF_C)
if (HAS_SPLIT_DWARF_CXX)
  add_compile_options($<$<AND:$<CONFIG:Debug>,$<COMPILE_LANGUAGE:CXX>>:-gsplit-dwarf>)
endif()
if (HAS_SPLIT_DWARF_C)
  add_compile_options($<$<AND:$<CONFIG:Debug>,$<COMPILE_LANGUAGE:C>>:-gsplit-dwarf>)
endif()

include(third_party)
include(internal)
# helio's internal.cmake sets CMAKE_CXX_STANDARD=17 for its own legacy targets;
# dragonfly itself uses C++20 (operator<=>, std::contiguous_iterator, concepts),
# so restore the C++20 standard before configuring our own subdirectories.
set(CMAKE_CXX_STANDARD 20)

# Fetch the EVE (Expressive Vector Engine) SIMD library used by OAHSet.
# Mirrors the FetchContent pattern in helio/cmake/third_party.cmake.
include(FetchContent)
FetchContent_Declare(
  eve
  URL https://github.com/jfalcou/eve/archive/refs/tags/v2023.02.15.tar.gz
  URL_HASH SHA256=7a5fb59c0e6ef3bef3e8b36d62e138d31e7f2a9f1bdfe95a8e96512b207f84c5
)
FetchContent_GetProperties(eve)
if (NOT eve_POPULATED)
  FetchContent_Populate(eve)
  set(EVE_BUILD_TEST OFF CACHE BOOL "")
  set(EVE_BUILD_DOCUMENTATION OFF CACHE BOOL "")
  set(EVE_BUILD_BENCHMARKS OFF CACHE BOOL "")
  set(EVE_BUILD_INTEGRATION OFF CACHE BOOL "")
  set(EVE_BUILD_RANDOM OFF CACHE BOOL "")
  add_subdirectory(${eve_SOURCE_DIR} ${eve_BINARY_DIR})
endif()

include_directories(src)
include_directories(helio)

add_subdirectory(helio)
add_subdirectory(src)
