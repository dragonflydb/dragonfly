cmake_minimum_required(VERSION 3.15 FATAL_ERROR)
set(PROJECT_CONTACT romange@gmail.com)

include(CheckCXXCompilerFlag)

enable_testing()

set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

# Set targets in folders
set_property(GLOBAL PROPERTY USE_FOLDERS ON)
project(DRAGONFLY C CXX)
set(CMAKE_CXX_STANDARD 17)

# Disabled because it has false positives with ref-counted intrusive pointers.
CHECK_CXX_COMPILER_FLAG("-Wuse-after-free" HAS_USE_AFTER_FREE_WARN)
if (HAS_USE_AFTER_FREE_WARN)
    set(CMAKE_CXX_FLAGS "-Wno-use-after-free ${CMAKE_CXX_FLAGS}")
endif()

# We must define all the required variables from the root cmakefile, otherwise
# they just disappear.
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/helio/cmake" ${CMAKE_MODULE_PATH})
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
option(DF_USE_SSL "Provide support for SSL connections" ON)

find_package(OpenSSL)

include(third_party)
include(internal)

set(USE_FB2 ON CACHE BOOL "")

include_directories(src)
include_directories(helio)

add_subdirectory(helio)
add_subdirectory(src)

include(cmake/Packing.cmake)
