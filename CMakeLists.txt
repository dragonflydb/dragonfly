cmake_minimum_required(VERSION 3.15 FATAL_ERROR)
set(PROJECT_CONTACT romange@gmail.com)

enable_testing()

set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

# Set targets in folders
set_property(GLOBAL PROPERTY USE_FOLDERS ON)
project(DRAGONFLY C CXX)
set(CMAKE_CXX_STANDARD 17)

# We must define all the required variables from the root cmakefile, otherwise
# they just disappear.
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/async/cmake" ${CMAKE_MODULE_PATH})
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)

include(third_party)
include(internal)

Message(STATUS "THIRD_PARTY_LIB_DIR ${THIRD_PARTY_LIB_DIR}")

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(async)

add_subdirectory(async)
