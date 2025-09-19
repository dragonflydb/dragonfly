add_third_party(
  lua
  GIT_REPOSITORY https://github.com/dragonflydb/lua
  GIT_TAG Dragonfly-5.4.6a
  CONFIGURE_COMMAND echo
  BUILD_IN_SOURCE 1
  BUILD_COMMAND ${DFLY_TOOLS_MAKE} all
  INSTALL_COMMAND cp <SOURCE_DIR>/liblua.a ${THIRD_PARTY_LIB_DIR}/lua/lib/
  COMMAND cp <SOURCE_DIR>/lualib.h <SOURCE_DIR>/lua.h <SOURCE_DIR>/lauxlib.h
          <SOURCE_DIR>/luaconf.h ${THIRD_PARTY_LIB_DIR}/lua/include
)


if (APPLE OR ${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
  set(SED_REPL sed "-i" '')
else()
  set(SED_REPL sed "-i")
endif()

add_third_party(
  dconv
  URL https://github.com/google/double-conversion/archive/refs/tags/v3.3.0.tar.gz
  PATCH_COMMAND ${SED_REPL} "/static const std::ctype/d"
                <SOURCE_DIR>/double-conversion/string-to-double.cc
  COMMAND ${SED_REPL} "/std::use_facet</d" <SOURCE_DIR>/double-conversion/string-to-double.cc
  COMMAND ${SED_REPL} "s/cType.tolower/std::tolower/g" <SOURCE_DIR>/double-conversion/string-to-double.cc
  LIB libdouble-conversion.a
)

add_third_party(
  reflex
  URL https://github.com/Genivia/RE-flex/archive/refs/tags/v5.2.2.tar.gz
  PATCH_COMMAND autoreconf -fi
  CONFIGURE_COMMAND <SOURCE_DIR>/configure --disable-avx2 --prefix=${THIRD_PARTY_LIB_DIR}/reflex
          CXX=${CMAKE_CXX_COMPILER} CC=${CMAKE_C_COMPILER}
)

set(REFLEX "${THIRD_PARTY_LIB_DIR}/reflex/bin/reflex")



add_third_party(
  jsoncons
  GIT_REPOSITORY https://github.com/dragonflydb/jsoncons
  GIT_TAG Dragonfly.178
  GIT_SHALLOW 1
  CMAKE_PASS_FLAGS "-DJSONCONS_BUILD_TESTS=OFF -DJSONCONS_HAS_POLYMORPHIC_ALLOCATOR=ON"
  LIB "none"
)

add_third_party(
  lz4
  URL https://github.com/lz4/lz4/archive/refs/tags/v1.10.0.tar.gz

  BUILD_IN_SOURCE 1
  CONFIGURE_COMMAND echo skip
  BUILD_COMMAND ${DFLY_TOOLS_MAKE} lib-release
  INSTALL_COMMAND ${DFLY_TOOLS_MAKE} install BUILD_SHARED=no PREFIX=${THIRD_PARTY_LIB_DIR}/lz4
)

set(MIMALLOC_ROOT_DIR ${THIRD_PARTY_LIB_DIR}/mimalloc2)
set(MIMALLOC_INCLUDE_DIR ${MIMALLOC_ROOT_DIR}/include)
set(MIMALLOC_PATCH_DIR ${CMAKE_CURRENT_LIST_DIR}/../patches/mimalloc-v2.2.4)
set(MIMALLOC_C_FLAGS "-O3 -g -DMI_STAT=1 -DNDEBUG")
file(MAKE_DIRECTORY ${MIMALLOC_INCLUDE_DIR})

ExternalProject_Add(mimalloc2_project
  URL https://github.com/microsoft/mimalloc/archive/refs/tags/v2.2.4.tar.gz
  DOWNLOAD_DIR ${THIRD_PARTY_DIR}/mimalloc2
  SOURCE_DIR ${THIRD_PARTY_DIR}/mimalloc2
  # INSTALL_DIR ${MIMALLOC_ROOT_DIR}
  UPDATE_COMMAND ""

  PATCH_COMMAND
      patch -p1 -d ${THIRD_PARTY_DIR}/mimalloc2/ -i ${MIMALLOC_PATCH_DIR}/0_base.patch
      COMMAND patch -p1 -d ${THIRD_PARTY_DIR}/mimalloc2/ -i ${MIMALLOC_PATCH_DIR}/1_add_stat_type.patch
      COMMAND patch -p1 -d ${THIRD_PARTY_DIR}/mimalloc2/ -i ${MIMALLOC_PATCH_DIR}/2_return_stat.patch
      COMMAND patch -p1 -d ${THIRD_PARTY_DIR}/mimalloc2/ -i ${MIMALLOC_PATCH_DIR}/3_track_full_size.patch
  BUILD_COMMAND make mimalloc-static

  INSTALL_COMMAND make install
  # Copy internal types like mi_page_usage_stats_s and mi_heap_s
  COMMAND cp -r <SOURCE_DIR>/include/mimalloc ${MIMALLOC_INCLUDE_DIR}/

  LOG_INSTALL ON
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON
  LOG_PATCH ON
  LOG_UPDATE ON
  DOWNLOAD_EXTRACT_TIMESTAMP YES

  CMAKE_GENERATOR "Unix Makefiles"

  # Add -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_FLAGS=-O0 to debug, and set BUILD_BYPRODUCTS to
  # libmimalloc-debug.a

  BUILD_BYPRODUCTS ${MIMALLOC_ROOT_DIR}/lib/libmimalloc.a

  CMAKE_ARGS -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY:PATH=${MIMALLOC_ROOT_DIR}/lib
        -DCMAKE_LIBRARY_OUTPUT_DIRECTORY:PATH=${MIMALLOC_ROOT_DIR}/lib
        -DCMAKE_BUILD_TYPE:STRING=Release
        -DCMAKE_CXX_COMPILER:STRING=${CMAKE_CXX_COMPILER}
        -DMI_INSTALL_TOPLEVEL=ON
        -DMI_OVERRIDE=OFF
        -DMI_NO_PADDING=ON
        -DMI_BUILD_TESTS=OFF
        -DMI_BUILD_SHARED=OFF
        -DMI_BUILD_OBJECT=OFF
        -DCMAKE_C_FLAGS=${MIMALLOC_C_FLAGS}
        -DCMAKE_INSTALL_PREFIX:PATH=${MIMALLOC_ROOT_DIR}
)

add_library(TRDP::mimalloc2 STATIC IMPORTED)
add_dependencies(TRDP::mimalloc2 mimalloc2_project)
set_target_properties(TRDP::mimalloc2 PROPERTIES IMPORTED_LOCATION ${MIMALLOC_ROOT_DIR}/lib/libmimalloc.a
                      INTERFACE_INCLUDE_DIRECTORIES ${MIMALLOC_ROOT_DIR}/include)

add_third_party(
  croncpp
  URL https://github.com/mariusbancila/croncpp/archive/refs/tags/v2023.03.30.tar.gz
  LIB "none"
)

if (WITH_SEARCH)
  add_third_party(
    uni-algo
    URL https://github.com/uni-algo/uni-algo/archive/refs/tags/v1.0.0.tar.gz

    CMAKE_PASS_FLAGS "-DCMAKE_CXX_STANDARD:STRING=17"
  )

  add_third_party(
    hnswlib
    URL https://github.com/nmslib/hnswlib/archive/refs/tags/v0.7.0.tar.gz

    BUILD_COMMAND echo SKIP
    INSTALL_COMMAND cp -R <SOURCE_DIR>/hnswlib ${THIRD_PARTY_LIB_DIR}/hnswlib/include/
    LIB "none"
  )
endif()

add_third_party(
  fast_float
  URL https://github.com/fastfloat/fast_float/archive/refs/tags/v5.2.0.tar.gz
  LIB "none"
)

add_third_party(
  flatbuffers
  URL https://github.com/google/flatbuffers/archive/refs/tags/v23.5.26.tar.gz
  CMAKE_PASS_FLAGS "-DFLATBUFFERS_BUILD_TESTS=OFF -DFLATBUFFERS_LIBCXX_WITH_CLANG=OFF
                    -DFLATBUFFERS_BUILD_FLATC=OFF"
)

add_third_party(
  hdr_histogram
  GIT_REPOSITORY https://github.com/HdrHistogram/HdrHistogram_c/
  GIT_TAG 652d51bcc36744fd1a6debfeb1a8a5f58b14022c
  CMAKE_PASS_FLAGS "-DHDR_LOG_REQUIRED=OFF -DHDR_HISTOGRAM_BUILD_PROGRAMS=OFF
                    -DHDR_HISTOGRAM_INSTALL_SHARED=OFF"
  LIB libhdr_histogram_static.a
)

if(USE_SIMSIMD)
  add_third_party(
    simsimd
    URL https://github.com/ashvardanian/SimSIMD/archive/refs/tags/v6.4.9.tar.gz
    BUILD_COMMAND echo SKIP
    INSTALL_COMMAND cp -R <SOURCE_DIR>/include ${THIRD_PARTY_LIB_DIR}/simsimd/
    LIB "none"
  )
endif()


add_library(TRDP::jsoncons INTERFACE IMPORTED)
add_dependencies(TRDP::jsoncons jsoncons_project)
set_target_properties(TRDP::jsoncons PROPERTIES
                      INTERFACE_INCLUDE_DIRECTORIES "${JSONCONS_INCLUDE_DIR}")

add_library(TRDP::croncpp INTERFACE IMPORTED)
add_dependencies(TRDP::croncpp croncpp_project)
set_target_properties(TRDP::croncpp PROPERTIES
                      INTERFACE_INCLUDE_DIRECTORIES "${CRONCPP_INCLUDE_DIR}")

if (WITH_SEARCH)
  add_library(TRDP::hnswlib INTERFACE IMPORTED)
  add_dependencies(TRDP::hnswlib hnswlib_project)
  set_target_properties(TRDP::hnswlib PROPERTIES
                        INTERFACE_INCLUDE_DIRECTORIES "${HNSWLIB_INCLUDE_DIR}")
endif()

add_library(TRDP::fast_float INTERFACE IMPORTED)
add_dependencies(TRDP::fast_float fast_float_project)
set_target_properties(TRDP::fast_float PROPERTIES
                      INTERFACE_INCLUDE_DIRECTORIES "${FAST_FLOAT_INCLUDE_DIR}")

if(USE_SIMSIMD)
  add_library(TRDP::simsimd INTERFACE IMPORTED)
  add_dependencies(TRDP::simsimd simsimd_project)
  set_target_properties(TRDP::simsimd PROPERTIES
                        INTERFACE_INCLUDE_DIRECTORIES "${SIMSIMD_INCLUDE_DIR}")
endif()

if (USE_PROMETHEUS)
  FetchContent_Declare(
          prometheus
          DOWNLOAD_EXTRACT_TIMESTAMP true
          URL "https://github.com/prometheus/prometheus/releases/download/v3.5.0/prometheus-3.5.0.linux-amd64.tar.gz"
  )
  FetchContent_MakeAvailable(prometheus)
  FetchContent_GetProperties(prometheus)
  if (prometheus_POPULATED)
    file(COPY ${prometheus_SOURCE_DIR}/prometheus DESTINATION ${CMAKE_BINARY_DIR})
  endif (prometheus_POPULATED)
endif (USE_PROMETHEUS)
