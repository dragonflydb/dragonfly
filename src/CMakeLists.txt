
if ("${CMAKE_SYSTEM_NAME}" STREQUAL "FreeBSD")
  set(DFLY_TOOLS_MAKE "gmake")
else()
  set(DFLY_TOOLS_MAKE "make")
endif()

add_third_party(
  lua
  URL https://github.com/lua/lua/archive/refs/tags/v5.4.4.tar.gz
  PATCH_COMMAND patch -p1 -i "${CMAKE_SOURCE_DIR}/patches/lua-v5.4.4.patch"
  CONFIGURE_COMMAND echo
  BUILD_IN_SOURCE 1
  BUILD_COMMAND ${DFLY_TOOLS_MAKE} all
  INSTALL_COMMAND cp <SOURCE_DIR>/liblua.a ${THIRD_PARTY_LIB_DIR}/lua/lib/
  COMMAND cp <SOURCE_DIR>/lualib.h <SOURCE_DIR>/lua.h <SOURCE_DIR>/lauxlib.h
          <SOURCE_DIR>/luaconf.h ${THIRD_PARTY_LIB_DIR}/lua/include
)

function(cur_gen_dir out_dir)
  file(RELATIVE_PATH _rel_folder "${PROJECT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}")

  set(_tmp_dir ${ROOT_GEN_DIR}/${_rel_folder})
  set(${out_dir} ${_tmp_dir} PARENT_SCOPE)
  file(MAKE_DIRECTORY ${_tmp_dir})
endfunction()

set(ROOT_GEN_DIR ${CMAKE_SOURCE_DIR}/genfiles)
file(MAKE_DIRECTORY ${ROOT_GEN_DIR})
include_directories(${ROOT_GEN_DIR}/src)

function(gen_bison name)
  GET_FILENAME_COMPONENT(_in ${name}.y ABSOLUTE)
  cur_gen_dir(gen_dir)
  # add_library(${lib_name} ${gen_dir}/${name}.cc)
  set(full_path_cc ${gen_dir}/${name}.cc ${gen_dir}/${name}.hh)

  ADD_CUSTOM_COMMAND(
           OUTPUT ${full_path_cc}
           COMMAND mkdir -p ${gen_dir}
           COMMAND bison --language=c++ -o ${gen_dir}/${name}.cc ${name}.y -Wconflicts-sr
           DEPENDS ${_in}
           WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
           COMMENT "Generating parser from ${name}.y" VERBATIM)
 set_source_files_properties(${name}.cc ${name}_base.h PROPERTIES GENERATED TRUE)
endfunction()

if (APPLE)
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
  URL https://github.com/Genivia/RE-flex/archive/refs/tags/v3.3.2.tar.gz
  CONFIGURE_COMMAND <SOURCE_DIR>/configure --disable-avx --prefix=${THIRD_PARTY_LIB_DIR}/reflex
)

set(REFLEX "${THIRD_PARTY_LIB_DIR}/reflex/bin/reflex")

add_third_party(
  jsoncons
  URL https://github.com/danielaparker/jsoncons/archive/refs/tags/v0.171.0.tar.gz
  CMAKE_PASS_FLAGS "-DJSONCONS_BUILD_TESTS=OFF -DJSONCONS_HAS_POLYMORPHIC_ALLOCATOR=ON"
  LIB "none"
)

add_third_party(
  lz4
  URL https://github.com/lz4/lz4/archive/refs/tags/v1.9.4.tar.gz

  BUILD_IN_SOURCE 1
  CONFIGURE_COMMAND echo skip
  BUILD_COMMAND ${DFLY_TOOLS_MAKE} lib-release
  INSTALL_COMMAND ${DFLY_TOOLS_MAKE} install BUILD_SHARED=no PREFIX=${THIRD_PARTY_LIB_DIR}/lz4
)

add_third_party(
  croncpp
  URL https://github.com/mariusbancila/croncpp/archive/refs/tags/v2023.03.30.tar.gz
  LIB "none"
)

add_third_party(
  uni-algo
  URL https://github.com/uni-algo/uni-algo/archive/refs/tags/v1.0.0.tar.gz

  BUILD_IN_SOURCE 1
)

add_third_party(
  hnswlib
  URL https://github.com/nmslib/hnswlib/archive/refs/tags/v0.7.0.tar.gz

  INSTALL_COMMAND cp -RT <SOURCE_DIR>/hnswlib ${THIRD_PARTY_LIB_DIR}/hnswlib/include
  LIB "none"
)

add_library(TRDP::jsoncons INTERFACE IMPORTED)
add_dependencies(TRDP::jsoncons jsoncons_project)
set_target_properties(TRDP::jsoncons PROPERTIES
                      INTERFACE_INCLUDE_DIRECTORIES "${JSONCONS_INCLUDE_DIR}")

add_library(TRDP::croncpp INTERFACE IMPORTED)
add_dependencies(TRDP::croncpp croncpp_project)
set_target_properties(TRDP::croncpp PROPERTIES
                      INTERFACE_INCLUDE_DIRECTORIES "${CRONCPP_INCLUDE_DIR}")

add_library(TRDP::hnswlib INTERFACE IMPORTED)
add_dependencies(TRDP::hnswlib hnswlib_project)
set_target_properties(TRDP::hnswlib PROPERTIES
                      INTERFACE_INCLUDE_DIRECTORIES "${HNSWLIB_INCLUDE_DIR}")

Message(STATUS "THIRD_PARTY_LIB_DIR ${THIRD_PARTY_LIB_DIR}")


option(ENABLE_GIT_VERSION "Build with Git metadata" OFF)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/" )

if(ENABLE_GIT_VERSION)
    include(GetGitRevisionDescription)
    get_git_head_revision(GIT_REFSPEC GIT_SHA1)
    git_local_changes(GIT_CLEAN_DIRTY)
    if("${GIT_CLEAN_DIRTY}" STREQUAL "DIRTY")
        set(GIT_CLEAN_DIRTY "-dirty")
        else()
        set(GIT_CLEAN_DIRTY "")
    endif()
    Message(STATUS "GIT_SHA1 ${GIT_SHA1}")
    git_describe(GIT_VER --always)
    Message(STATUS "GIT_VER ${GIT_VER}")
    string(TIMESTAMP PRJ_BUILD_TIME "%Y-%m-%d %H:%M:%S" UTC)
else(ENABLE_GIT_VERSION)
    set(GIT_VER "dev")
    set(GIT_SHA1 "0000000")
    set(GIT_CLEAN_DIRTY "-dev")
    set(PRJ_BUILD_TIME "bigbang")
endif(ENABLE_GIT_VERSION)


function(gen_flex name)
  GET_FILENAME_COMPONENT(_in ${name}.lex ABSOLUTE)
  cur_gen_dir(gen_dir)

  ADD_CUSTOM_COMMAND(
           OUTPUT ${gen_dir}/${name}.cc ${gen_dir}/${name}.h
           COMMAND mkdir -p ${gen_dir}

           COMMAND ${REFLEX} -o ${gen_dir}/${name}.cc  --unicode --header-file=${gen_dir}/${name}.h
                             --bison-complete  --bison-locations  ${_in}
           DEPENDS ${_in} reflex_project
           WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
           COMMENT "Generating lexer from ${name}.lex" VERBATIM)

  set_source_files_properties(${gen_dir}/${name}.h ${gen_dir}/${name}.cc
                              PROPERTIES GENERATED TRUE)
endfunction()

# the output file resides in the build directory.
configure_file(server/version.cc.in "${CMAKE_CURRENT_SOURCE_DIR}/server/version.cc" @ONLY)

add_definitions(-DUSE_FB2)

add_subdirectory(redis)
add_subdirectory(core)
add_subdirectory(facade)
add_subdirectory(server)
