# Packages the dragonfly binary into a .deb package
# Use this to generate .deb package from build directory
#    cpack -G DEB
#
# Resulting packages can be found under _packages/

set(CPACK_PACKAGE_NAME "dragonflydb"
    CACHE STRING "dragonflydb"
)

set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "A modern replacement for Redis and Memcached"
    CACHE STRING "A modern replacement for Redis and Memcached"
)

set(CPACK_PACKAGE_VENDOR "DragonflyDB")

set(CPACK_OUTPUT_FILE_PREFIX "${CMAKE_SOURCE_DIR}/_packages")

set(CPACK_PACKAGING_INSTALL_PREFIX "/usr/share/dragonfly")

set(CPACK_PACKAGE_VERSION_MAJOR ${PROJECT_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${PROJECT_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${PROJECT_VERSION_PATCH})

set(CPACK_PACKAGE_CONTACT "support@dragonflydb.io")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "DragonflyDB maintainers")

set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE.md")
set(CPACK_RESOURCE_FILE_README "${CMAKE_CURRENT_SOURCE_DIR}/README.md")

set(CPACK_COMPONENTS_GROUPING ALL_COMPONENTS_IN_ONE)

set(CPACK_DEB_COMPONENT_INSTALL YES)

install(TARGETS dragonfly DESTINATION . COMPONENT dragonfly)

set(CPACK_COMPONENTS_ALL dragonfly)

include(CPack)
