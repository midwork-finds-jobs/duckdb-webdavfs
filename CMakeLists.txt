cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME webdavfs)

project(${TARGET_NAME})

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
# For clang-tidy checks, we make these optional as they're not needed for static analysis
if(CLANG_TIDY)
  find_package(OpenSSL)
  find_package(CURL)
else()
  find_package(OpenSSL REQUIRED)
  find_package(CURL REQUIRED)
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

include_directories(src/include)

set(EXTENSION_SOURCES
    src/webdav_extension.cpp
    src/webdavfs.cpp
    src/webdav_secrets.cpp
    src/crypto.cpp
    src/http_state.cpp
    src/httpfs.cpp
    src/httpfs_curl_client.cpp
    src/httpfs_util_stub.cpp
    src/hash_functions.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL and CURL in both the static library as the loadable extension
if(NOT CLANG_TIDY)
  target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto CURL::libcurl)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto CURL::libcurl)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
