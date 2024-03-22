# ---------------------------------------------------------------------------
# AnyBlob Include CMake
#
# This should be used to include AnyBlob as static library for your project
# ---------------------------------------------------------------------------

Include(ExternalProject)

# ---------------------------------------------------------------------------
# Get AnyBlob
# ---------------------------------------------------------------------------

ExternalProject_Add(anyblob
  GIT_REPOSITORY      https://github.com/durner/AnyBlob.git
  GIT_TAG             "master"
  PREFIX              "thirdparty/anyblob"
  INSTALL_COMMAND     ""
  CMAKE_ARGS
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
    -DCMAKE_CXX_STANDARD=20
    -DCMAKE_CXX_STANDARD_REQUIRED=ON
)

# ---------------------------------------------------------------------------
# Get SOURCE and BINARY DIR
# ---------------------------------------------------------------------------

ExternalProject_Get_Property(anyblob SOURCE_DIR)
ExternalProject_Get_Property(anyblob BINARY_DIR)

set(ANYBLOB_INCLUDE_DIR ${SOURCE_DIR}/include)
file(MAKE_DIRECTORY ${ANYBLOB_INCLUDE_DIR})

# ---------------------------------------------------------------------------
# Configure OpenSSL, Threads, and Uring
# ---------------------------------------------------------------------------

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)

find_path(LIBURING_INCLUDE_DIR NAMES liburing.h)
mark_as_advanced(LIBURING_INCLUDE_DIR)
find_library(LIBURING_LIBRARY NAMES uring)
mark_as_advanced(LIBURING_LIBRARY)
include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
        LIBURING
        REQUIRED_VARS LIBURING_LIBRARY LIBURING_INCLUDE_DIR)
if(LIBURING_FOUND)
  set(LIBURING_LIBRARIES ${LIBURING_LIBRARY})
  set(LIBURING_INCLUDE_DIRS ${LIBURING_INCLUDE_DIR})
endif()

# ---------------------------------------------------------------------------
# Build Library with dependencies
# ---------------------------------------------------------------------------

add_library(AnyBlob STATIC IMPORTED)
set_property(TARGET AnyBlob PROPERTY IMPORTED_LOCATION ${BINARY_DIR}/libAnyBlob.a)
set_property(TARGET AnyBlob APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${ANYBLOB_INCLUDE_DIR})
add_dependencies(AnyBlob anyblob)
target_link_libraries(AnyBlob INTERFACE OpenSSL::SSL Threads::Threads ${LIBURING_LIBRARY} jemalloc)
