# -------------------------------------------------------------------------------
# btrscan
# -------------------------------------------------------------------------------
cmake_minimum_required(VERSION 3.22)
project(btrscan VERSION 0.0.1)

# -------------------------------------------------------------------------------
# C++ build options
# -------------------------------------------------------------------------------

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -g")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pthread -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -g")
add_compile_options(-march=native -Wall -Wextra)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/thirdparty)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(BUILD_ARROW "BUILD_ARROW" ON)

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

if (${BUILD_ARROW})
include("thirdparty/arrow.cmake")
endif()

include("thirdparty/anyblob.cmake")
include("thirdparty/btrblocks.cmake")
include("thirdparty/gtest.cmake")
include("thirdparty/gflags.cmake")

# ---------------------------------------------------------------------------
# include
# ---------------------------------------------------------------------------

include("${PROJECT_SOURCE_DIR}/include/local.cmake")

# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

include("${PROJECT_SOURCE_DIR}/src/local.cmake")

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

include("${PROJECT_SOURCE_DIR}/test/local.cmake")

# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------

add_library(BtrScan STATIC ${SRC_CPP} ${INCLUDE_HPP})
target_link_libraries(BtrScan PUBLIC AnyBlob btrblocks Arrow)
target_include_directories(BtrScan PUBLIC ${PROJECT_SOURCE_DIR}/include)
