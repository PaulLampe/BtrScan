enable_testing()

# ---------------------------------------------------------------------------
# BtrScan Tests
# ---------------------------------------------------------------------------

set(BTRSCAN_TEST_DIR ${CMAKE_CURRENT_LIST_DIR})
set(BTRSCAN_TEST_CASES_DIR ${BTRSCAN_TEST_DIR}/test-cases)

# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------

file(GLOB_RECURSE BTRSCAN_TEST_CASES ${BTRSCAN_TEST_CASES_DIR}/**.cpp ${BTRSCAN_TEST_CASES_DIR}/**.hpp)

# ---------------------------------------------------------------------------
# Tester
# ---------------------------------------------------------------------------

add_executable(btrscan_tester ${BTRSCAN_TEST_DIR}/tester.cpp ${BTRSCAN_TEST_CASES})
target_link_libraries(btrscan_tester BtrScan btrblocks Threads::Threads Arrow Parquet tbb GTest::gtest_main gflags)

include(GoogleTest)
gtest_discover_tests(btrscan_tester)

enable_testing()