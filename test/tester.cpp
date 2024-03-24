// ---------------------------------------------------------------------------
// BtrScan
// ---------------------------------------------------------------------------
#include <iostream>
#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include "scheme/SchemePool.hpp"
// ---------------------------------------------------------------------------
using namespace btrblocks;
// ---------------------------------------------------------------------------
int main(int argc, char *argv[])
{
   testing::InitGoogleTest(&argc, argv);
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   return RUN_ALL_TESTS();
}
// ---------------------------------------------------------------------------
