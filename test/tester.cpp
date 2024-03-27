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
DEFINE_string(account_id, "Account Id", "Account id");

DEFINE_string(access_key, "Access key", "Private access key");
// ---------------------------------------------------------------------------
int main(int argc, char *argv[])
{
   testing::InitGoogleTest(&argc, argv);
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   return RUN_ALL_TESTS();
}
// ---------------------------------------------------------------------------
