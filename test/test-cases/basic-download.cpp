#include "Downloader.hpp"
#include "PartsResolver.hpp"

#include <cloud/aws.hpp>
#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <string>
#include <vector>


DEFINE_string(account_id, "Account Id",
                 "Account id");

DEFINE_string(access_key, "Access key",
                 "Private access key");

TEST(BasicDownload, SingleFile) {

  std::string key = FLAGS_access_key;
  
  auto downloader = btrscan::Downloader("s3://lampe-btrblocks-arrow-scan:eu-central-1", 1,FLAGS_account_id, key);
  auto tracker = btrscan::ProgressTracker();

  std::vector<btrscan::RangeType> ranges = {{0,0}};

  downloader.start(tracker, "data/medicare1_1/",ranges);
}