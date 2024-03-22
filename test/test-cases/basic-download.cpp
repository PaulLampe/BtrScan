#include "Downloader.hpp"
#include "PartsResolver.hpp"

#include <gtest/gtest.h>
#include <vector>

TEST(BasicDownload, SingleFile) {
  auto downloader = btrscan::Downloader("s3://lampe-btrblocks-arrow-scan:eu-central-1", 1);
  auto tracker = btrscan::ProgressTracker();

  std::vector<btrscan::RangeType> ranges = {{0,0}};

  downloader.start(tracker, "data/medicare1_1/",ranges);
}