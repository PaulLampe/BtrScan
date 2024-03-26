#include "Downloader.hpp"
#include "PartsResolver.hpp"
#include "ProgressTracker.hpp"
#include "arrow/ArrowMetaData.hpp"
#include "types.hpp"

#include <cloud/aws.hpp>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

DEFINE_string(account_id, "Account Id", "Account id");

DEFINE_string(access_key, "Access key", "Private access key");

TEST(DownloadBasic, SingleFile) {
  btrblocks::ArrowMetaData btrBlocksMeta{0, 0, {}};
  auto columns = std::vector<size_t>{0};
  auto ranges = std::vector<btrscan::Range>{{0, 0}};
  auto dummyMeta = btrscan::PartsResolver::resolveDownloadParts({}, {}, {});

  auto downloader =
      btrscan::Downloader("s3://lampe-btrblocks-arrow-scan:eu-central-1", 1,
                          FLAGS_account_id, FLAGS_access_key);
  auto tracker = btrscan::ProgressTracker(1, {0}, dummyMeta);

  std::vector<btrscan::FileIdentifier> fileIds = {{0, 0}};

  downloader.start(tracker, "data/medicare1_1/", fileIds);
}

TEST(DownloadBasic, SingleColumnSingleRow) {
  std::string filePrefix = "data/medicare1_1/";

  auto downloader =
      btrscan::Downloader("s3://lampe-btrblocks-arrow-scan:eu-central-1", 1,
                          FLAGS_account_id, FLAGS_access_key);

  auto [data, offset, size] = downloader.fetchMetaData(filePrefix);
  auto btrBlocksMeta = btrblocks::ArrowMetaData(
      std::vector<uint8_t>(data.get() + offset, data.get() + offset + size));

  auto columns = std::vector<size_t>{1};
  auto ranges = std::vector<btrscan::Range>{{40, 100}};

  auto partsMeta = btrscan::PartsResolver::resolveDownloadParts(columns, ranges,
                                                                btrBlocksMeta);

  auto tracker = btrscan::ProgressTracker(
      btrscan::PartsResolver::numberOfRowGroupsInRanges(ranges), columns,
      partsMeta);

  std::vector<btrscan::FileIdentifier> fileIds =
      btrscan::PartsResolver::getFileIdentifiers(partsMeta);

  std::vector<btrscan::FileIdentifier> expectedFileIds{{1, 1}, {1, 2}};

  ASSERT_EQ(fileIds, expectedFileIds);
  downloader.start(tracker, "data/medicare1_1/", fileIds);
}