#include "Downloader.hpp"
#include "PartsResolver.hpp"
#include "ProgressTracker.hpp"
#include "arrow/ArrowMetaData.hpp"
#include "types.hpp"
#include <cloud/aws.hpp>
#include <cstddef>
#include <future>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <vector>

DECLARE_string(account_id);
DECLARE_string(access_key);

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

  auto columns = std::vector<size_t>{0, 1, 2, 3, 4, 5};
  auto ranges = std::vector<btrscan::Range>{{0, 131}};

  auto partsMeta = btrscan::PartsResolver::resolveDownloadParts(columns, ranges,
                                                                btrBlocksMeta);

  auto numberOfRowGroups =
      btrscan::PartsResolver::numberOfRowGroupsInRanges(ranges);

  auto tracker =
      btrscan::ProgressTracker(numberOfRowGroups, columns, partsMeta);

  std::vector<btrscan::FileIdentifier> fileIds =
      btrscan::PartsResolver::getFileIdentifiers(partsMeta);

  auto starter = [&downloader, &tracker, &fileIds]() {
    downloader.start(tracker, "data/medicare1_1/", fileIds);
  };

  size_t availableRowGroups = 0;

  auto checkAvailable = [&tracker, &availableRowGroups, numberOfRowGroups]() {
    while (availableRowGroups != numberOfRowGroups) {
      while (tracker.getNextRowGroup().has_value()) {
        availableRowGroups++;
      }
    }
  };

  std::thread starter_thread(starter);

  std::future<void> f(async(std::launch::async, checkAvailable));

  starter_thread.join();

  f.get();
}