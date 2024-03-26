#include "PartsResolver.hpp"
#include "ProgressTracker.hpp"
#include "types.hpp"

#include <gtest/gtest.h>
#include <string>
#include <vector>

TEST(ProgressTracker, Begin) {}

TEST(ProgressTracker, SingleColumnSingleRowGroup) {
  btrblocks::ArrowMetaData btrBlocksMeta{
      1,
      1,
      {
          btrblocks::ArrowMetaData::ArrowColumnMetaData{
              1,
              btrblocks::ColumnType::INTEGER,
              "column0_int",
              std::vector<btrblocks::u32>{1},
          },
      }};

  auto columns = std::vector<size_t>{0};
  auto ranges = std::vector<btrscan::Range>{{0, 0}};

  auto partsMeta = btrscan::PartsResolver::resolveDownloadParts(columns, ranges,
                                                                btrBlocksMeta);

  auto tracker = btrscan::ProgressTracker(
      btrscan::PartsResolver::numberOfRowGroupsInRanges(ranges), columns,
      partsMeta);

  std::vector<btrscan::FileIdentifier> fileIds = {{0, 0}};

  tracker.registerDownload(0, 0, std::make_unique<std::vector<uint8_t>>());

  auto availableRowGroup = tracker.getNextRowGroup();

  ASSERT_TRUE(availableRowGroup.has_value());

  ASSERT_EQ(availableRowGroup.value().size(), 1);
  ASSERT_EQ(availableRowGroup.value().size(), 1);
}

TEST(ProgressTracker, SingleColumnMultiRowGroup) {
  btrblocks::ArrowMetaData btrBlocksMeta{
      1,
      6,
      {
          btrblocks::ArrowMetaData::ArrowColumnMetaData{
              3,
              btrblocks::ColumnType::INTEGER,
              "column0_int",
              std::vector<btrblocks::u32>{1, 2, 3},
          },
      }};

  auto columns = std::vector<size_t>{0};
  auto ranges = std::vector<btrscan::Range>{{0, 5}};

  auto partsMeta = btrscan::PartsResolver::resolveDownloadParts(columns, ranges,
                                                                btrBlocksMeta);

  auto tracker = btrscan::ProgressTracker(
      btrscan::PartsResolver::numberOfRowGroupsInRanges(ranges), columns,
      partsMeta);

  std::vector<btrscan::FileIdentifier> fileIds =
      btrscan::PartsResolver::getFileIdentifiers(partsMeta);

  std::vector<btrscan::FileIdentifier> expectedFileIds = {
      {0, 0}, {0, 1}, {0, 2}};

  ASSERT_EQ(fileIds, expectedFileIds);

  tracker.registerDownload(0, 0, std::make_unique<std::vector<uint8_t>>());

  for (int i = 0; i < 1; ++i) {
    auto availableRowGroup = tracker.getNextRowGroup();
    ASSERT_TRUE(availableRowGroup.has_value());
    ASSERT_EQ(availableRowGroup.value().size(), 1);
    ASSERT_EQ(availableRowGroup.value()[0].second, i);
  }

  ASSERT_FALSE(tracker.getNextRowGroup().has_value());

  tracker.registerDownload(0, 1, std::make_unique<std::vector<uint8_t>>());

  for (int i = 0; i < 2; ++i) {
    auto availableRowGroup = tracker.getNextRowGroup();
    ASSERT_TRUE(availableRowGroup.has_value());
    ASSERT_EQ(availableRowGroup.value().size(), 1);
    ASSERT_EQ(availableRowGroup.value()[0].second, i);
  }

  ASSERT_FALSE(tracker.getNextRowGroup().has_value());

  tracker.registerDownload(0, 2, std::make_unique<std::vector<uint8_t>>());

  for (int i = 0; i < 3; ++i) {
    auto availableRowGroup = tracker.getNextRowGroup();
    ASSERT_TRUE(availableRowGroup.has_value());
    ASSERT_EQ(availableRowGroup.value().size(), 1);
    ASSERT_EQ(availableRowGroup.value()[0].second, i);
  }

  ASSERT_FALSE(tracker.getNextRowGroup().has_value());
}

TEST(ProgressTracker, MultiColumnMultiRowGroup) {
  btrblocks::ArrowMetaData btrBlocksMeta{
      1,
      6,
      {
          btrblocks::ArrowMetaData::ArrowColumnMetaData{
              3,
              btrblocks::ColumnType::INTEGER,
              "column0_int",
              std::vector<btrblocks::u32>{1, 2, 3},
          },
          btrblocks::ArrowMetaData::ArrowColumnMetaData{
              4,
              btrblocks::ColumnType::STRING,
              "column1_str",
              std::vector<btrblocks::u32>{2, 1, 2, 1},
          },
          btrblocks::ArrowMetaData::ArrowColumnMetaData{
              2,
              btrblocks::ColumnType::DOUBLE,
              "column2_dbl",
              std::vector<btrblocks::u32>{5, 1},
          },
      }};

  auto columns = std::vector<size_t>{0, 1, 2};
  auto ranges = std::vector<btrscan::Range>{{0, 5}};

  auto partsMeta = btrscan::PartsResolver::resolveDownloadParts(columns, ranges,
                                                                btrBlocksMeta);

  auto tracker = btrscan::ProgressTracker(
      btrscan::PartsResolver::numberOfRowGroupsInRanges(ranges), columns,
      partsMeta);

  std::vector<btrscan::FileIdentifier> fileIds =
      btrscan::PartsResolver::getFileIdentifiers(partsMeta);

  std::vector<btrscan::FileIdentifier> expectedFileIds = {
      {0, 0}, {1, 0}, {2, 0}, {0, 1}, {1, 1}, {0, 2}, {1, 2}, {1, 3},{2, 1}};

  ASSERT_EQ(fileIds, expectedFileIds);

  tracker.registerDownload(0, 0, std::make_unique<std::vector<uint8_t>>());
  ASSERT_FALSE(tracker.getNextRowGroup().has_value());

  tracker.registerDownload(1, 0, std::make_unique<std::vector<uint8_t>>());
  ASSERT_FALSE(tracker.getNextRowGroup().has_value());

  tracker.registerDownload(2, 0, std::make_unique<std::vector<uint8_t>>());

  auto availableRowGroup = tracker.getNextRowGroup();
  ASSERT_TRUE(availableRowGroup.has_value());
  ASSERT_EQ(availableRowGroup.value().size(), 3);
  // TODO
  ASSERT_FALSE(tracker.getNextRowGroup().has_value());

  tracker.registerDownload(0, 1, std::make_unique<std::vector<uint8_t>>());

  availableRowGroup = tracker.getNextRowGroup();
  ASSERT_TRUE(availableRowGroup.has_value());
  ASSERT_EQ(availableRowGroup.value().size(), 3);
  // TODO
  ASSERT_FALSE(tracker.getNextRowGroup().has_value());

  tracker.registerDownload(1, 1, std::make_unique<std::vector<uint8_t>>());

  availableRowGroup = tracker.getNextRowGroup();
  ASSERT_TRUE(availableRowGroup.has_value());
  ASSERT_EQ(availableRowGroup.value().size(), 3);
  // TODO
  ASSERT_FALSE(tracker.getNextRowGroup().has_value());

  tracker.registerDownload(0,2, std::make_unique<std::vector<uint8_t>>());
  ASSERT_FALSE(tracker.getNextRowGroup().has_value());

  tracker.registerDownload(1,2, std::make_unique<std::vector<uint8_t>>());
  
  availableRowGroup = tracker.getNextRowGroup();
  ASSERT_TRUE(availableRowGroup.has_value());
  ASSERT_EQ(availableRowGroup.value().size(), 3);
  // TODO
  availableRowGroup = tracker.getNextRowGroup();
  ASSERT_TRUE(availableRowGroup.has_value());
  ASSERT_EQ(availableRowGroup.value().size(), 3);
  
  ASSERT_FALSE(tracker.getNextRowGroup().has_value());

  tracker.registerDownload(1,3, std::make_unique<std::vector<uint8_t>>());
  ASSERT_FALSE(tracker.getNextRowGroup().has_value());

  tracker.registerDownload(2,1, std::make_unique<std::vector<uint8_t>>());
  
  availableRowGroup = tracker.getNextRowGroup();
  ASSERT_TRUE(availableRowGroup.has_value());
  ASSERT_EQ(availableRowGroup.value().size(), 3);

  ASSERT_FALSE(tracker.getNextRowGroup().has_value());
}