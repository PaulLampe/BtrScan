#include "arrow/ArrowMetaData.hpp"
#include "PartsResolver.hpp"
#include "common/Units.hpp"
#include "types.hpp"
#include <gtest/gtest.h>
#include <tuple>
#include <utility>
#include <vector>


TEST(MetaDataResolver, ShouldResolve) {
  btrblocks::ArrowMetaData meta{
    3,
    3,
    {
      btrblocks::ArrowMetaData::ArrowColumnMetaData{
        2,
        btrblocks::ColumnType::INTEGER,
        "column0_int",
        std::vector<btrblocks::u32>{1,2},
      },
      btrblocks::ArrowMetaData::ArrowColumnMetaData{
        3,
        btrblocks::ColumnType::FLOAT,
        "column1_float",
        std::vector<btrblocks::u32>{1,1,1},
      },
      btrblocks::ArrowMetaData::ArrowColumnMetaData{
        1,
        btrblocks::ColumnType::STRING,
        "column2_str",
        std::vector<btrblocks::u32>{3},
      }
    }
  };

  auto resolvedMeta = btrscan::PartsResolver::resolveDownloadParts({0, 1, 2}, {{0,3}}, meta);

  auto rowGroupLocations = resolvedMeta.rowGroupLocations;

  auto rg_0_locations = rowGroupLocations[0];

  auto rgl = [](size_t c, size_t p) {return btrscan::RowGroupColumnLocation(c,p);};

  ASSERT_EQ(rg_0_locations[0], rgl(0,0));
  ASSERT_EQ(rg_0_locations[1], rgl(0,0));
  ASSERT_EQ(rg_0_locations[2], rgl(0,0));

  auto rg_1_locations = rowGroupLocations[1];

  ASSERT_EQ(rg_1_locations[0], rgl(1,0));
  ASSERT_EQ(rg_1_locations[1], rgl(1,0));
  ASSERT_EQ(rg_1_locations[2], rgl(0,1));

  auto rg_2_locations = rowGroupLocations[2];

  ASSERT_EQ(rg_2_locations[0], rgl(1,1));
  ASSERT_EQ(rg_2_locations[1], rgl(2,0));
  ASSERT_EQ(rg_2_locations[2], rgl(0,2));

  auto coveredRanges = resolvedMeta.columnPartCoveringRanges;

  auto col_0_ranges = coveredRanges[0];

  auto rgVec = [](size_t c, size_t p) {return std::vector<btrscan::Range>{{c,p}};};

  ASSERT_EQ(col_0_ranges[0], rgVec(0,0));
  ASSERT_EQ(col_0_ranges[1], rgVec(1,2));

  auto col_1_ranges = coveredRanges[1];

  ASSERT_EQ(col_1_ranges[0], rgVec(0,0));
  ASSERT_EQ(col_1_ranges[1], rgVec(1,1));
  ASSERT_EQ(col_1_ranges[2], rgVec(2,2));

  auto col_2_ranges = coveredRanges[2];

  ASSERT_EQ(col_2_ranges[0], rgVec(0,2));

  std::vector<btrscan::FileIdentifier> fileIds = btrscan::PartsResolver::getFileIdentifiers(resolvedMeta);

  std::vector<btrscan::FileIdentifier> expectedFileIds = {{0,0},{1,0},{2,0},{0,1}, {1,1}, {1,2}};

  ASSERT_EQ(fileIds, expectedFileIds);
}