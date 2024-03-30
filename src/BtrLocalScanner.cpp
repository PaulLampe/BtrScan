#include "BtrLocalScanner.hpp"
#include "PartsResolver.hpp"
#include "btrblocks.hpp"
#include "common/Utils.hpp"
#include "types.hpp"
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <cstddef>
#include <cstdint>
#include <map>
#include <sys/types.h>
#include <tbb/blocked_range.h>
#include <tbb/parallel_for.h>
#include <utility>

namespace btrscan {

void BtrLocalScanner::prepareDataset(const string &filePrefix) {
  vector<uint8_t> data;
  btrblocks::Utils::readFileToMemory(filePrefix + "/metadata.btr", data);
  auto btrBlocksMeta = btrblocks::ArrowMetaData(data);
  _metaDataMap[filePrefix] = btrBlocksMeta;
};

shared_ptr<arrow::Schema> BtrLocalScanner::getSchema(const string &filePrefix) {
  if (!_metaDataMap.contains(filePrefix)) {
    prepareDataset(filePrefix);
  }
  auto &btrBlocksMeta = _metaDataMap[filePrefix];

  vector<size_t> columnIndices(btrBlocksMeta.numColumns);
  std::iota(columnIndices.begin(), columnIndices.end(), 0);

  return btrblocks::MetaDataUtils::resolveSchema(btrBlocksMeta,columnIndices);
};


map<ColumnIndex, map<PartIndex, vector<uint8_t>>>
BtrLocalScanner::mmapColumns(const string &filePrefix, vector<size_t> &columnIndices) {
  if (!_metaDataMap.contains(filePrefix)) {
    prepareDataset(filePrefix);
  }
  auto &btrBlocksMeta = _metaDataMap[filePrefix];

  map<ColumnIndex, map<PartIndex, vector<uint8_t>>> output{};

  for (auto &columnIndex : columnIndices) {
    auto columnMeta = btrBlocksMeta.columnMeta[columnIndex];

    auto columnPartPrefix = filePrefix + "/column" + to_string(columnIndex) + "_part";

    for (size_t part_i = 0; part_i < columnMeta.numParts; part_i++) {
      auto filePath = columnPartPrefix + to_string(part_i) + ".btr";
      output[columnIndex][part_i] = btrblocks::Utils::mmapFile(filePath);
    }
  }

  return output;
}

void BtrLocalScanner::scan(
    const string &filePrefix, const vector<string> &columns,
    const function<void(shared_ptr<arrow::RecordBatch>)> &callback,
    const optional<vector<Range>> &ranges) {

  if (!_metaDataMap.contains(filePrefix)) {
    prepareDataset(filePrefix);
  }
  auto &btrBlocksMeta = _metaDataMap[filePrefix];

  const vector<Range> fullRange{{0, btrBlocksMeta.numChunks}};
  auto requestedRanges = ranges.value_or(std::move(fullRange));

  auto columnIndices =
      btrblocks::MetaDataUtils::resolveColumnIndices(btrBlocksMeta, columns);

  auto columnParts = mmapColumns(filePrefix, columnIndices);

  auto schema =
      btrblocks::MetaDataUtils::resolveSchema(btrBlocksMeta, columnIndices);

  auto resolvedParts = PartsResolver::resolveDownloadParts(
      columnIndices, requestedRanges, btrBlocksMeta);

  for (auto &requestedRange : requestedRanges) {
    tbb::blocked_range<size_t> range(requestedRange.start, requestedRange.end);

    tbb::parallel_for(range, [&](const auto &r) {
      for (auto chunk_i = r.begin(); chunk_i < r.end(); chunk_i++) {

        auto &rowGroupLocations = resolvedParts.rowGroupLocations[chunk_i];

        map<ColumnIndex, std::pair<CompressedDataType, PartInternalOffset>>
            data{};

        for (auto &column_i : columnIndices) {
          auto &location = rowGroupLocations[column_i];

          auto span = std::span(columnParts[column_i][location.part].begin(),
                                columnParts[column_i][location.part].end());

          data[column_i] = make_pair(span, location.offset);
        }

        auto batch = btrblocks::ArrowRowGroupDecompressor::decompressRowGroup(
            schema, data);

        callback(batch);
      }
    });
  }
}

} // namespace btrscan