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
#include <tbb/blocked_range.h>
#include <tbb/parallel_for.h>
#include <utility>

namespace btrscan {

map<ColumnIndex, map<PartIndex, vector<uint8_t>>>
BtrLocalScanner::mmapColumns(vector<size_t> &columnIndices) {
  map<ColumnIndex, map<PartIndex, vector<uint8_t>>> output{};

  for (auto &columnIndex : columnIndices) {
    auto columnMeta = metadata.columnMeta[columnIndex];

    auto filePrefix = folder + "/column" + to_string(columnIndex) + "_part";

    for (size_t part_i = 0; part_i < columnMeta.numParts; part_i++) {
      auto filePath = filePrefix + to_string(part_i) + ".btr";
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
      btrblocks::MetaDataUtils::resolveColumnIndices(metadata, columns);

  auto columnParts = mmapColumns(columnIndices);

  auto schema =
      btrblocks::MetaDataUtils::resolveSchema(metadata, columnIndices);

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