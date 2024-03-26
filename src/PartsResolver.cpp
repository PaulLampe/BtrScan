#include "PartsResolver.hpp"
#include "algorithm"
#include "btrblocks.hpp"
#include "types.hpp"
#include <algorithm>
#include <cstddef>
#include <map>
#include <optional>
#include <unordered_set>
#include <utility>
#include <vector>

namespace btrscan {

using namespace std;

// columns -> Indices of the requested columns relative to the metadata
// ranges -> Pairs of row group indices specifying the requested
// (non-overlapping) ranges of row groups [start_row_group_id, end_row_group_id]
// meta -> BtrBlocks metadata for the requested table

PartResolverMeta
PartsResolver::resolveDownloadParts(const vector<ColumnIndex> &columns,
                                    const vector<Range> &possiblyUnsortedRanges,
                                    const btrblocks::ArrowMetaData &meta) {

  PartResolverMeta result;

  // Sort ranges for sequential processings
  vector<Range> ranges = possiblyUnsortedRanges;
  sort(ranges.begin(), ranges.end());

  for (ColumnIndex column_i : columns) {
    auto columnMeta = meta.columnMeta[column_i];

    size_t range_i = 0, part_i = 0, currentChunk_i = 0, rowGroup_i = 0;

    // Iterate over all parts of this column
    while (part_i < columnMeta.numParts) {

      // Get the range of row group indices that is covered by this column part
      RowGroupIndex partStart = currentChunk_i;
      RowGroupIndex partEnd =
          currentChunk_i + columnMeta.chunksPerPart[part_i] - 1;

      // Get all ranges that are (partly) covered by this part
      while (range_i < ranges.size()) {
        auto range = ranges[range_i];

        // Get overlap interval
        size_t overlapStart = max(range.start, partStart);
        size_t overlapEnd = min(range.end, partEnd);

        // If range does not overlap with the current part break and check the
        // next columnpart
        int overlapLength = overlapEnd - overlapStart + 1;
        if (overlapLength <= 0)
          break;

        // Add covered range to info about this part
        result.columnPartCoveringRanges[column_i][part_i].push_back(
            Range(rowGroup_i, rowGroup_i + overlapLength - 1));

        // If overlap does not start at the beginning of the part we need to
        // take that offset into account
        int partOffset = overlapStart - partStart;

        for (int i = 0; i < overlapLength; ++i) {
          result.rowGroupLocations[rowGroup_i][column_i] =
              RowGroupColumnLocation(part_i, partOffset + i);
          rowGroup_i++;
        }

        if (range.end <= partEnd) {
          range_i++;
        }

        if (partEnd <= range.end) {
          break;
        }
      }

      currentChunk_i += columnMeta.chunksPerPart[part_i];
      part_i++;
    }
  }

  return result;
}

vector<FileIdentifier>
PartsResolver::getFileIdentifiers(PartResolverMeta &meta) {
  vector<pair<RowGroupIndex, FileIdentifier>> rowGroupFileIDs;
  unordered_set<FileIdentifier, FileIdentifierHash> included{};

  for (auto &[rowGroup, columnPartOffsetMap] : meta.rowGroupLocations) {
    for (auto &[column, partOffset] : columnPartOffsetMap) {
      FileIdentifier fileId(column, partOffset.part);

      if (!included.contains(fileId)) {
        included.insert(fileId);
        rowGroupFileIDs.push_back(
            make_pair(rowGroup, FileIdentifier(column, partOffset.part)));
      }
    }
  }

  sort(rowGroupFileIDs.begin(), rowGroupFileIDs.end(),
       [](const auto &p1, const auto &p2) {
         // sort by first covered row group, else by part, then column
         if (p1.first == p2.first) {
           return p1.second < p2.second;
         }
         return p1.first < p2.first;
       });

  vector<FileIdentifier> fileIds{};

  for (auto &[_, fileId] : rowGroupFileIDs) {
    fileIds.push_back(fileId);
  }

  return fileIds;
}

} // namespace btrscan