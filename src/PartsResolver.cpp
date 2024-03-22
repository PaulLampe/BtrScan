#include "PartsResolver.hpp"
#include "algorithm"
#include "btrblocks.hpp"
#include <algorithm>
#include <cstddef>
#include <map>
#include <optional>
#include <vector>

namespace btrscan {

using namespace std;

// columns -> Indices of the requested columns relative to the metadata
// ranges -> Pairs of row group indices specifying the requested (non-overlapping) ranges of row groups [start_row_group_id, end_row_group_id]
// meta -> BtrBlocks metadata for the requested table

map<ColumnIndex, pair<RowGroupMappedToChunkAndPart, PartMappedToCoveredRanges>>
PartsResolver::resolveDownloadParts(const vector<ColumnIndex> &columns,
                                    const vector<RangeType> &possiblyUnsortedRanges,
                                    const btrblocks::ArrowMetaData &meta) {

  map<ColumnIndex,
      pair<RowGroupMappedToChunkAndPart, PartMappedToCoveredRanges>>
      result;

  // Sort ranges for sequential processings
  vector<RangeType> ranges = possiblyUnsortedRanges;
  sort(ranges.begin(), ranges.end());

  for (ColumnIndex column_i : columns) {
    // RowGroupId mapped to the chunk_i and part_i where their chunk for this column can be retrieved from
    RowGroupMappedToChunkAndPart rowGroupToChunkAndPart(sumRanges(ranges));

    // (RowGroupId-) Ranges that are covered by this part -> when this part is downloaded we can set these ranges as available for this column
    PartMappedToCoveredRanges partCoveredRanges;

    auto columnMeta = meta.columnMeta[column_i];

    size_t range_i = 0, part_i = 0, currentChunk_i = 0, rowGroup_i = 0;

    // Iterate over all parts of this column
    while (part_i < columnMeta.numParts) {

      // Get the range of row group indices that is covered by this column part
      RowGroupIndex partStart = currentChunk_i;
      RowGroupIndex partEnd = currentChunk_i + columnMeta.chunksPerPart[part_i] - 1;

      // Get all ranges that are (partly) covered by this part
      while (range_i < ranges.size()) {
        auto range = ranges[range_i];

        // Get overlap interval
        size_t overlapStart = max(get<0>(range), partStart);
        size_t overlapEnd = min(get<1>(range), partEnd);

        // If range does not overlap with the current part break and check the next columnpart
        int overlapLength = overlapEnd - overlapStart + 1;
        if (overlapLength <= 0)
          break;

        // Add covered range to info about this part
        partCoveredRanges[part_i].push_back(
            make_tuple(rowGroup_i, rowGroup_i + overlapLength - 1));

        // If overlap does not start at the beginning of the part we need to take that offset into account
        int partOffset = overlapStart - partStart;
        
        for (int i = 0; i < overlapLength; ++i) {
          rowGroupToChunkAndPart[rowGroup_i] = make_tuple(part_i, partOffset + i);
          rowGroup_i++;
        }

        if (get<1>(range) <= partEnd) {
          range_i++;
        }

        if (partEnd <= get<1>(range)) {
          break;
        }
      }

      currentChunk_i += columnMeta.chunksPerPart[part_i];
      part_i++;
    }

    result[column_i] = make_pair(rowGroupToChunkAndPart, partCoveredRanges);
  }

  return result;
}
} // namespace btrscan