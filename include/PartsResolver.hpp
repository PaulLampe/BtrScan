#include "btrblocks.hpp"
#include <cstddef>
#include <map>
#include <vector>

namespace btrscan {

using namespace std;

using ColumnIndex = size_t;
using PartIndex = size_t;
using ChunkIndex = size_t;

using RowGroupIndex = size_t;

using RangeType = tuple<RowGroupIndex, RowGroupIndex>;

using PartMappedToCoveredRanges = map<PartIndex, vector<RangeType>>;

using RowGroupMappedToChunkAndPart = vector<tuple<PartIndex, ChunkIndex>>;

class PartsResolver {

public:
  map<ColumnIndex,
      pair<RowGroupMappedToChunkAndPart, PartMappedToCoveredRanges>>
  resolveDownloadParts(const vector<ColumnIndex> &columns,
                       const vector<RangeType> &ranges,
                       const btrblocks::ArrowMetaData &meta);

private:
  int sumRanges(const vector<RangeType> &ranges) {
    int sum = 0;
    for (const auto &range : ranges) {
      sum += (get<1>(range) - get<0>(range) + 1);
    }
    return sum;
  }
};
} // namespace btrscan