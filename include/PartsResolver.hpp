#include "btrblocks.hpp"
#include <cstddef>
#include <vector>
#include "types.hpp"

namespace btrscan {

using namespace std;

class PartsResolver {

public:
  static PartResolverMeta resolveDownloadParts(const vector<ColumnIndex> &columns,
                       const vector<Range> &ranges,
                       const btrblocks::ArrowMetaData &meta);

  static vector<FileIdentifier> getFileIdentifiers(PartResolverMeta& meta);

  static size_t numberOfRowGroupsInRanges(const vector<Range> &ranges) {
    size_t sum = 0;
    for (const auto &range : ranges) {
      sum += (range.end - range.start + 1);
    }
    return sum;
  }
};
} // namespace btrscan