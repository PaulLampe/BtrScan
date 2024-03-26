#include "ProgressTracker.hpp"
#include "types.hpp"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace btrscan {

using namespace std;

/*void ProgressTracker::registerDownload(uint column, uint part,
                                       unique_ptr<vector<uint8_t>> result) {

};*/

ProgressTracker::ProgressTracker(int numberOfRowGroups,
                                 const vector<ColumnIndex> &columns,
                                 const PartResolverMeta &meta)
    : _columns(columns), _meta(meta) {
  for (auto col : columns) {
    _columnDict[col] = std::vector<bool>(numberOfRowGroups, false);
  }
}

void ProgressTracker::registerDownload(uint column, uint part,
                                       unique_ptr<uint8_t[]> result,
                                       size_t offset, size_t size) {
  std::map<int, bool> finishedRowGroupStatus;

  _data[column][part] =
      make_pair(std::move(result), DataRange{.offset = offset, .size = size});

  auto coveredRanges = _meta.columnPartCoveringRanges[column][part];

  {
    lock_guard<mutex> lock(_global_lock);

    auto &finishedColRg = _columnDict[column];

    // Check for finished row_groups
    for (auto &range : coveredRanges) {
      for (size_t rg_i = range.start; rg_i <= range.end; ++rg_i) {
        finishedColRg[rg_i] = true;
        finishedRowGroupStatus[rg_i] = true;
      }
    }

    // Check for finished row_groups
    for (auto &colRg : _columnDict) {
      for (auto &range : coveredRanges) {
        for (size_t rg_i = range.start; rg_i <= range.end; ++rg_i) {
          if (finishedRowGroupStatus.find(rg_i) !=
              finishedRowGroupStatus.end()) {
            finishedRowGroupStatus[rg_i] =
                finishedRowGroupStatus[rg_i] && colRg.second[rg_i];
          }
        }
      }
    }

    for (auto &rgFinishStatus : finishedRowGroupStatus) {
      if (rgFinishStatus.second) {
        _availableRowGroups.insert(rgFinishStatus.first);
      }
    }
  }
}

optional<map<ColumnIndex, pair<DownloadDataVectorType, PartInternalOffset>>>
ProgressTracker::getNextRowGroup() {
  size_t row_group_i;

  {
    lock_guard<mutex> lock(_global_lock);
    if (_availableRowGroups.empty()) {
      return nullopt;
    }
    row_group_i = *_availableRowGroups.begin();
    _availableRowGroups.erase(_availableRowGroups.begin());
  }

  map<ColumnIndex, pair<DownloadDataVectorType, PartInternalOffset>>
      rowGroupData;

  auto rowGroupToChunkAndPart = _meta.rowGroupLocations[row_group_i];

  for (auto &column : _columns) {
    auto partAndOffset = rowGroupToChunkAndPart[column];

    // ownership stays with progresstracker, is guaranteed to stay alive until
    // the last rowroup in this part is finished
    auto &[data_ptr, range] = _data[column][partAndOffset.part];

    vector<uint8_t> data_vector(data_ptr.get() + range.offset,
                                data_ptr.get() + range.offset + range.size);

    rowGroupData[column] = make_pair(data_vector, partAndOffset.offset);
  }

  return rowGroupData;
}
} // namespace btrscan
