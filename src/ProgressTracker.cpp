#include "ProgressTracker.hpp"
#include "types.hpp"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <span>
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
  for (auto &col : _columns) {
    _columnDict[col] = std::vector<bool>(numberOfRowGroups, false);
  }
}

void ProgressTracker::registerDownload(uint column, uint part,
                                       unique_ptr<uint8_t[]> result,
                                       size_t offset, size_t size) {
  map<size_t, bool> finishedRowGroupStatus;

  {
    lock_guard<mutex> lock(_global_lock);

    this->_data[column][part] = CompressedColumnPartReference{
        .data = std::move(result),
        .range = DataRange{.offset = offset, .size = size}};

    auto &coveredRanges = _meta.columnPartCoveringRanges[column][part];

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

optional<
    map<ColumnIndex, pair<CompressedDataType, PartInternalOffset>>>
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

  map<ColumnIndex, pair<CompressedDataType, PartInternalOffset>>
      rowGroupData;

  auto rowGroupToChunkAndPart = _meta.rowGroupLocations[row_group_i];

  for (auto &column : _columns) {
    auto& partAndOffset = rowGroupToChunkAndPart[column];

    // ownership stays with progresstracker, is guaranteed to stay alive until
    // the last rowroup in this part is finished
    auto &dataReference = _data[column][partAndOffset.part];
    auto startPtr = dataReference.data.get() + dataReference.range.offset;

    auto span =
        std::span<uint8_t>(startPtr, startPtr + dataReference.range.size);

    rowGroupData[column] = make_pair(span, partAndOffset.offset);
  }

  return rowGroupData;
}
} // namespace btrscan
