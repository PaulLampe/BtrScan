#pragma once
#include "types.hpp"
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <set>
#include <utils/data_vector.hpp>
#include <vector>

namespace btrscan {

using namespace std;

struct DataRange {
  size_t offset;
  size_t size;
};

class ProgressTracker {
private:
  vector<ColumnIndex> _columns;
  set<RowGroupIndex> _availableRowGroups;
  map<ColumnIndex, vector<bool>> _columnDict;
  PartResolverMeta _meta;

  map<ColumnIndex, map<PartIndex, pair<unique_ptr<uint8_t[]>, DataRange>>>
      _data;

  mutex _global_lock{};

public:
  explicit ProgressTracker(int numberOfRowGroups,
                           const vector<ColumnIndex> &columns,
                           const PartResolverMeta &meta);
  void registerDownload(uint column, uint part, unique_ptr<uint8_t[]> result,
                        size_t offset, size_t size);
  void registerProcessed();
  optional<map<ColumnIndex, pair<DownloadDataVectorType, PartInternalOffset>>>
  getNextRowGroup();
};

} // namespace btrscan