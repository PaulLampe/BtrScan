#pragma once
#include "types.hpp"
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <utils/data_vector.hpp>
#include <vector>

namespace btrscan {

using namespace std;

class ProgressTracker {
private:
  vector<ColumnIndex> _columns;
  set<RowGroupIndex> _availableRowGroups;
  unordered_map<ColumnIndex, vector<bool>> _columnDict;
  PartResolverMeta _meta;

  unordered_map<ColumnIndex, unordered_map<PartIndex, CompressedColumnPartReference>> _data;

  mutex _global_lock{};

public:
  explicit ProgressTracker(int numberOfRowGroups,
                           const vector<ColumnIndex> &columns,
                           const PartResolverMeta &meta);
  void registerDownload(uint column, uint part, unique_ptr<uint8_t[]> result,
                        size_t offset, size_t size);
  void registerProcessed();
  optional<unordered_map<ColumnIndex, pair<CompressedDataType, PartInternalOffset>>>
  getNextRowGroup();
};

} // namespace btrscan