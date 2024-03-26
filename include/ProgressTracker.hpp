#pragma once
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <set>
#include <utils/data_vector.hpp>
#include <vector>
#include "types.hpp"

namespace btrscan {

using namespace std;

class ProgressTracker {
private:
    vector<ColumnIndex> _columns;
    set<RowGroupIndex> _availableRowGroups;
    map<ColumnIndex, vector<bool>> _columnDict;
    PartResolverMeta _meta;

    map< ColumnIndex, map< PartIndex, shared_ptr<DownloadDataVectorType> > > _data;

    mutex _global_lock{};

public:
  explicit ProgressTracker(int numberOfRowGroups, const vector<ColumnIndex>& columns, const PartResolverMeta& meta);
  void registerDownload(uint column, uint part, unique_ptr<vector<uint8_t>> result);
  void registerProcessed();
  optional<map<ColumnIndex, pair<shared_ptr<DownloadDataVectorType>, PartInternalOffset>>> getNextRowGroup();
};

} // namespace btrscan