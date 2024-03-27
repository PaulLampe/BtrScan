#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include "types.hpp"
#include <memory>
#include "arrow/ArrowMetaData.hpp"

namespace btrscan {

using namespace std;

class BtrLocalScanner {

unordered_map<string, btrblocks::ArrowMetaData> _metaDataMap;


public:

  void prepareDataset(const string &filePrefix);

  void scan(
    const string &filePrefix, const vector<string> &columns,
    const function<void(shared_ptr<arrow::RecordBatch>)> &callback,
    const optional<vector<Range>>& ranges);

  shared_ptr<arrow::Schema> getSchema();

private:

  btrblocks::ArrowMetaData metadata;
  string folder;

  map<ColumnIndex, map<PartIndex, vector<uint8_t>>> mmapColumns(vector<size_t>& columnIndices);
};


} // namespace btrblocks