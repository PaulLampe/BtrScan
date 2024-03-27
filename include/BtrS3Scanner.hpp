
#include "Downloader.hpp"
#include "arrow/ArrowMetaData.hpp"
#include <arrow/api.h>
#include <memory>
#include <string>
#include <sys/types.h>
#include <unordered_map>
namespace btrscan {

using namespace std;

class BtrS3Scanner {
  Downloader &_downloader;
  unordered_map<string, btrblocks::ArrowMetaData> _metaDataMap;

public:
  explicit BtrS3Scanner(Downloader &downloader) : _downloader(downloader) {}

  void prepareDataset(const string &filePrefix);

  void scan(const string &filePrefix, const vector<string> &columns,
            const function<void(shared_ptr<arrow::RecordBatch>)> &callback,
            const optional<vector<Range>> &ranges);
};

} // namespace btrscan