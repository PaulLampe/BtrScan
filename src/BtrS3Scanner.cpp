#include "BtrS3Scanner.hpp"
#include "PartsResolver.hpp"
#include "types.hpp"
#include <arrow/api.h>
#include <future>
#include <sys/types.h>
#include <vector>

namespace btrscan {

using namespace std;

void BtrS3Scanner::prepareDataset(const string &filePrefix) {
  auto [data, offset, size] = _downloader.fetchMetaData(filePrefix);
  auto btrBlocksMeta = btrblocks::ArrowMetaData(
      std::vector<uint8_t>(data.get() + offset, data.get() + offset + size));

  _metaDataMap[filePrefix] = btrBlocksMeta;
};

void BtrS3Scanner::scan(
    const string &filePrefix, const vector<string> &columns,
    const function<void(shared_ptr<arrow::RecordBatch>)> &callback,
    const optional<vector<Range>> &ranges) {

  if (!_metaDataMap.contains(filePrefix)) {
    prepareDataset(filePrefix);
  }

  // Get btrblocks metadata
  auto &btrBlocksMeta = _metaDataMap[filePrefix];

  // Resolve column indices from meta and column names
  auto columnIndices =
      btrblocks::MetaDataUtils::resolveColumnIndices(btrBlocksMeta, columns);

  // Use full range if no range is passed
  const vector<Range> fullRange{{0, btrBlocksMeta.numChunks}};
  auto requestedRanges = ranges.value_or(std::move(fullRange));

  // Resolve parts meta
  auto partsMeta = btrscan::PartsResolver::resolveDownloadParts(
      columnIndices, requestedRanges, btrBlocksMeta);

  // Get number of row groups
  auto numberOfRowGroups =
      btrscan::PartsResolver::numberOfRowGroupsInRanges(requestedRanges);

  // Create progress tracker
  auto tracker =
      btrscan::ProgressTracker(numberOfRowGroups, columnIndices, partsMeta);

  // Get all files that are needed to cover the requested row groups
  std::vector<btrscan::FileIdentifier> fileIds =
      btrscan::PartsResolver::getFileIdentifiers(partsMeta);

  // Starter callback
  auto starter = [this, &tracker, &filePrefix, &fileIds]() {
    _downloader.start(tracker, filePrefix, fileIds);
  };

  // Keep track of the number fully downloaded (and therefore available) row groups
  size_t availableRowGroups = 0;

  // Get schema from metadata
  auto schema =
      btrblocks::MetaDataUtils::resolveSchema(btrBlocksMeta, columnIndices);

  // Actual callback 
  auto handleCallback = [&tracker, &availableRowGroups, &callback, &schema,
                         numberOfRowGroups]() {
    while (availableRowGroups != numberOfRowGroups) {
      do {
        auto data = tracker.getNextRowGroup();

        availableRowGroups++;
        auto batch = btrblocks::ArrowRowGroupDecompressor::decompressRowGroup(
            schema, data.value());

        callback(batch);
      } while (tracker.getNextRowGroup().has_value());
      usleep(100);
    }
  };

  std::thread starter_thread(starter);

  // start one (for now) processing thread
  std::future<void> f(async(std::launch::async, handleCallback));

  starter_thread.join();

  f.get();
};

} // namespace btrscan