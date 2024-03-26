#include "Downloader.hpp"
#include "BtrBlocksFileNameResolver.hpp"
#include "btrblocks.hpp"
#include "cloud/aws.hpp"
#include "network/tasked_send_receiver.hpp"
#include "network/transaction.hpp"
#include "types.hpp"
#include <cstddef>
#include <cstdint>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <sys/types.h>
#include <tuple>
#include <unistd.h>
#include <vector>

namespace btrscan {

using namespace anyblob;
using namespace std;

Downloader::Downloader(string uri, uint concurrentThreads, string accountId,
                       string key) {
  // ---------------------------------------------------------------
  // # Setup
  // ---------------------------------------------------------------

  // Setup tasked send receiver group
  _group.setConcurrentRequests(concurrentThreads);

  // Setup send-receivers
  for (auto i = 0u; i < concurrentThreads; i++) {
    _sendReceivers.push_back(make_unique<network::TaskedSendReceiver>(_group));
  }

  // Setup AWS cloud provider
  _provider = cloud::Provider::makeProvider(uri, false, accountId, key,
                                            _sendReceivers.back().get());

  if (_provider->getType() != cloud::Provider::CloudService::AWS) {
    throw runtime_error("Can only handle aws buckets atm");
  }

  // Register resolver with send-receivers
  // TODO: needed?
  /*
  auto awsProvider = static_cast<cloud::AWS *>(cloudProvider.get());
  for (auto i = 0u; i < concurrentThreads; i++) {
    awsProvider->initResolver(*sendReceivers[i].get());
  }
  */
}

tuple<unique_ptr<uint8_t[]>, size_t, size_t>
Downloader::fetchMetaData(string filePrefix) {
  anyblob::network::Transaction getTxn(_provider.get());
  getTxn.getObjectRequest(filePrefix + "metadata.btr");

  getTxn.processSync(*_sendReceivers.back());

  // Get and print the result
  for (auto &result : getTxn) {
    auto dataVector = result.moveDataVector();

    return make_tuple(dataVector->transferBuffer(), result.getOffset(),
                      result.getSize());
  }

  throw std::runtime_error("Could not fetch metadata");
}

void Downloader::start(ProgressTracker &tracker, string filePrefix,
                       vector<FileIdentifier> &fileIDs) {
  // ---------------------------------------------------------------
  // # Requests + Processing
  // ---------------------------------------------------------------

  auto resolver = BtrBlocksFileNameResolver(filePrefix);

  vector<future<void>> asyncSendReceiverThreads;
  vector<future<void>> requestCreatorThreads;

  size_t finishedMessages = 0;

  auto createCallback = [&tracker, &finishedMessages](FileIdentifier fileID) {
    return [&tracker, &finishedMessages,
            fileID](network::MessageResult &result) {
      auto [column, part] = fileID;
      finishedMessages++;
      if (!result.success()) {
        cerr << "Request was not successful: " << result.getFailureCode()
             << endl;
      } else {
        auto vec = result.moveDataVector();

        auto buf = vec->transferBuffer();

        auto new_vec = make_unique<vector<uint8_t>>(
            buf.get(), buf.get() + result.getSize());

        new_vec->erase(new_vec->begin(), new_vec->begin() + result.getOffset());

        tracker.registerDownload(column, part, std::move(new_vec));
      }
    };
  };

  network::Transaction getTxn[_concurrentThreads];

  for (auto i = 0u; i < _concurrentThreads; i++) {
    getTxn[i].setProvider(_provider.get());
  }

  auto numberOfFiles = fileIDs.size();

  pair<uint64_t, uint64_t> fullRange = {0, 0};

  // Create requests in round robin fashion so globally the sorted file order is
  // kept
  auto createRequests = [this, &fileIDs, &numberOfFiles, &getTxn, &fullRange,
                         &resolver,
                         &createCallback](uint startIndex, uint threadId) {
    auto currIndex = startIndex;

    while (currIndex < numberOfFiles) {
      auto fileIdentifier = fileIDs[currIndex];
      auto filePath = resolver.resolve(fileIdentifier);

      auto getObjectRequest = [&getTxn, &fileIdentifier, &filePath, &fullRange,
                               &createCallback, threadId]() {
        return getTxn[threadId].getObjectRequest(
            createCallback(fileIdentifier), filePath, fullRange, nullptr, 0);
      };
      getTxn[threadId].verifyKeyRequest(*_sendReceivers.back(),
                                        std::move(getObjectRequest));

      currIndex += _concurrentThreads;
    }
  };

  // Create the requests asynchronously
  for (auto i = 0u; i < _concurrentThreads; i++) {
    requestCreatorThreads.push_back(async(launch::async, createRequests, i, i));
  }

  // Wait for request creation
  for (auto i = 0u; i < _concurrentThreads; i++) {
    requestCreatorThreads[i].get();
  }

  for (auto i = 0u; i < _concurrentThreads; i++)
    getTxn[i].processAsync(_group);

  for (auto i = 0u; i < _concurrentThreads; i++) {
    auto start = [this](uint i) { _sendReceivers[i]->run(); };
    asyncSendReceiverThreads.push_back(async(launch::async, start, i));
  }

  while (finishedMessages != numberOfFiles) {
    usleep(100);
  }

  cout << "finished"
       << "\n";

  for (auto i = 0u; i < _concurrentThreads; i++)
    _sendReceivers[i]->stop();
}

} // namespace btrscan