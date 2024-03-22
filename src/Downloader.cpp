#include "Downloader.hpp"
#include "BtrBlocksFileNameResolver.hpp"
#include "types.hpp"
#include "btrblocks.hpp"
#include "cloud/aws.hpp"
#include "network/tasked_send_receiver.hpp"
#include "network/transaction.hpp"
#include <cstddef>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/types.h>
#include <tuple>
#include <unistd.h>
#include <vector>

namespace btrscan {

using namespace anyblob;
using namespace std;

Downloader::Downloader(string uri, uint concurrentThreads) {
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
  _provider = cloud::Provider::makeProvider(uri, false, "", "",
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

void Downloader::start(ProgressTracker &tracker,
                      string filePrefix,
                       vector<FileIdentifier> &fileIDs) {
  // ---------------------------------------------------------------
  // # Requests + Processing
  // ---------------------------------------------------------------

  auto resolver = BtrBlocksFileNameResolver(filePrefix);

  vector<future<void>> asyncSendReceiverThreads;
  vector<future<void>> requestCreatorThreads;

  auto createCallback = [&tracker](FileIdentifier fileID) {
    return [&tracker, fileID](network::MessageResult &result) {
      auto [column, part] = fileID;
      if (!result.success()) {
        cerr << "Request was not successful: " << result.getFailureCode()
             << endl;
      }
      tracker.registerDownload(column, part, result.moveDataVector());
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
  auto createRequests = [this, &fileIDs, &numberOfFiles, &getTxn,
                         &fullRange, &resolver,
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
}

} // namespace btrscan