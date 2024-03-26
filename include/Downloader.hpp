#pragma once
#include "BtrBlocksFileNameResolver.hpp"
#include "types.hpp"
#include "ProgressTracker.hpp"
#include <cloud/provider.hpp>
#include <cstdint>
#include <network/tasked_send_receiver.hpp>
#include <memory>
#include <string>
#include <vector>

namespace btrscan {

using namespace std;

class Downloader {
public:
  explicit Downloader(string uri, uint concurrentThreads, string accountId, string key);

  void start(ProgressTracker& tracker, string filePrefix, vector<FileIdentifier>& fileIDs);

  tuple<unique_ptr<uint8_t[]>, size_t, size_t> fetchMetaData(string filePrefix);

private:
  uint _concurrentThreads = 1;
  std::unique_ptr<anyblob::cloud::Provider> _provider;
  std::unique_ptr<BtrBlocksFileNameResolver> _fileNameResolver;
  vector<unique_ptr<anyblob::network::TaskedSendReceiver>> _sendReceivers{};
  anyblob::network::TaskedSendReceiverGroup _group{};
};

} // namespace btrscan