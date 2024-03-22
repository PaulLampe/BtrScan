#pragma once
#include <cstdint>
#include <utils/data_vector.hpp>
namespace btrscan {

using namespace std;

class ProgressTracker {

public:
  void registerDownload(uint column, uint part, unique_ptr<anyblob::utils::DataVector<uint8_t>> result);
  void registerProcessed();
  void getNextRowGroup();
};

} // namespace btrscan