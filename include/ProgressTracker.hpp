#pragma once
#include <cstdint>
#include <utils/data_vector.hpp>
#include <vector>
namespace btrscan {

using namespace std;

class ProgressTracker {

public:
  void registerDownload(uint column, uint part, unique_ptr<vector<uint8_t>> result);
  void registerProcessed();
  void getNextRowGroup();
};

} // namespace btrscan