#include "ProgressTracker.hpp"
#include <iostream>
#include <string>
#include <vector>

namespace btrscan {

using namespace std;

void ProgressTracker::registerDownload(
    uint column, uint part,
    unique_ptr<vector<uint8_t>> result
    ) {
  cout << "Downloaded column " + to_string(column) + " part " + to_string(part) + "\n";
};

} // namespace btrscan
