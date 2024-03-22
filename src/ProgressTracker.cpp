#include "ProgressTracker.hpp"
#include <iostream>
#include <string>

namespace btrscan {

using namespace std;

void ProgressTracker::registerDownload(
    uint column, uint part,
    unique_ptr<anyblob::utils::DataVector<uint8_t>> result
    ) {
  cout << "Downloaded column " + to_string(column) + " part " + to_string(part);
};

} // namespace btrscan
