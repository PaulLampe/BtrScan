#pragma once
#include "types.hpp"
#include <string>

namespace btrscan {

using namespace std;

class BtrBlocksFileNameResolver {
public:
  explicit BtrBlocksFileNameResolver(string prefix) { _filePrefix = prefix; }

  string resolve(FileIdentifier &fileID) {
    return _filePrefix + "column" + to_string(fileID.column) + "_part" +
           to_string(fileID.part) + ".btr";
  };

private:
  string _filePrefix;
};

} // namespace btrscan