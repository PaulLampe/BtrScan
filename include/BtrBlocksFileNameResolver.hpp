#pragma once
#include "types.hpp"
#include <string>
#include <tuple>

namespace btrscan {

using namespace std;

class BtrBlocksFileNameResolver {
public:
  explicit BtrBlocksFileNameResolver(string prefix) { _filePrefix = prefix; }

  string resolve(FileIdentifier &fileID) {
    return _filePrefix + "column" + to_string(get<0>(fileID)) + "_part" +
           to_string(get<1>(fileID)) + ".btr";
  };

private:
  string _filePrefix;
};

} // namespace btrscan