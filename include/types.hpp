#pragma once
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <map>
#include <tuple>
#include <utility>
#include <vector>

namespace btrscan {

using namespace std;

using ColumnIndex = size_t;
using PartIndex = size_t;
using PartInternalOffset = size_t;

using RowGroupIndex = size_t;

struct FileIdentifier {
  ColumnIndex column;
  PartIndex part;

  FileIdentifier(ColumnIndex column, PartIndex part)
      : column(column), part(part) {}

  FileIdentifier() = default;

  bool operator<(const FileIdentifier &r) const {
    if (column == r.column) {
      return part < r.part;
    }
    return column < r.column;
  }

  bool operator==(const FileIdentifier &r) const {
    return part == r.part && column == r.column;
  }

  std::string toString() const {
    return "{ col: " + to_string(column) + ", part: " + to_string(part) + " }";
  }
};

struct FileIdentifierHash {
  size_t operator()(const FileIdentifier& f) const {
    return hash<size_t>()(f.column) * 31 + hash<size_t>()(f.part);
  }
};

inline void PrintTo(const FileIdentifier &id, std::ostream *os) {
  *os << id.toString();
}

class Range {
public:
  size_t start;
  size_t end;

  Range(size_t start, size_t end) : start(start), end(end) {}

  bool operator<(const Range &r) const {
    if (start == r.start) {
      return end < r.end;
    }
    return start < r.start;
  }

  bool operator==(const Range &r) const {
    return start == r.start && end == r.end;
  }
};

using RangeVector = vector<Range>;

class PartCoveringRangesMap {
public:
  RangeVector &operator[](PartIndex i) { return _map[i]; }

private:
  map<PartIndex, RangeVector> _map{};
};

class ColumnPartCoveringRangesMap {
public:
  PartCoveringRangesMap &operator[](RowGroupIndex i) { return _map[i]; }

private:
  map<ColumnIndex, PartCoveringRangesMap> _map{};
};

using DownloadDataVectorType = vector<uint8_t>;

class RowGroupColumnLocation {
public:
  PartIndex part;
  PartInternalOffset offset;

  RowGroupColumnLocation(PartIndex part, PartInternalOffset offset)
      : part(part), offset(offset) {}

  RowGroupColumnLocation() = default;

  bool operator==(const RowGroupColumnLocation &r) const {
    return part == r.part && offset == r.offset;
  }
};

class ColumnPartOffsetMap {
public:
  map<ColumnIndex, RowGroupColumnLocation>::iterator begin() {
    return _map.begin();
  }

  map<ColumnIndex, RowGroupColumnLocation>::iterator end() {
    return _map.end();
  }

  RowGroupColumnLocation &operator[](ColumnIndex i) { return _map[i]; }

private:
  map<ColumnIndex, RowGroupColumnLocation> _map{};
};

using RowGroupLocationMap = map<RowGroupIndex, ColumnPartOffsetMap>;

class PartResolverMeta {
public:
  RowGroupLocationMap rowGroupLocations{};
  ColumnPartCoveringRangesMap columnPartCoveringRanges{};
};

} // namespace btrscan