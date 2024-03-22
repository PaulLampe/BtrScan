#pragma once
#include <cstddef>
#include <tuple>

namespace btrscan {

using namespace std;

using ColumnIndex = size_t;
using PartIndex = size_t;

using FileIdentifier = tuple<ColumnIndex, PartIndex>;
}