# BtrScan

A repo orchestrating Downloads using BtrBlocks (https://github.com/maxi-k/btrblocks) and AnyBlob (https://github.com/durner/AnyBlob).

AnyBlob relies on some third-party libraries:
- uring
- openssl
- jemalloc

For Ubuntu 22.04+ the following command installs the third-party libraries:
```
sudo apt update && sudo apt install liburing-dev openssl libssl-dev libjemalloc-dev lld g++ cmake
```

BtrBlocks relies on arrow and tbb. IF your project does not include them anyways, install them via:

tbb:
```
sudo apt update && sudo apt install libtbb-dev 
```

arrow: 
https://arrow.apache.org/install/
