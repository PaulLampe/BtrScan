#include "Downloader.hpp"
#include "PartsResolver.hpp"
#include "BtrS3Scanner.hpp"
#include "gflags/gflags.h"
#include <arrow/record_batch.h>
#include <cloud/aws.hpp>
#include <gtest/gtest.h>
#include <string>

DECLARE_string(account_id);
DECLARE_string(access_key);

void SetupSchemes() {
  btrblocks::BtrBlocksConfig::get().integers.schemes = btrblocks::defaultIntegerSchemes();
  btrblocks::BtrBlocksConfig::get().doubles.schemes = btrblocks::defaultDoubleSchemes();
  btrblocks::BtrBlocksConfig::get().strings.schemes = btrblocks::defaultStringSchemes();

  btrblocks::SchemePool::refresh();
}


TEST(ScannerBasic, MultiColumnMultiRowGroup) {
  SetupSchemes();
  std::string filePrefix = "data/medicare1_1/";

  auto downloader =
      btrscan::Downloader("s3://lampe-btrblocks-arrow-scan:eu-central-1", 1,
                          FLAGS_account_id, FLAGS_access_key);

  auto scanner = btrscan::BtrS3Scanner(downloader);

  scanner.prepareDataset(filePrefix);

  std::vector<std::string> columns = {"calculation3170826185336909"};
  auto ranges = std::vector<btrscan::Range>{{0, 131}};

  auto callback = [](std::shared_ptr<arrow::RecordBatch> batch){ 
    std::cout << batch->num_rows() << "\n";
  };

  scanner.scan(filePrefix, columns, callback, ranges);
}