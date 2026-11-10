// Contract tests for the PS-11475 clone implementation.
// The implementation is included unchanged; only its two engine accessors
// are supplied by this fixture. Filesystem operations use real temporary files.
#undef MYSQL_DYNAMIC_PLUGIN
#define DISABLE_ERROR_LOGGING
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include "storage/rocksdb/rdb_clone.cc"

namespace fs = std::filesystem;
namespace myrocks {
static std::string test_datadir;
rocksdb::TransactionDB *rdb_get_rocksdb_db() { return nullptr; }
const char *rdb_get_rocksdb_datadir() { return test_datadir.c_str(); }
}

class CloneContract : public ::testing::Test {
 protected:
  std::string root;
  void SetUp() override {
    char pattern[] = "/tmp/ps11475-contract-XXXXXX";
    root = mkdtemp(pattern);
    myrocks::test_datadir = root + "/live";
  }
  void TearDown() override {
    myrocks::rdb_clone_destroy();
    fs::remove_all(root);
  }
  void create_database(const std::string &path) {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB *db = nullptr;
    ASSERT_TRUE(rocksdb::DB::Open(options, path, &db).ok());
    ASSERT_TRUE(db->Put(rocksdb::WriteOptions(), "old-data", "preserve-me").ok());
    delete db;
  }
};

TEST_F(CloneContract, NoPendingReceivePreservesLiveDatabase) {
  create_database(myrocks::test_datadir);
  EXPECT_FALSE(myrocks::rdb_clone_apply_pending(myrocks::test_datadir.c_str()));
  EXPECT_TRUE(fs::exists(myrocks::test_datadir + "/CURRENT"));
}

TEST_F(CloneContract, IncompleteReceiveMustNotReplaceLiveDatabase) {
  create_database(myrocks::test_datadir);
  const uchar *locator = nullptr;
  uint length = 0, task = 0;
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply_begin(nullptr, nullptr, locator,
      length, task, HA_CLONE_MODE_START, nullptr));
  // Simulate the loss of session memory at restart without a successful end.
  myrocks::rdb_clone_destroy();
  delete[] locator;
  EXPECT_FALSE(myrocks::rdb_clone_apply_pending(myrocks::test_datadir.c_str()));
  EXPECT_TRUE(fs::exists(myrocks::test_datadir + "/CURRENT"));
}

int main(int argc, char **argv) {
  MY_INIT(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  const int result = RUN_ALL_TESTS();
  my_end(0);
  return result;
}
