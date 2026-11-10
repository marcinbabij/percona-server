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
  EXPECT_EQ(myrocks::Rdb_clone_recovery::NONE,
            myrocks::rdb_clone_apply_pending(myrocks::test_datadir.c_str()));
  EXPECT_TRUE(fs::exists(myrocks::test_datadir + "/CURRENT"));
}

TEST_F(CloneContract, IncompleteReceiveMustNotReplaceLiveDatabase) {
  create_database(myrocks::test_datadir);
  uint length = 0, task = 0;
  std::unique_ptr<uchar[]> donor(myrocks::alloc_locator(123, length));
  const uchar *locator = donor.get();
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply_begin(nullptr, nullptr, locator,
      length, task, HA_CLONE_MODE_START, nullptr));
  // Simulate the loss of session memory at restart without a successful end.
  myrocks::rdb_clone_destroy();
  EXPECT_EQ(myrocks::Rdb_clone_recovery::NONE,
            myrocks::rdb_clone_apply_pending(myrocks::test_datadir.c_str()));
  EXPECT_TRUE(fs::exists(myrocks::test_datadir + "/CURRENT"));
}

TEST_F(CloneContract, RestartBetweenRenamesMustPreserveOldDatabase) {
  const auto live = myrocks::test_datadir;
  create_database(live);
  create_database(live + "_clone_recv");
  std::ofstream(live + "_clone_recv/CLONE_COMPLETE").close();
  // This is the on-disk state after the first rename and before the second.
  fs::rename(live, live + "_clone_old");
  const auto installed = myrocks::rdb_clone_apply_pending(live.c_str());
  EXPECT_EQ(myrocks::Rdb_clone_recovery::INSTALLED, installed);
  EXPECT_TRUE(fs::exists(live + "/CURRENT"));
  EXPECT_TRUE(fs::exists(live + "_clone_old/CURRENT"));
  EXPECT_EQ(myrocks::Rdb_clone_recovery::INSTALLED,
            myrocks::rdb_clone_apply_pending(live.c_str()));
  EXPECT_EQ(0, myrocks::rdb_clone_finish_pending(live.c_str()));
  EXPECT_FALSE(fs::exists(live + "_clone_old"));
  EXPECT_TRUE(fs::exists(live + "/CURRENT"));
}

TEST_F(CloneContract, MissingStageRestoresSavedDatabase) {
  const auto live = myrocks::test_datadir;
  create_database(live + "_clone_old");
  EXPECT_EQ(myrocks::Rdb_clone_recovery::NONE,
            myrocks::rdb_clone_apply_pending(live.c_str()));
  EXPECT_TRUE(fs::exists(live + "/CURRENT"));
}

TEST_F(CloneContract, AmbiguousRecoveryPreservesEveryDatabase) {
  const auto live = myrocks::test_datadir;
  create_database(live);
  create_database(live + "_clone_old");
  create_database(live + "_clone_recv");
  std::ofstream(live + "_clone_recv/CLONE_COMPLETE").close();
  EXPECT_EQ(myrocks::Rdb_clone_recovery::ERROR,
            myrocks::rdb_clone_apply_pending(live.c_str()));
  EXPECT_TRUE(fs::exists(live + "/CURRENT"));
  EXPECT_TRUE(fs::exists(live + "_clone_old/CURRENT"));
  EXPECT_TRUE(fs::exists(live + "_clone_recv/CURRENT"));
}

TEST_F(CloneContract, ApplyWorkersShareStageAndKeepItUntilLastEnd) {
  uint length = 0, master_task = 0, worker_task = 0;
  std::unique_ptr<uchar[]> donor(myrocks::alloc_locator(123, length));
  const uchar *master = donor.get();
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply_begin(nullptr, nullptr, master,
      length, master_task, HA_CLONE_MODE_START, nullptr));
  const auto stage = myrocks::test_datadir + "_clone_recv";
  std::ofstream(stage + "/CURRENT") << "manifest";
  const uchar *worker = donor.get();
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply_begin(nullptr, nullptr, worker,
      length, worker_task, HA_CLONE_MODE_ADD_TASK, nullptr));
  EXPECT_NE(master_task, worker_task);
  EXPECT_EQ(donor.get(), worker);
  EXPECT_TRUE(fs::exists(stage + "/CURRENT"));
  EXPECT_EQ(0, myrocks::rocksdb_clone_apply_end(nullptr, nullptr, master,
      length, master_task, ER_QUERY_INTERRUPTED));
  EXPECT_FALSE(fs::exists(stage + "/CLONE_COMPLETE"));
  EXPECT_TRUE(fs::exists(stage));
  EXPECT_EQ(0, myrocks::rocksdb_clone_apply_end(nullptr, nullptr, worker,
      length, worker_task, 0));
  EXPECT_FALSE(fs::exists(stage + "/CLONE_COMPLETE"));
  EXPECT_TRUE(myrocks::g_apply_sessions.empty());
}

TEST_F(CloneContract, ConcurrentStartCannotEraseActiveReceive) {
  uint length = 0, task = 0;
  std::unique_ptr<uchar[]> donor(myrocks::alloc_locator(123, length));
  const uchar *first = donor.get();
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply_begin(nullptr, nullptr, first,
      length, task, HA_CLONE_MODE_START, nullptr));
  const auto stage = myrocks::test_datadir + "_clone_recv";
  std::ofstream(stage + "/CURRENT") << "manifest";
  std::unique_ptr<uchar[]> other(myrocks::alloc_locator(124, length));
  const uchar *second = other.get();
  EXPECT_EQ(ER_CLONE_PROTOCOL, myrocks::rocksdb_clone_apply_begin(nullptr,
      nullptr, second, length, task, HA_CLONE_MODE_START, nullptr));
  EXPECT_TRUE(fs::exists(stage + "/CURRENT"));
  EXPECT_EQ(0, myrocks::rocksdb_clone_apply_end(nullptr, nullptr, first,
      length, 0, ER_QUERY_INTERRUPTED));
}

int main(int argc, char **argv) {
  MY_INIT(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  const int result = RUN_ALL_TESTS();
  my_end(0);
  return result;
}
