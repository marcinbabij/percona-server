// Contract tests for the MyRocks clone implementation.
// The implementation is included unchanged; only its two engine accessors
// are supplied by this fixture. Filesystem operations use real temporary files.
#define DISABLE_ERROR_LOGGING
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <cstdarg>
#include <vector>
#include "mysql/components/services/log_builtins.h"

static std::vector<std::string> clone_messages;
static void capture_clone_log(const char *format, ...) {
  char buffer[4096];
  va_list args;
  va_start(args, format);
  vsnprintf(buffer, sizeof(buffer), format, args);
  va_end(args);
  clone_messages.emplace_back(buffer);
}
#undef LogPluginErrMsg
#define LogPluginErrMsg(severity, code, ...) capture_clone_log(__VA_ARGS__)

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
  fs::path saved_cwd;
  void SetUp() override {
    clone_messages.clear();
    char pattern[] = "/tmp/ps11475-contract-XXXXXX";
    root = mkdtemp(pattern);
    myrocks::test_datadir = root + "/live";
    // The commit marker is relative to the server data directory.
    saved_cwd = fs::current_path();
    fs::current_path(root);
    fs::create_directory("#clone");
  }
  void TearDown() override {
    myrocks::rdb_clone_destroy();
    fs::current_path(saved_cwd);
    fs::remove_all(root);
  }
  // Commit the clone for all engines, as the clone plugin does.
  void commit() { std::ofstream(Clone_handler::READY_TO_APPLY_FILE).close(); }
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
  commit();
  // This is the on-disk state after the first rename and before the second.
  fs::rename(live, live + "_clone_old");
  const auto installed = myrocks::rdb_clone_apply_pending(live.c_str());
  EXPECT_EQ(myrocks::Rdb_clone_recovery::INSTALLED, installed);
  EXPECT_TRUE(fs::exists(live + "/CURRENT"));
  EXPECT_TRUE(fs::exists(live + "_clone_old/CURRENT"));
  EXPECT_EQ(myrocks::Rdb_clone_recovery::INSTALLED,
            myrocks::rdb_clone_apply_pending(live.c_str()));
  myrocks::rdb_clone_finish_pending(live.c_str());
  EXPECT_FALSE(fs::exists(live + "_clone_old"));
  EXPECT_TRUE(fs::exists(live + "/CURRENT"));
}

TEST_F(CloneContract, PreparedStageInstallsOnlyAfterCommit) {
  const auto live = myrocks::test_datadir;
  create_database(live);
  create_database(live + "_clone_recv");
  EXPECT_EQ(myrocks::Rdb_clone_recovery::NONE,
            myrocks::rdb_clone_apply_pending(live.c_str()));
  EXPECT_FALSE(fs::exists(live + "_clone_recv"));
  EXPECT_FALSE(fs::exists(live + "_clone_old"));
  create_database(live + "_clone_recv");
  commit();
  EXPECT_EQ(myrocks::Rdb_clone_recovery::INSTALLED,
            myrocks::rdb_clone_apply_pending(live.c_str()));
  EXPECT_FALSE(fs::exists(live + "_clone_recv"));
  EXPECT_TRUE(fs::exists(live + "_clone_old/CURRENT"));
  myrocks::rdb_clone_finish_pending(live.c_str());
  EXPECT_FALSE(fs::exists(live + "_clone_old"));
}

TEST_F(CloneContract, LeftoverBackupWarnsAndBlocksProvisioning) {
  const auto live = myrocks::test_datadir;
  create_database(live);
  create_database(live + "_clone_old");
  // DestroyDB() keeps a directory holding files RocksDB did not create.
  fs::create_directory(live + "_clone_old/trace");
  commit();
  EXPECT_EQ(myrocks::Rdb_clone_recovery::INSTALLED,
            myrocks::rdb_clone_apply_pending(live.c_str()));
  myrocks::rdb_clone_finish_pending(live.c_str());
  EXPECT_TRUE(fs::exists(live + "_clone_old/trace"));
  fs::remove(Clone_handler::READY_TO_APPLY_FILE);
  EXPECT_EQ(myrocks::Rdb_clone_recovery::NONE,
            myrocks::rdb_clone_apply_pending(live.c_str()));
  EXPECT_TRUE(fs::exists(live + "/CURRENT"));
  uint length = 0, task = 0;
  const uchar *locator = nullptr;
  EXPECT_EQ(ER_FILE_EXISTS_ERROR, myrocks::rocksdb_clone_apply_begin(nullptr,
      nullptr, locator, length, task, HA_CLONE_MODE_VERSION, nullptr));
  fs::remove_all(live + "_clone_old");
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply_begin(nullptr, nullptr, locator,
      length, task, HA_CLONE_MODE_VERSION, nullptr));
  EXPECT_EQ(0, myrocks::rocksdb_clone_apply_end(nullptr, nullptr, locator,
      length, task, 0));
}

TEST_F(CloneContract, InitRemovesStaleCheckpointsOfThisDatadirOnly) {
  create_database(myrocks::test_datadir + "_clone_ckpt_7");
  create_database(root + "/other_clone_ckpt_7");
  myrocks::rdb_clone_init();
  EXPECT_FALSE(fs::exists(myrocks::test_datadir + "_clone_ckpt_7"));
  EXPECT_TRUE(fs::exists(root + "/other_clone_ckpt_7/CURRENT"));
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
  commit();
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
  EXPECT_TRUE(fs::exists(stage));
  EXPECT_EQ(ER_QUERY_INTERRUPTED, myrocks::rocksdb_clone_apply_end(nullptr,
      nullptr, worker, length, worker_task, 0));
  EXPECT_FALSE(fs::exists(stage));
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

class ApplyCallback : public Ha_clone_cbk {
 public:
  std::string payload{"payload", 8};
  int file_cbk(Ha_clone_file, uint) override { return 1; }
  int buffer_cbk(uchar *, uint) override { return 1; }
  int apply_buffer_cbk(uchar *&, uint &) override { return 1; }
  int apply_file_cbk(Ha_clone_file file) override {
    return my_write(file.file_desc,
                    reinterpret_cast<const uchar *>(payload.data()),
                    payload.size(), MYF(0)) == payload.size() ? 0 : 1;
  }
};


TEST_F(CloneContract, ValidDescriptorWritesInsideStage) {
  uint length = 0, task = 0;
  std::unique_ptr<uchar[]> donor(myrocks::alloc_locator(123, length));
  const uchar *locator = donor.get();
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply_begin(nullptr, nullptr, locator,
      length, task, HA_CLONE_MODE_START, nullptr));
  myrocks::Rdb_clone_data_desc descriptor{};
  descriptor.version = myrocks::RDB_CLONE_DESC_VERSION;
  descriptor.file_size = 8;
  descriptor.data_size = 8;
  descriptor.file_count = 1;
  strcpy(descriptor.rel_path, "000001.sst");
  ApplyCallback callback;
  callback.set_data_desc(reinterpret_cast<const uchar *>(&descriptor),
                         sizeof(descriptor));
  EXPECT_EQ(0, myrocks::rocksdb_clone_apply(nullptr, nullptr, locator,
      length, task, 0, &callback));
  EXPECT_EQ(8U, fs::file_size(myrocks::test_datadir + "_clone_recv/000001.sst"));
  myrocks::rocksdb_clone_apply_end(nullptr, nullptr, locator, length, task, 0);
}

TEST_F(CloneContract, RejectParentPath) {
  uint length = 0, task = 0;
  std::unique_ptr<uchar[]> donor(myrocks::alloc_locator(123, length));
  const uchar *locator = donor.get();
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply_begin(nullptr, nullptr, locator,
      length, task, HA_CLONE_MODE_START, nullptr));
  myrocks::Rdb_clone_data_desc descriptor{};
  descriptor.version = myrocks::RDB_CLONE_DESC_VERSION;
  descriptor.file_size = 8;
  descriptor.data_size = 8;
  descriptor.file_count = 1;
  strcpy(descriptor.rel_path, "../outside");
  ApplyCallback callback;
  callback.set_data_desc(reinterpret_cast<const uchar *>(&descriptor),
                         sizeof(descriptor));
  EXPECT_NE(0, myrocks::rocksdb_clone_apply(nullptr, nullptr, locator,
      length, task, 0, &callback));
  EXPECT_FALSE(fs::exists(root + "/outside"));
  myrocks::rocksdb_clone_apply_end(nullptr, nullptr, locator, length, task, 1);
}

TEST_F(CloneContract, RejectUnknownDescriptorVersion) {
  uint length = 0, task = 0;
  std::unique_ptr<uchar[]> donor(myrocks::alloc_locator(123, length));
  const uchar *locator = donor.get();
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply_begin(nullptr, nullptr, locator,
      length, task, HA_CLONE_MODE_START, nullptr));
  myrocks::Rdb_clone_data_desc descriptor{};
  descriptor.version = 999;
  descriptor.file_size = 8;
  descriptor.data_size = 8;
  descriptor.file_count = 1;
  strcpy(descriptor.rel_path, "000001.sst");
  ApplyCallback callback;
  callback.set_data_desc(reinterpret_cast<const uchar *>(&descriptor),
                         sizeof(descriptor));
  EXPECT_NE(0, myrocks::rocksdb_clone_apply(nullptr, nullptr, locator,
      length, task, 0, &callback));
  myrocks::rocksdb_clone_apply_end(nullptr, nullptr, locator, length, task, 1);
}

TEST_F(CloneContract, RejectTruncatedDescriptor) {
  uint length = 0, task = 0;
  std::unique_ptr<uchar[]> donor(myrocks::alloc_locator(123, length));
  const uchar *locator = donor.get();
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply_begin(nullptr, nullptr, locator,
      length, task, HA_CLONE_MODE_START, nullptr));
  uchar descriptor = 1;
  ApplyCallback callback;
  callback.set_data_desc(&descriptor, sizeof(descriptor));
  EXPECT_NE(0, myrocks::rocksdb_clone_apply(nullptr, nullptr, locator,
      length, task, 0, &callback));
  myrocks::rocksdb_clone_apply_end(nullptr, nullptr, locator, length, task, 1);
}


TEST_F(CloneContract, RejectMalformedNamesAndDescriptorBounds) {
  uint length = 0, task = 0;
  std::unique_ptr<uchar[]> donor(myrocks::alloc_locator(123, length));
  const uchar *locator = donor.get();
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply_begin(nullptr, nullptr, locator,
      length, task, HA_CLONE_MODE_START, nullptr));
  myrocks::Rdb_clone_data_desc descriptor{};
  descriptor.version = myrocks::RDB_CLONE_DESC_VERSION;
  descriptor.file_size = 8;
  descriptor.data_size = 8;
  descriptor.file_count = 1;
  ApplyCallback callback;
  for (const auto &name : {std::string("/tmp/escape"), std::string(".."),
                          std::string("a/000001.sst"),
                          std::string("LOCK"),
                          std::string(FN_REFLEN, 'x')}) {
    memset(descriptor.rel_path, 0, sizeof(descriptor.rel_path));
    memcpy(descriptor.rel_path, name.data(), name.size());
    callback.set_data_desc(reinterpret_cast<const uchar *>(&descriptor),
                           sizeof(descriptor));
    EXPECT_EQ(ER_CLONE_PROTOCOL, myrocks::rocksdb_clone_apply(nullptr, nullptr,
        locator, length, task, 0, &callback));
  }
  strcpy(descriptor.rel_path, "000001.sst");
  callback.set_data_desc(reinterpret_cast<const uchar *>(&descriptor),
                         sizeof(descriptor) + 1);
  EXPECT_EQ(ER_CLONE_PROTOCOL, myrocks::rocksdb_clone_apply(nullptr, nullptr,
      locator, length, task, 0, &callback));
  callback.set_data_desc(nullptr, 0);
  EXPECT_EQ(ER_CLONE_PROTOCOL, myrocks::rocksdb_clone_apply(nullptr, nullptr,
      locator, length, task, 0, &callback));
  myrocks::rocksdb_clone_apply_end(nullptr, nullptr, locator, length, task, 1);
}

TEST_F(CloneContract, RejectSymlinkDestinationAndWrongPayloadSize) {
  uint length = 0, task = 0;
  std::unique_ptr<uchar[]> donor(myrocks::alloc_locator(123, length));
  const uchar *locator = donor.get();
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply_begin(nullptr, nullptr, locator,
      length, task, HA_CLONE_MODE_START, nullptr));
  const auto stage = myrocks::test_datadir + "_clone_recv";
  std::ofstream(root + "/outside") << "keep";
  fs::create_symlink(root + "/outside", stage + "/000001.sst");
  myrocks::Rdb_clone_data_desc descriptor{};
  descriptor.version = myrocks::RDB_CLONE_DESC_VERSION;
  descriptor.file_size = 8;
  descriptor.data_size = 8;
  descriptor.file_count = 1;
  strcpy(descriptor.rel_path, "000001.sst");
  ApplyCallback callback;
  callback.set_data_desc(reinterpret_cast<const uchar *>(&descriptor),
                         sizeof(descriptor));
  EXPECT_NE(0, myrocks::rocksdb_clone_apply(nullptr, nullptr, locator,
      length, task, 0, &callback));
  EXPECT_EQ(4U, fs::file_size(root + "/outside"));
  fs::remove(stage + "/000001.sst");
  descriptor.file_size = 9;
  descriptor.data_size = 9;
  EXPECT_EQ(ER_CLONE_PROTOCOL, myrocks::rocksdb_clone_apply(nullptr, nullptr,
      locator, length, task, 0, &callback));
  myrocks::rocksdb_clone_apply_end(nullptr, nullptr, locator, length, task, 1);
}

TEST_F(CloneContract, ChunksAreOrderedAndCompleteBeforePrepare) {
  uint length = 0, task = 0;
  std::unique_ptr<uchar[]> donor(myrocks::alloc_locator(123, length));
  const uchar *locator = donor.get();
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply_begin(nullptr, nullptr, locator,
      length, task, HA_CLONE_MODE_START, nullptr));
  myrocks::Rdb_clone_data_desc descriptor{};
  descriptor.version = myrocks::RDB_CLONE_DESC_VERSION;
  descriptor.file_size = 16;
  descriptor.data_size = 8;
  descriptor.file_count = 1;
  strcpy(descriptor.rel_path, "000001.sst");
  ApplyCallback callback;
  callback.set_data_desc(reinterpret_cast<const uchar *>(&descriptor),
                         sizeof(descriptor));
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply(nullptr, nullptr, locator,
      length, task, 0, &callback));
  const auto stage = myrocks::test_datadir + "_clone_recv";
  // Duplicating a chunk or skipping an offset must not truncate the file.
  EXPECT_EQ(ER_CLONE_PROTOCOL, myrocks::rocksdb_clone_apply(nullptr, nullptr,
      locator, length, task, 0, &callback));
  descriptor.offset = 9;
  descriptor.data_size = 7;
  EXPECT_EQ(ER_CLONE_PROTOCOL, myrocks::rocksdb_clone_apply(nullptr, nullptr,
      locator, length, task, 0, &callback));
  descriptor.offset = 8;
  descriptor.data_size = 8;
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply(nullptr, nullptr, locator,
      length, task, 0, &callback));
  std::ifstream file(stage + "/000001.sst", std::ios::binary);
  const std::string data((std::istreambuf_iterator<char>(file)), {});
  EXPECT_EQ(callback.payload + callback.payload, data);
  EXPECT_EQ(0, myrocks::rocksdb_clone_apply_end(nullptr, nullptr, locator,
      length, task, 0));
  // Only the clone plugin commits: the engine keeps the prepared stage.
  EXPECT_TRUE(fs::exists(stage + "/000001.sst"));
  EXPECT_FALSE(fs::exists(Clone_handler::READY_TO_APPLY_FILE));
}

TEST_F(CloneContract, IncompleteChunksCannotPrepare) {
  uint length = 0, task = 0;
  std::unique_ptr<uchar[]> donor(myrocks::alloc_locator(123, length));
  const uchar *locator = donor.get();
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply_begin(nullptr, nullptr, locator,
      length, task, HA_CLONE_MODE_START, nullptr));
  myrocks::Rdb_clone_data_desc descriptor{};
  descriptor.version = myrocks::RDB_CLONE_DESC_VERSION;
  descriptor.file_size = 16;
  descriptor.data_size = 8;
  descriptor.file_count = 1;
  strcpy(descriptor.rel_path, "000001.sst");
  ApplyCallback callback;
  callback.set_data_desc(reinterpret_cast<const uchar *>(&descriptor),
                         sizeof(descriptor));
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply(nullptr, nullptr, locator,
      length, task, 0, &callback));
  EXPECT_EQ(ER_CLONE_PROTOCOL, myrocks::rocksdb_clone_apply_end(nullptr,
      nullptr, locator, length, task, 0));
  EXPECT_FALSE(fs::exists(myrocks::test_datadir + "_clone_recv"));
}

TEST_F(CloneContract, DonorChunksFilesLargerThanFourGiB) {
  const uint64_t size = static_cast<uint64_t>(UINT_MAX) + 17;
  const auto checkpoint = root + "/checkpoint";
  fs::create_directory(checkpoint);
  std::ofstream(checkpoint + "/000001.sst").close();
  fs::resize_file(checkpoint + "/000001.sst", size);
  uint length = 0;
  myrocks::Rdb_clone_session session;
  session.locator.reset(myrocks::alloc_locator(123, length));
  const uchar *locator = session.locator.get();
  session.checkpoint_dir = checkpoint;
  session.files.push_back({"000001.sst", size});
  myrocks::g_clone_sessions.emplace(123, std::move(session));
  class ChunkCallback : public ApplyCallback {
   public:
    uint64_t copied{0};
    int file_cbk(Ha_clone_file file, uint length) override {
      EXPECT_LE(length, myrocks::RDB_CLONE_CHUNK_SIZE);
      uint desc_size = 0;
      const auto *desc = reinterpret_cast<const myrocks::Rdb_clone_data_desc *>(
          get_data_desc(&desc_size));
      EXPECT_EQ(copied, desc->offset);
      EXPECT_EQ(length, desc->data_size);
      copied += length;
      return my_seek(file.file_desc, length, MY_SEEK_CUR, MYF(0)) == copied
                 ? 0 : 1;
    }
  } callback;
  EXPECT_EQ(0, myrocks::rocksdb_clone_copy(nullptr, nullptr, locator, length,
                                         0, &callback));
  EXPECT_EQ(size, callback.copied);
  EXPECT_EQ(0, myrocks::rocksdb_clone_end(nullptr, nullptr, locator, length,
                                        0, 0));
}

TEST_F(CloneContract, LocalReceiveWaitsForRootAndRetainsRelativeLayout) {
  const auto clone_root = root + "/local";
  myrocks::test_datadir = "./nested/.rocksdb";
  uint length = 0, task = 0;
  std::unique_ptr<uchar[]> donor(myrocks::alloc_locator(123, length));
  const uchar *locator = donor.get();
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply_begin(nullptr, nullptr, locator,
      length, task, HA_CLONE_MODE_START, clone_root.c_str()));
  EXPECT_FALSE(fs::exists(clone_root));
  // InnoDB creates the root during its apply_begin callback.
  fs::create_directory(clone_root);
  myrocks::Rdb_clone_data_desc descriptor{};
  descriptor.version = myrocks::RDB_CLONE_DESC_VERSION;
  descriptor.file_size = 8;
  descriptor.data_size = 8;
  descriptor.file_count = 1;
  strcpy(descriptor.rel_path, "000001.sst");
  ApplyCallback callback;
  callback.set_data_desc(reinterpret_cast<const uchar *>(&descriptor),
                         sizeof(descriptor));
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply(nullptr, nullptr, locator,
      length, task, 0, &callback));
  EXPECT_EQ(8U, fs::file_size(clone_root + "/nested/.rocksdb/000001.sst"));
  EXPECT_EQ(0, myrocks::rocksdb_clone_apply_end(nullptr, nullptr, locator,
      length, task, 0));
}

TEST_F(CloneContract, LocalReceiveRejectsPathsOutsideCloneRoot) {
  uint length = 0, task = 0;
  std::unique_ptr<uchar[]> donor(myrocks::alloc_locator(123, length));
  const auto clone_root = root + "/local";
  for (const auto &path : {root + "/absolute", std::string("../outside"),
                          std::string("./"), std::string("nested/../outside")}) {
    myrocks::test_datadir = path;
    const uchar *locator = donor.get();
    EXPECT_EQ(ER_NOT_SUPPORTED_YET, myrocks::rocksdb_clone_apply_begin(nullptr,
        nullptr, locator, length, task, HA_CLONE_MODE_START, clone_root.c_str()));
  }
  EXPECT_FALSE(fs::exists(clone_root));
}

TEST_F(CloneContract, ApplyOwnsDescriptorAcrossCallbackBufferReuse) {
  uint length = 0, task = 0;
  std::unique_ptr<uchar[]> donor(myrocks::alloc_locator(123, length));
  const uchar *locator = donor.get();
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply_begin(nullptr, nullptr, locator,
      length, task, HA_CLONE_MODE_START, nullptr));
  myrocks::Rdb_clone_data_desc descriptor{};
  descriptor.version = myrocks::RDB_CLONE_DESC_VERSION;
  descriptor.file_size = 8;
  descriptor.data_size = 8;
  descriptor.file_count = 1;
  strcpy(descriptor.rel_path, "000001.sst");
  class ReuseCallback : public ApplyCallback {
   public:
    myrocks::Rdb_clone_data_desc *descriptor;
    int apply_file_cbk(Ha_clone_file file) override {
      const int error = ApplyCallback::apply_file_cbk(file);
      // Network receive reuses the packet that held the descriptor.
      strcpy(descriptor->rel_path, "overwritten");
      return error;
    }
  } callback;
  callback.descriptor = &descriptor;
  callback.set_data_desc(reinterpret_cast<const uchar *>(&descriptor),
                         sizeof(descriptor));
  ASSERT_EQ(0, myrocks::rocksdb_clone_apply(nullptr, nullptr, locator,
      length, task, 0, &callback));
  EXPECT_EQ("RocksDB clone_apply: apply_file_cbk for '000001.sst' returned 0",
            clone_messages.back());
  EXPECT_EQ(0, myrocks::rocksdb_clone_apply_end(nullptr, nullptr, locator,
      length, task, 0));
}
