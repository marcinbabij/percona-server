/* Copyright (c) 2025 Percona LLC and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA */

#include "./rdb_clone.h"

/* C++ standard headers */
#include <atomic>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

/* MySQL includes */
#include "my_dir.h"
#include "my_sys.h"
#include "mysqld_error.h"

/* RocksDB includes */
#include "rocksdb/utilities/checkpoint.h"

/* MyRocks includes */
#include "./ha_rocksdb_proto.h"
#include "rdb_utils.h"

namespace myrocks {

/* Magic value embedded in every locator blob so we can detect corruption. */
static const uint32_t RDB_CLONE_LOCATOR_MAGIC = 0x524B4C4F;  /* "RKLO" */
static const uint32_t RDB_CLONE_VERSION = 1;
static const uint32_t RDB_CLONE_DESC_VERSION = 1;

/* Binary layout of the opaque locator blob the clone plugin passes back
   to us on every subsequent call.  Must be POD and have stable layout. */
#pragma pack(push, 1)
struct Rdb_clone_locator {
  uint32_t magic;
  uint32_t version;
  uint32_t session_id;
};

/* Per-file descriptor serialized by clone_copy and deserialized in
   clone_apply.  rel_path is relative to the checkpoint root. */
struct Rdb_clone_data_desc {
  uint32_t version;
  uint32_t file_index;
  uint64_t file_size;
  char rel_path[FN_REFLEN];
};
#pragma pack(pop)

struct Rdb_clone_file_info {
  std::string rel_path;
  uint64_t file_size;
};

/* Donor-side clone session: holds the checkpoint directory and file list. */
struct Rdb_clone_session {
  std::string checkpoint_dir;
  std::vector<Rdb_clone_file_info> files;
};

/* Recipient-side clone session.
   In provisioning mode (data_dir==nullptr), files are staged in `write_dir`
   (a temporary sibling directory) and atomically renamed into `rocksdb_dir`
   in apply_end so the live RocksDB instance is never disturbed mid-clone. */
struct Rdb_clone_apply_session {
  std::string rocksdb_dir;   /* final destination */
  std::string write_dir;     /* where files are written during apply */
  bool is_provisioning;      /* true when data_dir was nullptr */
};

static std::mutex g_clone_mutex;
static std::atomic<uint32_t> g_next_session_id{1};
static std::unordered_map<uint32_t, Rdb_clone_session> g_clone_sessions;
static std::unordered_map<uint32_t, Rdb_clone_apply_session> g_apply_sessions;

void rdb_clone_init() {
  /* g_clone_mutex and atomics self-initialize; nothing else needed here. */
}

void rdb_clone_destroy() {
  std::lock_guard<std::mutex> guard(g_clone_mutex);
  for (auto &kv : g_clone_sessions) {
    rocksdb::DestroyDB(kv.second.checkpoint_dir, rocksdb::Options());
  }
  g_clone_sessions.clear();
  g_apply_sessions.clear();
}

bool rdb_clone_apply_pending(const char *rocksdb_dir) {
  if (rocksdb_dir == nullptr) return false;

  const std::string live_dir(rocksdb_dir);
  const std::string staging_dir = live_dir + "_clone_recv";

  MY_STAT st;
  if (my_stat(staging_dir.c_str(), &st, MYF(0)) == nullptr ||
      !S_ISDIR(st.st_mode))
    return false;  /* no pending clone */

  const std::string backup_dir = live_dir + "_clone_old";

  LogPluginErrMsg(INFORMATION_LEVEL, 0,
                  "RocksDB init: pending clone found, swapping %s -> %s",
                  staging_dir.c_str(), live_dir.c_str());

  rocksdb::DestroyDB(backup_dir, rocksdb::Options());

  if (my_rename(live_dir.c_str(), backup_dir.c_str(), MYF(MY_WME)) != 0) {
    LogPluginErrMsg(ERROR_LEVEL, 0,
                    "RocksDB init: rename %s -> %s failed: %d",
                    live_dir.c_str(), backup_dir.c_str(), errno);
    return false;
  }

  if (my_rename(staging_dir.c_str(), live_dir.c_str(), MYF(MY_WME)) != 0) {
    LogPluginErrMsg(ERROR_LEVEL, 0,
                    "RocksDB init: rename %s -> %s failed: %d, "
                    "restoring backup",
                    staging_dir.c_str(), live_dir.c_str(), errno);
    my_rename(backup_dir.c_str(), live_dir.c_str(), MYF(0));
    return false;
  }

  rocksdb::DestroyDB(backup_dir, rocksdb::Options());

  LogPluginErrMsg(INFORMATION_LEVEL, 0,
                  "RocksDB init: pending clone installed at %s",
                  live_dir.c_str());
  return true;
}

/* ---- Internal helpers ---- */

static uchar *alloc_locator(uint32_t session_id, uint &loc_len) {
  loc_len = sizeof(Rdb_clone_locator);
  auto *buf = new uchar[loc_len];
  auto *l = reinterpret_cast<Rdb_clone_locator *>(buf);
  l->magic = RDB_CLONE_LOCATOR_MAGIC;
  l->version = RDB_CLONE_VERSION;
  l->session_id = session_id;
  return buf;
}

static bool validate_locator(const uchar *loc, uint loc_len) {
  if (loc == nullptr || loc_len < sizeof(Rdb_clone_locator)) return false;
  const auto *l = reinterpret_cast<const Rdb_clone_locator *>(loc);
  return l->magic == RDB_CLONE_LOCATOR_MAGIC &&
         l->version == RDB_CLONE_VERSION;
}

static uint32_t locator_session_id(const uchar *loc) {
  return reinterpret_cast<const Rdb_clone_locator *>(loc)->session_id;
}

/* ---- Donor (source) side ---- */

void rocksdb_clone_get_capability(Ha_clone_flagset &flags) {
  flags.reset();
  flags.set(HA_CLONE_BLOCKING);
}

int rocksdb_clone_begin(handlerton *, THD *, const uchar *&loc, uint &loc_len,
                        uint &task_id, Ha_clone_type, Ha_clone_mode mode) {
  if (mode == HA_CLONE_MODE_RESTART) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0), "RocksDB clone restart");
    return HA_ERR_UNSUPPORTED;
  }

  /* Version negotiation only — return a minimal locator, no checkpoint. */
  if (mode == HA_CLONE_MODE_VERSION) {
    task_id = 0;
    loc = alloc_locator(0, loc_len);
    return 0;
  }

  /* HA_CLONE_MODE_START: create a RocksDB checkpoint and enumerate files. */
  LogPluginErrMsg(INFORMATION_LEVEL, 0,
                  "RocksDB clone_begin: starting checkpoint creation");
  const uint32_t session_id = g_next_session_id.fetch_add(1);
  task_id = 0;

  /* Place the checkpoint as a sibling of rocksdb_datadir so that RocksDB
     can hard-link SST files (same filesystem, no data copying needed). */
  std::string base(rdb_get_rocksdb_datadir());
  while (!base.empty() && base.back() == '/') base.pop_back();
  const auto sep = base.rfind('/');
  const std::string parent =
      (sep != std::string::npos) ? base.substr(0, sep) : ".";
  const std::string checkpoint_dir =
      parent + "/.rocksdb_clone_" + std::to_string(session_id);

  rocksdb::Checkpoint *ckpt = nullptr;
  auto status = rocksdb::Checkpoint::Create(rdb_get_rocksdb_db(), &ckpt);
  if (!status.ok()) {
    my_error(ER_GET_ERRMSG, MYF(0), status.code(),
             status.ToString().c_str(), "rocksdb");
    return HA_EXIT_FAILURE;
  }
  LogPluginErrMsg(INFORMATION_LEVEL, 0,
                  "RocksDB clone_begin: creating checkpoint at %s",
                  checkpoint_dir.c_str());
  status = ckpt->CreateCheckpoint(checkpoint_dir.c_str());
  delete ckpt;
  if (!status.ok()) {
    my_error(ER_GET_ERRMSG, MYF(0), status.code(),
             status.ToString().c_str(), "rocksdb");
    return HA_EXIT_FAILURE;
  }

  Rdb_clone_session session;
  session.checkpoint_dir = checkpoint_dir;

  MY_DIR *dir = my_dir(checkpoint_dir.c_str(), MYF(MY_WANT_STAT));
  if (dir == nullptr) {
    rocksdb::DestroyDB(checkpoint_dir, rocksdb::Options());
    my_error(ER_CANT_READ_DIR, MYF(0), checkpoint_dir.c_str(), errno);
    return HA_EXIT_FAILURE;
  }
  for (uint i = 0; i < dir->number_off_files; i++) {
    const char *name = dir->dir_entry[i].name;
    if (name[0] == '.') continue;
    if (S_ISDIR(dir->dir_entry[i].mystat->st_mode)) continue;
    Rdb_clone_file_info fi;
    fi.rel_path = name;
    fi.file_size =
        static_cast<uint64_t>(dir->dir_entry[i].mystat->st_size);
    session.files.push_back(std::move(fi));
  }
  my_dirend(dir);

  const size_t num_files = session.files.size();
  {
    std::lock_guard<std::mutex> guard(g_clone_mutex);
    g_clone_sessions.emplace(session_id, std::move(session));
  }

  LogPluginErrMsg(INFORMATION_LEVEL, 0,
                  "RocksDB clone_begin: session %u, %zu files in checkpoint",
                  session_id, num_files);
  loc = alloc_locator(session_id, loc_len);
  return 0;
}

int rocksdb_clone_copy(handlerton *hton, THD *, const uchar *loc,
                       uint loc_len, uint, Ha_clone_cbk *cbk) {
  if (!validate_locator(loc, loc_len)) return HA_EXIT_FAILURE;

  const uint32_t sid = locator_session_id(loc);

  std::string checkpoint_dir;
  std::vector<Rdb_clone_file_info> files;
  {
    std::lock_guard<std::mutex> guard(g_clone_mutex);
    const auto it = g_clone_sessions.find(sid);
    if (it == g_clone_sessions.end()) return HA_EXIT_FAILURE;
    checkpoint_dir = it->second.checkpoint_dir;
    files = it->second.files;
  }

  LogPluginErrMsg(INFORMATION_LEVEL, 0,
                  "RocksDB clone_copy: sid=%u, %zu files from %s", sid,
                  files.size(), checkpoint_dir.c_str());

  cbk->set_hton(hton);

  for (uint32_t idx = 0; idx < static_cast<uint32_t>(files.size()); idx++) {
    const std::string full_path =
        checkpoint_dir + "/" + files[idx].rel_path;
    const uint64_t file_size = files[idx].file_size;

    LogPluginErrMsg(INFORMATION_LEVEL, 0,
                    "RocksDB clone_copy: file[%u] %s size=%llu", idx,
                    files[idx].rel_path.c_str(),
                    static_cast<unsigned long long>(file_size));

    Rdb_clone_data_desc desc{};
    desc.version = RDB_CLONE_DESC_VERSION;
    desc.file_index = idx;
    desc.file_size = file_size;
    snprintf(desc.rel_path, sizeof(desc.rel_path), "%s",
             files[idx].rel_path.c_str());

    cbk->set_data_desc(reinterpret_cast<const uchar *>(&desc), sizeof(desc));
    cbk->set_source_name(full_path.c_str());
    cbk->clear_flags();
    cbk->set_os_buffer_cache();

    if (file_size > static_cast<uint64_t>(UINT_MAX)) {
      my_error(ER_GET_ERRMSG, MYF(0), 1,
               "RocksDB clone: file exceeds 4 GiB transfer limit", "rocksdb");
      return HA_EXIT_FAILURE;
    }

    const File fd = my_open(full_path.c_str(), O_RDONLY, MYF(MY_WME));
    if (fd < 0) return HA_EXIT_FAILURE;

    Ha_clone_file clone_file{};
    clone_file.type = Ha_clone_file::FILE_DESC;
    clone_file.file_desc = fd;

    const int err = cbk->file_cbk(clone_file, static_cast<uint>(file_size));
    my_close(fd, MYF(0));
    LogPluginErrMsg(INFORMATION_LEVEL, 0,
                    "RocksDB clone_copy: file_cbk[%u] returned %d", idx, err);
    if (err != 0) return err;
  }

  LogPluginErrMsg(INFORMATION_LEVEL, 0,
                  "RocksDB clone_copy: all %zu files sent", files.size());
  return 0;
}

int rocksdb_clone_ack(handlerton *, THD *, const uchar *, uint, uint, int,
                      Ha_clone_cbk *) {
  return 0;  /* no-op: HA_CLONE_BLOCKING does not use ACKs */
}

int rocksdb_clone_end(handlerton *, THD *, const uchar *loc, uint loc_len,
                      uint, int) {
  std::string checkpoint_dir;

  if (validate_locator(loc, loc_len)) {
    const uint32_t sid = locator_session_id(loc);
    std::lock_guard<std::mutex> guard(g_clone_mutex);
    const auto it = g_clone_sessions.find(sid);
    if (it != g_clone_sessions.end()) {
      checkpoint_dir = it->second.checkpoint_dir;
      g_clone_sessions.erase(it);
    }
  }

  delete[] loc;

  if (!checkpoint_dir.empty())
    rocksdb::DestroyDB(checkpoint_dir, rocksdb::Options());

  return 0;
}

/* ---- Recipient (apply) side ---- */

int rocksdb_clone_apply_begin(handlerton *, THD *, const uchar *&loc,
                              uint &loc_len, uint &task_id, Ha_clone_mode mode,
                              const char *data_dir) {
  if (mode == HA_CLONE_MODE_RESTART) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0), "RocksDB clone restart");
    return HA_ERR_UNSUPPORTED;
  }

  /* Version negotiation only — data_dir is null at this stage. */
  if (mode == HA_CLONE_MODE_VERSION) {
    task_id = 0;
    loc = alloc_locator(0, loc_len);
    return 0;
  }

  LogPluginErrMsg(INFORMATION_LEVEL, 0,
                  "RocksDB clone_apply_begin: START mode, data_dir=%s",
                  (data_dir != nullptr) ? data_dir : "(provisioning)");

  const bool is_provisioning = (data_dir == nullptr);

  /* Resolve the final destination directory. */
  std::string rocksdb_dir;
  if (!is_provisioning) {
    rocksdb_dir = std::string(data_dir) + "/rocksdb";
  } else {
    const char *live_dir = rdb_get_rocksdb_datadir();
    if (live_dir == nullptr) {
      my_error(ER_INTERNAL_ERROR, MYF(0),
               "RocksDB datadir not available for clone");
      return HA_EXIT_FAILURE;
    }
    rocksdb_dir = std::string(live_dir);
  }

  /* In provisioning mode the live RocksDB is running, so writing donor files
     directly into its directory risks the compactor deleting our files.
     Stage into a sibling directory and rename atomically in apply_end. */
  const std::string write_dir =
      is_provisioning ? rocksdb_dir + "_clone_recv" : rocksdb_dir;

  /* Remove any leftover staging dir from a previous failed attempt. */
  if (is_provisioning) {
    rocksdb::DestroyDB(write_dir, rocksdb::Options());
  }

  if (my_mkdir(write_dir.c_str(), 0750, MYF(0)) != 0 && errno != EEXIST) {
    my_error(ER_CANT_CREATE_FILE, MYF(0), write_dir.c_str(), errno);
    return HA_EXIT_FAILURE;
  }

  if (!is_provisioning) {
    if (my_mkdir(rocksdb_dir.c_str(), 0750, MYF(0)) != 0 && errno != EEXIST) {
      my_error(ER_CANT_CREATE_FILE, MYF(0), rocksdb_dir.c_str(), errno);
      return HA_EXIT_FAILURE;
    }
  }

  const uint32_t session_id = g_next_session_id.fetch_add(1);
  {
    std::lock_guard<std::mutex> guard(g_clone_mutex);
    g_apply_sessions.emplace(
        session_id,
        Rdb_clone_apply_session{rocksdb_dir, write_dir, is_provisioning});
  }

  task_id = 0;
  loc = alloc_locator(session_id, loc_len);
  LogPluginErrMsg(INFORMATION_LEVEL, 0,
                  "RocksDB clone_apply_begin: session %u, write_dir: %s",
                  session_id, write_dir.c_str());
  return 0;
}

int rocksdb_clone_apply(handlerton *, THD *, const uchar *loc, uint loc_len,
                        uint, int in_err, Ha_clone_cbk *cbk) {
  if (in_err != 0) return in_err;
  if (!validate_locator(loc, loc_len)) return HA_EXIT_FAILURE;

  uint desc_len = 0;
  const uchar *desc_buf = cbk->get_data_desc(&desc_len);
  if (desc_buf == nullptr || desc_len < sizeof(Rdb_clone_data_desc)) {
    LogPluginErrMsg(INFORMATION_LEVEL, 0,
                    "RocksDB clone_apply: no/short descriptor (len=%u), "
                    "skipping", desc_len);
    return 0;
  }

  const auto *desc =
      reinterpret_cast<const Rdb_clone_data_desc *>(desc_buf);

  /* State-change sentinel: empty rel_path with zero file_size. */
  if (desc->file_size == 0 && desc->rel_path[0] == '\0') {
    LogPluginErrMsg(INFORMATION_LEVEL, 0,
                    "RocksDB clone_apply: sentinel descriptor, skipping");
    return 0;
  }

  LogPluginErrMsg(INFORMATION_LEVEL, 0,
                  "RocksDB clone_apply: file '%s' size=%llu",
                  desc->rel_path,
                  static_cast<unsigned long long>(desc->file_size));

  const uint32_t sid = locator_session_id(loc);

  std::string write_dir;
  {
    std::lock_guard<std::mutex> guard(g_clone_mutex);
    const auto it = g_apply_sessions.find(sid);
    if (it == g_apply_sessions.end()) {
      LogPluginErrMsg(INFORMATION_LEVEL, 0,
                      "RocksDB clone_apply: session %u not found!", sid);
      return HA_EXIT_FAILURE;
    }
    write_dir = it->second.write_dir;
  }

  const std::string dest_path = write_dir + "/" + desc->rel_path;
  cbk->set_dest_name(dest_path.c_str());

  const File dest_fd =
      my_open(dest_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, MYF(MY_WME));
  if (dest_fd < 0) return HA_EXIT_FAILURE;

  Ha_clone_file dest_file{};
  dest_file.type = Ha_clone_file::FILE_DESC;
  dest_file.file_desc = dest_fd;

  LogPluginErrMsg(INFORMATION_LEVEL, 0,
                  "RocksDB clone_apply: calling apply_file_cbk for '%s'",
                  desc->rel_path);
  const int err = cbk->apply_file_cbk(dest_file);
  my_close(dest_fd, MYF(0));
  LogPluginErrMsg(INFORMATION_LEVEL, 0,
                  "RocksDB clone_apply: apply_file_cbk for '%s' returned %d",
                  desc->rel_path, err);
  return err;
}

int rocksdb_clone_apply_end(handlerton *, THD *, const uchar *loc,
                            uint loc_len, uint, int in_err) {
  std::string rocksdb_dir;
  std::string write_dir;
  bool is_provisioning = false;

  if (validate_locator(loc, loc_len)) {
    const uint32_t sid = locator_session_id(loc);
    std::lock_guard<std::mutex> guard(g_clone_mutex);
    const auto it = g_apply_sessions.find(sid);
    if (it != g_apply_sessions.end()) {
      rocksdb_dir = it->second.rocksdb_dir;
      write_dir = it->second.write_dir;
      is_provisioning = it->second.is_provisioning;
      g_apply_sessions.erase(it);
    }
  }

  delete[] loc;

  if (!is_provisioning || write_dir.empty() || rocksdb_dir.empty()) {
    return 0;
  }

  if (in_err != 0) {
    /* Clone failed — remove the incomplete staging directory. */
    LogPluginErrMsg(INFORMATION_LEVEL, 0,
                    "RocksDB clone_apply_end: clone error %d, "
                    "removing staging dir %s", in_err, write_dir.c_str());
    rocksdb::DestroyDB(write_dir, rocksdb::Options());
    return 0;
  }

  /* Provisioning clone succeeded.  Leave the staging directory in place as a
     "pending swap" signal.  The live RocksDB instance is still running and
     will flush its memtable during shutdown — swapping the directory now would
     let that flush write local-server files into the donor's directory,
     corrupting the MANIFEST.  rdb_clone_apply_pending() performs the rename
     during rocksdb_init_internal() before TransactionDB::Open(), once the
     old instance is fully closed. */
  LogPluginErrMsg(INFORMATION_LEVEL, 0,
                  "RocksDB clone_apply_end: staging dir %s ready; "
                  "swap into %s deferred to next init",
                  write_dir.c_str(), rocksdb_dir.c_str());
  return 0;
}

}  // namespace myrocks
