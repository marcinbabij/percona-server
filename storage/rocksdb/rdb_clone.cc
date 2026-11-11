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
#include <algorithm>
#include <mutex>
#include <memory>
#include <limits>
#include <unordered_set>
#include <string>
#include <unordered_map>
#include <vector>

/* MySQL includes */
#include "my_dir.h"
#include "my_sys.h"
#include "mysqld_error.h"
#include "sql/clone_handler.h"

/* RocksDB includes */
#include "rocksdb/utilities/checkpoint.h"

/* MyRocks includes */
#include "./ha_rocksdb_proto.h"
#include "rdb_utils.h"

namespace myrocks {

/** Magic value embedded in every locator blob so we can detect corruption. */
static constexpr uint32_t RDB_CLONE_LOCATOR_MAGIC = 0x524B4C4F; /* "RKLO" */
/** Locator format version. Donor and recipient must use the same one: the
recipient rejects any other version as a protocol error. */
static constexpr uint32_t RDB_CLONE_VERSION = 2;
/** Rdb_clone_data_desc format version, validated by the recipient. */
static constexpr uint32_t RDB_CLONE_DESC_VERSION = 2;

/** Largest payload sent with one descriptor. Bounds the network packet and
the copy buffer independently of the SST size; the recipient rejects larger
chunks. It must stay below the minimum max_allowed_packet accepted by clone. */
static constexpr uint32_t RDB_CLONE_CHUNK_SIZE = 1024 * 1024;

/** Binary layout of the opaque locator blob the clone plugin passes back
to us on every subsequent call. Must be POD and have stable layout. */
#pragma pack(push, 1)
struct Rdb_clone_locator {
  /** RDB_CLONE_LOCATOR_MAGIC */
  uint32_t magic;
  /** RDB_CLONE_VERSION */
  uint32_t version;
  /** Donor session; the recipient keys its session by the same value. Zero
  for a version-negotiation locator, which the caller owns and the end
  callback frees. */
  uint32_t session_id;
};

/** Per-chunk descriptor serialized by clone_copy and deserialized in
clone_apply. Each file is sent as a sequence of chunks in file order, and
files are sent in file_index order, all by the master task. */
struct Rdb_clone_data_desc {
  /** RDB_CLONE_DESC_VERSION */
  uint32_t version;
  /** Position of the file in the donor checkpoint list */
  uint32_t file_index;
  /** Size of the whole file */
  uint64_t file_size;
  /** File offset of the chunk payload */
  uint64_t offset;
  /** Payload length, at most RDB_CLONE_CHUNK_SIZE; zero only for an empty
  file, which is sent as one empty chunk */
  uint32_t data_size;
  /** Number of files in the donor checkpoint */
  uint32_t file_count;
  /** NUL terminated flat file name in the checkpoint root */
  char rel_path[FN_REFLEN];
};
#pragma pack(pop)

/** Donor checkpoint file to send. */
struct Rdb_clone_file_info {
  /** File name in the checkpoint directory */
  std::string rel_path;
  /** File size when the checkpoint was listed; checkpoint files are not
  modified afterwards */
  uint64_t file_size;
};

/** Tasks attached to one donor or recipient clone session. The master task
creates the session in HA_CLONE_MODE_START and workers attach to it with
HA_CLONE_MODE_ADD_TASK. The session, its locator and its directory are kept
until the last attached task ends. */
struct Rdb_clone_tasks {
  /** Locator returned to every task. Owned by the session, so it stays valid
  for the clone plugin until the session is erased by the last end call. */
  std::unique_ptr<uchar[]> locator;
  /** Identifier for the next attached worker task. */
  uint next_task{1};
  /** Tasks which have not ended yet. Initialized with the master task 0:
  the braces select the initializer-list constructor, not a bucket count. */
  std::unordered_set<uint> active{0};
  /** First error reported by any task, used by the last task to end. */
  int error{0};
};

/** Donor-side clone session: holds the checkpoint directory and file list. */
struct Rdb_clone_session : Rdb_clone_tasks {
  /** Checkpoint directory, a sibling of rocksdb_datadir named by
  RDB_CLONE_CHECKPOINT, destroyed by the last end call */
  std::string checkpoint_dir;
  /** Checkpoint files, sent in this order by the master task */
  std::vector<Rdb_clone_file_info> files;
};

/** Recipient-side clone session.
In provisioning mode (data_dir==nullptr), files are staged in write_dir
(a temporary sibling directory). apply_end makes the stage durable, and the
next startup installs it if the clone plugin committed the clone, see
Clone_handler::READY_TO_APPLY_FILE. */
struct Rdb_clone_apply_session : Rdb_clone_tasks {
  /** Final destination */
  std::string rocksdb_dir;
  /** Where files are written during apply */
  std::string write_dir;
  /** True when data_dir was nullptr */
  bool is_provisioning;
  /** DATA DIRECTORY of a local or remote non-provisioning clone, created by
  InnoDB; empty for provisioning */
  std::string local_root;
  /** Whether write_dir and its parents below local_root exist */
  bool directory_ready{false};
  /** Number of files announced by the donor, zero before the first chunk */
  uint32_t file_count{0};
  /** file_index expected in the next chunk */
  uint32_t next_file{0};
  /** Size of current_file */
  uint64_t file_size{0};
  /** Offset expected in the next chunk */
  uint64_t next_offset{0};
  /** File being received, empty between files */
  std::string current_file;
  /** Names of all files started, to reject a file sent twice */
  std::unordered_set<std::string> received_names;
};

/** Get the directory whose entry must be synced to persist a rename or a
creation of path.
@param[in]  path  directory path, may end with '/'
@return parent directory path ending with '/', or "." for a relative name */
static std::string clone_parent_dir(const std::string &path) {
  const auto end = path.find_last_not_of('/');
  const auto slash = path.rfind('/', end);
  return slash == std::string::npos ? "." : path.substr(0, slash + 1);
}

/** Persist directory entries created, removed or renamed in a directory.
@param[in]  path  directory to sync
@return true on success, false after reporting the error */
static bool sync_clone_directory(const std::string &path) {
  const File fd = my_open(path.c_str(), O_RDONLY | O_DIRECTORY, MYF(MY_WME));
  if (fd < 0) return false;
  const int err = my_sync(fd, MYF(MY_WME));
  const int close_err = my_close(fd, MYF(MY_WME));
  return err == 0 && close_err == 0;
}

/** Remove a receive directory. It holds only files received from the donor,
see valid_clone_filename(), so DestroyDB() removes it completely.
@param[in]  path  receive directory */
static void remove_clone_stage(const std::string &path) {
  rocksdb::DestroyDB(path, rocksdb::Options());
}

/** Report a failure to write or persist clone files.
@return error code reported to the session */
static int clone_io_error() {
  my_error(ER_INTERNAL_ERROR, MYF(0), "Cannot persist RocksDB clone files");
  return ER_INTERNAL_ERROR;
}

/** Protects g_clone_sessions, g_apply_sessions and the sessions in them. */
static std::mutex g_clone_mutex;
/** Next donor session identifier; zero is reserved for version locators. */
static std::atomic<uint32_t> g_next_session_id{1};
/** Donor sessions by session identifier */
static std::unordered_map<uint32_t, Rdb_clone_session> g_clone_sessions;
/** Recipient sessions by donor session identifier */
static std::unordered_map<uint32_t, Rdb_clone_apply_session> g_apply_sessions;

void rdb_clone_destroy() {
  std::lock_guard<std::mutex> guard(g_clone_mutex);
  for (auto &kv : g_clone_sessions) {
    rocksdb::DestroyDB(kv.second.checkpoint_dir, rocksdb::Options());
  }
  g_clone_sessions.clear();
  g_apply_sessions.clear();
}

/** Check if a path names an existing directory.
@param[in]  path  path to check
@return true if path is a directory */
static bool clone_directory_exists(const std::string &path) {
  MY_STAT st;
  return my_stat(path.c_str(), &st, MYF(0)) != nullptr && S_ISDIR(st.st_mode);
}

/** Rename a clone directory and persist the rename.
Errors are logged here: during plugin initialization there is no session to
receive the error raised by my_rename().
@param[in]  from  existing directory
@param[in]  to    new name
@return true on success */
static bool clone_rename_directory(const std::string &from,
                                   const std::string &to) {
  if (my_rename(from.c_str(), to.c_str(), MYF(MY_WME)) != 0 ||
      !sync_clone_directory(clone_parent_dir(to))) {
    LogPluginErrMsg(ERROR_LEVEL, 0, "RocksDB clone: cannot rename %s to %s: %d",
                    from.c_str(), to.c_str(), errno);
    return false;
  }
  return true;
}

/** Get rocksdb_datadir without trailing '/'. The clone directories are named
by appending a suffix, and must be siblings of the live directory: with a
trailing '/' they would be created inside it, and the startup rename of the
live directory into its own child would fail.
@param[in]  dir  configured rocksdb_datadir
@return directory path without trailing separators, "/" for the root */
static std::string clone_live_dir(const char *dir) {
  std::string path(dir);
  while (path.size() > 1 && path.back() == '/') path.pop_back();
  return path;
}

/** Suffix of rocksdb_datadir naming a donor checkpoint, followed by the
session identifier. */
static constexpr const char *RDB_CLONE_CHECKPOINT = "_clone_ckpt_";

void rdb_clone_init() {
  /* A donor crash leaves its checkpoint behind. It keeps old files allocated
  through hard links, and would make the next clone fail, as session
  identifiers restart from 1. No session exists yet, so any checkpoint of
  this rocksdb_datadir is stale. */
  const auto prefix =
      clone_live_dir(rdb_get_rocksdb_datadir()) + RDB_CLONE_CHECKPOINT;
  const auto slash = prefix.rfind('/');
  const auto parent = prefix.substr(0, slash + 1);
  const auto name_prefix = prefix.substr(slash + 1);
  MY_DIR *dir = my_dir(parent.empty() ? "." : parent.c_str(), MYF(0));
  if (dir == nullptr) return;
  for (uint i = 0; i < dir->number_off_files; i++) {
    const std::string name = dir->dir_entry[i].name;
    if (name.compare(0, name_prefix.size(), name_prefix) != 0) continue;
    LogPluginErrMsg(WARNING_LEVEL, 0,
                    "RocksDB clone: removing stale checkpoint %s%s",
                    parent.c_str(), name.c_str());
    rocksdb::DestroyDB(parent + name, rocksdb::Options());
  }
  my_dirend(dir);
}

/* Installation is a sequence of two durable renames, each of them atomic:
     live -> live_clone_old (skipped if there is no live directory),
     live_clone_recv -> live.
   It starts only if the clone plugin committed the clone (M, see
   Clone_handler::READY_TO_APPLY_FILE), which no engine changes once any
   engine installed. The old database is destroyed only after the received
   one opened. Every crash point leaves one of these states (S = stage,
   B = backup, L = live):
     S  L  M : not started            -> both renames
     S     M : no previous database   -> second rename
     S  B  M : first rename done      -> second rename
     B  L  M : both renames done      -> INSTALLED, finish removes B
     S  !M   : clone not committed    -> stage discarded
     B  !L   : stage lost             -> B restored
     B  L !M : leftover backup        -> warning, server starts
   S B L and S B !M cannot be explained by this sequence and are rejected. */
Rdb_clone_recovery rdb_clone_apply_pending(const char *rocksdb_dir) {
  if (rocksdb_dir == nullptr) return Rdb_clone_recovery::NONE;
  const std::string live_dir = clone_live_dir(rocksdb_dir);
  const std::string staging_dir = live_dir + "_clone_recv";
  const std::string backup_dir = live_dir + "_clone_old";
  const bool have_live = clone_directory_exists(live_dir);
  const bool have_backup = clone_directory_exists(backup_dir);
  const bool committed = Clone_handler::ready_to_apply();

  if (!clone_directory_exists(staging_dir)) {
    if (!have_backup) return Rdb_clone_recovery::NONE;
    if (!have_live) {
      LogPluginErrMsg(WARNING_LEVEL, 0,
                      "RocksDB clone: restoring %s from %s",
                      live_dir.c_str(), backup_dir.c_str());
      if (!clone_rename_directory(backup_dir, live_dir))
        return Rdb_clone_recovery::ERROR;
      return Rdb_clone_recovery::NONE;
    }
    /* Installation finished, but engine recovery or backup cleanup did not. */
    if (committed) return Rdb_clone_recovery::INSTALLED;
    LogPluginErrMsg(WARNING_LEVEL, 0,
                    "RocksDB clone: %s is left from an installed clone; "
                    "remove it to allow provisioning clones again",
                    backup_dir.c_str());
    return Rdb_clone_recovery::NONE;
  }

  if (!committed) {
    /* The clone failed, crashed before its commit, or was abandoned by
    another engine. */
    if (have_backup) {
      LogPluginErrMsg(ERROR_LEVEL, 0,
                      "RocksDB clone: uncommitted %s found next to saved %s",
                      staging_dir.c_str(), backup_dir.c_str());
      return Rdb_clone_recovery::ERROR;
    }
    LogPluginErrMsg(INFORMATION_LEVEL, 0,
                    "RocksDB clone: discarding uncommitted %s",
                    staging_dir.c_str());
    remove_clone_stage(staging_dir);
    return Rdb_clone_recovery::NONE;
  }

  LogPluginErrMsg(INFORMATION_LEVEL, 0,
                  "RocksDB clone: installing %s into %s",
                  staging_dir.c_str(), live_dir.c_str());
  if (have_backup) {
    /* After the first rename, the backup is the only old copy. Never
    destroy it or replace an unexpected live directory on a retry. */
    if (have_live) {
      LogPluginErrMsg(ERROR_LEVEL, 0,
                      "RocksDB clone: %s, %s and committed %s all exist",
                      live_dir.c_str(), backup_dir.c_str(),
                      staging_dir.c_str());
      return Rdb_clone_recovery::ERROR;
    }
  } else if (have_live && !clone_rename_directory(live_dir, backup_dir)) {
    return Rdb_clone_recovery::ERROR;
  }

  if (!clone_rename_directory(staging_dir, live_dir))
    return Rdb_clone_recovery::ERROR;
  return Rdb_clone_recovery::INSTALLED;
}

void rdb_clone_finish_pending(const char *rocksdb_dir) {
  const std::string live_dir = clone_live_dir(rocksdb_dir);
  const std::string backup_dir = live_dir + "_clone_old";
  if (!clone_directory_exists(backup_dir) || !Clone_handler::ready_to_apply())
    return;
  /* DestroyDB() keeps the directory if it holds files RocksDB did not
  create. The received database is in use already: only warn, and retry at
  the next startup. Provisioning is rejected meanwhile, see
  rocksdb_clone_apply_begin(). */
  const auto status = rocksdb::DestroyDB(backup_dir, rocksdb::Options());
  if (!status.ok() || clone_directory_exists(backup_dir) ||
      !sync_clone_directory(clone_parent_dir(live_dir))) {
    LogPluginErrMsg(WARNING_LEVEL, 0,
                    "RocksDB clone: cannot remove %s: %s; remove it to "
                    "allow provisioning clones again",
                    backup_dir.c_str(), status.ToString().c_str());
    return;
  }
  LogPluginErrMsg(INFORMATION_LEVEL, 0, "RocksDB clone: installed %s",
                  live_dir.c_str());
}

/* ---- Internal helpers ---- */

/** Allocate a locator.
@param[in]   session_id  session identifier, zero for version negotiation
@param[out]  loc_len     locator length
@return locator allocated with new[] */
static uchar *alloc_locator(uint32_t session_id, uint &loc_len) {
  loc_len = sizeof(Rdb_clone_locator);
  auto *buf = new uchar[loc_len];
  auto *l = reinterpret_cast<Rdb_clone_locator *>(buf);
  l->magic = RDB_CLONE_LOCATOR_MAGIC;
  l->version = RDB_CLONE_VERSION;
  l->session_id = session_id;
  return buf;
}

/** Check a locator received from the clone plugin.
@param[in]  loc      locator
@param[in]  loc_len  locator length
@return true if loc is a locator of this format version */
static bool validate_locator(const uchar *loc, uint loc_len) {
  if (loc == nullptr || loc_len != sizeof(Rdb_clone_locator)) return false;
  const auto *l = reinterpret_cast<const Rdb_clone_locator *>(loc);
  return l->magic == RDB_CLONE_LOCATOR_MAGIC &&
         l->version == RDB_CLONE_VERSION;
}

/** Get the session identifier of a validated locator.
@param[in]  loc  locator, see validate_locator()
@return session identifier */
static uint32_t locator_session_id(const uchar *loc) {
  return reinterpret_cast<const Rdb_clone_locator *>(loc)->session_id;
}

/** Report a clone protocol violation to the session. Every error returned to
the clone plugin must be raised: the plugin forwards the diagnostics area to
the client, or reports "Unknown Error" when it is empty.
@param[in]  message  description of the violation
@return ER_CLONE_PROTOCOL */
static int clone_protocol_error(
    const char *message = "Invalid RocksDB clone descriptor") {
  my_error(ER_CLONE_PROTOCOL, MYF(0), message);
  return ER_CLONE_PROTOCOL;
}

static constexpr const char *RDB_CLONE_BAD_LOCATOR =
    "Invalid RocksDB clone locator";
static constexpr const char *RDB_CLONE_NO_SESSION =
    "RocksDB clone session not found";
static constexpr const char *RDB_CLONE_NO_TASK =
    "RocksDB clone task not attached";

/* Only flat checkpoint files may be supplied by the donor. In particular,
paths outside the stage are never wire filenames. */
static bool valid_clone_filename(const char *name, size_t capacity) {
  const char *end = static_cast<const char *>(memchr(name, '\0', capacity));
  if (end == nullptr || end == name) return false;
  const std::string file(name, end);
  if (file == "CURRENT" || file == "IDENTITY") return true;
  size_t first = 0, last = file.size();
  if (file.rfind("MANIFEST-", 0) == 0) first = 9;
  else if (file.rfind("OPTIONS-", 0) == 0) first = 8;
  else {
    const auto dot = file.find('.');
    if (dot == std::string::npos) return false;
    const auto suffix = file.substr(dot);
    if (suffix != ".sst" && suffix != ".log" && suffix != ".blob") return false;
    last = dot;
  }
  return first < last && std::all_of(file.begin() + first, file.begin() + last,
                                   [](char c) { return c >= '0' && c <= '9'; });
}

/* ---- Donor (source) side ---- */

void rocksdb_clone_get_capability(Ha_clone_flagset &flags) {
  flags.reset();
  flags.set(HA_CLONE_BLOCKING);
  flags.set(HA_CLONE_MULTI_TASK);
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

  if (mode == HA_CLONE_MODE_ADD_TASK) {
    if (!validate_locator(loc, loc_len))
      return clone_protocol_error(RDB_CLONE_BAD_LOCATOR);
    std::lock_guard<std::mutex> guard(g_clone_mutex);
    const auto it = g_clone_sessions.find(locator_session_id(loc));
    if (it == g_clone_sessions.end())
      return clone_protocol_error(RDB_CLONE_NO_SESSION);
    auto &session = it->second;
    task_id = session.next_task++;
    session.active.insert(task_id);
    loc = session.locator.get();
    return 0;
  }

  /* HA_CLONE_MODE_START: create a RocksDB checkpoint and enumerate files. */
  LogPluginErrMsg(INFORMATION_LEVEL, 0,
                  "RocksDB clone_begin: starting checkpoint creation");
  const uint32_t session_id = g_next_session_id.fetch_add(1);
  task_id = 0;

  /* Place the checkpoint as a sibling of rocksdb_datadir so that RocksDB
     can hard-link SST files (same filesystem, no data copying needed).
     If rocksdb_datadir is a mount point, the sibling is on the parent file
     system, and RocksDB copies every file instead. */
  const std::string checkpoint_dir =
      clone_live_dir(rdb_get_rocksdb_datadir()) + RDB_CLONE_CHECKPOINT +
      std::to_string(session_id);

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
  uint locator_length = 0;
  session.locator.reset(alloc_locator(session_id, locator_length));
  /* Moving the session into the map keeps the locator address. */
  const uchar *const locator = session.locator.get();

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
  /* Set the output locator only on success: the session owning it is
  destroyed on any earlier return. */
  loc = locator;
  loc_len = locator_length;
  return 0;
}

int rocksdb_clone_copy(handlerton *hton, THD *, const uchar *loc,
                       uint loc_len, uint task_id, Ha_clone_cbk *cbk) {
  if (!validate_locator(loc, loc_len))
    return clone_protocol_error(RDB_CLONE_BAD_LOCATOR);

  const uint32_t sid = locator_session_id(loc);

  std::string checkpoint_dir;
  std::vector<Rdb_clone_file_info> files;
  {
    std::lock_guard<std::mutex> guard(g_clone_mutex);
    const auto it = g_clone_sessions.find(sid);
    if (it == g_clone_sessions.end())
      return clone_protocol_error(RDB_CLONE_NO_SESSION);
    if (it->second.active.count(task_id) == 0)
      return clone_protocol_error(RDB_CLONE_NO_TASK);
    /* The master streams the checkpoint; workers participate in InnoDB. */
    if (task_id != 0) return 0;
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

    desc.file_count = static_cast<uint32_t>(files.size());
    cbk->set_source_name(full_path.c_str());
    cbk->clear_flags();
    cbk->set_os_buffer_cache();

    const File fd = my_open(full_path.c_str(), O_RDONLY, MYF(MY_WME));
    if (fd < 0) return HA_EXIT_FAILURE;
    Ha_clone_file clone_file{};
    clone_file.type = Ha_clone_file::FILE_DESC;
    clone_file.file_desc = fd;
    const uint32_t client_size = cbk->get_client_buffer_size();
    const uint32_t chunk_size = client_size == 0 ? RDB_CLONE_CHUNK_SIZE :
        std::min(client_size, RDB_CLONE_CHUNK_SIZE);
    uint64_t offset = 0;
    int err = 0;
    do {
      desc.offset = offset;
      desc.data_size = static_cast<uint32_t>(
          std::min<uint64_t>(file_size - offset, chunk_size));
      cbk->set_data_desc(reinterpret_cast<const uchar *>(&desc), sizeof(desc));
      err = cbk->file_cbk(clone_file, desc.data_size);
      if (err != 0) break;
      offset += desc.data_size;
    } while (offset < file_size);
    if (my_close(fd, MYF(MY_WME)) != 0 && err == 0) err = clone_io_error();
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
                      uint task_id, int in_err) {
  if (!validate_locator(loc, loc_len))
    return clone_protocol_error(RDB_CLONE_BAD_LOCATOR);
  const uint32_t sid = locator_session_id(loc);
  if (sid == 0) {
    delete[] loc;
    return 0;
  }
  std::string checkpoint_dir;
  {
    std::lock_guard<std::mutex> guard(g_clone_mutex);
    const auto it = g_clone_sessions.find(sid);
    if (it == g_clone_sessions.end())
      return clone_protocol_error(RDB_CLONE_NO_SESSION);
    if (it->second.active.erase(task_id) != 1)
      return clone_protocol_error(RDB_CLONE_NO_TASK);
    if (in_err != 0) it->second.error = in_err;
    if (!it->second.active.empty()) return 0;
    checkpoint_dir = it->second.checkpoint_dir;
    g_clone_sessions.erase(it);
  }
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

  /* Version negotiation only. A provisioning clone must be rejected here,
  before InnoDB drops all user data in HA_CLONE_MODE_START. The backup of
  an earlier clone would block the installation at the next startup. */
  if (mode == HA_CLONE_MODE_VERSION) {
    const char *live_dir = rdb_get_rocksdb_datadir();
    if (data_dir == nullptr && live_dir != nullptr) {
      const auto live = clone_live_dir(live_dir);
      const auto backup_dir = live + "_clone_old";
      if (clone_directory_exists(backup_dir)) {
        my_error(ER_FILE_EXISTS_ERROR, MYF(0), backup_dir.c_str());
        return ER_FILE_EXISTS_ERROR;
      }
      /* Installation renames whole directories next to rocksdb_datadir.
      rename(2) fails for a mount point with EBUSY or EXDEV. InnoDB renames
      files only within their directories and has no such limitation. */
      MY_STAT live_stat, parent_stat;
      if (my_stat(live.c_str(), &live_stat, MYF(0)) != nullptr &&
          my_stat(clone_parent_dir(live).c_str(), &parent_stat, MYF(0)) !=
              nullptr &&
          live_stat.st_dev != parent_stat.st_dev) {
        my_error(ER_NOT_SUPPORTED_YET, MYF(0),
                 "RocksDB provisioning clone into a mount point");
        return ER_NOT_SUPPORTED_YET;
      }
    }
    task_id = 0;
    loc = alloc_locator(0, loc_len);
    return 0;
  }

  if (!validate_locator(loc, loc_len) || locator_session_id(loc) == 0)
    return clone_protocol_error(RDB_CLONE_BAD_LOCATOR);
  const uint32_t session_id = locator_session_id(loc);
  std::lock_guard<std::mutex> guard(g_clone_mutex);
  if (mode == HA_CLONE_MODE_ADD_TASK) {
    const auto it = g_apply_sessions.find(session_id);
    if (it == g_apply_sessions.end())
      return clone_protocol_error(RDB_CLONE_NO_SESSION);
    task_id = it->second.next_task++;
    it->second.active.insert(task_id);
    /* Auxiliary apply must not replace a donor locator used by local copy. */
    return 0;
  }
  if (g_apply_sessions.count(session_id) != 0 ||
      (data_dir == nullptr && !g_apply_sessions.empty())) {
    my_error(ER_CLONE_PROTOCOL, MYF(0), "RocksDB clone receive already active");
    return ER_CLONE_PROTOCOL;
  }

  LogPluginErrMsg(INFORMATION_LEVEL, 0,
                  "RocksDB clone_apply_begin: START mode, data_dir=%s",
                  (data_dir != nullptr) ? data_dir : "(provisioning)");

  const bool is_provisioning = (data_dir == nullptr);

  /* Resolve the final destination directory. */
  std::string rocksdb_dir;
  if (!is_provisioning) {
    std::string relative(rdb_get_rocksdb_datadir());
    while (relative.rfind("./", 0) == 0) relative.erase(0, 2);
    while (!relative.empty() && relative.back() == '/') relative.pop_back();
    /* An absolute source path cannot be reused inside an isolated clone. */
    if (relative.empty() || relative.front() == '/') {
      my_error(ER_NOT_SUPPORTED_YET, MYF(0),
               "RocksDB DATA DIRECTORY clone with an absolute engine path");
      return ER_NOT_SUPPORTED_YET;
    }
    for (size_t begin = 0; begin < relative.size();) {
      const auto slash = relative.find('/', begin);
      const auto part = relative.substr(begin, slash - begin);
      if (part.empty() || part == "." || part == "..") {
        my_error(ER_NOT_SUPPORTED_YET, MYF(0),
                 "RocksDB DATA DIRECTORY clone with a non-contained path");
        return ER_NOT_SUPPORTED_YET;
      }
      if (slash == std::string::npos) break;
      begin = slash + 1;
    }
    rocksdb_dir = std::string(data_dir) + "/" + relative;
  } else {
    const char *live_dir = rdb_get_rocksdb_datadir();
    if (live_dir == nullptr) {
      my_error(ER_INTERNAL_ERROR, MYF(0),
               "RocksDB datadir not available for clone");
      return HA_EXIT_FAILURE;
    }
    rocksdb_dir = clone_live_dir(live_dir);
  }

  /* In provisioning mode the live RocksDB is running, so writing donor files
     directly into its directory risks the compactor deleting our files.
     Stage into a sibling directory, prepare it in apply_end, and install
     it during the next startup if the clone plugin committed the clone. */
  const std::string write_dir =
      is_provisioning ? rocksdb_dir + "_clone_recv" : rocksdb_dir;

  /* Remove a stage left by an earlier clone. It is not committed anymore:
  the clone plugin withdrew any commit before this call. */
  if (is_provisioning) remove_clone_stage(write_dir);

  if (is_provisioning &&
      my_mkdir(write_dir.c_str(), 0750, MYF(0)) != 0 && errno != EEXIST) {
    my_error(ER_CANT_CREATE_FILE, MYF(0), write_dir.c_str(), errno);
    return HA_EXIT_FAILURE;
  }

  Rdb_clone_apply_session session;
  session.rocksdb_dir = rocksdb_dir;
  session.write_dir = write_dir;
  session.is_provisioning = is_provisioning;
  if (!is_provisioning) session.local_root = data_dir;
  session.directory_ready = is_provisioning;
  session.locator.reset(alloc_locator(session_id, loc_len));
  loc = session.locator.get();
  task_id = 0;
  g_apply_sessions.emplace(session_id, std::move(session));
  LogPluginErrMsg(INFORMATION_LEVEL, 0,
                  "RocksDB clone_apply_begin: session %u, write_dir: %s",
                  session_id, write_dir.c_str());
  return 0;
}

int rocksdb_clone_apply(handlerton *, THD *, const uchar *loc, uint loc_len,
                        uint task_id, int in_err, Ha_clone_cbk *cbk) {
  if (in_err != 0) return in_err;
  if (!validate_locator(loc, loc_len))
    return clone_protocol_error(RDB_CLONE_BAD_LOCATOR);

  uint desc_len = 0;
  const uchar *desc_buf = cbk->get_data_desc(&desc_len);
  if (desc_buf == nullptr || desc_len != sizeof(Rdb_clone_data_desc))
    return clone_protocol_error();
  /* The callback receives another network packet and can replace or free
  the buffer backing get_data_desc(). Own every field used across it. */
  Rdb_clone_data_desc owned_desc;
  memcpy(&owned_desc, desc_buf, sizeof(owned_desc));
  const auto *desc = &owned_desc;
  if (desc->version != RDB_CLONE_DESC_VERSION ||
      !valid_clone_filename(desc->rel_path, sizeof(desc->rel_path)))
    return clone_protocol_error();
  if (desc->file_count == 0 || desc->file_index >= desc->file_count ||
      desc->data_size > RDB_CLONE_CHUNK_SIZE ||
      desc->offset > desc->file_size ||
      desc->data_size > desc->file_size - desc->offset ||
      (desc->data_size == 0 && desc->file_size != 0) ||
      desc->file_size > static_cast<uint64_t>(std::numeric_limits<off_t>::max()))
    return clone_protocol_error();
  const uint64_t offset = desc->offset;
  const uint64_t next_offset = offset + desc->data_size;
  const bool last_chunk = next_offset == desc->file_size;

  /* Called once per chunk: log only file boundaries and failures. */
  if (offset == 0)
    LogPluginErrMsg(INFORMATION_LEVEL, 0,
                    "RocksDB clone_apply: file '%s' size=%llu",
                    desc->rel_path,
                    static_cast<unsigned long long>(desc->file_size));

  const uint32_t sid = locator_session_id(loc);

  std::string write_dir;
  {
    std::lock_guard<std::mutex> guard(g_clone_mutex);
    const auto it = g_apply_sessions.find(sid);
    if (it == g_apply_sessions.end())
      return clone_protocol_error(RDB_CLONE_NO_SESSION);
    auto &session = it->second;
    if (task_id != 0 || session.active.count(task_id) == 0 ||
        desc->file_index != session.next_file ||
        offset != session.next_offset ||
        (session.file_count != 0 && session.file_count != desc->file_count))
      return clone_protocol_error();
    if (session.current_file.empty()) {
      if (offset != 0 || !session.received_names.insert(desc->rel_path).second)
        return clone_protocol_error();
      session.current_file = desc->rel_path;
      session.file_size = desc->file_size;
      session.file_count = desc->file_count;
    } else if (session.current_file != desc->rel_path ||
               session.file_size != desc->file_size) {
      return clone_protocol_error();
    }
    if (!session.directory_ready) {
      /* InnoDB owns creation of the clone root in apply_begin. Defer our
      subdirectories until all engines have completed that phase. */
      for (size_t begin = session.local_root.size() + 1;
           begin <= session.write_dir.size();) {
        const auto slash = session.write_dir.find('/', begin);
        const auto path = session.write_dir.substr(0, slash);
        if (my_mkdir(path.c_str(), 0750, MYF(0)) != 0 && errno != EEXIST) {
          my_error(ER_CANT_CREATE_FILE, MYF(0), path.c_str(), errno);
          return HA_EXIT_FAILURE;
        }
        if (slash == std::string::npos) break;
        begin = slash + 1;
      }
      session.directory_ready = true;
    }
    write_dir = session.write_dir;
  }

  const std::string dest_path = write_dir + "/" + desc->rel_path;
  cbk->set_dest_name(dest_path.c_str());

  const int open_flags = O_WRONLY | O_NOFOLLOW |
                         (offset == 0 ? O_CREAT | O_EXCL : 0);
  const File dest_fd = my_open(dest_path.c_str(), open_flags, MYF(MY_WME));
  if (dest_fd < 0) return HA_EXIT_FAILURE;
  if (my_seek(dest_fd, offset, MY_SEEK_SET, MYF(MY_WME)) == MY_FILEPOS_ERROR) {
    my_close(dest_fd, MYF(0));
    return clone_io_error();
  }

  Ha_clone_file dest_file{};
  dest_file.type = Ha_clone_file::FILE_DESC;
  dest_file.file_desc = dest_fd;

  int err = cbk->apply_file_cbk(dest_file);
  if (err == 0 && my_seek(dest_fd, 0, MY_SEEK_CUR, MYF(MY_WME)) !=
                      next_offset)
    err = clone_protocol_error();
  if (err == 0 && last_chunk && my_sync(dest_fd, MYF(MY_WME)) != 0)
    err = clone_io_error();
  if (my_close(dest_fd, MYF(MY_WME)) != 0 && err == 0)
    err = clone_io_error();
  if (err != 0 || last_chunk)
    LogPluginErrMsg(INFORMATION_LEVEL, 0,
                    "RocksDB clone_apply: apply_file_cbk for '%s' returned %d",
                    desc->rel_path, err);
  {
    std::lock_guard<std::mutex> guard(g_clone_mutex);
    /* The session cannot be erased meanwhile: only the master task 0
    applies, and it is still active until its own apply_end. */
    auto &session = g_apply_sessions.at(sid);
    if (err != 0) session.error = err;
    else if (last_chunk) {
      ++session.next_file;
      session.next_offset = 0;
      session.current_file.clear();
    } else session.next_offset = next_offset;
  }
  return err;
}

int rocksdb_clone_apply_end(handlerton *, THD *, const uchar *loc,
                            uint loc_len, uint task_id, int in_err) {
  /* Held until the stage is prepared or removed. The session is erased
  first, and a new provisioning receive started meanwhile would otherwise
  remove the same stage directory in its apply_begin. */
  std::lock_guard<std::mutex> guard(g_clone_mutex);
  std::string rocksdb_dir;
  std::string write_dir;
  bool is_provisioning = false;
  const int input_error = in_err;

  /* After an earlier error, only cleanup matters: do not report another. */
  if (!validate_locator(loc, loc_len))
    return input_error == 0 ? clone_protocol_error(RDB_CLONE_BAD_LOCATOR) : 0;
  const uint32_t sid = locator_session_id(loc);
  if (sid == 0) {
    delete[] loc;
    return 0;
  }
  {
    const auto it = g_apply_sessions.find(sid);
    if (it == g_apply_sessions.end())
      return input_error == 0 ? clone_protocol_error(RDB_CLONE_NO_SESSION) : 0;
    if (it->second.active.erase(task_id) != 1)
      return clone_protocol_error(RDB_CLONE_NO_TASK);
    if (in_err != 0) it->second.error = in_err;
    if (!it->second.active.empty()) return 0;
    in_err = it->second.error;
    if (in_err == 0 && (it->second.file_count == 0 ||
                       it->second.next_file != it->second.file_count))
      in_err = clone_protocol_error("RocksDB clone ended before all files");
    rocksdb_dir = it->second.rocksdb_dir;
    write_dir = it->second.write_dir;
    is_provisioning = it->second.is_provisioning;
    g_apply_sessions.erase(it);
  }

  if (!is_provisioning) {
    if (in_err == 0 && !write_dir.empty() &&
        (!sync_clone_directory(write_dir) ||
         !sync_clone_directory(clone_parent_dir(write_dir))))
      return clone_io_error();
    return input_error == 0 ? in_err : 0;
  }

  if (in_err != 0) {
    /* Clone failed — remove the incomplete staging directory. */
    LogPluginErrMsg(INFORMATION_LEVEL, 0,
                    "RocksDB clone_apply_end: clone error %d, "
                    "removing staging dir %s", in_err, write_dir.c_str());
    remove_clone_stage(write_dir);
    return input_error == 0 ? in_err : 0;
  }

  /* Prepare the stage: received files were synced, persist their directory
  entries. The live engine stays open until mysqld shuts down. */
  if (!sync_clone_directory(write_dir) ||
      !sync_clone_directory(clone_parent_dir(write_dir))) {
    remove_clone_stage(write_dir);
    return clone_io_error();
  }
  LogPluginErrMsg(INFORMATION_LEVEL, 0,
                  "RocksDB clone_apply_end: staging dir %s prepared; "
                  "install into %s deferred to next startup",
                  write_dir.c_str(), rocksdb_dir.c_str());
  return 0;
}

}  // namespace myrocks
