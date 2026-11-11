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

#pragma once

/* MySQL includes */
#include "sql/handler.h"

namespace myrocks {

/** Initialize the clone module. Call once from rocksdb_init_internal(). */
void rdb_clone_init();

/** Outcome of the startup check for a pending provisioning clone. */
enum class Rdb_clone_recovery {
  /** rocksdb_dir holds the database to open: no clone was pending, an
  incomplete receive was discarded, or the saved database was restored. */
  NONE,
  /** rocksdb_dir holds a received database and <rocksdb_dir>_clone_old may
  hold the previous one until rdb_clone_finish_pending() removes it. */
  INSTALLED,
  /** The directories are in a state which cannot be resolved without
  risking the only copy of a database. Startup must stop. */
  ERROR
};

/** Resume a completed provisioning clone, preserving the old database.
Call before discovering or opening any files in rocksdb_dir. ERROR means
startup must stop; NONE also covers discarding an incomplete receive.
@param[in]  rocksdb_dir  configured rocksdb_datadir
@return recovery outcome, see Rdb_clone_recovery */
Rdb_clone_recovery rdb_clone_apply_pending(const char *rocksdb_dir);

/** Remove the saved database only after the received engine initialized.
@param[in]  rocksdb_dir  configured rocksdb_datadir
@return zero on success, or a MySQL error code on persistence failure */
int rdb_clone_finish_pending(const char *rocksdb_dir);

/** Destroy the clone module: drop all sessions and their donor
checkpoints. Call once from rocksdb_shutdown(). */
void rdb_clone_destroy();

/* The 8 clone-interface callbacks registered in the RocksDB handlerton.
See Clone_interface_t in sql/handler.h for the contracts. Every error
returned is also raised in the session diagnostics area. */

void rocksdb_clone_get_capability(Ha_clone_flagset &flags);

int rocksdb_clone_begin(handlerton *hton, THD *thd, const uchar *&loc,
                        uint &loc_len, uint &task_id, Ha_clone_type type,
                        Ha_clone_mode mode);

int rocksdb_clone_copy(handlerton *hton, THD *thd, const uchar *loc,
                       uint loc_len, uint task_id, Ha_clone_cbk *cbk);

int rocksdb_clone_ack(handlerton *hton, THD *thd, const uchar *loc,
                      uint loc_len, uint task_id, int in_err,
                      Ha_clone_cbk *cbk);

int rocksdb_clone_end(handlerton *hton, THD *thd, const uchar *loc,
                      uint loc_len, uint task_id, int in_err);

int rocksdb_clone_apply_begin(handlerton *hton, THD *thd, const uchar *&loc,
                              uint &loc_len, uint &task_id, Ha_clone_mode mode,
                              const char *data_dir);

int rocksdb_clone_apply(handlerton *hton, THD *thd, const uchar *loc,
                        uint loc_len, uint task_id, int in_err,
                        Ha_clone_cbk *cbk);

int rocksdb_clone_apply_end(handlerton *hton, THD *thd, const uchar *loc,
                            uint loc_len, uint task_id, int in_err);

}  // namespace myrocks
