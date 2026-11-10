/* Copyright (c) 2018, 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
@file clone/src/clone_hton.cc
Clone Plugin: Interface with SE handlerton

*/

#include "plugin/clone/include/clone_hton.h"
#include "scope_guard.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"

/* Namespace for all clone data types */
namespace myclone {
/** Check if a storage engine copies a snapshot only consistent while donor
commits are blocked. plugin_foreach() callback.
@param[in]      plugin  storage engine plugin
@param[in,out]  arg     bool flag, set to true for a blocking engine
@return false, to continue with the next plugin */
static bool needs_read_lock(THD *, plugin_ref plugin, void *arg) {
  const auto hton = plugin_data<handlerton *>(plugin);
  if (hton->clone_interface.clone_begin != nullptr) {
    Ha_clone_flagset flags;
    hton->clone_interface.clone_capability(flags);
    if (flags[HA_CLONE_BLOCKING] && !flags[HA_CLONE_HYBRID])
      *static_cast<bool *>(arg) = true;
  }
  return false;
}

int Clone_read_lock::acquire(THD *thd, unsigned long timeout) {
  bool blocking = false;
  plugin_foreach(thd, needs_read_lock, MYSQL_STORAGE_ENGINE_PLUGIN, &blocking);
  if (!blocking) return 0;
  if (thd->locked_tables_mode || thd->in_active_multi_stmt_transaction()) {
    my_error(ER_LOCK_OR_ACTIVE_TRANSACTION, MYF(0));
    return ER_LOCK_OR_ACTIVE_TRANSACTION;
  }

  const auto saved_timeout = thd->variables.lock_wait_timeout;
  thd->variables.lock_wait_timeout = timeout;
  auto restore_timeout = create_scope_guard(
      [&]() { thd->variables.lock_wait_timeout = saved_timeout; });
  const bool already_locked = thd->global_read_lock.is_acquired();
  if (thd->global_read_lock.lock_global_read_lock(thd))
    return thd->get_stmt_da()->mysql_errno();
  if (!already_locked) m_owner = thd;

  /* Match FTWRL ordering: drain table users before blocking commits.
  Otherwise an update waiting for a row lock can prevent the transaction
  holding that row lock from committing. Both engines and the persisted
  GTIDs must remain at this boundary until copying has finished. */
  if (close_cached_tables(thd, nullptr, true, timeout) ||
      thd->global_read_lock.make_global_read_lock_block_commit(thd))
    return thd->get_stmt_da()->mysql_errno();
  return 0;
}

Clone_read_lock::~Clone_read_lock() {
  if (m_owner != nullptr)
    m_owner->global_read_lock.unlock_global_read_lock(m_owner);
}

/** Structure to pass clone information to each storage plugin */
struct Hton {
  /** Clone locator vector */
  Storage_Vector *m_loc_vec;

  /** Clone task vector */
  Task_Vector *m_task_vec;

  /** Current locator index */
  uint m_cur_index;

  /** Error reported during clone */
  int m_err;

  /** Clone type */
  Ha_clone_type m_type;

  /** Clone begin mode */
  Ha_clone_mode m_mode;

  /** clone target data directory */
  const char *m_data_dir;
};

}  // namespace myclone

/** Unwind only successful begin calls, in reverse initialization order.
The failing engine owns its own partial initialization. Borrowed locators
for engines whose begin was not called must never be ended.
@param[in,out]	thd		server thread handle
@param[in]	locators	locators returned by successful begin calls
@param[in]	tasks		task identifiers, or empty for task 0
@param[in]	count		number of successful begin calls
@param[in]	apply		true to call clone_apply_end, else clone_end
@param[in]	error		begin error passed to the end calls */
static void unwind_clone_begin(THD *thd, const Storage_Vector &locators,
                               const Task_Vector &tasks, size_t count,
                               bool apply, int error) {
  assert(count <= locators.size());
  assert(tasks.empty() || count <= tasks.size());
  while (count != 0) {
    const auto index = --count;
    const auto &loc = locators[index];
    const uint task = tasks.empty() ? 0 : tasks[index];
    const auto end = apply ? loc.m_hton->clone_interface.clone_apply_end
                           : loc.m_hton->clone_interface.clone_end;
    end(loc.m_hton, thd, loc.m_loc, loc.m_loc_len, task, error);
  }
}

/** Begin clone operation for current storage engine plugin
@param[in,out]	thd	server thread handle
@param[in]	plugin	storage plugin
@param[in]	arg	clone parameters
@return true if failure */
static bool run_hton_clone_begin(THD *thd, plugin_ref plugin, void *arg) {
  auto clone_arg = static_cast<myclone::Hton *>(arg);

  auto hton = plugin_data<handlerton *>(plugin);

  if (hton->clone_interface.clone_begin != nullptr) {
    myclone::Locator loc = {hton, nullptr, 0};
    uint32_t task_id = 0;

    assert(clone_arg->m_mode == HA_CLONE_MODE_START);

    clone_arg->m_err = hton->clone_interface.clone_begin(
        hton, thd, loc.m_loc, loc.m_loc_len, task_id, clone_arg->m_type,
        clone_arg->m_mode);

    if (clone_arg->m_err != 0) return true;
    clone_arg->m_loc_vec->push_back(loc);
    clone_arg->m_task_vec->push_back(task_id);
  }

  return (false);
}

int hton_clone_begin(THD *thd, Storage_Vector &clone_loc_vec,
                     Task_Vector &task_vec, Ha_clone_type clone_type,
                     Ha_clone_mode clone_mode) {
  assert(task_vec.empty());
  /* If Storage locators are empty, construct them here. */
  if (clone_loc_vec.empty()) {
    myclone::Hton clone_args;

    clone_args.m_loc_vec = &clone_loc_vec;
    clone_args.m_task_vec = &task_vec;
    clone_args.m_cur_index = 0;
    clone_args.m_err = 0;
    clone_args.m_type = clone_type;
    clone_args.m_mode = clone_mode;
    clone_args.m_data_dir = nullptr;

    plugin_foreach(thd, run_hton_clone_begin, MYSQL_STORAGE_ENGINE_PLUGIN,
                   &clone_args);
    if (clone_args.m_err != 0) {
      unwind_clone_begin(thd, clone_loc_vec, task_vec, clone_loc_vec.size(),
                         false, clone_args.m_err);
      clone_loc_vec.clear();
      task_vec.clear();
    }
    return clone_args.m_err;
  }

  const auto original_locators = clone_loc_vec;
  auto rollback = [&](int error) {
    if (clone_mode != HA_CLONE_MODE_RESTART) {
      unwind_clone_begin(thd, clone_loc_vec, task_vec, task_vec.size(),
                         false, error);
      clone_loc_vec = original_locators;
      task_vec.clear();
    }
    return error;
  };

  for (auto &loc_iter : clone_loc_vec) {
    uint32_t task_id = 0;

    Ha_clone_flagset flags;
    loc_iter.m_hton->clone_interface.clone_capability(flags);
    if ((clone_mode == HA_CLONE_MODE_ADD_TASK && !flags[HA_CLONE_MULTI_TASK]) ||
        (clone_mode == HA_CLONE_MODE_RESTART && !flags[HA_CLONE_RESTART])) {
      my_error(ER_NOT_SUPPORTED_YET, MYF(0), "Clone engine task/restart mode");
      return rollback(ER_NOT_SUPPORTED_YET);
    }
    auto err = loc_iter.m_hton->clone_interface.clone_begin(
        loc_iter.m_hton, thd, loc_iter.m_loc, loc_iter.m_loc_len, task_id,
        clone_type, clone_mode);

    if (err != 0) {
      return rollback(err);
    }

    task_vec.push_back(task_id);
  }

  return (0);
}

int hton_clone_copy(THD *thd, Storage_Vector &clone_loc_vec,
                    Task_Vector &task_vec, Ha_clone_cbk *clone_cbk) {
  uint index = 0;

  for (auto &loc_iter : clone_loc_vec) {
    assert(index < task_vec.size());
    clone_cbk->set_loc_index(index);

    auto err = loc_iter.m_hton->clone_interface.clone_copy(
        loc_iter.m_hton, thd, loc_iter.m_loc, loc_iter.m_loc_len,
        task_vec[index], clone_cbk);

    if (err != 0) {
      return (err);
    }
    index++;
  }

  return (0);
}

bool hton_clone_supports_restart(const Storage_Vector &locators) {
  for (const auto &locator : locators) {
    Ha_clone_flagset flags;
    locator.m_hton->clone_interface.clone_capability(flags);
    if (!flags[HA_CLONE_RESTART]) return false;
  }
  return true;
}

int hton_clone_end(THD *thd, Storage_Vector &clone_loc_vec,
                   Task_Vector &task_vec, int in_err) {
  uint index = 0;
  int first_error = 0;
  /* A multi-engine clone cannot resume unless every engine can resume.
  Give engines a terminal error so InnoDB does not retain the donor lock
  while waiting for a reconnect that the client cannot perform. */
  int end_error =
      (in_err != 0 && !hton_clone_supports_restart(clone_loc_vec))
          ? ER_CLONE_PROTOCOL
          : in_err;

  for (auto &loc_iter : clone_loc_vec) {
    assert(index < task_vec.size());
    auto err = loc_iter.m_hton->clone_interface.clone_end(
        loc_iter.m_hton, thd, loc_iter.m_loc, loc_iter.m_loc_len,
        task_vec[index], end_error);

    if (err != 0 && first_error == 0) first_error = err;
    if (end_error == 0) end_error = err;
    ++index;
  }

  return first_error;
}

/** Begin clone apply for current storage engine plugin
@param[in,out]	thd	server thread handle
@param[in]	plugin	storage plugin
@param[in]	arg	clone parameters
@return true if failure */
static bool run_hton_clone_apply_begin(THD *thd, plugin_ref plugin, void *arg) {
  auto clone_arg = static_cast<myclone::Hton *>(arg);

  auto hton = plugin_data<handlerton *>(plugin);

  if (hton->clone_interface.clone_apply_begin != nullptr) {
    myclone::Locator loc = {hton, nullptr, 0};
    uint32_t task_id = 0;

    assert(clone_arg->m_mode == HA_CLONE_MODE_VERSION);

    clone_arg->m_err = hton->clone_interface.clone_apply_begin(
        hton, thd, loc.m_loc, loc.m_loc_len, task_id, clone_arg->m_mode,
        clone_arg->m_data_dir);

    if (clone_arg->m_err != 0) return true;
    clone_arg->m_loc_vec->push_back(loc);
  }

  return (false);
}

int hton_clone_apply_begin(THD *thd, const char *clone_data_dir,
                           Storage_Vector &clone_loc_vec, Task_Vector &task_vec,
                           Ha_clone_mode clone_mode) {
  /* If Storage locators are empty, construct them here. */
  auto add_task = task_vec.empty();

  assert(clone_mode == HA_CLONE_MODE_RESTART || task_vec.empty());

  if (clone_loc_vec.empty()) {
    myclone::Hton clone_args;

    clone_args.m_loc_vec = &clone_loc_vec;
    clone_args.m_task_vec = &task_vec;
    clone_args.m_cur_index = 0;
    clone_args.m_err = 0;
    clone_args.m_type = HA_CLONE_HYBRID;
    clone_args.m_mode = clone_mode;
    clone_args.m_data_dir = clone_data_dir;

    plugin_foreach(thd, run_hton_clone_apply_begin, MYSQL_STORAGE_ENGINE_PLUGIN,
                   &clone_args);
    if (clone_args.m_err != 0) {
      unwind_clone_begin(thd, clone_loc_vec, task_vec, clone_loc_vec.size(),
                         true, clone_args.m_err);
      clone_loc_vec.clear();
      task_vec.clear();
    }
    return clone_args.m_err;
  }

  uint32_t loop_index [[maybe_unused]] = 0;

  const auto original_locators = clone_loc_vec;
  auto rollback = [&](int error) {
    if (clone_mode != HA_CLONE_MODE_RESTART) {
      unwind_clone_begin(thd, clone_loc_vec, task_vec, task_vec.size(),
                         true, error);
      clone_loc_vec = original_locators;
      task_vec.clear();
    }
    return error;
  };

  for (auto &loc_iter : clone_loc_vec) {
    uint32_t task_id = 0;

    Ha_clone_flagset flags;
    loc_iter.m_hton->clone_interface.clone_capability(flags);
    if ((clone_mode == HA_CLONE_MODE_ADD_TASK && !flags[HA_CLONE_MULTI_TASK]) ||
        (clone_mode == HA_CLONE_MODE_RESTART && !flags[HA_CLONE_RESTART])) {
      my_error(ER_NOT_SUPPORTED_YET, MYF(0), "Clone engine task/restart mode");
      return rollback(ER_NOT_SUPPORTED_YET);
    }
    auto err = loc_iter.m_hton->clone_interface.clone_apply_begin(
        loc_iter.m_hton, thd, loc_iter.m_loc, loc_iter.m_loc_len, task_id,
        clone_mode, clone_data_dir);

    if (err != 0) {
      return rollback(err);
    }

    if (add_task) {
      task_vec.push_back(task_id);
    }

    assert(task_vec[loop_index] == task_id);
    ++loop_index;
  }

  return (0);
}

int hton_clone_apply_error(THD *thd, Storage_Vector &clone_loc_vec,
                           Task_Vector &task_vec, int in_err) {
  assert(in_err != 0);

  uint index = 0;
  int first_error = 0;
  for (auto &loc_iter : clone_loc_vec) {
    assert(index < task_vec.size());
    auto err = loc_iter.m_hton->clone_interface.clone_apply(
        loc_iter.m_hton, thd, loc_iter.m_loc, loc_iter.m_loc_len,
        task_vec[index], in_err, nullptr);

    if (err != 0 && first_error == 0) first_error = err;
    ++index;
  }

  return first_error;
}

int hton_clone_apply_end(THD *thd, Storage_Vector &clone_loc_vec,
                         Task_Vector &task_vec, int in_err) {
  int first_error = 0;
  /* Blocking engines publish their completed checkpoint only after the
  other engines finish. Any earlier failure must reach their cleanup. */
  assert(task_vec.empty() || task_vec.size() == clone_loc_vec.size());
  for (int blocking_pass = 0; blocking_pass != 2; ++blocking_pass) {
    for (size_t index = 0; index < clone_loc_vec.size(); ++index) {
      const auto &loc = clone_loc_vec[index];
      Ha_clone_flagset flags;
      loc.m_hton->clone_interface.clone_capability(flags);
      const bool blocking = flags[HA_CLONE_BLOCKING] && !flags[HA_CLONE_HYBRID];
      if (blocking != (blocking_pass != 0)) continue;
      const uint task = task_vec.empty() ? 0 : task_vec[index];
      const int err = loc.m_hton->clone_interface.clone_apply_end(
          loc.m_hton, thd, loc.m_loc, loc.m_loc_len, task, in_err);
      if (err != 0 && first_error == 0) first_error = err;
      if (in_err == 0) in_err = err;
    }
  }
  return first_error;
}
