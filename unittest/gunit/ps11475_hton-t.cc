// Exercise the production clone coordinator with small engine callbacks.
// The plugin registry is the only server service replaced by this fixture.
#undef MYSQL_DYNAMIC_PLUGIN
#define DISABLE_ERROR_LOGGING
#include <gtest/gtest.h>
#include <array>
#include "plugin/clone/src/clone_hton.cc"

namespace {
std::array<handlerton, 3> engines;
std::array<uchar, 3> owned, borrowed;
std::array<int, 3> begin_errors, end_errors;
std::vector<int> ended, delivered_errors, applied;

void blocking_capability(Ha_clone_flagset &flags) {
  flags.reset();
  flags.set(HA_CLONE_BLOCKING);
  flags.set(HA_CLONE_MULTI_TASK);
  flags.set(HA_CLONE_RESTART);
}
void hybrid_capability(Ha_clone_flagset &flags) {
  blocking_capability(flags);
  flags.set(HA_CLONE_HYBRID);
}
int begin(handlerton *hton, THD *, const uchar *&loc, uint &length,
          uint &task, Ha_clone_type, Ha_clone_mode) {
  const auto id = hton->slot;
  if (begin_errors[id] != 0) return begin_errors[id];
  loc = &owned[id];
  length = 1;
  task = id + 10;
  return 0;
}
int apply_begin(handlerton *hton, THD *thd, const uchar *&loc, uint &length,
                uint &task, Ha_clone_mode mode, const char *) {
  return begin(hton, thd, loc, length, task, HA_CLONE_HYBRID, mode);
}
int end(handlerton *hton, THD *, const uchar *, uint, uint, int error) {
  ended.push_back(hton->slot);
  delivered_errors.push_back(error);
  return end_errors[hton->slot];
}
int apply_error(handlerton *hton, THD *, const uchar *, uint, uint, int error,
                Ha_clone_cbk *) {
  applied.push_back(hton->slot);
  return error;
}
}  // namespace

bool plugin_foreach_with_mask(THD *thd, plugin_foreach_func *callback,
                              int, uint, void *arg) {
  for (auto &engine : engines) {
    st_plugin_int plugin;
    plugin.data = &engine;
    auto *ptr = &plugin;
    if (callback(thd, plugin_int_to_ref(ptr), arg)) return true;
  }
  return false;
}

class CloneCoordinator : public ::testing::Test {
 protected:
  void SetUp() override {
    begin_errors.fill(0);
    end_errors.fill(0);
    ended.clear();
    delivered_errors.clear();
    applied.clear();
    for (uint i = 0; i < engines.size(); ++i) {
      auto &engine = engines[i];
      engine.slot = i;
      engine.clone_interface.clone_capability =
          i == 0 ? blocking_capability : hybrid_capability;
      engine.clone_interface.clone_begin = begin;
      engine.clone_interface.clone_end = end;
      engine.clone_interface.clone_apply_begin = apply_begin;
      engine.clone_interface.clone_apply_end = end;
      engine.clone_interface.clone_apply = apply_error;
    }
  }
  Storage_Vector locators() {
    Storage_Vector result;
    for (size_t i = 0; i < engines.size(); ++i)
      result.push_back({&engines[i], &borrowed[i], 1});
    return result;
  }
};

TEST_F(CloneCoordinator, DonorEnumerationUnwindsOnlySuccessfulEngines) {
  begin_errors[2] = ER_INTERNAL_ERROR;
  end_errors[1] = ER_QUERY_INTERRUPTED;
  Storage_Vector locators;
  Task_Vector tasks;
  EXPECT_EQ(ER_INTERNAL_ERROR, hton_clone_begin(nullptr, locators, tasks,
                                               HA_CLONE_HYBRID,
                                               HA_CLONE_MODE_START));
  EXPECT_EQ((std::vector<int>{1, 0}), ended);
  EXPECT_TRUE(locators.empty());
  EXPECT_TRUE(tasks.empty());
}

TEST_F(CloneCoordinator, VersionEnumerationUnwindsOnlySuccessfulEngines) {
  begin_errors[2] = ER_INTERNAL_ERROR;
  Storage_Vector locators;
  Task_Vector tasks;
  EXPECT_EQ(ER_INTERNAL_ERROR, hton_clone_apply_begin(nullptr, nullptr,
      locators, tasks, HA_CLONE_MODE_VERSION));
  EXPECT_EQ((std::vector<int>{1, 0}), ended);
  EXPECT_TRUE(locators.empty());
  EXPECT_TRUE(tasks.empty());
}

TEST_F(CloneCoordinator, ApplyFailureRestoresBorrowedLocators) {
  for (const auto mode : {HA_CLONE_MODE_START, HA_CLONE_MODE_ADD_TASK}) {
    ended.clear();
    begin_errors[2] = ER_INTERNAL_ERROR;
    auto original = locators();
    auto active = original;
    Task_Vector tasks;
    EXPECT_EQ(ER_INTERNAL_ERROR, hton_clone_apply_begin(nullptr, nullptr,
        active, tasks, mode));
    EXPECT_EQ((std::vector<int>{1, 0}), ended);
    EXPECT_TRUE(tasks.empty());
    for (size_t i = 0; i < original.size(); ++i)
      EXPECT_EQ(original[i].m_loc, active[i].m_loc);
  }
}

TEST_F(CloneCoordinator, DonorAttachFailureUnwindsEarlierTasks) {
  begin_errors[2] = ER_INTERNAL_ERROR;
  auto active = locators();
  Task_Vector tasks;
  EXPECT_EQ(ER_INTERNAL_ERROR, hton_clone_begin(nullptr, active, tasks,
      HA_CLONE_HYBRID, HA_CLONE_MODE_ADD_TASK));
  EXPECT_EQ((std::vector<int>{1, 0}), ended);
  EXPECT_TRUE(tasks.empty());
}

TEST_F(CloneCoordinator, EndFailureReachesEveryEngine) {
  end_errors[0] = ER_INTERNAL_ERROR;
  auto active = locators();
  Task_Vector tasks{10, 11, 12};
  EXPECT_EQ(ER_INTERNAL_ERROR, hton_clone_end(nullptr, active, tasks, 0));
  EXPECT_EQ((std::vector<int>{0, 1, 2}), ended);
  EXPECT_EQ((std::vector<int>{0, ER_INTERNAL_ERROR, ER_INTERNAL_ERROR}),
            delivered_errors);
}

TEST_F(CloneCoordinator, ApplyFinalizesBlockingEngineAfterOtherErrors) {
  end_errors[1] = ER_INTERNAL_ERROR;
  auto active = locators();
  Task_Vector tasks{10, 11, 12};
  EXPECT_EQ(ER_INTERNAL_ERROR, hton_clone_apply_end(nullptr, active, tasks, 0));
  EXPECT_EQ((std::vector<int>{1, 2, 0}), ended);
  EXPECT_EQ((std::vector<int>{0, ER_INTERNAL_ERROR, ER_INTERNAL_ERROR}),
            delivered_errors);
}

TEST_F(CloneCoordinator, ApplyErrorNotifiesEveryEngine) {
  auto active = locators();
  Task_Vector tasks{10, 11, 12};
  EXPECT_EQ(ER_INTERNAL_ERROR,
            hton_clone_apply_error(nullptr, active, tasks, ER_INTERNAL_ERROR));
  EXPECT_EQ((std::vector<int>{0, 1, 2}), applied);
}
