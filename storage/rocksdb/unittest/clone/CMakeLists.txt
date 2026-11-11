# Copyright (c) 2026 Percona LLC and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA

# The test includes rdb_clone.cc, so it inherits the MyRocks build settings
# of the parent directory, except for the RocksDB copy of googletest.
GET_PROPERTY(ROCKSDB_CLONE_TEST_INCLUDES DIRECTORY PROPERTY INCLUDE_DIRECTORIES)
LIST(FILTER ROCKSDB_CLONE_TEST_INCLUDES EXCLUDE REGEX "/third-party/gtest")
SET_PROPERTY(DIRECTORY PROPERTY
  INCLUDE_DIRECTORIES ${ROCKSDB_CLONE_TEST_INCLUDES})
INCLUDE_DIRECTORIES(SYSTEM ${GMOCK_INCLUDE_DIRS})

MYSQL_ADD_EXECUTABLE(rdb_clone-t rdb_clone-t.cc
  ADD_TEST rdb_clone
  DEPENDENCIES GenError
  ENABLE_EXPORTS
  SKIP_INSTALL
  LINK_LIBRARIES gunit_small rocksdb_sources
  )
