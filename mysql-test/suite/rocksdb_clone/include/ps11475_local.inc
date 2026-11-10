--source include/have_rocksdb.inc
--source ../include/ps11475_suppress.inc
--let $clone_dir = $MYSQL_TMP_DIR/ps11475_local
--replace_result $CLONE_PLUGIN CLONE_PLUGIN
--eval INSTALL PLUGIN clone SONAME '$CLONE_PLUGIN'
SET GLOBAL clone_autotune_concurrency = OFF;
SET GLOBAL clone_max_concurrency = 2;
CREATE TABLE clone_i (id INT PRIMARY KEY) ENGINE=InnoDB;
CREATE TABLE clone_r (id INT PRIMARY KEY) ENGINE=RocksDB;
INSERT INTO clone_i VALUES (1);
INSERT INTO clone_r VALUES (1);
--replace_result $clone_dir CLONE_DIR
--eval CLONE LOCAL DATA DIRECTORY = '$clone_dir'
--let $restart_parameters = restart: --datadir=$clone_dir
--replace_result $clone_dir CLONE_DIR
--source include/restart_mysqld.inc
USE test;
SELECT * FROM clone_i;
SELECT * FROM clone_r;
--let $assert_text = Both storage engines must retain their rows after clone recovery
--let $assert_cond = `SELECT (SELECT COUNT(*) FROM clone_i)=1 AND (SELECT COUNT(*) FROM clone_r)=1`
--source include/assert.inc
--let $restart_parameters = restart:
--source include/restart_mysqld.inc
DROP TABLE clone_i, clone_r;
UNINSTALL PLUGIN clone;
--force-rmdir $clone_dir
