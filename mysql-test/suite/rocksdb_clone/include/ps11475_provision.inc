--source include/have_rocksdb.inc
--source ../include/ps11475_suppress.inc
--let $port = `SELECT @@port`
--replace_result $CLONE_PLUGIN CLONE_PLUGIN
--eval INSTALL PLUGIN clone SONAME '$CLONE_PLUGIN'
if ($custom_column_family)
{
  CREATE TABLE clone_r (id INT, PRIMARY KEY(id) COMMENT 'cfname=donor_only') ENGINE=RocksDB;
}
if (!$custom_column_family)
{
  CREATE TABLE clone_r (id INT PRIMARY KEY) ENGINE=RocksDB;
}
CREATE TABLE clone_i (id INT PRIMARY KEY) ENGINE=InnoDB;
INSERT INTO clone_r VALUES (7);
INSERT INTO clone_i VALUES (7);
if ($large_file)
{
  ALTER TABLE clone_r ADD COLUMN payload LONGBLOB;
  UPDATE clone_r SET payload=REPEAT('x', 1024*1024);
  INSERT INTO clone_r VALUES (8, REPEAT('x', 1024*1024));
  INSERT INTO clone_r VALUES (9, REPEAT('x', 1024*1024));
  INSERT INTO clone_r VALUES (10, REPEAT('x', 1024*1024));
  SET GLOBAL rocksdb_force_flush_memtable_now = ON;
}
--connect (recipient,127.0.0.1,root,,test,$SERVER_PORT_2)
--source include/have_rocksdb.inc
--source ../include/ps11475_suppress.inc
--replace_result $CLONE_PLUGIN CLONE_PLUGIN
--eval INSTALL PLUGIN clone SONAME '$CLONE_PLUGIN'
CREATE TABLE old_recipient (id INT PRIMARY KEY) ENGINE=RocksDB;
INSERT INTO old_recipient VALUES (99);
SET GLOBAL clone_autotune_concurrency = OFF;
if (!$clone_workers)
{
  --let $clone_workers = 1
}
--eval SET GLOBAL clone_max_concurrency = $clone_workers
--replace_result $port PORT
--eval SET GLOBAL clone_valid_donor_list = '127.0.0.1:$port'
# MTR starts mysqld without a supervisor. A successful provisioning clone
# requests shutdown when it cannot request an automatic supervised restart.
--source include/expect_crash.inc
--replace_result $port PORT
--error 0,ER_RESTART_SERVER_FAILED,CR_SERVER_LOST,CR_SERVER_GONE_ERROR
--eval CLONE INSTANCE FROM 'root'@'127.0.0.1':$port IDENTIFIED BY ''
--source include/wait_until_disconnected.inc
--let $restart_parameters = restart:
--source include/start_mysqld.inc
USE test;
SELECT * FROM clone_i;
SELECT id FROM clone_r ORDER BY id;
if ($large_file)
{
  SELECT id, LENGTH(payload), SHA2(payload, 256) FROM clone_r ORDER BY id;
}
--let $assert_text = Provisioning must open the donor's column families and retain its rows
--let $assert_cond = `SELECT (SELECT COUNT(*) FROM clone_i WHERE id=7)=1 AND (SELECT COUNT(*) FROM clone_r WHERE id=7)=1`
--source include/assert.inc
--let $assert_text = Provisioning must remove the recipient's old tables
--let $assert_cond = `SELECT COUNT(*)=0 FROM information_schema.tables WHERE table_schema='test' AND table_name='old_recipient'`
--source include/assert.inc
# The old RocksDB database is destroyed once the received one opened.
--let $backup = `SELECT CONCAT(@@datadir, @@rocksdb_datadir, '_clone_old')`
--exec test ! -d "$backup"
DROP TABLE clone_i, clone_r;
UNINSTALL PLUGIN clone;
--disconnect recipient
--connection default
DROP TABLE clone_i, clone_r;
UNINSTALL PLUGIN clone;
