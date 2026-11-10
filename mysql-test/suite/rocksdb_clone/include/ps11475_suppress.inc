--disable_query_log
CALL mtr.add_suppression('Clone removing all user data for provisioning');
CALL mtr.add_suppression('Clone shutting down server as RESTART failed');
--enable_query_log
