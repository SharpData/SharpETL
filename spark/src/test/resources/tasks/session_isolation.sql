-- workflow=session_isolation
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=temp
-- target=temp
--  tableName=do_nothing_table
SELECT 'step1';

-- step=2
-- source=temp
-- target=temp
--  tableName=do_nothing_table
-- conf
--  spark.sql.shuffle.partitions=1
SELECT 'step2';

-- step=3
-- source=temp
-- target=temp
--  tableName=do_nothing_table
-- conf
--  spark.sql.shuffle.partitions=5
SET spark.sql.hive.version=0.12.1;

