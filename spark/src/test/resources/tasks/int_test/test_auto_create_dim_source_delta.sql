-- workflow=test_auto_create_dim_source_delta
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=mysql
--  dbName=int_test
--  tableName=test_delta_table
-- target=delta_lake
--  tableName=test_fact
-- writeMode=overwrite
SELECT id,
       bz_time
FROM int_test.test_delta_table
