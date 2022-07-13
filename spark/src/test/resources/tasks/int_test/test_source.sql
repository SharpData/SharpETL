-- workflow=test_source
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=mysql
--  dbName=int_test
--  tableName=test_source
-- target=mysql
--  dbName=int_test
--  tableName=test_ods
-- writeMode=append
SELECT `order_id`,
`value`,
`bz_time`,
       '${JOB_ID}' as `job_id`,
 '${DATA_RANGE_START}' AS dt,
       now() as `job_time`
FROM int_test.`test_source`;
