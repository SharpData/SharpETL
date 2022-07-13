-- workflow=test_source_for_quality_check
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=mysql
--  dbName=int_test
--  tableName=test_source_for_quality_check
-- target=mysql
--  dbName=int_test
--  tableName=test_ods_for_quality_check
--  transaction=true
-- writeMode=append
SELECT `order_id`,
`phone`,
`value`,
`bz_time`,
       '${JOB_ID}' as `job_id`,
 '${DATA_RANGE_START}' AS dt,
       now() as `job_time`
FROM `test_source_for_quality_check`;
