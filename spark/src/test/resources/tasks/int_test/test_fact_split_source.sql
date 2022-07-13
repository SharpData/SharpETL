-- workflow=test_fact_split_source
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=mysql
--  dbName=int_test
--  tableName=test_fact_split_source
-- target=mysql
--  dbName=int_test
--  tableName=test_split
-- writeMode=append
SELECT id,
       user_id,
       user_name,
       user_account,
       bz_time,
       '${JOB_ID}' as `job_id`,
       now() as `job_time`,
       '${DATA_RANGE_START}' AS dt
FROM int_test.`test_fact_split_source`;
