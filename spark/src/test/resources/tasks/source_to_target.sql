-- workflow=source_to_target
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=h2
--  dbName=int_test
--  tableName=source
-- target=h2
--  dbName=int_test
--  tableName=target
-- writeMode=append
SELECT id,
       value,
       '${JOB_ID}' as `job_id`,
       now() as `job_time`,
       bz_time
FROM source;
