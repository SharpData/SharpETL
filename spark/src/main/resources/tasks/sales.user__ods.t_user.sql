-- workflow=sales.user__ods.t_user
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=postgres
--  dbName=postgres
--  tableName=sales.user
-- target=postgres
--  dbName=postgres
--  tableName=ods.t_user
-- writeMode=append
SELECT "user_code" AS "user_code",
       "user_name" AS "user_name",
       "user_age" AS "user_age",
       "user_address" AS "user_address",
       "create_time" AS "create_time",
       "update_time" AS "update_time",
       '${JOB_ID}' AS "job_id"
FROM "postgres"."sales"."user"
WHERE "update_time" >= '${DATA_RANGE_START}' AND "update_time" < '${DATA_RANGE_END}';
