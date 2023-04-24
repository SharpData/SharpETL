-- workflow=ods-template2
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=postgres
--  dbName=postgres
--  tableName=sales.product
-- target=hive
--  dbName=ods
--  tableName=t_product
-- writeMode=append
SELECT "mid" AS "product_code",
       "name" AS "product_name",
       "version" AS "product_version",
       "status" AS "product_status",
       "create_time" AS "create_time",
       "update_time" AS "update_time",
       '${JOB_ID}' AS "job_id",
       to_char("update_time", 'yyyy') as "year",
       to_char("update_time", 'MM') as "month",
       to_char("update_time", 'DD') as "day"
FROM "postgres"."sales"."product"
WHERE "update_time" >= '${DATA_RANGE_START}' AND "update_time" < '${DATA_RANGE_END}'
AND version=1 and (name= 'sss' or status= false);
