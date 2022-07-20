-- workflow=sales.product__ods.t_product
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=postgres
--  dbName=postgres
--  tableName=sales.product
-- target=postgres
--  dbName=postgres
--  tableName=ods.t_product
-- writeMode=append
SELECT "mid" AS "product_code",
       "name" AS "product_name",
       "version" AS "product_version",
       "status" AS "product_status",
       "create_time" AS "create_time",
       "update_time" AS "update_time",
       ${JOB_ID} AS "job_id"
FROM "postgres"."sales"."product"
WHERE "update_time" >= '${DATA_RANGE_START}' AND "update_time" < '${DATA_RANGE_END}';
