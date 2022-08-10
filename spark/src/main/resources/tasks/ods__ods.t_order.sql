-- workflow=ods__ods.t_order
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=postgres
--  dbName=postgres
--  tableName=sales.order
-- target=postgres
--  dbName=postgres
--  tableName=ods.t_order
-- writeMode=append
SELECT "order_sn" AS "order_sn",
       "product_code" AS "product_code",
       "product_name" AS "product_name",
       "product_version" AS "product_version",
       "product_status" AS "product_status",
       "user_code" AS "user_code",
       "user_name" AS "user_name",
       "user_age" AS "user_age",
       "user_address" AS "user_address",
       "product_count" AS "product_count",
       "price" AS "price",
       "discount" AS "discount",
       "order_status" AS "order_status",
       "order_create_time" AS "order_create_time",
       "order_update_time" AS "order_update_time",
       ${JOB_ID} AS "job_id"
FROM "postgres"."sales"."order"
WHERE "order_update_time" >= '${DATA_RANGE_START}' AND "order_update_time" < '${DATA_RANGE_END}'
AND "order_sn" = 'AAA';
