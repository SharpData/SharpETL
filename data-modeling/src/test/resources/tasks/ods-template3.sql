-- workflow=ods-template3
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=postgres
--  dbName=postgres
--  tableName=sales.order
-- target=hive
--  dbName=ods
--  tableName=t_order
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
       ${JOB_ID} AS "job_id",
       to_char("order_update_time", 'yyyy') as "year",
       to_char("order_update_time", 'MM') as "month",
       to_char("order_update_time", 'DD') as "day"
FROM "postgres"."sales"."order"
WHERE "order_update_time" >= '${DATA_RANGE_START}' AND "order_update_time" < '${DATA_RANGE_END}'
AND a=1 and (c= 'sss' or d= false);
