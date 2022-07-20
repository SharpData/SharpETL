-- workflow=ods.t_order_dwd.t_fact_order
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=postgres
--  dbName=postgres
--  tableName=ods.t_order
-- target=temp
--  tableName=ods_t_order__extracted
-- writeMode=overwrite
select
	"order_sn" as "order_sn",
	"product_code" as "product_code",
	"product_name" as "product_name",
	"product_version" as "product_version",
	"product_status" as "product_status",
	"user_code" as "user_code",
	"user_name" as "user_name",
	"user_age" as "user_age",
	"user_address" as "user_address",
	"product_count" as "product_count",
	"price" as "price",
	"discount" as "discount",
	"order_status" as "order_status",
	"order_create_time" as "order_create_time",
	"order_update_time" as "order_update_time",
	price - discount as "actual"
from "postgres"."ods"."t_order"
where "job_id" = '${DATA_RANGE_START}';

-- step=2
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.JdbcAutoCreateDimTransformer
--  methodName=transform
--   createDimMode=once
--   currentAndDimColumnsMapping={"order_create_time":"create_time","user_code":"user_info_code","user_name":"user_name","user_address":"user_address","user_age":"user_age"}
--   currentAndDimPrimaryMapping={"user_code":"user_info_code"}
--   currentBusinessCreateTime=order_create_time
--   dimDb=postgres
--   dimDbType=postgres
--   dimTable=dwd.t_dim_user
--   dimTableColumnsAndType={"user_info_code":"varchar(128)","create_time":"timestamp","user_name":"varchar(128)","user_address":"varchar(128)","user_age":"int"}
--   updateTable=ods_t_order__extracted
--  transformerType=object
-- target=do_nothing

-- step=3
-- source=postgres
--  dbName=postgres
--  tableName=dwd.t_dim_product
-- target=temp
--  tableName=postgres_dwd_t_dim_product__matched
-- writeMode=append
select
 "product_id", "mid", "start_time", "end_time"
from "postgres"."dwd"."t_dim_product";

-- step=4
-- source=postgres
--  dbName=postgres
--  tableName=dwd.t_dim_user
-- target=temp
--  tableName=postgres_dwd_t_dim_user__matched
-- writeMode=append
select
 "dim_user_id", "user_info_code", "start_time", "end_time"
from "postgres"."dwd"."t_dim_user";

-- step=5
-- source=temp
--  tableName=ods_t_order__extracted
-- target=temp
--  tableName=ods_t_order__joined
-- writeMode=append
select
	`ods_t_order__extracted`.*,
	case when `postgres_dwd_t_dim_product__matched`.`product_id` is null then '-1'
		else `postgres_dwd_t_dim_product__matched`.`product_id` end as `product_id`,
	case when `postgres_dwd_t_dim_user__matched`.`dim_user_id` is null then '-1'
		else `postgres_dwd_t_dim_user__matched`.`dim_user_id` end as `user_id`
from `ods_t_order__extracted`
left join `postgres_dwd_t_dim_product__matched`
 on `ods_t_order__extracted`.`product_code` = `postgres_dwd_t_dim_product__matched`.`mid`
 and `ods_t_order__extracted`.`order_create_time` >= `postgres_dwd_t_dim_product__matched`.`start_time`
 and (`ods_t_order__extracted`.`order_create_time` < `postgres_dwd_t_dim_product__matched`.`end_time`
      or `postgres_dwd_t_dim_product__matched`.`end_time` is null)

left join `postgres_dwd_t_dim_user__matched`
 on `ods_t_order__extracted`.`user_code` = `postgres_dwd_t_dim_user__matched`.`user_info_code`
 and `ods_t_order__extracted`.`order_create_time` >= `postgres_dwd_t_dim_user__matched`.`start_time`
 and (`ods_t_order__extracted`.`order_create_time` < `postgres_dwd_t_dim_user__matched`.`end_time`
      or `postgres_dwd_t_dim_user__matched`.`end_time` is null);

-- step=6
-- source=temp
--  tableName=ods_t_order__joined
--  options
--   idColumn=order_sn
--   column.product_id.qualityCheckRules=mismatch dim check
--   column.order_sn.qualityCheckRules=duplicated check
-- target=temp
--  tableName=ods_t_order__target_selected
-- writeMode=overwrite
select
	`order_sn`,
	`product_id`,
	`user_id`,
	`product_count`,
	`price`,
	`discount`,
	`order_status`,
	`order_create_time`,
	`order_update_time`,
	`actual`
from `ods_t_order__joined`;

-- step=7
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.JdbcLoadTransformer
--  methodName=transform
--   businessCreateTime=order_create_time
--   businessUpdateTime=order_update_time
--   currentDb=postgres
--   currentDbType=postgres
--   currentTable=dwd.t_fact_order
--   currentTableColumnsAndType={"order_status":"varchar(128)","actual":"decimal(10,4)","order_create_time":"timestamp","user_id":"varchar(128)","product_count":"int","price":"decimal(10,4)","product_id":"varchar(128)","discount":"decimal(10,4)","order_update_time":"timestamp","order_sn":"varchar(128)"}
--   primaryFields=order_sn
--   slowChanging=false
--   updateTable=ods_t_order__target_selected
--   updateType=incremental
--  transformerType=object
-- target=do_nothing

