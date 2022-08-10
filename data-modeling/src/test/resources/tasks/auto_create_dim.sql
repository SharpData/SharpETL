-- workflow=auto_create_dim
--  loadType=incremental
--  logDrivenType=upstream
--  upstream=ods__t_order

-- step=1
-- source=hive
--  dbName=ods
--  tableName=t_order
-- target=temp
--  tableName=t_order__extracted
-- writeMode=overwrite
select
	`order_id` as `order_id`,
	`order_sn` as `order_sn`,
	`product_code` as `product_code`,
	`product_name` as `product_name`,
	`product_version` as `product_version`,
	`product_status` as `product_status`,
	`user_code` as `user_code`,
	`user_name` as `user_name`,
	`user_age` as `user_age`,
	`user_address` as `user_address`,
	`class_code` as `class_code`,
	`class_name` as `class_name`,
	`class_address` as `class_address`,
	`product_count` as `product_count`,
	`price` as `price`,
	`discount` as `discount`,
	`order_status` as `order_status`,
	`order_create_time` as `order_create_time`,
	`order_update_time` as `order_update_time`,
	price - discount as `actual`,
	`year` as `year`,
	`month` as `month`,
	`day` as `day`
from `ods`.`t_order`
where `year` = '${YEAR}'
  and `month` = '${MONTH}'
  and `day` = '${DAY}'
and a = 'aa' or xxx;

-- step=2
-- source=temp
--  tableName=t_order__extracted
-- target=temp
--  tableName=t_order__grouped_dim
-- writeMode=overwrite
select `t_order`.`class_code` as `dim_t_dim_class____class_code`,
       `t_order`.`class_name` as `dim_t_dim_class____class_name`,
       `t_order`.`class_address` as `dim_t_dim_class____class_address`,
       `t_order`.`product_code` as `dim_t_dim_product____mid`,
       `t_order`.`product_name` as `dim_t_dim_product____name`,
       `t_order`.`product_version` as `dim_t_dim_product____product_version`,
       `t_order`.`product_status` as `dim_t_dim_product____product_status`,
       case
           when (
                `t_dim_class`.`class_code` is null
           ) then 'new'
           when (
                `t_order`.`class_code` != `t_dim_class`.`class_code` or
                `t_order`.`class_name` != `t_dim_class`.`class_name` or
                `t_order`.`class_address` != `t_dim_class`.`class_address`
           ) then 'updated'
           else 'nochange'
       end as `auto_created_t_dim_class_status`,
       case
           when (
                `t_dim_product`.`mid` is null
           ) then 'new'
           when (
                `t_order`.`product_code` != `t_dim_product`.`mid` or
                `t_order`.`product_name` != `t_dim_product`.`name` or
                `t_order`.`product_version` != `t_dim_product`.`product_version` or
                `t_order`.`product_status` != `t_dim_product`.`product_status`
           ) then 'updated'
           else 'nochange'
       end as `auto_created_t_dim_product_status`,
       `t_order`.`year` as `year`,
       `t_order`.`month` as `month`,
       `t_order`.`day` as `day`,
       `t_order`.`order_update_time` as `order_update_time`,
       `t_order`.`order_create_time` as `order_create_time`
from t_order__extracted `t_order`
     left join `dim`.`t_dim_class` `t_dim_class` -- TODO: year/month/day
               on `t_order`.`class_code` = `t_dim_class`.`class_code`
     left join `dim`.`t_dim_product` `t_dim_product` -- TODO: year/month/day
               on `t_order`.`product_code` = `t_dim_product`.`mid`;

-- step=3
-- source=temp
--  tableName=t_order__grouped_dim
-- target=temp
--  tableName=t_order__selected_dim
-- writeMode=overwrite
select `class_id`,`class_code`,`class_name`,`class_address`,`order_update_time`,`order_create_time`,`is_auto_created`,`year`,`month`,`day` from (
  select uuid() as `class_id`,
       `dim_t_dim_class____class_code` as `class_code`,
       `dim_t_dim_class____class_name` as `class_name`,
       `dim_t_dim_class____class_address` as `class_address`,
       '1' as `is_auto_created`,
       `year`,
       `month`,
       `day`,
       `order_update_time`,
       `order_create_time`,
       row_number() OVER (PARTITION BY `dim_t_dim_class____class_code` ORDER BY `order_update_time` DESC) as row_number
  from t_order__grouped_dim t_order__grouped_dim
  where (`dim_t_dim_class____class_code` is not null)
    and (`auto_created_t_dim_class_status` = 'new')
) where row_number = 1;

-- step=4
-- source=hive
--  dbName=dim
--  tableName=t_dim_class
-- target=variables
select concat('where (',
  ifEmpty(
    concat_ws(')\n   or (', collect_set(concat_ws(' and ', concat('`year` = ', `year`), concat('`month` = ', `month`), concat('`day` = ', `day`)))),
    '1 = 1'),
  ')') as `DWD_UPDATED_PARTITION`
from (
  select dwd.*
  from `dim`.`t_dim_class` dwd
  left join `t_order__selected_dim` incremental_data on dwd.class_code = incremental_data.class_code
  where incremental_data.class_code is not null
        and dwd.is_latest = 1
);

-- step=5
-- source=hive
--  dbName=dim
--  tableName=t_dim_class
-- target=temp
--  tableName=t_dim_class__changed_partition_view
select *
from `dim`.`t_dim_class`
${DWD_UPDATED_PARTITION};

-- step=6
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.SCDTransformer
--  methodName=transform
--   createTimeField=order_create_time
--   dropUpdateTimeField=true
--   dwUpdateType=incremental
--   dwViewName=t_dim_class__changed_partition_view
--   odsViewName=t_order__selected_dim
--   primaryFields=class_code
--   surrogateField=class_id
--   timeFormat=yyyy-MM-dd HH:mm:ss
--   updateTimeField=order_update_time
--  transformerType=object
-- target=hive
--  dbName=dim
--  tableName=t_dim_class
-- writeMode=overwrite

-- step=7
-- source=temp
--  tableName=t_order__grouped_dim
-- target=temp
--  tableName=t_order__selected_dim
-- writeMode=overwrite
select uuid() as `product_id`,
       `dim_t_dim_product____mid` as `mid`,
       `dim_t_dim_product____name` as `name`,
       `dim_t_dim_product____product_version` as `product_version`,
       `dim_t_dim_product____product_status` as `product_status`,
       '1' as `is_auto_created`,
       `year`,
       `month`,
       `day`,
       `order_update_time`,
       `order_create_time`
from t_order__grouped_dim t_order__grouped_dim
where (`dim_t_dim_product____mid` is not null)
  and (
      `auto_created_t_dim_product_status` = 'new'
       or `auto_created_t_dim_product_status` = 'updated');

-- step=8
-- source=hive
--  dbName=dim
--  tableName=t_dim_product
-- target=variables
select concat('where (',
  ifEmpty(
    concat_ws(')\n   or (', collect_set(concat_ws(' and ', concat('`year` = ', `year`), concat('`month` = ', `month`), concat('`day` = ', `day`)))),
    '1 = 1'),
  ')') as `DWD_UPDATED_PARTITION`
from (
  select dwd.*
  from `dim`.`t_dim_product` dwd
  left join `t_order__selected_dim` incremental_data on dwd.mid = incremental_data.mid
  where incremental_data.mid is not null
        and dwd.is_latest = 1
);

-- step=9
-- source=hive
--  dbName=dim
--  tableName=t_dim_product
-- target=temp
--  tableName=t_dim_product__changed_partition_view
select *
from `dim`.`t_dim_product`
${DWD_UPDATED_PARTITION};

-- step=10
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.SCDTransformer
--  methodName=transform
--   createTimeField=order_create_time
--   dropUpdateTimeField=true
--   dwUpdateType=incremental
--   dwViewName=t_dim_product__changed_partition_view
--   odsViewName=t_order__selected_dim
--   primaryFields=mid
--   surrogateField=product_id
--   timeFormat=yyyy-MM-dd HH:mm:ss
--   updateTimeField=order_update_time
--  transformerType=object
-- target=hive
--  dbName=dim
--  tableName=t_dim_product
-- writeMode=overwrite

-- step=11
-- source=temp
--  tableName=t_order__extracted
-- target=temp
--  tableName=t_order__joined
-- writeMode=append
select
	`t_order__extracted`.*,
	case when `t_dim_class`.`class_id` is null then '-1'
		else `t_dim_class`.`class_id`
	end as `class_id`,
	case when `t_dim_product`.`product_id` is null then '-1'
		else `t_dim_product`.`product_id`
	end as `product_id`,
	case when `t_dim_user`.`dim_user_id` is null then '-1'
		else `t_dim_user`.`dim_user_id`
	end as `user_id`
from `t_order__extracted`
left join `dim`.`t_dim_class` `t_dim_class`
 on `t_order__extracted`.`class_code` = `t_dim_class`.`class_code`
 and `t_order__extracted`.`order_create_time` >= `t_dim_class`.`start_time`
 and (`t_order__extracted`.`order_create_time` < `t_dim_class`.`end_time`
      or `t_dim_class`.`end_time` is null)

left join `dim`.`t_dim_product` `t_dim_product`
 on `t_order__extracted`.`product_code` = `t_dim_product`.`mid`
 and `t_order__extracted`.`order_create_time` >= `t_dim_product`.`start_time`
 and (`t_order__extracted`.`order_create_time` < `t_dim_product`.`end_time`
      or `t_dim_product`.`end_time` is null)

left join `dim`.`t_dim_user` `t_dim_user`
 on `t_order__extracted`.`user_code` = `t_dim_user`.`user_info_code`
 and `t_order__extracted`.`order_create_time` >= `t_dim_user`.`start_time`
 and (`t_order__extracted`.`order_create_time` < `t_dim_user`.`end_time`
      or `t_dim_user`.`end_time` is null);

-- step=12
-- source=temp
--  tableName=t_order__joined
-- target=temp
--  tableName=t_order__target_selected
-- writeMode=overwrite
select
	`order_id`,
	`order_sn`,
	`product_id`,
	`user_id`,
	`class_id`,
	`product_count`,
	`price`,
	`discount`,
	`order_status`,
	`order_create_time`,
	`order_update_time`,
	`actual`,
	`year`,
	`month`,
	`day`
from `t_order__joined`;

-- step=13
-- source=hive
--  dbName=dwd
--  tableName=t_fact_order
-- target=variables
select concat('where (',
  ifEmpty(
    concat_ws(')\n   or (', collect_set(concat_ws(' and ', concat('`year` = ', `year`), concat('`month` = ', `month`), concat('`day` = ', `day`)))),
    '1 = 1'),
  ')') as `DWD_UPDATED_PARTITION`
from (
  select
  dwd.*
  from `dwd`.`t_fact_order` dwd
  left join `t_order__target_selected` incremental_data on dwd.order_id = incremental_data.order_id
  where incremental_data.order_id is not null
        and dwd.is_latest = 1
);

-- step=14
-- source=hive
--  dbName=dwd
--  tableName=t_fact_order
-- target=temp
--  tableName=t_fact_order__changed_partition_view
select *
from `dwd`.`t_fact_order`
${DWD_UPDATED_PARTITION};

-- step=15
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.SCDTransformer
--  methodName=transform
--   createTimeField=order_create_time
--   dwUpdateType=incremental
--   dwViewName=t_fact_order__changed_partition_view
--   odsViewName=t_order__target_selected
--   partitionField=order_create_time
--   partitionFormat=year/month/day
--   primaryFields=order_id
--   surrogateField=
--   timeFormat=yyyy-MM-dd HH:mm:ss
--   updateTimeField=order_update_time
--  transformerType=object
-- target=hive
--  dbName=dwd
--  tableName=t_fact_order
-- writeMode=overwrite
