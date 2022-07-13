-- workflow=fact_device
--  loadType=incremental
--  logDrivenType=upstream
--  upstream=ods__t_device

-- step=1
-- source=hive
--  dbName=ods
--  tableName=t_device
-- target=temp
--  tableName=t_device__extracted
-- writeMode=overwrite
select
	`device_id` as `device_id`,
	`manufacturer` as `manufacturer`,
	`status` as `status`,
	`online` as `online`,
	`create_time` as `create_time`,
	`update_time` as `update_time`,
	`year` as `year`,
	`month` as `month`,
	`day` as `day`
from `ods`.`t_device`
where `year` = '${YEAR}'
  and `month` = '${MONTH}'
  and `day` = '${DAY}';

-- step=2
-- source=temp
--  tableName=t_device__extracted
-- target=temp
--  tableName=t_device__joined
-- writeMode=append
select
	`t_device__extracted`.*,
	case when `t_dim_user`.`user_code` is null then '-1'
		else `t_dim_user`.`user_code`
	end as `user_id`
from `t_device__extracted`
left join `dim`.`t_dim_user` `t_dim_user`
 on `t_device__extracted`.`user_id` = `t_dim_user`.`user_code`
 and `t_device__extracted`.`create_time` >= `t_dim_user`.`start_time`
 and (`t_device__extracted`.`create_time` < `t_dim_user`.`end_time`
      or `t_dim_user`.`end_time` is null);

-- step=3
-- source=temp
--  tableName=t_device__joined
-- target=temp
--  tableName=t_device__target_selected
-- writeMode=overwrite
select
	`device_id`,
	`manufacturer`,
	`user_id`,
	`status`,
	`online`,
	`create_time`,
	`update_time`,
	`year`,
	`month`,
	`day`
from `t_device__joined`;

-- step=4
-- source=hive
--  dbName=dwd
--  tableName=t_fact_device
-- target=variables
select concat('where (',
  ifEmpty(
    concat_ws(')\n   or (', collect_set(concat_ws(' and ', concat('`year` = ', `year`), concat('`month` = ', `month`), concat('`day` = ', `day`)))),
    '1 = 1'),
  ')') as `DWD_UPDATED_PARTITION`
from (
  select
  dwd.*
  from `dwd`.`t_fact_device` dwd
  left join `t_device__target_selected` incremental_data on dwd.device_id = incremental_data.device_id
  where incremental_data.device_id is not null
        and dwd.is_latest = 1
);

-- step=5
-- source=hive
--  dbName=dwd
--  tableName=t_fact_device
-- target=temp
--  tableName=t_fact_device__changed_partition_view
select *
from `dwd`.`t_fact_device`
${DWD_UPDATED_PARTITION};

-- step=6
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.SCDTransformer
--  methodName=transform
--   createTimeField=create_time
--   dwUpdateType=incremental
--   dwViewName=t_fact_device__changed_partition_view
--   odsViewName=t_device__target_selected
--   partitionField=create_time
--   partitionFormat=year/month/day
--   primaryFields=device_id
--   surrogateField=
--   timeFormat=yyyy-MM-dd HH:mm:ss
--   updateTimeField=update_time
--  transformerType=object
-- target=hive
--  dbName=dwd
--  tableName=t_fact_device
-- writeMode=overwrite