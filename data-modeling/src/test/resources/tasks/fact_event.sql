-- workflow=fact_event
--  loadType=incremental
--  logDrivenType=upstream
--  upstream=ods__t_event

-- step=1
-- source=hive
--  dbName=ods
--  tableName=t_event
-- target=temp
--  tableName=t_event__extracted
-- writeMode=overwrite
select
	`event_id` as `event_id`,
	`device_IMEI` as `device_IMEI`,
	`device_model` as `device_model`,
	`device_version` as `device_version`,
	`device_language` as `device_language`,
	`event_status` as `event_status`,
	`create_time` as `create_time`,
	`update_time` as `update_time`,
	`year` as `year`,
	`month` as `month`,
	`day` as `day`
from `ods`.`t_event`
where `year` = '${YEAR}'
  and `month` = '${MONTH}'
  and `day` = '${DAY}';

-- step=2
-- source=temp
--  tableName=t_event__extracted
-- target=temp
--  tableName=t_event__grouped_dim
-- writeMode=overwrite
select `t_event`.`device_IMEI` as `dim_t_dim_device____device_imei`,
       `t_event`.`device_model` as `dim_t_dim_device____device_model`,
       `t_event`.`device_version` as `dim_t_dim_device____device_version`,
       `t_event`.`device_language` as `dim_t_dim_device____device_language`,
       case
           when (
                `t_dim_device`.`device_imei` is null
           ) then 'new'
           when (
                `t_event`.`device_IMEI` != `t_dim_device`.`device_imei` or
                `t_event`.`device_model` != `t_dim_device`.`device_model` or
                `t_event`.`device_version` != `t_dim_device`.`device_version` or
                `t_event`.`device_language` != `t_dim_device`.`device_language`
           ) then 'updated'
           else 'nochange'
       end as `auto_created_t_dim_device_status`,
       `t_event`.`year` as `year`,
       `t_event`.`month` as `month`,
       `t_event`.`day` as `day`,
       `t_event`.`update_time` as `update_time`,
       `t_event`.`create_time` as `create_time`
from t_event__extracted `t_event`
     left join `dim`.`t_dim_device` `t_dim_device` -- TODO: year/month/day
               on `t_event`.`device_IMEI` = `t_dim_device`.`device_imei`;

-- step=3
-- source=temp
--  tableName=t_event__grouped_dim
-- target=temp
--  tableName=t_event__selected_dim
-- writeMode=overwrite
select uuid() as `device_id`,
       `dim_t_dim_device____device_imei` as `device_imei`,
       `dim_t_dim_device____device_model` as `device_model`,
       `dim_t_dim_device____device_version` as `device_version`,
       `dim_t_dim_device____device_language` as `device_language`,
       '1' as `is_auto_created`,
       `year`,
       `month`,
       `day`,
       `update_time`,
       `create_time`
from t_event__grouped_dim t_event__grouped_dim
where (`dim_t_dim_device____device_imei` is not null)
  and (
      `auto_created_t_dim_device_status` = 'new'
       or `auto_created_t_dim_device_status` = 'updated');

-- step=4
-- source=hive
--  dbName=dim
--  tableName=t_dim_device
-- target=variables
select concat('where (',
  ifEmpty(
    concat_ws(')\n   or (', collect_set(concat_ws(' and ', concat('`year` = ', `year`), concat('`month` = ', `month`), concat('`day` = ', `day`)))),
    '1 = 1'),
  ')') as `DWD_UPDATED_PARTITION`
from (
  select dwd.*
  from `dim`.`t_dim_device` dwd
  left join `t_event__selected_dim` incremental_data on dwd.device_imei = incremental_data.device_imei
  where incremental_data.device_imei is not null
        and dwd.is_latest = 1
);

-- step=5
-- source=hive
--  dbName=dim
--  tableName=t_dim_device
-- target=temp
--  tableName=t_dim_device__changed_partition_view
select *
from `dim`.`t_dim_device`
${DWD_UPDATED_PARTITION};

-- step=6
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.SCDTransformer
--  methodName=transform
--   createTimeField=create_time
--   dropUpdateTimeField=true
--   dwUpdateType=incremental
--   dwViewName=t_dim_device__changed_partition_view
--   odsViewName=t_event__selected_dim
--   primaryFields=device_imei
--   surrogateField=device_id
--   timeFormat=yyyy-MM-dd HH:mm:ss
--   updateTimeField=update_time
--  transformerType=object
-- target=hive
--  dbName=dim
--  tableName=t_dim_device
-- writeMode=overwrite

-- step=7
-- source=temp
--  tableName=t_event__extracted
-- target=temp
--  tableName=t_event__joined
-- writeMode=append
select
	`t_event__extracted`.*,
	case when `t_dim_device`.`device_id` is null then '-1'
		else `t_dim_device`.`device_id`
	end as `device_id`
from `t_event__extracted`
left join `dim`.`t_dim_device` `t_dim_device`
 on `t_event__extracted`.`device_IMEI` = `t_dim_device`.`device_imei`
 and `t_event__extracted`.`create_time` >= `t_dim_device`.`start_time`
 and (`t_event__extracted`.`create_time` < `t_dim_device`.`end_time`
      or `t_dim_device`.`end_time` is null);

-- step=8
-- source=temp
--  tableName=t_event__joined
-- target=temp
--  tableName=t_event__target_selected
-- writeMode=overwrite
select
	`event_id`,
	`device_id`,
	`event_status`,
	`create_time`,
	`update_time`,
	`year`,
	`month`,
	`day`
from `t_event__joined`;

-- step=9
-- source=hive
--  dbName=dwd
--  tableName=t_fact_event
-- target=variables
select concat('where (',
  ifEmpty(
    concat_ws(')\n   or (', collect_set(concat_ws(' and ', concat('`year` = ', `year`), concat('`month` = ', `month`), concat('`day` = ', `day`)))),
    '1 = 1'),
  ')') as `DWD_UPDATED_PARTITION`
from (
  select
  dwd.*
  from `dwd`.`t_fact_event` dwd
  left join `t_event__target_selected` incremental_data on dwd.event_id = incremental_data.event_id
  where incremental_data.event_id is not null
        and '1' = '1'
);

-- step=10
-- source=hive
--  dbName=dwd
--  tableName=t_fact_event
-- target=temp
--  tableName=t_fact_event__changed_partition_view
select *
from `dwd`.`t_fact_event`
${DWD_UPDATED_PARTITION};

-- step=11
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.NonSCDTransformer
--  methodName=transform
--   createTimeField=create_time
--   dwUpdateType=incremental
--   dwViewName=t_fact_event__changed_partition_view
--   odsViewName=t_event__target_selected
--   partitionField=create_time
--   partitionFormat=year/month/day
--   primaryFields=event_id
--   surrogateField=
--   timeFormat=yyyy-MM-dd HH:mm:ss
--   updateTimeField=update_time
--  transformerType=object
-- target=hive
--  dbName=dwd
--  tableName=t_fact_event
-- writeMode=overwrite
