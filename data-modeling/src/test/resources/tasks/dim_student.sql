-- workflow=dim_student
--  loadType=incremental
--  logDrivenType=upstream
--  upstream=ods__t_student

-- step=1
-- source=hive
--  dbName=ods
--  tableName=t_student
-- target=temp
--  tableName=t_student__extracted
-- writeMode=overwrite
select
	uuid() as `student_id`,
	`student_code` as `student_code`,
	`student_name` as `student_name`,
	`student_age` as `student_age`,
	`student_address` as `student_address`,
	`student_blabla` as `student_blabla`,
	`student_create_time` as `student_create_time`,
	`student_update_time` as `student_update_time`,
	`year` as `year`,
	`month` as `month`,
	`day` as `day`
from `ods`.`t_student`
where `year` = '${YEAR}'
  and `month` = '${MONTH}'
  and `day` = '${DAY}'
and  1 = 1;

-- step=2
-- source=temp
--  tableName=t_student__extracted
-- target=temp
--  tableName=t_student__target_selected
-- writeMode=overwrite
select
	`student_id`,
	`student_code`,
	`student_name`,
	`student_age`,
	`student_address`,
	`student_create_time`,
	`student_update_time`,
	`year`,
	`month`,
	`day`,
	'0' as `is_auto_created`
from `t_student__extracted`;

-- step=3
-- source=hive
--  dbName=dim
--  tableName=t_dim_student
-- target=variables
select concat('where (',
  ifEmpty(
    concat_ws(')\n   or (', collect_set(concat_ws(' and ', concat('`year` = ', `year`), concat('`month` = ', `month`), concat('`day` = ', `day`)))),
    '1 = 1'),
  ')') as `DWD_UPDATED_PARTITION`
from (
  select
  dwd.*
  from `dim`.`t_dim_student` dwd
  left join `t_student__target_selected` incremental_data on dwd.student_code = incremental_data.student_code
  where incremental_data.student_code is not null
        and dwd.is_latest = 1
);

-- step=4
-- source=hive
--  dbName=dim
--  tableName=t_dim_student
-- target=temp
--  tableName=t_dim_student__changed_partition_view
select *
from `dim`.`t_dim_student`
${DWD_UPDATED_PARTITION};

-- step=5
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.SCDTransformer
--  methodName=transform
--   createTimeField=student_create_time
--   dwUpdateType=incremental
--   dwViewName=t_dim_student__changed_partition_view
--   odsViewName=t_student__target_selected
--   partitionField=student_create_time
--   partitionFormat=year/month/day
--   primaryFields=student_code
--   surrogateField=student_id
--   timeFormat=yyyy-MM-dd HH:mm:ss
--   updateTimeField=student_update_time
--  transformerType=object
-- target=hive
--  dbName=dim
--  tableName=t_dim_student
-- writeMode=overwrite
