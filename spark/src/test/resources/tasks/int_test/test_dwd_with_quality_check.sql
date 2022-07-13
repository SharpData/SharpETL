-- workflow=test_dwd_with_quality_check
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow


-- step=1
-- source=temp
-- target=variables
select from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMdd')               as `DATE_END`,
       from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), 'HH')                     as `HOUR_END`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')  as `EFFECTIVE_START_TIME`;

-- step=2
-- source=mysql
--  dbName=int_test
--  tableName=test_ods_for_quality_check
--  options
--   idColumn=order_id
--   column.phone.qualityCheckRules=power null check(error)
--   column.value.qualityCheckRules=empty check(warn)
-- target=temp
--  tableName=`643e9314`
select `order_id` as `order_id`,
       `phone` as `phone`,
       `value` as `value`,
       `bz_time` as `bz_time`,
       job_id as `job_id`,
        '${EFFECTIVE_START_TIME}' as effective_start_time,
       '9999-01-01 00:00:00'     as effective_end_time,
       '1'                       as is_active,
       '1'                       as is_latest,
       '${DATA_RANGE_START}'     as idempotent_key,
       '${DATE_END}'             as dw_insert_date
from `int_test`.`test_ods_for_quality_check`
where `dt` = '${DATA_RANGE_START}';

-- step=3
-- source=mysql
--  dbName=int_test
--  tableName=test_dwd_for_quality_check
-- target=temp
--  tableName=`e4eac1e9`
SELECT order_id,
       phone,
       value,
       job_id,
       job_time,
        bz_time,
       effective_start_time,
       effective_end_time,
       is_active,
       is_latest,
       idempotent_key,
       '${DATE_END}' as dw_insert_date
FROM test_dwd_for_quality_check
where dw_insert_date = (
    select max(dw_insert_date)
    from test_dwd_for_quality_check
);

-- step=4
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.ZipTableTransformer
--  methodName=transform
--   dwDataLoadType=full
--   sortFields=bz_time
--   odsViewName=`643e9314`
--   dwTableName=test_dwd_for_quality_check
--   dwViewName=`e4eac1e9`
--   primaryFields=order_id
--  transformerType=object
-- target=mysql
--  dbName=int_test
--  tableName=test_dwd_for_quality_check
-- writeMode=append

