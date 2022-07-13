-- workflow=test_fact_split
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
--  tableName=test_split
--  options
--   idColumn=id
-- target=temp
--  tableName=`90fab300`
select
         `test_split`.`id` as `id`,
       IFNULL(`test_user`.`id`, '-1') as `user_id`,
       `bz_time`                   as `bz_time`,
        '${EFFECTIVE_START_TIME}' as effective_start_time,
        '9999-01-01 00:00:00'      as effective_end_time,
        '1'                        as is_active,
        '1'                        as is_latest,
        '${DATA_RANGE_START}'     as idempotent_key,
        '${DATE_END}'             as dw_insert_date

from `test_split` `test_split`
         left join `test_user` `test_user`
                   on `test_split`.`user_id` = `test_user`.`id`

where `test_split`.`dt` = '${DATA_RANGE_START}';

-- step=3
-- source=mysql
--  dbName=int_test
--  tableName=test_fact_split
-- target=temp
--  tableName=`6bec659c`
SELECT *
FROM test_fact_split
where dw_insert_date = (
    select max(dw_insert_date)
    from test_fact_split
);

-- step=4
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.ZipTableTransformer
--  methodName=transform
--   dwDataLoadType=full
--   sortFields=bz_time
--   odsViewName=`90fab300`
--   dwTableName=test_fact_split
--   dwViewName=`6bec659c`
--   primaryFields=id
--  transformerType=object
-- target=mysql
--  dbName=int_test
--  tableName=test_fact_split
-- writeMode=append

