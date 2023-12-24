-- workflow=test_dwd_with_quality_check
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow


-- step=var setup
-- source=temp
-- target=variables
select DATE_FORMAT(TO_TIMESTAMP('${DATA_RANGE_END}' , 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMdd')          as `DATE_END`,
       extract(hour from TO_TIMESTAMP('${DATA_RANGE_START}' , 'yyyy-MM-dd HH:mm:ss'))              as `HOUR_END`,
       TO_TIMESTAMP('${DATA_RANGE_START}' , 'yyyy-MM-dd HH:mm:ss')                                 as `EFFECTIVE_START_TIME`;

-- step=read temp data
-- source=temp
--  options
--   idColumn=order_id
--   sortColumn=order_id
--   column.phone.qualityCheckRules=power null check
--   column.value.qualityCheckRules=negative check
-- target=console
select 1212121242                as `order_id`,
       '11'                      as `phone`,
       '-1'                      as `value`,
       '${JOB_ID}'               as `job_id`,
       '${EFFECTIVE_START_TIME}' as effective_start_time,
       '9999-01-01 00:00:00'     as effective_end_time,
       '1'                       as is_active,
       '1'                       as is_latest,
       '${DATA_RANGE_START}'     as idempotent_key,
       '${DATE_END}'             as dw_insert_date

UNION ALL select 1212121243      as `order_id`,
       'null'                    as `phone`,
       '1'                       as `value`,
       '${JOB_ID}'               as `job_id`,
       '${EFFECTIVE_START_TIME}' as effective_start_time,
       '9999-01-01 00:00:00'     as effective_end_time,
       '1'                       as is_active,
       '1'                       as is_latest,
       '${DATA_RANGE_START}'     as idempotent_key,
       '${DATE_END}'             as dw_insert_date;

-- step=read check result
-- source=built_in
-- target=console
select * from `paimon`.`sharp_etl`.`quality_check_log` where job_id='${JOB_ID}';
