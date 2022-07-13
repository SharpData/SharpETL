-- workflow=auto_inc_id_mode
--  loadType=incremental
--  logDrivenType=auto_inc_id

-- step=1
-- source=mysql
--  dbName=int_test
--  tableName=test_inc_id_table
-- target=variables
SELECT ${DATA_RANGE_START} AS `lowerBound`,
       MAX(`id`) AS `upperBound`
FROM `int_test`.`test_inc_id_table`;

-- step=2
-- source=mysql
--  dbName=int_test
--  tableName=test_inc_id_table
--  numPartitions=4
--  lowerBound=${lowerBound}
--  upperBound=${upperBound}
--  partitionColumn=id
-- target=mysql
--  dbName=int_test
--  tableName=ods_inc_id_table
--  transaction=false
-- writeMode=upsert
SELECT `id` AS `id`,
       `value` AS `value`,
       ${JOB_ID} AS `job_id`,
       now() AS `job_time`
FROM `int_test`.`test_inc_id_table`
WHERE `id` > ${lowerBound}
  AND `id` <= ${upperBound};
