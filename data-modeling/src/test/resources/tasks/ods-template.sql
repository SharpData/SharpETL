-- workflow=ods-template
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=mysql
--  dbName=db
--  tableName=table_name
-- target=hive
--  dbName=ods
--  tableName=ods_table_name
-- writeMode=append
SELECT `xx1` AS `xx1`,
       `xx2` AS `xx2`,
       `xx3` AS `xx3`,
       `xx4` AS `xx4`,
       `xx5` AS `xx5`,
       `xx6` AS `xx6`,
       `xx7` AS `xx7`,
       `xx8` AS `xx8`,
       `xx9` AS `xx9`,
       `xx10` AS `xx10`,
       now() AS `job_time`,
       ${JOB_ID} AS `job_id`,
       from_unixtime(unix_timestamp(xx10), '%Y') as `year`,
       from_unixtime(unix_timestamp(xx10), '%m') as `month`,
       from_unixtime(unix_timestamp(xx10), '%d') as `day`
FROM `db`.`table_name`
WHERE `xx10` >= '${DATA_RANGE_START}' AND `xx10` < '${DATA_RANGE_END}';