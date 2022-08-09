-- workflow=ods-template2
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
       `xx11` AS `xx11`,
       `xx12` AS `xx12`,
       `xx13` AS `xx13`,
       `xx14` AS `xx14`,
       `xx15` AS `xx15`,
       `xx16` AS `xx16`,
       `xx17` AS `xx17`,
       `xx18` AS `xx18`,
       `xx19` AS `xx19`,
       `xx20` AS `xx20`,
       `xx21` AS `xx21`,
       `xx22` AS `xx22`,
       `xx23` AS `xx23`,
       `xx24` AS `xx24`,
       ${JOB_ID} AS `job_id`,
       from_unixtime(unix_timestamp(xx20), '%Y') as `year`,
       from_unixtime(unix_timestamp(xx20), '%m') as `month`,
       from_unixtime(unix_timestamp(xx20), '%d') as `day`
FROM `db`.`table_name`
WHERE `xx20` >= '${DATA_RANGE_START}' AND `xx20` < '${DATA_RANGE_END}'
AND `row1` = 'a' AND `row2` = 'b';
