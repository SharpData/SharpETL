-- workflow=replace_template_tablename
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=temp
-- target=variables
select from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'yyyy') as `YEAR`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'MM')   as `MONTH`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'dd')   as `DAY`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'HH')   as `HOUR`,
       'temp_source' as `sources`,
       'temp_target' as `target`,
       'temp_end' as `end`


-- step=2
-- source=temp
--  tableName=${sources}
-- target=temp
--  tableName=${target}
-- writeMode=overwrite
select * from ${sources}


-- step=3
-- source=temp
--  tableName=${target}
-- target=temp
--  tableName=${end}
-- writeMode=overwrite
select * from ${target}
