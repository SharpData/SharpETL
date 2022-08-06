-- workflow=http_transformation_to_variables_test
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
       date_format(to_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), "YYYY-MM-dd'T'HH:mm:ssXXX") as `START_TIME_TIMESTAMP`,
       date_format(to_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), "YYYY-MM-dd'T'HH:mm:ssXXX") as `START_TIME_TIMESTAMP`;


-- step=2
-- source=http
--  url=http://localhost:1080/get_workday?satrt=${START_TIME_TIMESTAMP}&end=${START_TIME_TIMESTAMP}
--  fieldName=types
--  jsonPath=$.phoneNumbers[*].type
--  splitBy=__
-- target=variables


-- step=3
-- source=temp
-- target=temp
--  tableName=`target_data_types`
-- writeMode=append
select '${types}' as `types`;