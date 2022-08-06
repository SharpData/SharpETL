-- workflow=http_transformation_test
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
-- target=temp
--  tableName=`source_data`


-- step=3
-- source=temp
--  tableName='source_data'
-- target=temp
--  tableName=`source_data_workday`
-- writeMode=append
with `workday_temp` as (select explode(from_json(value,
                                                 'struct<Report_Entry:array<struct<a:string,b:string,c:string,d:string,e:string,f:string,g:string,h:string,i:string,j:string,k:string,l:string,m:string,n:string,o:string,p:string,q:string>>>').Report_Entry)
                                   as Report_Entry
                        from `source_data`)


select Report_Entry.`a` as `a`,
       Report_Entry.`b` as `b`,
       Report_Entry.`c` as `c`,
       Report_Entry.`d` as `d`,
       Report_Entry.`e` as `e`,
       Report_Entry.`f` as `f`,
       Report_Entry.`g` as `g`,
       Report_Entry.`h` as `h`,
       Report_Entry.`i` as `i`,
       Report_Entry.`j` as `j`,
       Report_Entry.`k` as `k`,
       Report_Entry.`l` as `l`,
       Report_Entry.`m` as `m`,
       Report_Entry.`n` as `n`,
       Report_Entry.`o` as `o`,
       Report_Entry.`p` as `p`,
       Report_Entry.`q` as `q`,
       '${YEAR}'  as `year`,
       '${MONTH}' as `month`,
       '${DAY}'   as `day`,
       '${HOUR}'  as `hour`
from `workday_temp`;