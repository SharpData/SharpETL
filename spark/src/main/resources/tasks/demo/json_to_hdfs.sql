-- workflow=json_to_hdfs
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=ftp
--  configPrefix=ehr
--  fileNamePattern=()(Org_\d{14}\.json)()
-- target=hdfs
--  configPrefix=ehr
-- writeMode=overwrite

-- step=2
-- source=json
--  configPrefix=ehr
--  fileNamePattern=()(Org_\d{14}\.json)()
--  multiline=false
-- target=temp
--  tableName=ehr_org

-- step=3
-- source=temp
-- target=hdfs
--  separator=\t
--  filePath=/tmp/load/csv/ods_ehr_org.csv
select line.dataStatus   as data_status,
       line.input_field1 as input_field1,
       line.id           as id,
       line.name         as name,
       line.director_id  as director_id,
       line.idCard       as id_card,
       line.parent_id    as parent_id,
       line.attribute    as attribute,
       line.path_depth   as path_depth,
       ${TASK_ID}        as task_id,
       file_name,
       ${DATA_RANGE_END} as ods_insert_date
from ehr_org
         LATERAL VIEW explode(data) line AS line;