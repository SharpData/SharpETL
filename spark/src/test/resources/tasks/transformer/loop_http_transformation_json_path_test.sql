-- workflow=loop_http_transformation_json_path_test
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=temp
-- target=variables
select 'test1,test2,test3' as `centerIds`

-- step=2
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.LoopHttpTransformer
--  methodName=transform
--  transformerType=dynamic_object
--  url=http://localhost:1080/report?centerId=${centerId}
--  items=${centerIds}
--  splitBy=,
--  varName=centerId
--  jsonPath=$.users[*].address
-- target=temp
--  tableName=`target_db`
-- skipFollowStepWhenEmpty=true

-- step=3
-- source=temp
-- target=temp
--  tableName=target_db_2
select city from `target_db`
